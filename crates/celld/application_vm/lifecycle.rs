//! Assigned-VM execution ownership. The supervisor owns transitions, API I/O
//! and memory charges; this gate serializes event admission against them.

use anyhow::{ensure, Context, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    Running,
    Pausing,
    Cached,
    Resuming,
    Retiring,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Status {
    pub phase: Phase,
    pub active_events: u32,
    pub last_drain_ms: u64,
    pub idle_deadline_ms: u64,
    pub resume_requested: bool,
    pub epoch: u64,
}

#[derive(Debug)]
struct State {
    status: Status,
    completed_in_wave_ms: Option<u64>,
    admission_waiters: u32,
    // Only a serialized guest probe can distinguish detached work from idle.
    guest_idle: Option<bool>,
}

impl State {
    fn retire(&mut self) {
        if self.status.phase != Phase::Retiring {
            self.status.epoch = self.status.epoch.saturating_add(1);
            self.status.phase = Phase::Retiring;
        }
        self.status.resume_requested = false;
    }

    fn expired(&self, now_ms: u64) -> bool {
        self.status.active_events == 0
            && self.guest_idle == Some(true)
            && now_ms >= self.status.idle_deadline_ms
    }

    fn transition(&mut self, phase: Phase) -> Option<u64> {
        let Some(epoch) = self.status.epoch.checked_add(1) else {
            self.retire();
            return None;
        };
        self.status.epoch = epoch;
        self.status.phase = phase;
        Some(epoch)
    }
}

#[derive(Debug)]
pub(crate) struct Gate {
    state: Mutex<State>,
    idle_ttl_ms: u64,
    /// Register/enable the notification before reading status in the monitor.
    pub(crate) changed: Notify,
    #[cfg(test)]
    clock: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl Gate {
    /// Construct only after initialized guest readiness. This establishes the
    /// initial idle interval; later intervals require completed admitted work.
    pub(crate) fn new(idle_ttl: Duration) -> Result<Arc<Self>> {
        let idle_ttl_ms = u64::try_from(idle_ttl.as_millis())?;
        ensure!(idle_ttl_ms > 0, "VM idle TTL must be positive");
        Ok(Self::at(idle_ttl_ms, crate::asyncrt::mono_ms()))
    }

    fn at(idle_ttl_ms: u64, now_ms: u64) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                status: Status {
                    phase: Phase::Running,
                    active_events: 0,
                    last_drain_ms: now_ms,
                    idle_deadline_ms: now_ms.saturating_add(idle_ttl_ms),
                    resume_requested: false,
                    epoch: 0,
                },
                completed_in_wave_ms: None,
                admission_waiters: 0,
                guest_idle: None,
            }),
            idle_ttl_ms,
            changed: Notify::new(),
            #[cfg(test)]
            clock: None,
        })
    }

    fn now_ms(&self) -> u64 {
        #[cfg(test)]
        if let Some(clock) = &self.clock {
            return clock.load(Ordering::Acquire);
        }
        crate::asyncrt::mono_ms()
    }

    pub(crate) fn status(&self) -> Status {
        self.state.lock().expect("VM lifecycle poisoned").status
    }

    pub(crate) fn known_idle(&self) -> bool {
        let state = self.state.lock().expect("VM lifecycle poisoned");
        state.status.active_events == 0 && state.guest_idle == Some(true)
    }

    pub(crate) fn lease(&self, leases: &Arc<()>) -> Option<Arc<()>> {
        let mut state = self.state.lock().expect("VM lifecycle poisoned");
        if state.expired(self.now_ms()) {
            state.retire();
        }
        let lease =
            matches!(state.status.phase, Phase::Running | Phase::Cached).then(|| leases.clone());
        let retired = state.status.phase == Phase::Retiring;
        drop(state);
        if retired {
            self.changed.notify_waiters();
        }
        lease
    }

    pub(crate) fn retire_if_expired(&self) -> bool {
        let mut state = self.state.lock().expect("VM lifecycle poisoned");
        if state.expired(self.now_ms()) {
            state.retire();
        }
        let retired = state.status.phase == Phase::Retiring;
        drop(state);
        if retired {
            self.changed.notify_waiters();
        }
        retired
    }

    pub(crate) fn retire_if_idle(&self) -> bool {
        let mut state = self.state.lock().expect("VM lifecycle poisoned");
        let idle = state.status.active_events == 0 && state.guest_idle == Some(true);
        if idle {
            state.retire();
        }
        drop(state);
        if idle {
            self.changed.notify_waiters();
        }
        idle
    }

    fn try_enter(
        self: &Arc<Self>,
        deadline_ms: u64,
        cancellation: &CancellationToken,
    ) -> Result<Option<EventHold>> {
        let now_ms = self.now_ms();
        ensure!(
            !cancellation.is_cancelled(),
            "VM execution admission cancelled"
        );
        ensure!(
            now_ms < deadline_ms,
            "VM execution admission deadline exceeded"
        );
        let mut state = self.state.lock().expect("VM lifecycle poisoned");
        if state.expired(now_ms) {
            state.retire();
            drop(state);
            self.changed.notify_waiters();
            anyhow::bail!("VM idle retention expired");
        }
        match state.status.phase {
            Phase::Running => {
                if state.status.active_events == 0
                    && state.guest_idle.is_none()
                    && now_ms >= state.status.idle_deadline_ms
                {
                    state.status.resume_requested = true;
                    drop(state);
                    self.changed.notify_waiters();
                    return Ok(None);
                }
                let next = state
                    .status
                    .active_events
                    .checked_add(1)
                    .context("VM execution ownership limit reached")?;
                if state.status.active_events == 0 {
                    state.completed_in_wave_ms = None;
                }
                state.status.active_events = next;
                Ok(Some(EventHold(Arc::new(EventOwner {
                    gate: self.clone(),
                    admitted: AtomicBool::new(false),
                }))))
            }
            Phase::Pausing | Phase::Cached => {
                let notify = !state.status.resume_requested;
                state.status.resume_requested = true;
                drop(state);
                if notify {
                    self.changed.notify_waiters();
                }
                Ok(None)
            }
            Phase::Resuming => Ok(None),
            Phase::Retiring => anyhow::bail!("VM is retiring"),
        }
    }

    /// Wait within the original event deadline. A waiter never owns/cancels
    /// the monitor's pause or resume transition.
    pub(crate) async fn enter(
        self: &Arc<Self>,
        deadline_ms: u64,
        cancellation: &CancellationToken,
    ) -> Result<EventHold> {
        let _waiter = AdmissionWaiter::new(self)?;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(hold) = self.try_enter(deadline_ms, cancellation)? {
                return Ok(hold);
            }
            crate::asyncrt::select! {
                result = crate::asyncrt::timeout_at(deadline_ms, &mut changed) => {
                    result.context("VM execution admission deadline exceeded")?;
                },
                _ = cancellation.cancelled() => anyhow::bail!("VM execution admission cancelled"),
            }
        }
    }

    /// Root establishes lease and guest-obligation eligibility first. This
    /// closes admission; then await the writer fence before API Pause. Every
    /// execution-capable task must already own an event hold.
    pub(crate) fn begin_pause(self: &Arc<Self>) -> Option<Transition> {
        let now_ms = self.now_ms();
        let mut state = self.state.lock().expect("VM lifecycle poisoned");
        if state.expired(now_ms) {
            state.retire();
            drop(state);
            self.changed.notify_waiters();
            return None;
        }
        if state.status.phase != Phase::Running || state.status.active_events != 0 {
            return None;
        }
        let epoch = state.transition(Phase::Pausing);
        drop(state);
        self.changed.notify_waiters();
        epoch.map(|epoch| Transition {
            gate: self.clone(),
            phase: Phase::Pausing,
            epoch,
            confirmed: false,
        })
    }

    /// Only the supervisor calls this. Concurrent wake requests share one
    /// transition; the original idle deadline is unchanged until work drains.
    pub(crate) fn begin_resume(self: &Arc<Self>) -> Option<Transition> {
        let now_ms = self.now_ms();
        let mut state = self.state.lock().expect("VM lifecycle poisoned");
        if state.expired(now_ms) {
            state.retire();
            drop(state);
            self.changed.notify_waiters();
            return None;
        }
        if state.status.phase != Phase::Cached
            || state.admission_waiters == 0
            || !state.status.resume_requested
        {
            return None;
        }
        let epoch = state.transition(Phase::Resuming);
        state.status.resume_requested = false;
        drop(state);
        self.changed.notify_waiters();
        epoch.map(|epoch| Transition {
            gate: self.clone(),
            phase: Phase::Resuming,
            epoch,
            confirmed: false,
        })
    }

    fn check_transition(&self, transition: &Transition) -> Result<()> {
        let now_ms = self.now_ms();
        let mut state = self.state.lock().expect("VM lifecycle poisoned");
        ensure!(
            state.status.phase == transition.phase && state.status.epoch == transition.epoch,
            "VM transition no longer owns this identity"
        );
        if state.expired(now_ms) {
            state.retire();
            drop(state);
            self.changed.notify_waiters();
            anyhow::bail!("VM idle retention expired during transition");
        }
        Ok(())
    }

    fn confirm(self: &Arc<Self>, mut transition: Transition, from: Phase, to: Phase) -> Result<()> {
        ensure!(
            Arc::ptr_eq(self, &transition.gate) && transition.phase == from,
            "VM transition belongs to another operation"
        );
        let now_ms = self.now_ms();
        let mut state = self.state.lock().expect("VM lifecycle poisoned");
        ensure!(
            state.status.phase == from && state.status.epoch == transition.epoch,
            "VM transition no longer owns this identity"
        );
        if state.expired(now_ms) {
            state.retire();
            drop(state);
            self.changed.notify_waiters();
            anyhow::bail!("VM idle retention expired during transition");
        }
        state.status.phase = to;
        transition.confirmed = true;
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) fn confirm_pause(self: &Arc<Self>, transition: Transition) -> Result<()> {
        ensure!(
            self.state.lock().expect("VM lifecycle poisoned").guest_idle == Some(true),
            "VM pause lacks a trusted idle proof"
        );
        self.confirm(transition, Phase::Pausing, Phase::Cached)
    }

    pub(crate) fn confirm_resume(self: &Arc<Self>, transition: Transition) -> Result<()> {
        self.confirm(transition, Phase::Resuming, Phase::Running)
    }

    pub(crate) fn retire(&self) {
        self.state.lock().expect("VM lifecycle poisoned").retire();
        self.changed.notify_waiters();
    }
}

/// A dropped admission future withdraws demand, never the monitor's transition.
struct AdmissionWaiter(Arc<Gate>);

impl AdmissionWaiter {
    fn new(gate: &Arc<Gate>) -> Result<Self> {
        let mut state = gate.state.lock().expect("VM lifecycle poisoned");
        state.admission_waiters = state
            .admission_waiters
            .checked_add(1)
            .context("VM admission waiter limit reached")?;
        Ok(Self(gate.clone()))
    }
}

impl Drop for AdmissionWaiter {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().expect("VM lifecycle poisoned");
        state.admission_waiters -= 1;
        if state.admission_waiters == 0 {
            state.status.resume_requested = false;
            drop(state);
            self.0.changed.notify_waiters();
        }
    }
}
/// Clones share one event owner. Mark only after real native/event admission;
/// preparation that fails before admission never refreshes the idle interval.
#[derive(Clone, Debug)]
pub(crate) struct EventHold(Arc<EventOwner>);

impl EventHold {
    pub(crate) fn mark_admitted(&self) {
        self.0.admitted.store(true, Ordering::Release);
    }
}

#[derive(Debug)]
struct EventOwner {
    gate: Arc<Gate>,
    admitted: AtomicBool,
}

impl Drop for EventOwner {
    fn drop(&mut self) {
        let now_ms = self.gate.now_ms();
        let mut state = self.gate.state.lock().expect("VM lifecycle poisoned");
        state.status.active_events -= 1;
        if self.admitted.load(Ordering::Acquire) {
            state.completed_in_wave_ms = Some(
                state
                    .completed_in_wave_ms
                    .map_or(now_ms, |previous| previous.max(now_ms)),
            );
        }
        if state.status.active_events == 0 {
            if let Some(completed_ms) = state.completed_in_wave_ms.take() {
                state.status.last_drain_ms = completed_ms;
                state.status.idle_deadline_ms = completed_ms.saturating_add(self.gate.idle_ttl_ms);
                state.guest_idle = None;
            }
        }
        drop(state);
        self.gate.changed.notify_waiters();
    }
}

/// Move this guard into the supervisor, never a requesting frontend future.
/// Missing/failed acknowledgements retire conservatively when the guard drops.
#[derive(Debug)]
pub(crate) struct Transition {
    gate: Arc<Gate>,
    phase: Phase,
    epoch: u64,
    confirmed: bool,
}

impl Transition {
    /// A false probe proves no physical pause was issued; keep running work.
    pub(crate) fn cancel_pause(self) -> Result<()> {
        self.cancel_pause_with_idle(false)
    }

    pub(crate) fn cancel_pause_idle(self) -> Result<()> {
        self.cancel_pause_with_idle(true)
    }

    fn cancel_pause_with_idle(mut self, idle: bool) -> Result<()> {
        let mut state = self.gate.state.lock().expect("VM lifecycle poisoned");
        ensure!(
            self.phase == Phase::Pausing
                && state.status.phase == self.phase
                && state.status.epoch == self.epoch,
            "VM pause no longer owns this identity"
        );
        state.guest_idle = Some(idle);
        state.status.phase = Phase::Running;
        state.status.resume_requested = false;
        self.confirmed = true;
        drop(state);
        self.gate.changed.notify_waiters();
        Ok(())
    }

    pub(crate) fn confirm_idle(&mut self) -> Result<()> {
        {
            let mut state = self.gate.state.lock().expect("VM lifecycle poisoned");
            ensure!(
                self.phase == Phase::Pausing
                    && state.status.phase == self.phase
                    && state.status.epoch == self.epoch
                    && state.status.active_events == 0,
                "VM idle proof no longer owns this identity"
            );
            state.guest_idle = Some(true);
        }
        self.check()
    }

    /// Check immediately before issuing the API call, after any capacity wait.
    pub(crate) fn check(&self) -> Result<()> {
        self.gate.check_transition(self)
    }
}

impl Drop for Transition {
    fn drop(&mut self) {
        if self.confirmed {
            return;
        }
        let mut state = self.gate.state.lock().expect("VM lifecycle poisoned");
        if state.status.phase == self.phase && state.status.epoch == self.epoch {
            state.retire();
        }
        drop(state);
        self.gate.changed.notify_waiters();
    }
}

#[cfg(test)]
#[path = "../unit_tests/application_vm/lifecycle/tests.rs"]
mod tests;
