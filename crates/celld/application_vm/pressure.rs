//! Admission latch for an explicitly guarded VM pool. No RSS reservation credit.
use crate::application_template::cgroup::MemorySample;
use anyhow::{ensure, Context, Result};
use std::io::Read;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Copy)]
pub(super) struct Sample {
    pub memory: MemorySample,
    pub available_bytes: u64,
}

pub(super) struct Guard {
    limits: Option<(u64, u64)>,
    poll: Duration,
    previous: Option<MemorySample>,
    sampled: Option<Instant>,
    blocked: bool,
    stable_since: Option<Instant>,
}

impl Guard {
    pub(super) fn new(limits: Option<(u64, u64)>, poll: Duration) -> Self {
        Self {
            limits,
            poll,
            previous: None,
            sampled: None,
            blocked: false,
            stable_since: None,
        }
    }

    pub(super) fn enabled(&self) -> bool {
        self.limits.is_some()
    }

    pub(super) fn blocked(&self, now: Instant) -> bool {
        self.enabled()
            && (self.blocked
                || self
                    .sampled
                    .is_none_or(|sampled| now.saturating_duration_since(sampled) > self.poll * 2))
    }

    pub(super) fn observe(&mut self, now: Instant, sample: Option<Sample>) {
        let Some((high, max)) = self.limits else {
            return;
        };
        let thresholds = max
            .checked_sub(high)
            .filter(|gap| *gap > 0)
            .and_then(|gap| high.checked_sub(gap).map(|low| (gap, low)));
        let (Some((gap, low)), Some(sample)) = (thresholds, sample) else {
            self.blocked = true;
            self.stable_since = None;
            self.sampled = None;
            return;
        };
        if self
            .sampled
            .is_some_and(|sampled| now.saturating_duration_since(sampled) > self.poll * 2)
        {
            self.blocked = true;
            self.stable_since = None;
        }
        let current = counters(sample.memory);
        // Growth includes max/OOM and PSI; decreases mean a reset or stale ordering.
        let changed = self
            .previous
            .is_some_and(|previous| counters(previous) != current);
        self.previous = Some(sample.memory);
        self.sampled = Some(now);
        if sample.memory.current_bytes >= high || sample.available_bytes < gap || changed {
            self.blocked = true;
            self.stable_since = None;
        } else if self.blocked {
            if sample.memory.current_bytes < low {
                let since = self.stable_since.get_or_insert(now);
                if now.saturating_duration_since(*since) >= self.poll {
                    self.blocked = false;
                    self.stable_since = None;
                }
            } else {
                self.stable_since = None;
            }
        }
    }
}

fn counters(sample: MemorySample) -> [u64; 7] {
    [
        sample.high,
        sample.max,
        sample.oom,
        sample.oom_kill,
        sample.oom_group_kill,
        sample.some_total_us,
        sample.full_total_us,
    ]
}

pub(super) fn host_available() -> Result<u64> {
    // /proc/meminfo is a bounded host scalar input, never guest supplied.
    let mut bytes = Vec::new();
    std::fs::File::open("/proc/meminfo")?
        .take(16_385)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= 16_384, "host memory sample exceeds bound");
    crate::machine::meminfo_available_bytes(std::str::from_utf8(&bytes)?)
        .context("host available memory absent or malformed")
}

#[cfg(test)]
#[path = "../unit_tests/application_vm/pressure/tests.rs"]
mod tests;
