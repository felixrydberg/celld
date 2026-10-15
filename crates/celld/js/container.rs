//! Host ops behind `ctx.container`.
//!
//! JavaScript owns the surface and every validation message, copied from
//! workerd's `api/container.c++`; these ops perform the effects through
//! `crate::container`. Each takes the cell scope as its first argument,
//! the way the storage ops do, and an exec process is addressed by the id
//! `__container_exec` returned.

use super::*;
use crate::container::{self, CellContainer, ContainerEngine, ExecParams, StartParams};
use celld_applications::resource_wire::{ContainerOperation as Op, ResourceOperation};
use std::time::Duration;

mod guest;
pub(crate) use guest::*;

async fn cell_for(scope: &str) -> Result<(Arc<ContainerEngine>, Arc<CellContainer>), String> {
    let engine = container::engine()
        .await
        .map_err(|error| format!("{error:#}"))?;
    let cell = engine
        .cell(scope)
        .ok_or_else(|| "this Durable Object has no container".to_string())?;
    Ok((engine, cell))
}

fn string_arg(
    scope: &mut v8::PinScope,
    args: &v8::FunctionCallbackArguments,
    index: i32,
) -> String {
    args.get(index).to_rust_string_lossy(scope)
}

fn number_arg(scope: &mut v8::PinScope, args: &v8::FunctionCallbackArguments, index: i32) -> u64 {
    args.get(index).integer_value(scope).unwrap_or(0).max(0) as u64
}

pub(super) fn op_container_running(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if guest_storage::enabled(scope) {
        let cell = string_arg(scope, &args, 0);
        match guest_state(scope, &cell, false) {
            Ok((running, _)) => rv.set(v8::Boolean::new(scope, running).into()),
            Err(message) => loader_throw(scope, &message),
        }
        return;
    }
    let cell = string_arg(scope, &args, 0);
    let running = container::engine_if_ready()
        .and_then(|engine| engine.cell(&cell))
        .is_some_and(|cell| cell.running());
    rv.set(v8::Boolean::new(scope, running).into());
}

/// `host:port` the node dials for `getTcpPort(port)`, or a throw.
pub(super) fn op_container_address(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if unconnected(scope) {
        return;
    }
    let cell = string_arg(scope, &args, 0);
    let port = number_arg(scope, &args, 1) as u16;
    let address = container::engine_if_ready()
        .and_then(|engine| engine.cell(&cell))
        .ok_or_else(|| "this Durable Object has no container".to_string())
        .and_then(|cell| cell.address(port));
    match address {
        Ok(address) => rv.set(v8::String::new(scope, &address).unwrap().into()),
        Err(message) => loader_throw(scope, &message),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct StartArgs {
    entrypoint: Option<Vec<String>>,
    #[serde(default)]
    env: Vec<(String, String)>,
    #[serde(default)]
    enable_internet: bool,
    #[serde(default)]
    labels: Vec<(String, String)>,
}

pub(super) fn op_container_start(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = string_arg(scope, &args, 0);
    let raw = string_arg(scope, &args, 1);
    let request: StartArgs = match serde_json::from_str(&raw) {
        Ok(request) => request,
        Err(error) => return loader_throw(scope, &format!("start(): {error}")),
    };
    if guest_storage::enabled(scope) {
        // The run opens through the synchronous storage op, so the monitor()
        // that follows waits for it; the start itself is brokered.
        let (_, run) = match guest_state(scope, &cell, true) {
            Ok(state) => state,
            Err(message) => return loader_throw(scope, &message),
        };
        let response = brokered(
            scope,
            &cell,
            Op::Start {
                run,
                entrypoint: request.entrypoint,
                env: request.env,
                enable_internet: request.enable_internet,
                labels: request.labels,
            },
            Vec::new(),
        );
        return promise(scope, &mut rv, response, Life::IoContext, false);
    }
    // The run opens here, on the calling thread, so the monitor() that
    // follows this call in the same turn waits for this start's exit.
    let run = container::engine_if_ready()
        .and_then(|engine| engine.cell(&cell))
        .map(|cell| cell.begin_run());
    let async_id = asyncrt::enqueue_io_context(async move {
        let (engine, cell) = cell_for(&cell).await?;
        let run = run.unwrap_or_else(|| cell.begin_run());
        engine
            .start(
                &cell,
                run,
                StartParams {
                    entrypoint: request.entrypoint,
                    env: request.env,
                    enable_internet: request.enable_internet,
                    labels: request.labels,
                },
            )
            .await
            .map_err(|error| format!("{error:#}"))?;
        Ok::<String, String>(String::new())
    });
    rv.set(promise_for(scope, async_id));
}

/// Resolves with the root process's exit code as text once the current
/// run ends. Unrefed: a handler can await it, but once the handler has
/// answered it cannot keep the event, and so the cell, alive. A drain
/// would otherwise wait on a promise that settles only when the container
/// exits. After the event, `running` reports the host's state.
pub(super) fn op_container_monitor(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if guest_storage::enabled(scope) {
        let cell = string_arg(scope, &args, 0);
        let response = guest_state(scope, &cell, false)
            .and_then(|(_, run)| brokered(scope, &cell, Op::Monitor { run }, Vec::new()));
        return promise(scope, &mut rv, response, Life::Unrefed, false);
    }
    let cell = string_arg(scope, &args, 0);
    let run = container::engine_if_ready()
        .and_then(|engine| engine.cell(&cell))
        .map_or(0, |cell| cell.current_run());
    let async_id = asyncrt::enqueue_unrefed(async move {
        let (engine, cell) = cell_for(&cell).await?;
        let code = engine.monitor(&cell, run).await?;
        Ok::<String, String>(code.to_string())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_container_destroy(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if guest_storage::enabled(scope) {
        let cell = string_arg(scope, &args, 0);
        let response = brokered(scope, &cell, Op::Destroy {}, Vec::new());
        return promise(scope, &mut rv, response, Life::Event, false);
    }
    let cell = string_arg(scope, &args, 0);
    let async_id = asyncrt::enqueue(async move {
        let (engine, cell) = cell_for(&cell).await?;
        engine
            .destroy(&cell)
            .await
            .map_err(|error| format!("{error:#}"))?;
        Ok::<String, String>(String::new())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_container_signal(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if guest_storage::enabled(scope) {
        let cell = string_arg(scope, &args, 0);
        let signal = number_arg(scope, &args, 1) as u32;
        let response = brokered(scope, &cell, Op::Signal { signal }, Vec::new());
        return promise(scope, &mut rv, response, Life::Event, false);
    }
    let cell = string_arg(scope, &args, 0);
    let signal = number_arg(scope, &args, 1) as u32;
    let async_id = asyncrt::enqueue(async move {
        let (engine, cell) = cell_for(&cell).await?;
        engine
            .signal(&cell, signal)
            .await
            .map_err(|error| format!("{error:#}"))?;
        Ok::<String, String>(String::new())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_container_inactivity(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if guest_storage::enabled(scope) {
        let cell = string_arg(scope, &args, 0);
        let millis = number_arg(scope, &args, 1);
        let response = brokered(scope, &cell, Op::Inactivity { millis }, Vec::new());
        return promise(scope, &mut rv, response, Life::Event, false);
    }
    let cell = string_arg(scope, &args, 0);
    let duration = number_arg(scope, &args, 1);
    let async_id = asyncrt::enqueue(async move {
        let (_, cell) = cell_for(&cell).await?;
        cell.set_inactivity(Duration::from_millis(duration));
        Ok::<String, String>(String::new())
    });
    rv.set(promise_for(scope, async_id));
}

#[derive(serde::Deserialize)]
struct ExecArgs {
    cmd: Vec<String>,
    #[serde(default)]
    env: Vec<(String, String)>,
    cwd: Option<String>,
    user: Option<String>,
    #[serde(default)]
    combined: bool,
}

pub(super) fn op_container_exec(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = string_arg(scope, &args, 0);
    let raw = string_arg(scope, &args, 1);
    let request: ExecArgs = match serde_json::from_str(&raw) {
        Ok(request) => request,
        Err(error) => return loader_throw(scope, &format!("exec(): {error}")),
    };
    if guest_storage::enabled(scope) {
        let response = brokered(
            scope,
            &cell,
            Op::Exec {
                cmd: request.cmd,
                env: request.env,
                cwd: request.cwd,
                user: request.user,
                combined: request.combined,
            },
            Vec::new(),
        );
        return promise(scope, &mut rv, response, Life::Event, false);
    }
    let async_id = asyncrt::enqueue(async move {
        let (engine, cell) = cell_for(&cell).await?;
        let process = engine
            .exec(
                &cell,
                ExecParams {
                    cmd: request.cmd,
                    env: request.env,
                    cwd: request.cwd,
                    user: request.user,
                    combined: request.combined,
                },
            )
            .await
            .map_err(|error| format!("{error:#}"))?;
        Ok::<String, String>(
            serde_json::json!({ "id": process.id, "pid": process.pid }).to_string(),
        )
    });
    rv.set(promise_for(scope, async_id));
}

// The process ops below are unrefed like `monitor()`: a handler awaits
// them, and a process left running when the handler answers does not pin
// the cell.
fn process_arg(
    scope: &mut v8::PinScope,
    args: &v8::FunctionCallbackArguments,
) -> Result<Arc<container::ExecProcess>, String> {
    let id = number_arg(scope, args, 0);
    container::process(id).ok_or_else(|| "the process is closed".to_string())
}

/// One chunk of stdout (1) or stderr (2); an empty chunk is the end.
pub(super) fn op_container_exec_read(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if guest_storage::enabled(scope) {
        let (id, which) = (number_arg(scope, &args, 0), number_arg(scope, &args, 1) as u8);
        let response = process_call(scope, Op::ExecRead { id, which }, Vec::new());
        return promise(scope, &mut rv, response, Life::Unrefed, true);
    }
    let process = process_arg(scope, &args);
    let which = number_arg(scope, &args, 1) as u8;
    let async_id = asyncrt::enqueue_unrefed(async move {
        let process = process?;
        Ok::<Vec<u8>, String>(process.read(which).await.to_vec())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_container_exec_write(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if guest_storage::enabled(scope) {
        let id = number_arg(scope, &args, 0);
        let Some(bytes) = view_bytes(args.get(1)) else {
            return loader_throw(scope, "stdin write needs bytes");
        };
        let response = process_call(scope, Op::ExecWrite { id }, bytes.to_vec());
        return promise(scope, &mut rv, response, Life::Unrefed, false);
    }
    let process = process_arg(scope, &args);
    let Some(bytes) = view_bytes(args.get(1)) else {
        return loader_throw(scope, "stdin write needs bytes");
    };
    let async_id = asyncrt::enqueue_unrefed(async move {
        process?.write(&bytes).await?;
        Ok::<String, String>(String::new())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_container_exec_close(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if guest_storage::enabled(scope) {
        let id = number_arg(scope, &args, 0);
        let response = process_call(scope, Op::ExecClose { id }, Vec::new());
        return promise(scope, &mut rv, response, Life::Unrefed, false);
    }
    let process = process_arg(scope, &args);
    let async_id = asyncrt::enqueue_unrefed(async move {
        process?.close_stdin().await;
        Ok::<String, String>(String::new())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_container_exec_wait(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if guest_storage::enabled(scope) {
        let id = number_arg(scope, &args, 0);
        let response = process_call(scope, Op::ExecWait { id }, Vec::new());
        return promise(scope, &mut rv, response, Life::Unrefed, false);
    }
    let process = process_arg(scope, &args);
    let async_id = asyncrt::enqueue_unrefed(async move {
        let code = process?
            .wait()
            .await
            .map_err(|error| format!("{error:#}"))?;
        Ok::<String, String>(code.to_string())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_container_exec_kill(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if guest_storage::enabled(scope) {
        let id = number_arg(scope, &args, 0);
        let signal = number_arg(scope, &args, 1) as u32;
        let response = process_call(scope, Op::ExecKill { id, signal }, Vec::new());
        return promise(scope, &mut rv, response, Life::Unrefed, false);
    }
    let process = process_arg(scope, &args);
    let signal = number_arg(scope, &args, 1) as u32;
    let async_id = asyncrt::enqueue_unrefed(async move {
        process?
            .kill(signal)
            .await
            .map_err(|error| format!("{error:#}"))?;
        Ok::<String, String>(String::new())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_container_exec_drop(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if guest_storage::enabled(scope) {
        // The caller ignores the promise, so a failed drop must not reject.
        let id = number_arg(scope, &args, 0);
        let response = process_call(scope, Op::ExecDrop { id }, Vec::new());
        let id = asyncrt::enqueue(async move {
            let _ = collected(response).await;
            Ok::<String, String>(String::new())
        });
        rv.set(promise_for(scope, id));
        return;
    }
    let id = number_arg(scope, &args, 0);
    container::drop_process(id);
}
