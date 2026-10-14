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

/// A port address is a native pointer the guest never receives: `getTcpPort`
/// fetch and connect reach it only through the brokered ops, so the raw address
/// op refuses in a guest.
fn unconnected(scope: &mut v8::PinScope) -> bool {
    let guest = guest_storage::enabled(scope);
    if guest {
        loader_throw(scope, "container addresses are not exposed to guests");
    }
    guest
}

/// `running` and `start()`'s run through the synchronous storage ops, under
/// the issuing event's own grant and residency.
fn guest_state(
    scope: &mut v8::PinScope,
    cell: &str,
    begin: bool,
) -> Result<(bool, u64), String> {
    use celld_applications::storage_wire::{Operation, Value};
    let channel = guest_storage::channel(scope, cell)?.ok_or("guest storage is unavailable")?;
    let operation = if begin {
        Operation::ContainerBeginRun {}
    } else {
        Operation::ContainerState {}
    };
    match guest_storage::call(scope, &channel, operation)? {
        Value::ContainerState { running, run } => Ok((running, run)),
        _ => Err(guest_storage::unexpected(scope, &channel, "container state")),
    }
}

/// The binding alias the host pinned for this object's class
/// (`__cell.containers[class]`). The host re-derives and checks everything.
fn guest_alias(scope: &mut v8::PinScope, cell: &str) -> Result<String, String> {
    const MISSING: &str = "this Durable Object has no container";
    let class = cell.split_once(':').map_or(cell, |(class, _)| class);
    let state = cell_state(scope).map_err(|error| error.to_string())?;
    let key = v8::String::new(scope, "containers").ok_or(MISSING)?;
    let map = state
        .get(scope, key.into())
        .and_then(|map| map.to_object(scope))
        .ok_or(MISSING)?;
    let class = v8::String::new(scope, class).ok_or(MISSING)?;
    let alias = map.get(scope, class.into()).filter(|alias| alias.is_string());
    Ok(alias.ok_or(MISSING)?.to_rust_string_lossy(scope))
}

type Brokered = Result<tokio::sync::oneshot::Receiver<Result<HttpResponse, String>>, String>;

fn brokered(scope: &mut v8::PinScope, cell: &str, operation: Op, body: Vec<u8>) -> Brokered {
    let alias = guest_alias(scope, cell)?;
    guest_storage::resource_request(scope, alias, ResourceOperation::Container { operation }, body)
}

/// A process id is not a cell: the current event names its own object.
fn process_call(scope: &mut v8::PinScope, operation: Op, body: Vec<u8>) -> Brokered {
    let cell = guest_storage::current_cell(scope)?;
    brokered(scope, &cell, operation, body)
}

async fn collected(response: Brokered) -> Result<Vec<u8>, String> {
    use futures_util::StreamExt;
    let response = response?
        .await
        .map_err(|_| "container guest operation was abandoned".to_string())??;
    let mut bytes = response.body;
    if let Some(mut stream) = response.stream {
        while let Some(chunk) = stream.next().await {
            bytes.extend(chunk?);
        }
    }
    Ok(bytes)
}

/// How long a brokered op keeps its event: as the original op's enqueue.
enum Life {
    Event,
    IoContext,
    Unrefed,
}

fn promise(
    scope: &mut v8::PinScope,
    rv: &mut v8::ReturnValue<v8::Value>,
    response: Brokered,
    life: Life,
    bytes: bool,
) {
    let work = async move {
        let body = collected(response).await?;
        Ok::<asyncrt::OpOut, String>(if bytes {
            body.into()
        } else {
            String::from_utf8(body).map_err(|error| error.to_string())?.into()
        })
    };
    let id = match life {
        Life::Event => asyncrt::enqueue(work),
        Life::IoContext => asyncrt::enqueue_io_context(work),
        Life::Unrefed => asyncrt::enqueue_unrefed(work),
    };
    rv.set(promise_for(scope, id));
}

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

/// `getTcpPort(port).fetch` in a guest: the request is the brokered call.
pub(super) fn op_container_port_fetch(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = string_arg(scope, &args, 0);
    let port = number_arg(scope, &args, 1) as u16;
    let (method, url) = (string_arg(scope, &args, 2), string_arg(scope, &args, 3));
    let headers: Vec<(String, String)> = match serde_json::from_str(&string_arg(scope, &args, 4)) {
        Ok(headers) => headers,
        Err(error) => return loader_throw(scope, &format!("port fetch headers: {error}")),
    };
    let Some(body) = view_bytes(args.get(5)) else {
        return loader_throw(scope, "port fetch needs a body");
    };
    let request = celld_applications::invocation_wire::HttpRequest {
        method,
        url,
        headers,
        body,
    };
    let operation = ResourceOperation::Container {
        operation: Op::PortFetch { port },
    };
    let response = guest_alias(scope, &cell)
        .and_then(|alias| guest_storage::resource_http_request(scope, alias, operation, request));
    let stream_service = http_stream_service();
    let id = asyncrt::enqueue(async move {
        let response = response?
            .await
            .map_err(|_| "container port fetch was abandoned".to_string())??;
        encode_http_response(response, false, &stream_service)
    });
    rv.set(promise_for(scope, id));
}

/// Raw TCP to a container port. Sockets are the issuing event's: the host
/// frees them when it ends, and the ids mean nothing to another event.
pub(super) fn op_container_connect(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = string_arg(scope, &args, 0);
    let port = number_arg(scope, &args, 1) as u16;
    let response = brokered(scope, &cell, Op::PortConnect { port }, Vec::new());
    promise(scope, &mut rv, response, Life::Event, false);
}

pub(super) fn op_container_tcp_read(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = number_arg(scope, &args, 0);
    let response = process_call(scope, Op::TcpRead { id }, Vec::new());
    promise(scope, &mut rv, response, Life::Event, true);
}

pub(super) fn op_container_tcp_write(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = number_arg(scope, &args, 0);
    let Some(bytes) = view_bytes(args.get(1)) else {
        return loader_throw(scope, "socket write needs bytes");
    };
    let response = process_call(scope, Op::TcpWrite { id }, bytes);
    promise(scope, &mut rv, response, Life::Event, false);
}

pub(super) fn op_container_tcp_shutdown(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = number_arg(scope, &args, 0);
    let response = process_call(scope, Op::TcpShutdown { id }, Vec::new());
    promise(scope, &mut rv, response, Life::Event, false);
}

pub(super) fn op_container_tcp_close(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    // The caller ignores the promise, so a failed close must not reject.
    let id = number_arg(scope, &args, 0);
    let response = process_call(scope, Op::TcpClose { id }, Vec::new());
    let id = asyncrt::enqueue(async move {
        let _ = collected(response).await;
        Ok::<String, String>(String::new())
    });
    rv.set(promise_for(scope, id));
}

#[derive(serde::Deserialize)]
struct StartTlsArgs {
    id: u64,
    host: String,
}

pub(super) fn op_container_tcp_starttls(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let request: StartTlsArgs = match serde_json::from_str(&string_arg(scope, &args, 0)) {
        Ok(request) => request,
        Err(error) => return loader_throw(scope, &format!("startTls(): {error}")),
    };
    let operation = Op::TcpStartTls {
        id: request.id,
        host: request.host,
    };
    let response = process_call(scope, operation, Vec::new());
    promise(scope, &mut rv, response, Life::Event, false);
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

type Effect = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send>>;

/// One authenticated guest container operation against the issuing object's
/// own container. Begun mutations are owned effects: the caller's
/// cancellation drops only its wait, and the event's settlement joins them.
pub(crate) async fn perform(
    cell: Arc<CellContainer>,
    effects: &crate::application_container::Effects,
    sockets: &crate::application_container::Sockets,
    operation: Op,
    body: Vec<u8>,
) -> Result<Vec<u8>, String> {
    let engine = container::engine()
        .await
        .map_err(|error| format!("{error:#}"))?;
    let owned = |work: Effect| async move {
        effects
            .spawn(work)
            .map_err(|error| format!("{error:#}"))?
            .await
            .map_err(|error| format!("container effect failed: {error}"))?
    };
    let process = |id: u64| {
        cell.process(id)
            .ok_or_else(|| "the process is closed".to_string())
    };
    match operation {
        Op::Start {
            run,
            entrypoint,
            env,
            enable_internet,
            labels,
        } => {
            let (engine, started) = (engine.clone(), cell.clone());
            let params = StartParams {
                entrypoint,
                env,
                enable_internet,
                labels,
            };
            let handle = effects.spawn(async move {
                engine
                    .start(&started, run, params)
                    .await
                    .map(|()| Vec::new())
                    .map_err(|error| format!("{error:#}"))
            });
            match handle {
                Ok(handle) => handle
                    .await
                    .map_err(|error| format!("container effect failed: {error}"))?,
                Err(error) => {
                    // The effect never began: close the run this start opened.
                    cell.cancel_pending_run(run);
                    Err(format!("{error:#}"))
                }
            }
        }
        Op::Monitor { run } => {
            if run > cell.current_run() {
                return Err("container run does not exist".into());
            }
            Ok(engine.monitor(&cell, run).await?.to_string().into_bytes())
        }
        Op::Destroy {} => {
            let cell = cell.clone();
            owned(Box::pin(async move {
                engine
                    .destroy(&cell)
                    .await
                    .map(|()| Vec::new())
                    .map_err(|error| format!("{error:#}"))
            }))
            .await
        }
        Op::Signal { signal } => {
            let cell = cell.clone();
            owned(Box::pin(async move {
                engine
                    .signal(&cell, signal)
                    .await
                    .map(|()| Vec::new())
                    .map_err(|error| format!("{error:#}"))
            }))
            .await
        }
        Op::Inactivity { millis } => {
            cell.set_inactivity(Duration::from_millis(millis));
            Ok(Vec::new())
        }
        Op::Exec {
            cmd,
            env,
            cwd,
            user,
            combined,
        } => {
            let cell = cell.clone();
            owned(Box::pin(async move {
                let process = engine
                    .exec(
                        &cell,
                        ExecParams {
                            cmd,
                            env,
                            cwd,
                            user,
                            combined,
                        },
                    )
                    .await
                    .map_err(|error| format!("{error:#}"))?;
                Ok(serde_json::json!({ "id": process.id, "pid": process.pid })
                    .to_string()
                    .into_bytes())
            }))
            .await
        }
        Op::ExecRead { id, which } => Ok(process(id)?.read(which).await.to_vec()),
        Op::ExecWrite { id } => {
            process(id)?.write(&body).await?;
            Ok(Vec::new())
        }
        Op::ExecClose { id } => {
            process(id)?.close_stdin().await;
            Ok(Vec::new())
        }
        Op::ExecWait { id } => Ok(process(id)?
            .wait()
            .await
            .map_err(|error| format!("{error:#}"))?
            .to_string()
            .into_bytes()),
        Op::ExecKill { id, signal } => {
            process(id)?
                .kill(signal)
                .await
                .map_err(|error| format!("{error:#}"))?;
            Ok(Vec::new())
        }
        Op::ExecDrop { id } => {
            if cell.process(id).is_some() {
                container::drop_process(id);
            }
            Ok(Vec::new())
        }
        Op::PortFetch { .. } => Err("a port fetch carries an HTTP request".into()),
        Op::PortConnect { port } => {
            // The address is the container's own; no native address returns.
            let address = cell.address(port)?;
            let stream = tokio::net::TcpStream::connect(&address)
                .await
                .map_err(|error| format!("connection to the container port {port} failed: {error}"))?;
            let id = tcp::adopt(stream);
            if let Err(error) = sockets.register(id) {
                tcp::free(id);
                return Err(format!("{error:#}"));
            }
            Ok(serde_json::json!({ "id": id }).to_string().into_bytes())
        }
        Op::TcpRead { id } => {
            sockets.owns(id).map_err(|error| error.to_string())?;
            tcp::read_chunk(id).await
        }
        Op::TcpWrite { id } => {
            sockets.owns(id).map_err(|error| error.to_string())?;
            tcp::write_all(id, body).await.map(|()| Vec::new())
        }
        Op::TcpShutdown { id } => {
            sockets.owns(id).map_err(|error| error.to_string())?;
            tcp::shutdown_write(id).await.map(|()| Vec::new())
        }
        Op::TcpClose { id } => {
            if sockets.owns(id).is_ok() {
                sockets.forget(id);
                tcp::free(id);
            }
            Ok(Vec::new())
        }
        Op::TcpStartTls { id, host } => {
            sockets.owns(id).map_err(|error| error.to_string())?;
            let upgraded = tcp::allocate();
            sockets.register(upgraded).map_err(|error| error.to_string())?;
            tcp::upgrade(id, upgraded, host).await?;
            sockets.forget(id);
            Ok(serde_json::json!({ "id": upgraded }).to_string().into_bytes())
        }
    }
}

/// `getTcpPort(port).fetch`: the request goes to the container's own address
/// for `port` through the host's egress client; the request's authority only
/// becomes the Host header, as in the original. Redirects are returned, never
/// followed here, so nothing leaves the container's port unmediated.
pub(crate) async fn port_fetch(
    cell: &CellContainer,
    port: u16,
    request: celld_applications::invocation_wire::HttpRequest,
) -> Result<HttpResponse, String> {
    use futures_util::StreamExt;
    let original = url::Url::parse(&request.url).map_err(|error| error.to_string())?;
    if original.scheme() != "http" {
        return Err("container ports speak HTTP".into());
    }
    let mut authority = original.host_str().unwrap_or_default().to_owned();
    if let Some(port) = original.port() {
        authority = format!("{authority}:{port}");
    }
    let mut target = format!("http://{}{}", cell.address(port)?, original.path());
    if let Some(query) = original.query() {
        target = format!("{target}?{query}");
    }
    let mut headers = reqwest::header::HeaderMap::new();
    for (name, value) in &request.headers {
        if !name.eq_ignore_ascii_case("host") {
            headers.append(
                reqwest::header::HeaderName::from_bytes(name.as_bytes()).map_err(|e| e.to_string())?,
                reqwest::header::HeaderValue::from_str(value).map_err(|e| e.to_string())?,
            );
        }
    }
    headers.insert(
        reqwest::header::HOST,
        reqwest::header::HeaderValue::from_str(&authority).map_err(|e| e.to_string())?,
    );
    let response = crate::application_guest_transport::egress_client("manual")
        .map_err(|error| error.to_string())?
        .request(
            reqwest::Method::from_bytes(request.method.as_bytes()).map_err(|e| e.to_string())?,
            target,
        )
        .headers(headers)
        .body(request.body)
        .send()
        .await
        // Without the URL: the guest never learns the container's address.
        .map_err(|error| error.without_url().to_string())?;
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .filter_map(|(name, value)| Some((name.to_string(), value.to_str().ok()?.to_owned())))
        .collect();
    let body = response
        .bytes_stream()
        .map(|chunk| chunk.map(|chunk| chunk.to_vec()).map_err(|error| error.without_url().to_string()));
    Ok(HttpResponse {
        status,
        headers,
        body: Vec::new(),
        stream: Some(Box::pin(body)),
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

/// The transport's entry: the container is the one bound to the issuing
/// event's object, and a start consumes the run that event opened.
pub(crate) async fn broker_operation(
    access: &crate::application_guest::GuestStorage,
    operation: Op,
    request: celld_applications::invocation_wire::HttpRequest,
) -> Result<HttpResponse, String> {
    let cell = match &operation {
        Op::Start { run, .. } => access.take_container_run(*run).await,
        _ => access.container().await,
    }
    .map_err(|error| format!("{error:#}"))?;
    if let Op::PortFetch { port } = operation {
        return port_fetch(&cell, port, request).await;
    }
    let body = perform(cell, access.effects(), access.sockets(), operation, request.body).await?;
    Ok(HttpResponse {
        status: 200,
        headers: Vec::new(),
        body,
        stream: None,
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}
