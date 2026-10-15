//! Firecracker-guest-only container ops and the host broker for the guest's container bridge. Moved out of `container.rs` so upstream merges stay small.

use super::*;

/// A port address is a native pointer the guest never receives: `getTcpPort`
/// fetch and connect reach it only through the brokered ops, so the raw address
/// op refuses in a guest.
pub(super) fn unconnected(scope: &mut v8::PinScope) -> bool {
    let guest = guest_storage::enabled(scope);
    if guest {
        loader_throw(scope, "container addresses are not exposed to guests");
    }
    guest
}

/// `running` and `start()`'s run through the synchronous storage ops, under
/// the issuing event's own grant and residency.
pub(super) fn guest_state(
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
        _ => Err(guest_storage::unexpected(
            scope,
            &channel,
            "container state",
        )),
    }
}

/// The binding alias the host pinned for this object's class
/// (`__cell.containers[class]`). The host re-derives and checks everything.
pub(super) fn guest_alias(scope: &mut v8::PinScope, cell: &str) -> Result<String, String> {
    const MISSING: &str = "this Durable Object has no container";
    let class = cell.split_once(':').map_or(cell, |(class, _)| class);
    let state = cell_state(scope).map_err(|error| error.to_string())?;
    let key = v8::String::new(scope, "containers").ok_or(MISSING)?;
    let map = state
        .get(scope, key.into())
        .and_then(|map| map.to_object(scope))
        .ok_or(MISSING)?;
    let class = v8::String::new(scope, class).ok_or(MISSING)?;
    let alias = map
        .get(scope, class.into())
        .filter(|alias| alias.is_string());
    Ok(alias.ok_or(MISSING)?.to_rust_string_lossy(scope))
}

pub(super) type Brokered =
    Result<tokio::sync::oneshot::Receiver<Result<HttpResponse, String>>, String>;

pub(super) fn brokered(
    scope: &mut v8::PinScope,
    cell: &str,
    operation: Op,
    body: Vec<u8>,
) -> Brokered {
    let alias = guest_alias(scope, cell)?;
    guest_storage::resource_request(
        scope,
        alias,
        ResourceOperation::Container { operation },
        body,
    )
}

/// A process id is not a cell: the current event names its own object.
pub(super) fn process_call(scope: &mut v8::PinScope, operation: Op, body: Vec<u8>) -> Brokered {
    let cell = guest_storage::current_cell(scope)?;
    brokered(scope, &cell, operation, body)
}

pub(super) async fn collected(response: Brokered) -> Result<Vec<u8>, String> {
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
pub(super) enum Life {
    Event,
    IoContext,
    Unrefed,
}

pub(super) fn promise(
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
            String::from_utf8(body)
                .map_err(|error| error.to_string())?
                .into()
        })
    };
    let id = match life {
        Life::Event => asyncrt::enqueue(work),
        Life::IoContext => asyncrt::enqueue_io_context(work),
        Life::Unrefed => asyncrt::enqueue_unrefed(work),
    };
    rv.set(promise_for(scope, id));
}

/// `getTcpPort(port).fetch` in a guest: the request is the brokered call.
pub(in crate::js) fn op_container_port_fetch(
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
pub(in crate::js) fn op_container_connect(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = string_arg(scope, &args, 0);
    let port = number_arg(scope, &args, 1) as u16;
    let response = brokered(scope, &cell, Op::PortConnect { port }, Vec::new());
    promise(scope, &mut rv, response, Life::Event, false);
}

pub(in crate::js) fn op_container_tcp_read(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = number_arg(scope, &args, 0);
    let response = process_call(scope, Op::TcpRead { id }, Vec::new());
    promise(scope, &mut rv, response, Life::Event, true);
}

pub(in crate::js) fn op_container_tcp_write(
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

pub(in crate::js) fn op_container_tcp_shutdown(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = number_arg(scope, &args, 0);
    let response = process_call(scope, Op::TcpShutdown { id }, Vec::new());
    promise(scope, &mut rv, response, Life::Event, false);
}

pub(in crate::js) fn op_container_tcp_close(
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

pub(in crate::js) fn op_container_tcp_starttls(
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
                .map_err(|error| {
                    format!("connection to the container port {port} failed: {error}")
                })?;
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
            sockets
                .register(upgraded)
                .map_err(|error| error.to_string())?;
            tcp::upgrade(id, upgraded, host).await?;
            sockets.forget(id);
            Ok(serde_json::json!({ "id": upgraded })
                .to_string()
                .into_bytes())
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
                reqwest::header::HeaderName::from_bytes(name.as_bytes())
                    .map_err(|e| e.to_string())?,
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
    let body = response.bytes_stream().map(|chunk| {
        chunk
            .map(|chunk| chunk.to_vec())
            .map_err(|error| error.without_url().to_string())
    });
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
    let body = perform(
        cell,
        access.effects(),
        access.sockets(),
        operation,
        request.body,
    )
    .await?;
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
