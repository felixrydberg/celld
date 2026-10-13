use super::tests::{address, app};
use super::*;
use crate::container::{self, Release};
use crate::js::container::{perform, port_fetch};
use celld_applications::invocation_wire::HttpRequest;
use celld_applications::resource_wire::ContainerOperation as Op;
use std::time::Duration;

const NODE: &str = "t15-docker";

pub(crate) fn cli(args: &[&str], stdin: Option<&str>) -> String {
    use std::io::Write;
    use std::process::Stdio;
    let mut child = std::process::Command::new("docker")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let input = child.stdin.take();
    if let (Some(mut pipe), Some(text)) = (input, stdin) {
        pipe.write_all(text.as_bytes()).unwrap();
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "docker {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// The fence and workload images, built from inline Dockerfiles and tagged
/// as a node would hold them. The workload serves HTTP on 8080 (`/`,
/// `/dir` redirects to `/dir/`) and echoes TCP on 9000.
pub(crate) fn images() -> (String, String) {
    let fence = format!("celld-image:{}", "1".repeat(64));
    let workload = format!("celld-image:{}", "2".repeat(64));
    let dockerfile = "FROM alpine:3.20\nRUN apk add --no-cache nftables\n";
    cli(&["build", "-q", "-t", &fence, "-"], Some(dockerfile));
    let dockerfile = "FROM busybox:1.36\n\
            RUN mkdir -p /www/dir /www/cgi-bin && echo -n ok > /www/index.html && echo -n nested > /www/dir/index.html \
                && printf '#!/bin/sh\\necho \"Content-type: text/plain\"\\necho\\nprintf \"%%s\" \"$HTTP_HOST\"\\n' > /www/cgi-bin/host \
                && chmod +x /www/cgi-bin/host\n\
            CMD [\"sh\", \"-c\", \"httpd -f -p 8080 -h /www & exec tcpsvd 0.0.0.0 9000 cat\"]\n";
    cli(&["build", "-q", "-t", &workload, "-"], Some(dockerfile));
    (fence, workload)
}

/// The scopes that have a container for `node` on this engine.
pub(crate) fn live(node: &str) -> Vec<String> {
    let filter = format!("label=celld.node={node}");
    let listed = cli(
        &[
            "ps",
            "-a",
            "--filter",
            &filter,
            "--format",
            "{{.Label \"celld.cell\"}}",
        ],
        None,
    );
    let mut cells: Vec<String> = listed.lines().map(str::to_owned).collect();
    cells.sort();
    cells
}

type Cell = Arc<container::CellContainer>;

/// What one guest event owns: its effects and its sockets.
#[derive(Clone, Default)]
struct Event {
    fx: Effects,
    sockets: Sockets,
}

async fn run(cell: &Cell, event: &Event, op: Op) -> Result<Vec<u8>, String> {
    perform(cell.clone(), &event.fx, &event.sockets, op, Vec::new()).await
}

async fn text(cell: &Cell, event: &Event, op: Op) -> String {
    String::from_utf8(run(cell, event, op).await.unwrap()).unwrap()
}

fn start_op(run: u64, entrypoint: Option<Vec<&str>>) -> Op {
    Op::Start {
        run,
        entrypoint: entrypoint.map(|list| list.into_iter().map(str::to_owned).collect()),
        env: vec![],
        enable_internet: false,
        labels: vec![],
    }
}

async fn start(cell: &Cell, event: &Event, entrypoint: Option<Vec<&str>>) -> u64 {
    let run_id = cell.begin_run();
    run(cell, event, start_op(run_id, entrypoint))
        .await
        .unwrap();
    run_id
}

fn monitor(
    cell: &Cell,
    event: &Event,
    run: u64,
) -> tokio::task::JoinHandle<Result<Vec<u8>, String>> {
    let (cell, event) = (cell.clone(), event.clone());
    tokio::spawn(async move { run_op(cell, event, Op::Monitor { run }).await })
}

async fn run_op(cell: Cell, event: Event, op: Op) -> Result<Vec<u8>, String> {
    perform(cell, &event.fx, &event.sockets, op, Vec::new()).await
}

async fn output(cell: &Cell, event: &Event, id: u64, which: u8) -> String {
    let mut bytes = Vec::new();
    loop {
        let chunk = run(cell, event, Op::ExecRead { id, which }).await.unwrap();
        if chunk.is_empty() {
            return String::from_utf8(bytes).unwrap();
        }
        bytes.extend(chunk);
    }
}

async fn exec(cell: &Cell, event: &Event, cmd: &[&str]) -> u64 {
    let op = Op::Exec {
        cmd: cmd.iter().map(|part| part.to_string()).collect(),
        env: vec![("K".into(), "V".into())],
        cwd: None,
        user: None,
        combined: false,
    };
    let started: serde_json::Value = serde_json::from_str(&text(cell, event, op).await).unwrap();
    started["id"].as_u64().unwrap()
}

/// One GET to a container's port 8080, retried while the server comes up.
async fn get(cell: &Cell, path: &str) -> (u16, Vec<(String, String)>, String) {
    let request = || HttpRequest {
        method: "GET".into(),
        url: format!("http://app.test:81{path}"),
        headers: vec![],
        body: vec![],
    };
    let mut attempts = 0;
    let mut response = loop {
        match port_fetch(cell, 8080, request()).await {
            Ok(response) => break response,
            Err(error) if attempts < 50 => {
                attempts += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
                assert!(
                    !error.contains("10.213."),
                    "a native address reached the guest: {error}"
                );
            }
            Err(error) => panic!("port 8080 never answered: {error}"),
        }
    };
    use futures_util::StreamExt;
    let mut body = std::mem::take(&mut response.body);
    while let Some(chunk) = response.stream.as_mut().unwrap().next().await {
        body.extend(chunk.unwrap());
    }
    (
        response.status,
        response.headers,
        String::from_utf8(body).unwrap(),
    )
}

async fn echo(cell: &Cell, event: &Event, id: u64, expected: &str) {
    let mut got = String::new();
    while got.len() < expected.len() {
        let chunk = run(cell, event, Op::TcpRead { id }).await.unwrap();
        assert!(!chunk.is_empty(), "echo ended after {got:?}");
        got.push_str(&String::from_utf8(chunk).unwrap());
    }
    assert_eq!(got, expected);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires native Docker"]
async fn brokered_containers_against_a_native_engine() {
    crate::asyncrt::bind_test_runtime();
    assert!(
        std::env::var_os("DOCKER_HOST").is_some(),
        "set DOCKER_HOST to the native daemon"
    );
    let (fence, workload) = images();
    let live = || live(NODE);
    let data = tempfile::tempdir().unwrap();
    container::configure(NODE.into(), None, data.path().into());
    container::install_application_fence(Some(fence));
    let engine = container::engine().await.unwrap();
    let attach = |application: &'static str, name: &'static str, image: &str| {
        let objects = address(&app(application), ResourceKind::DurableObject, "objects");
        let spec = Arc::new(container::ContainerSpec {
            class_name: address(&app(application), ResourceKind::Container, "box").storage_root(),
            image: image.to_owned(),
            instance_type: None,
            max_instances: None,
            runtime: None,
        });
        let (engine, scope) = (engine.clone(), objects.cell_scope("Counter", name).unwrap());
        async move { engine.attach_with_spec(&scope, spec).await.unwrap() }
    };
    let fx = Event::default();
    // Two applications, one class, alias and object name.
    let one = attach("one", "same", &workload).await;
    let two = attach("two", "same", &workload).await;
    assert_ne!(one.scope(), two.scope());

    // start() then monitor() in the same turn: the monitor waits on this run.
    let run_one = one.begin_run();
    let watched = monitor(&one, &fx, run_one);
    run(&one, &fx, start_op(run_one, None)).await.unwrap();
    start(&two, &fx, None).await;
    assert!(one.running() && two.running() && !watched.is_finished());
    let mut both = vec![one.scope().to_owned(), two.scope().to_owned()];
    both.sort();
    assert_eq!(
        live(),
        both,
        "same class, alias and name must be distinct containers"
    );

    // exec: stdin, stdout, stderr, environment, exit code, drop.
    let id = exec(&one, &fx, &["sh", "-c", "cat; echo \"$K\" >&2; exit 5"]).await;
    let written = perform(
        one.clone(),
        &fx.fx,
        &fx.sockets,
        Op::ExecWrite { id },
        b"hello\n".to_vec(),
    )
    .await;
    written.unwrap();
    run(&one, &fx, Op::ExecClose { id }).await.unwrap();
    assert_eq!(output(&one, &fx, id, 1).await, "hello\n");
    assert_eq!(output(&one, &fx, id, 2).await, "V\n");
    assert_eq!(text(&one, &fx, Op::ExecWait { id }).await, "5");
    // A process id belongs to its own container only.
    let sleeper = exec(&one, &fx, &["sleep", "60"]).await;
    assert!(run(&two, &fx, Op::ExecWait { id: sleeper }).await.is_err());
    assert!(run(&two, &fx, Op::ExecDrop { id: sleeper }).await.is_ok());
    assert!(
        container::process(sleeper).is_some(),
        "a foreign drop removed the process"
    );
    run(
        &one,
        &fx,
        Op::ExecKill {
            id: sleeper,
            signal: 15,
        },
    )
    .await
    .unwrap();
    assert_eq!(text(&one, &fx, Op::ExecWait { id: sleeper }).await, "143");
    run(&one, &fx, Op::ExecDrop { id }).await.unwrap();
    assert!(run(&one, &fx, Op::ExecRead { id, which: 1 }).await.is_err());

    // Port fetch: each cell reaches only its own container, the Host header
    // is the request's, and a redirect comes back unfollowed.
    for (cell, who) in [(&one, "one"), (&two, "two")] {
        let script = format!("echo -n {who} > /www/who");
        let id = exec(cell, &fx, &["sh", "-c", &script]).await;
        assert_eq!(text(cell, &fx, Op::ExecWait { id }).await, "0");
    }
    assert_eq!(get(&one, "/who").await.2, "one");
    assert_eq!(get(&two, "/who").await.2, "two");
    let (status, _, body) = get(&one, "/index.html").await;
    assert_eq!((status, body.as_str()), (200, "ok"));
    let (status, headers, _) = get(&one, "/dir").await;
    let location = headers
        .iter()
        .find(|(name, _)| name == "location")
        .map(|(_, value)| value.as_str());
    assert_eq!((status, location), (302, Some("/dir/")));
    assert_eq!(
        get(&one, "/cgi-bin/host").await.2,
        "app.test:81",
        "the Host header changed"
    );
    assert_eq!(get(&one, "/dir/").await.2, "nested");
    // Another application's object holds no binding to this container.
    let object = |application: &str| {
        let scope = address(&app(application), ResourceKind::DurableObject, "objects")
            .cell_scope("Counter", "same")
            .unwrap();
        celld_applications::invocation_wire::ObjectTarget {
            class: "Counter".into(),
            id: scope.split_once(':').unwrap().1.into(),
            name: Some("same".into()),
        }
    };
    let binding = Binding {
        address: address(&app("one"), ResourceKind::Container, "box"),
        class: "Counter".into(),
        objects: vec![address(&app("one"), ResourceKind::DurableObject, "objects")],
        spec: Arc::new(container::ContainerSpec {
            class_name: String::new(),
            image: workload.clone(),
            instance_type: None,
            max_instances: None,
            runtime: None,
        }),
    };
    assert!(binding.authorizes(&app("one"), &object("one")));
    assert!(!binding.authorizes(&app("two"), &object("two")));
    assert!(!binding.authorizes(&app("one"), &object("two")));

    // Raw TCP: echo, half-close, ownership, TLS refusal, release with the event.
    let connect = |cell: &Cell, event: &Event| {
        let (cell, event) = (cell.clone(), event.clone());
        async move {
            let mut attempts = 0;
            loop {
                match run_op(cell.clone(), event.clone(), Op::PortConnect { port: 9000 }).await {
                    Ok(opened) => {
                        let opened: serde_json::Value = serde_json::from_slice(&opened).unwrap();
                        assert!(opened.get("remoteAddress").is_none());
                        return opened["id"].as_u64().unwrap();
                    }
                    Err(_) if attempts < 50 => {
                        attempts += 1;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(error) => panic!("port 9000 never answered: {error}"),
                }
            }
        }
    };
    let socket = connect(&one, &fx).await;
    let sent = perform(
        one.clone(),
        &fx.fx,
        &fx.sockets,
        Op::TcpWrite { id: socket },
        b"ping".to_vec(),
    )
    .await;
    sent.unwrap();
    echo(&one, &fx, socket, "ping").await;
    // Half-close: the peer sees EOF, answers what it had, and ends.
    let sent = perform(
        one.clone(),
        &fx.fx,
        &fx.sockets,
        Op::TcpWrite { id: socket },
        b"last".to_vec(),
    )
    .await;
    sent.unwrap();
    run(&one, &fx, Op::TcpShutdown { id: socket })
        .await
        .unwrap();
    echo(&one, &fx, socket, "last").await;
    assert!(
        run(&one, &fx, Op::TcpRead { id: socket })
            .await
            .unwrap()
            .is_empty(),
        "no EOF"
    );
    // Another event cannot use it, and closing makes it unusable.
    let other_event = Event::default();
    assert!(run(&one, &other_event, Op::TcpRead { id: socket })
        .await
        .is_err());
    run(&one, &fx, Op::TcpClose { id: socket }).await.unwrap();
    assert!(run(&one, &fx, Op::TcpRead { id: socket }).await.is_err());
    // startTls against a plaintext peer fails and consumes the plaintext socket.
    let plain = connect(&one, &fx).await;
    let upgrade = Op::TcpStartTls {
        id: plain,
        host: "app.test".into(),
    };
    assert!(run(&one, &fx, upgrade).await.is_err());
    assert!(run(&one, &fx, Op::TcpRead { id: plain }).await.is_err());
    // The other application's object has no way to name this socket.
    let held = connect(&one, &fx).await;
    assert!(run(&two, &other_event, Op::TcpWrite { id: held })
        .await
        .is_err());
    let ended = Event::default();
    let doomed = connect(&one, &ended).await;
    ended.sockets.close();
    assert!(
        crate::js::tcp::read_chunk(doomed).await.is_err(),
        "a socket outlived its event"
    );
    assert!(run(&one, &ended, Op::PortConnect { port: 9000 })
        .await
        .is_err());

    // A start that fails surfaces through monitor, as does an exit code.
    let missing = format!("celld-image:{}", "e".repeat(64));
    let failed = attach("one", "failed", &missing).await;
    let failing = failed.begin_run();
    let watching = monitor(&failed, &fx, failing);
    assert!(run(&failed, &fx, start_op(failing, None)).await.is_err());
    assert!(watching.await.unwrap().is_err() && !failed.running());
    let exits = attach("one", "exits", &workload).await;
    let code = start(&exits, &fx, Some(vec!["sh", "-c", "exit 3"])).await;
    assert_eq!(text(&exits, &fx, Op::Monitor { run: code }).await, "3");

    // A start refused before it begins leaves nothing running.
    let refused = attach("one", "refused", &workload).await;
    let closed = Event::default();
    closed.fx.join().await;
    let orphan = refused.begin_run();
    assert!(run(&refused, &closed, start_op(orphan, None))
        .await
        .is_err());
    assert!(!refused.running() && !live().contains(&refused.scope().to_owned()));

    // destroy, then inactivity: eviction keeps the container, a return reconnects, expiry removes it.
    let destroyed = attach("one", "destroyed", &workload).await;
    start(&destroyed, &fx, None).await;
    run(&destroyed, &fx, Op::Destroy {}).await.unwrap();
    assert!(!destroyed.running() && !live().contains(&destroyed.scope().to_owned()));
    run(&one, &fx, Op::Inactivity { millis: 60_000 })
        .await
        .unwrap();
    engine.release(one.scope(), Release::Keep).await;
    let readopted = attach("one", "same", &workload).await;
    assert!(
        Arc::ptr_eq(&readopted, &one) && one.running(),
        "eviction lost the container"
    );
    let idle = attach("one", "idle", &workload).await;
    start(&idle, &fx, None).await;
    run(&idle, &fx, Op::Inactivity { millis: 300 })
        .await
        .unwrap();
    engine.release(idle.scope(), Release::Keep).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(engine.cell(idle.scope()).is_none() && !live().contains(&idle.scope().to_owned()));

    // Owner retirement removes only that owner's container and processes.
    let pending = exec(&one, &fx, &["sleep", "60"]).await;
    engine.release(one.scope(), Release::Destroy).await;
    assert_eq!(
        String::from_utf8(watched.await.unwrap().unwrap()).unwrap(),
        "137"
    );
    assert!(container::process(pending).is_none() && engine.cell(one.scope()).is_none());
    let remaining = live();
    assert!(two.running() && remaining.contains(&two.scope().to_owned()));
    assert!(!remaining.contains(&one.scope().to_owned()));
    for cell in [&two, &failed, &exits, &refused, &destroyed] {
        engine.release(cell.scope(), Release::Destroy).await;
    }
    fx.fx.join().await;
    fx.sockets.close();
    assert!(live().is_empty(), "containers left behind: {:?}", live());
    container::shutdown().await;
}
