//! Focused M5 assigned object/RPC, facet/loader, socket and host cron cache wakes.
use super::freeze_baseline::OwnedVmm;
use super::lifecycle::{Gate, Phase};
use super::lifecycle_baseline::gate;
use super::memory_baseline::{bridge_check, config, name, no_vms, Recorder};
use super::*;
use celld_applications::invocation_wire::{HttpRequest, SocketEvent};
use futures_util::StreamExt;
use serde_json::{json, Value};

// Counter is also exported by the original actor_bridge host Worker.
const SOURCE: &str = r#"
import {DurableObject} from 'cloudflare:workers';
const facetCode = `import {DurableObject} from 'cloudflare:workers';
export class F extends DurableObject {
  constructor(ctx, env) { super(ctx, env); this.n = 0; }
  add(k) { const sql = this.ctx.storage.sql;
    sql.exec('CREATE TABLE IF NOT EXISTS t(v INTEGER)');
    const sum = sql.exec('SELECT COALESCE(SUM(v),0) AS s FROM t').one().s;
    sql.exec('INSERT INTO t VALUES(?)', k); return {n: ++this.n, sum: sum + k}; }
}
export default {fetch() {return new Response('facet');}};
`;
const childCode = `import {WorkerEntrypoint} from 'cloudflare:workers';
let n = 0; const identity = Math.random().toString(36);
export class Api extends WorkerEntrypoint { next() { return {n: ++n, identity}; } }
export default {fetch() {return new Response('child');}};`;
let scheduled = 0;
export class Counter extends DurableObject {
  constructor(ctx, env) { super(ctx, env); ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping','pong')); }
  async add(a,b) { return {sum:a+b, count: await this.ctx.storage.get('count') ?? 0}; }
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === '/facet') {
      const cls = this.env.LOADER.get('facet-code', () => ({compatibilityDate:'2026-03-01',mainModule:'main.js',modules:{'main.js':facetCode}})).getDurableObjectClass('F');
      return Response.json(await this.ctx.facets.get('child', () => ({class:cls})).add(Number(url.searchParams.get('n'))));
    }
    if (url.pathname === '/held') {
      this.ctx.waitUntil(new Promise(resolve => setTimeout(resolve,1200)).then(() => this.ctx.storage.put('held',true)));
      return Response.json({held:true});
    }
    if (url.pathname === '/socket') {
      const [client,server] = Object.values(new WebSocketPair());
      this.ctx.acceptWebSocket(server,['room']); server.serializeAttachment({n:0}); server.send('welcome');
      return new Response(null,{status:101,webSocket:client});
    }
    const count = (await this.ctx.storage.get('count') ?? 0) + 1;
    await this.ctx.storage.put('count',count); return Response.json({count});
  }
  async webSocketMessage(ws,message) {
    const attachment = ws.deserializeAttachment(); ws.serializeAttachment({n:++attachment.n});
    const count = (await this.ctx.storage.get('messages') ?? 0) + 1; await this.ctx.storage.put('messages',count);
    if (message === 'close') ws.close(1000,'bye');
    else ws.send('echo:'+message+':'+attachment.n+':'+count+':'+this.ctx.getTags(ws).join(','));
  }
  async webSocketClose(ws,code,reason,clean) { await this.ctx.storage.put('closed',code+':'+reason+':'+clean); }
  async socketState() { return {messages:await this.ctx.storage.get('messages') ?? 0,closed:await this.ctx.storage.get('closed') ?? ''}; }
}
export default {
  async fetch(request,env) {
    const path = new URL(request.url).pathname;
    if (path === '/loader') { let loads=0;
      const child=env.LOADER.get('child-code', () => {loads++; return {compatibilityDate:'2026-03-01',mainModule:'main.js',modules:{'main.js':childCode}};});
      const result = await child.getEntrypoint('Api').next();
      return Response.json({loads,...result}); }
    if (path === '/scheduled') return Response.json({scheduled});
    const stub=env.COUNTER.getByName(path === '/socket' || path === '/socket-state' ? 'socket' : 'shared');
    if (path === '/rpc') return Response.json(await stub.add(2,3));
    if (path === '/socket-state') return Response.json(await stub.socketState());
    return stub.fetch(request);
  },
  async scheduled() { scheduled++; }
};
"#;

async fn call(
    app: &crate::actor::AppHandle,
    dispatcher: &RouteDispatcher,
    host: &str,
    path: &str,
) -> crate::js::HttpResponse {
    dispatcher
        .dispatch(
            app,
            Duration::from_secs(10),
            HttpRequest {
                method: "GET".into(),
                url: format!("http://{host}{path}"),
                headers: if path == "/socket" {
                    vec![("upgrade".into(), "websocket".into())]
                } else {
                    vec![]
                },
                body: vec![],
            },
            None,
        )
        .await
        .unwrap()
        .expect("mixed assigned route disappeared")
}
async fn value(mut response: crate::js::HttpResponse) -> Value {
    assert_eq!(response.status, 200);
    if let Some(mut stream) = response.stream.take() {
        while let Some(chunk) = stream.next().await {
            response.body.extend(chunk.unwrap());
        }
    }
    serde_json::from_slice(&response.body).unwrap()
}
async fn cached(
    pool: &VmPool,
    gate: &Gate,
    vm: &OwnedVmm,
    owned: fn() -> HashSet<u32>,
    recorder: &Recorder,
) -> u64 {
    timeout(Duration::from_secs(4), async {
        loop {
            let status = gate.status();
            assert_ne!(
                status.phase,
                Phase::Retiring,
                "mixed owner retired before cache barrier"
            );
            if status.phase == Phase::Cached {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("mixed owner did not automatically pause");
    assert_eq!(owned().len(), 1);
    let snapshot = pool.snapshot();
    assert_eq!(
        (snapshot.vms, snapshot.memory_mib, snapshot.quarantined),
        (1, u64::from(ApplicationSizeClass::Basic.resources().1), 0)
    );
    assert_eq!(gate.status().active_events, 0);
    assert_eq!(vm.read_json("/", owned).await.unwrap()["state"], "Paused");
    recorder.host(
        "lifecycle-mixed-idle-cached",
        Some(ApplicationSizeClass::Basic),
        pool,
        owned,
    );
    gate.status().epoch
}

pub(super) async fn run(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    _directory: &Path,
    owned: fn() -> HashSet<u32>,
) {
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_MIXED_WAKE_CHECK_ONLY").is_some());
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_LIFECYCLE_CHECK_ONLY").is_some());
    assert!(owned().is_empty());
    let output = PathBuf::from(
        std::env::var_os("CELLD_APPLICATION_MEMORY_OUTPUT")
            .expect("set a fresh mixed-wake JSONL output"),
    );
    let recorder = Recorder::new(&output, "lifecycle-mixed-wake");
    recorder.emit(json!({"kind":"provenance","kernel":options.kernel.sha256,"runtime":options.runtime.sha256,
        "fixtureSha256":content_digest(SOURCE.as_bytes()),"cycles":3,"ttlSeconds":30,
        "scope":"assigned object HTTP/RPC, retained facet/loader, host-registry socket messages/close, catalog-authenticated scheduled event; no queue settlement or timed alarm claim"}));
    let application = ApplicationId {
        app: name("memory-mixed-wake"),
    };
    let counter = ResourceRef {
        kind: ResourceKind::DurableObject,
        id: name("counter"),
    };
    let loader = ResourceRef {
        kind: ResourceKind::WorkerLoader,
        id: name("loader"),
    };
    let cron = ResourceRef {
        kind: ResourceKind::Cron,
        id: name("tick"),
    };
    let mut catalog = store.read(&application).await.unwrap();
    for resource in [&loader, &cron] {
        catalog
            .register_resource(
                &application,
                crate::applications::Resource {
                    resource: resource.clone(),
                    configuration: json!({}),
                },
            )
            .unwrap();
    }
    catalog
        .set_limits(
            &application,
            Some(ApplicationLimits {
                size_class: ApplicationSizeClass::Basic,
                invocation_timeout_ms: 10000,
            }),
        )
        .unwrap();
    store.commit(catalog).await.unwrap();
    let artifact =
        crate::application_image::build(&[("main.mjs".into(), SOURCE.as_bytes().to_vec())])
            .unwrap();
    let digest = content_digest(&artifact);
    store
        .upload_artifact(&application, &digest, artifact.into())
        .await
        .unwrap();
    let manifest:Deployment=serde_json::from_value(json!({"manifest_version":1,"application":application,"id":"mixed-wake",
        "handlers":[{"id":"mixed","language":"javascript","entrypoint":"main.mjs","artifact_sha256":digest,
            "runtime_image_sha256":options.runtime.sha256,"http":true,
            "bindings":{"COUNTER":{"kind":"resource","resource":counter},"LOADER":{"kind":"resource","resource":loader}},
            "worker_config":{"do_classes":["Counter"],"durable_objects":{"bindings":[{"name":"COUNTER","class_name":"Counter"}]},
                "worker_loaders":[{"binding":"LOADER"}],"compatibility_date":"2026-03-01","compatibility_flags":["js_rpc"]}}],
        "triggers":[{"kind":"cron","id":"tick","expression":"* * * * *","handler":"mixed"}]})).unwrap();
    store.stage(&manifest).await.unwrap();
    let mut catalog = store.read(&application).await.unwrap();
    store.activate(&mut catalog, manifest).await.unwrap();
    store.commit(catalog).await.unwrap();
    // Existing facet host bridge; retain its task so successful cleanup joins it.
    let (requests, mut requested) = tokio::sync::mpsc::unbounded_channel();
    crate::js::set_facet_tx(requests);
    let host_app = app.clone();
    let facet_host = AbortOnDropHandle::new(tokio::spawn(async move {
        while let Some(request) = requested.recv().await {
            let runtime = host_app.runtime.as_ref().unwrap();
            match request {
                crate::js::FacetReq::Open {
                    root,
                    epoch,
                    names,
                    reply,
                } => drop(reply.send(runtime.open_facet(&root, epoch, &names).await)),
                crate::js::FacetReq::Delete {
                    root,
                    epoch,
                    names,
                    reply,
                } => drop(reply.send(runtime.delete_facet(&root, epoch, &names).await)),
                crate::js::FacetReq::Prove {
                    stream,
                    epoch,
                    reply,
                } => drop(reply.send(runtime.prove_facet(&stream, epoch).await)),
            }
        }
    }));
    let mut configuration = config(options, 0, 30);
    configuration.max_vms = 1;
    let dispatcher = RouteDispatcher::new(configuration, store.clone())
        .await
        .unwrap();
    dispatcher.install_alarm_host(app.clone()).unwrap();
    let pool = dispatcher.routes.pool.as_ref().unwrap();
    let host = super::tests::assigned_host(store, &application, "mixed").await;
    let lease = pool
        .acquire(app, &application, &name("mixed-wake"), &name("mixed"))
        .await
        .unwrap();
    let owner_gate = gate(pool, &lease.client);
    let vm = OwnedVmm::identify(owned);
    let original_pids = owned();
    drop(lease);
    let mut before = cached(pool, &owner_gate, &vm, owned, &recorder).await;
    let mut child_identity = Value::Null;
    for cycle in 1..=3 {
        for (path, expected) in [
            ("/bump".to_owned(), json!({"count":cycle})),
            ("/rpc".to_owned(), json!({"sum":5,"count":cycle})),
            ("/facet?n=2".to_owned(), json!({"n":cycle,"sum":2*cycle})),
            ("/loader".to_owned(), Value::Null),
        ] {
            let began = Instant::now();
            let response = value(call(app, &dispatcher, &host, &path).await).await;
            if path == "/loader" {
                assert_eq!(response["loads"], if cycle == 1 { 1 } else { 0 });
                assert_eq!(response["n"], cycle);
                if cycle == 1 {
                    child_identity = response["identity"].clone();
                    assert!(!child_identity.as_str().unwrap().is_empty());
                }
                assert_eq!(response["identity"], child_identity);
            } else {
                assert_eq!(response, expected);
            }
            let latency = began.elapsed().as_micros();
            let after = cached(pool, &owner_gate, &vm, owned, &recorder).await;
            assert!(after > before);
            assert_eq!(owned(), original_pids);
            recorder.emit(json!({"kind":"mixedWake","cycle":cycle,"path":path,"response":response,"latencyUs":latency,"beforeEpoch":before,"afterEpoch":after}));
            before = after;
        }
    }
    assert_eq!(
        value(call(app, &dispatcher, &host, "/held").await).await,
        json!({"held":true})
    );
    assert_eq!(vm.read_json("/", owned).await.unwrap()["state"], "Running");
    recorder.host(
        "lifecycle-mixed-held",
        Some(ApplicationSizeClass::Basic),
        pool,
        owned,
    );
    let until = Instant::now() + Duration::from_millis(150);
    while Instant::now() < until {
        let status = owner_gate.status();
        assert_eq!(status.phase, Phase::Running);
        assert!(status.active_events > 0);
        assert_eq!(
            pool.snapshot().memory_mib,
            u64::from(ApplicationSizeClass::Basic.resources().1)
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let after = cached(pool, &owner_gate, &vm, owned, &recorder).await;
    assert!(after > before);
    before = after;
    recorder.emit(json!({"kind":"mixedHeldWork","chargedMemoryMib":pool.snapshot().memory_mib,"pids":owned(),"policy":"object waitUntil prevented pause and retained full charge"}));
    let response = call(app, &dispatcher, &host, "/socket").await;
    assert_eq!(response.status, 101);
    let Some(crate::js::HttpResponseWebSocket::Guest(socket)) = response.websocket else {
        panic!("no host-owned socket");
    };
    let target = socket.into_target();
    let socket_address = ResourceAddress {
        application: application.clone(),
        resource: counter.clone(),
    };
    let socket_scope = socket_address.cell_scope("Counter", "socket").unwrap();
    assert_eq!(target.scope, socket_scope);
    // The library bridge has no node-global router to publish guest object registrations.
    dispatcher
        .register_cell(
            &socket_address,
            &celld_applications::invocation_wire::ObjectTarget {
                class: "Counter".into(),
                id: socket_scope.split_once(':').unwrap().1.into(),
                name: Some("socket".into()),
            },
        )
        .await
        .unwrap();
    let (frames, mut received) = tokio::sync::mpsc::unbounded_channel();
    crate::js::ws_register(target.id, frames);
    assert!(
        matches!(timeout(Duration::from_secs(10),received.recv()).await.unwrap().unwrap(),crate::js::WsOut::Text(text) if text=="welcome")
    );
    let after = cached(pool, &owner_gate, &vm, owned, &recorder).await;
    assert!(after > before);
    before = after;
    assert_eq!(
        crate::js::ws_auto_response(&target.scope, target.id, "ping").as_deref(),
        Some("pong")
    );
    assert_eq!(
        cached(pool, &owner_gate, &vm, owned, &recorder).await,
        before,
        "auto-response woke guest"
    );
    for (message, expected) in [("one", "echo:one:1:1:room"), ("two", "echo:two:2:2:room")] {
        dispatcher
            .dispatch_socket(
                app,
                &target.scope,
                target.id,
                SocketEvent::Text,
                message.as_bytes().to_vec(),
            )
            .await
            .unwrap();
        assert!(
            matches!(timeout(Duration::from_secs(10),received.recv()).await.unwrap().unwrap(),crate::js::WsOut::Text(text) if text==expected)
        );
        let after = cached(pool, &owner_gate, &vm, owned, &recorder).await;
        assert!(after > before);
        before = after;
    }
    dispatcher
        .dispatch_socket(
            app,
            &target.scope,
            target.id,
            SocketEvent::Text,
            b"close".to_vec(),
        )
        .await
        .unwrap();
    assert!(
        matches!(timeout(Duration::from_secs(10),received.recv()).await.unwrap().unwrap(),crate::js::WsOut::Close(1000,reason) if reason=="bye")
    );
    let after = cached(pool, &owner_gate, &vm, owned, &recorder).await;
    assert!(after > before);
    before = after;
    dispatcher
        .dispatch_socket(
            app,
            &target.scope,
            target.id,
            SocketEvent::Close {
                code: 1000,
                reason: "bye".into(),
                was_clean: true,
            },
            vec![],
        )
        .await
        .unwrap();
    let after = cached(pool, &owner_gate, &vm, owned, &recorder).await;
    assert!(after > before);
    before = after;
    assert_eq!(
        value(call(app, &dispatcher, &host, "/socket-state").await).await,
        json!({"messages":3,"closed":"1000:bye:true"})
    );
    let after = cached(pool, &owner_gate, &vm, owned, &recorder).await;
    assert!(after > before);
    before = after;
    app.websocket_closed(target.scope.clone(), target.id);
    crate::js::ws_unregister(target.id);
    recorder.emit(json!({"kind":"mixedSocketWake","messages":3,"closed":"1000:bye:true","autoResponseDidNotWake":true}));
    let cron_scope = ResourceAddress {
        application: application.clone(),
        resource: cron,
    }
    .cell_scope(celld_logic::cron::RESERVED_CLASS, "tick")
    .unwrap();
    let result: Value = serde_json::from_str(
        &dispatcher
            .dispatch_scheduled(&cron_scope, "* * * * *", 2)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(result["outcome"], "ok");
    assert_eq!(result["noRetry"], false);
    let after = cached(pool, &owner_gate, &vm, owned, &recorder).await;
    assert!(after > before);
    assert_eq!(
        value(call(app, &dispatcher, &host, "/scheduled").await).await,
        json!({"scheduled":1})
    );
    assert!(cached(pool, &owner_gate, &vm, owned, &recorder).await > after);
    assert_eq!(owned(), original_pids);
    recorder.emit(json!({"kind":"mixedScheduledWake","result":result,"source":"host catalog-authenticated dispatch_scheduled; no real minute boundary or timed object alarm"}));
    dispatcher.shutdown().await.unwrap();
    no_vms(pool, owned).await;
    facet_host.abort();
    assert!(facet_host.await.unwrap_err().is_cancelled());
    let leaked: Vec<_> = recorder
        .groups
        .lock()
        .unwrap()
        .iter()
        .filter(|path| path.exists())
        .cloned()
        .collect();
    assert!(leaked.is_empty());
    recorder.emit(json!({"kind":"cleanup","ownedPids":owned(),"remainingCgroups":leaked}));
    eprintln!(
        "T29_RESULT {}",
        json!({"scenario":"t33::mixed_wake","status":"passed","duration_ms":recorder.began.elapsed().as_millis(),"output":output,
        "scope":"same assigned PID/start ticks: object HTTP/RPC, facet/loader, socket and host-injected catalog cron; queue settlement and timed alarms remain"})
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires idle-capable JavaScript image, Linux KVM and assigned mixed lifecycle gates"]
async fn t33_mixed_wake() {
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_MIXED_WAKE_CHECK_ONLY").is_some());
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_LIFECYCLE_CHECK_ONLY").is_some());
    bridge_check("memory-mixed-wake").await;
}
