use super::*;

fn binding(kind: &str, id: &str) -> serde_json::Value {
    json!({"kind":"resource","resource":{"kind":kind,"id":id}})
}

async fn provision(h: &Harness, app: &str, label: &str) -> serde_json::Value {
    h.create(app).await;
    for (kind, id) in [
        ("durable_object", "counter"),
        ("durable_object", "room"),
        ("d1", "db"),
        ("kv", "cache"),
        ("queue", "jobs"),
        ("queue", "dead"),
        ("workflow", "flow"),
        ("r2", "blobs"),
        ("worker_loader", "loader"),
        ("cron", "tick"),
    ] {
        h.resource(app, kind, id, json!({})).await;
    }
    let digest = h.source(app, NODE_SOURCE).await;
    let manifest = json!({
        "manifest_version":1,"application":{"app":app},"id":"one",
        "handlers":[{
            "id":"javascript","language":"javascript","entrypoint":"main.mjs",
            "artifact_sha256":digest,"runtime_image_sha256":h.runtime_sha256,
            "http":true,"environment":{"LABEL":label},
            "bindings":{
                "COUNTER":binding("durable_object","counter"),
                "ROOM":binding("durable_object","room"),"DB":binding("d1","db"),
                "CACHE":binding("kv","cache"),"JOBS":binding("queue","jobs"),
                "FLOW":binding("workflow","flow"),"BLOBS":binding("r2","blobs"),
                "LOADER":binding("worker_loader","loader")
            },
            "worker_config":{
                "worker_loaders":[{"binding":"LOADER"}],"do_classes":["Counter","Room"],
                "durable_objects":{"bindings":[{"name":"COUNTER","class_name":"Counter"},{"name":"ROOM","class_name":"Room"}]},
                "compatibility_date":"2026-03-01","compatibility_flags":["js_rpc"],
                "queue_consumers":[
                    {"queue":"jobs","max_batch_size":10,"max_batch_timeout":1,"max_retries":1,"dead_letter_queue":"dead","retry_delay":1},
                    {"queue":"dead","max_batch_size":10,"max_batch_timeout":1,"max_retries":0}
                ],
                "workflows":[{"binding":"FLOW","name":"flow","class_name":"Flow"}],
                "r2_buckets":[{"binding":"BLOBS","bucket_name":"blobs"}]
            }
        }]
    });
    h.stage(app, &manifest).await;
    h.activate(app, "one").await;
    manifest
}

async fn completed_workflow(node: &Node, host: &str) -> serde_json::Value {
    timeout(Duration::from_secs(40), async {
        loop {
            let status = json(node, host, "/wf-status?id=shared").await;
            assert!(status.get("error").is_none(), "workflow error: {status}");
            if status["status"] == "complete" {
                break status;
            }
            assert!(
                !matches!(status["status"].as_str(), Some("errored" | "terminated")),
                "workflow failed: {status}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("workflow did not complete")
}

async fn application_pool(node: &Node) -> Value {
    let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{}/state", node.internal))
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    response.json::<Value>().await.unwrap()["applicationVms"].clone()
}

async fn slow_request(public: u16, host: &str) -> (u16, String) {
    let reply = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{public}/slow?ms=1000"))
        .header("host", host)
        .send()
        .await
        .unwrap();
    (reply.status().as_u16(), reply.text().await.unwrap())
}

async fn cache_replacement(h: &Harness, node: &Node, host: &str, mut manifest: Value) {
    let began = Instant::now();
    let mut samples = Vec::new();
    let started = Instant::now();
    assert_eq!(get(node, host, "/hello").await, "hello one");
    let initial_latency = started.elapsed().as_millis();
    assert!(
        initial_latency <= 2000,
        "initial prepared-node /hello exceeded 2s"
    );
    assert_eq!(
        get(node, host, "/bump?name=cache-replace").await,
        "cache-replace:1"
    );
    let old_pids = node.vm_pids();
    assert_eq!(
        old_pids.len(),
        1,
        "initial deployment did not keep one owner"
    );
    samples.push(json!({"version":1,"label":"one","phase":"initial","latencyMs":initial_latency,"pid":old_pids[0]}));
    let cached = timeout(Duration::from_secs(8), async {
        loop {
            let state = application_pool(node).await;
            if state["lifecycle"]["cached"] == 1 && state["memoryReservations"]["retainedVms"] == 1
            {
                break state;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("original deployment did not reach retained cached barrier");
    assert_eq!(cached["vms"], 1);
    assert_eq!(cached["memoryMib"], 1024);
    assert_eq!(cached["quarantined"], 0);
    assert!(
        cached["memoryReservations"]["physicalBytes"]
            .as_u64()
            .unwrap()
            <= 14 * 1024 * 1024 * 1024
    );
    let old_vm = cache_wakes::OwnedVm::new(node);
    old_vm.verify(node);
    let mut proof = json!({"cached": cached});

    // Keep one pinned old request in flight while the active deployment changes.
    let public = node.public;
    let pinned = host.to_owned();
    let old_request = tokio::spawn(async move {
        let reply = reqwest::Client::new()
            .get(format!("http://127.0.0.1:{public}/slow?ms=3000"))
            .header("host", pinned)
            .send()
            .await
            .unwrap();
        (reply.status().as_u16(), reply.text().await.unwrap())
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    old_vm.verify(node);
    let (mut parent_high, high_before) = old_vm.lower_parent_high();
    let pressure = timeout(Duration::from_secs(8), async {
        loop {
            let state = application_pool(node).await;
            if parent_high.high_events() > high_before && state["memoryPressureBlocked"] == true {
                break state;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("real parent memory.high did not latch pool pressure during pinned request");
    parent_high.restore();
    old_vm.verify(node);
    assert!(
        !old_request.is_finished(),
        "pressure observation outlived the pinned old request"
    );
    assert_eq!(pressure["lifecycle"]["running"], 1);
    assert_eq!(old_vm.scalar("memory.max"), 1280 * 1024 * 1024);
    proof["pressure"] = json!({
        "blocked": pressure["memoryPressureBlocked"],
        "highEventsBefore": high_before,
        "highEventsAfter": parent_high.high_events(),
        "memoryHighRestored": true,
        "memoryMaxUnchanged": true
    });

    manifest["id"] = json!("replacement-2");
    manifest["handlers"][0]["environment"]["LABEL"] = json!("replacement-2");
    h.stage("e2e-one", &manifest).await;
    h.activate("e2e-one", "replacement-2").await;

    let (status, body) = old_request.await.unwrap();
    assert_eq!((status, body), (200, "slow one".to_owned()));
    let started = Instant::now();
    assert_eq!(get(node, host, "/hello").await, "hello replacement-2");
    let first_latency = started.elapsed().as_millis();
    assert!(
        first_latency <= 2000,
        "first replacement /hello exceeded 2s"
    );
    let new_request = tokio::spawn({
        let host = host.to_owned();
        let public = node.public;
        async move { slow_request(public, &host).await }
    });
    let full = timeout(Duration::from_secs(8), async {
        loop {
            let state = application_pool(node).await;
            if state["lifecycle"]["running"] == 1
                && state["memoryReservations"]["retainedVms"] == 0
                && state["memoryReservations"]["fullExecutionMib"] == 1024
                && state["vms"] == 1
                && state["quarantined"] == 0
            {
                break state;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("replacement owner did not enter full execution while /slow was pending");
    let (status, body) = new_request.await.unwrap();
    assert_eq!((status, body), (200, "slow replacement-2".to_owned()));
    assert_eq!(
        get(node, host, "/bump?name=cache-replace").await,
        "cache-replace:2"
    );
    proof["fullAfterFirstReplacement"] = full;
    let mut previous_pids = timeout(Duration::from_secs(30), async {
        loop {
            let pids = node.vm_pids();
            if !pids.is_empty() && pids.iter().all(|pid| !old_pids.contains(pid)) {
                break pids;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("old cached owner was not physically reaped before replacement settled");
    samples.push(json!({"version":2,"label":"replacement-2","phase":"replacement","latencyMs":first_latency,"pid":previous_pids[0]}));

    for version in 3..=5 {
        let cached = timeout(Duration::from_secs(8), async {
            loop {
                let state = application_pool(node).await;
                if state["lifecycle"]["cached"] == 1
                    && state["memoryReservations"]["retainedVms"] == 1
                {
                    break state;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("replacement did not reach retained cached barrier before churn");
        assert_eq!(cached["vms"], 1);
        let old_owner = previous_pids;
        let label = format!("replacement-{version}");
        manifest["id"] = json!(label.clone());
        manifest["handlers"][0]["environment"]["LABEL"] = json!(label.clone());
        h.stage("e2e-one", &manifest).await;
        h.activate("e2e-one", &label).await;
        let started = Instant::now();
        assert_eq!(get(node, host, "/hello").await, format!("hello {label}"));
        let latency = started.elapsed().as_millis();
        assert!(latency <= 2000, "replacement /hello exceeded 2s");
        let new_request = tokio::spawn({
            let host = host.to_owned();
            let public = node.public;
            async move { slow_request(public, &host).await }
        });
        let full = timeout(Duration::from_secs(8), async {
            loop {
                let state = application_pool(node).await;
                if state["lifecycle"]["running"] == 1
                    && state["memoryReservations"]["retainedVms"] == 0
                    && state["memoryReservations"]["fullExecutionMib"] == 1024
                    && state["vms"] == 1
                    && state["quarantined"] == 0
                {
                    break state;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("replacement owner did not enter full execution while /slow was pending");
        let (status, body) = new_request.await.unwrap();
        assert_eq!((status, body), (200, format!("slow {label}")));
        assert_eq!(
            get(node, host, "/bump?name=cache-replace").await,
            format!("cache-replace:{version}")
        );
        previous_pids = timeout(Duration::from_secs(30), async {
            loop {
                let pids = node.vm_pids();
                if !pids.is_empty() && pids.iter().all(|pid| !old_owner.contains(pid)) {
                    break pids;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("old cached owner was not physically reaped before churn settled");
        samples.push(json!({"version":version,"label":label,"phase":"replacement","latencyMs":latency,"pid":previous_pids[0],"cached":cached,"full":full}));
    }

    proof["oldPids"] = json!(old_pids);
    proof["newPids"] = json!(previous_pids);
    proof["startupSamples"] = json!(samples);
    eprintln!("T33_CACHE_REPLACEMENT {}", proof);
    passed("services-t33-cache-replacement", began);
}

fn deliveries(rows: &[serde_json::Value], queue: &str, n: i64) -> Vec<i64> {
    rows.iter()
        .filter(|row| row[0] == queue && row[1] == n)
        .map(|row| row[2].as_i64().unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires real application VMs and the management CLI"]
async fn t29_services() {
    celld::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    let entire_began = Instant::now();
    timeout(Duration::from_secs(600), async {
        let h = Harness::new("services", true).await;
        let cache_replacement_enabled =
            std::env::var_os("CELLD_APPLICATION_CACHE_REPLACEMENT_CHECK_ONLY").is_some();
        if cache_replacement_enabled {
            let path = h.directory.join("vms.json");
            let mut configuration: Value =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            configuration["max_vms"] = json!(1);
            configuration["memory_guard"] = json!({
                "max_bytes":14 * 1024 * 1024 * 1024u64,
                "high_bytes":13 * 1024 * 1024 * 1024u64,
                "overhead_bytes":256 * 1024 * 1024u64,
                "shared_cache_bytes":256 * 1024 * 1024u64,
                "idle_cache":{"max_bytes":4 * 1024 * 1024 * 1024u64,"retained_margin_bytes":128 * 1024 * 1024u64}
            });
            std::fs::write(path, serde_json::to_vec(&configuration).unwrap()).unwrap();
        }
        let one_manifest = provision(&h, "e2e-one", "one").await;
        if cache_replacement_enabled {
            let one = h.hostname("e2e-one", "javascript").await;
            let node = h.start("services-cache-replacement").await;
            cache_replacement(&h, &node, &one, one_manifest).await;
            node.drain().await;
            return;
        }
        provision(&h, "e2e-two", "two").await;
        let one = h.hostname("e2e-one", "javascript").await;
        let two = h.hostname("e2e-two", "javascript").await;
        let node = h.start("services").await;

        let began = Instant::now();
        assert_eq!(get(&node, &one, "/bump?name=same").await, "same:1");
        assert_eq!(get(&node, &one, "/bump?name=same").await, "same:2");
        assert_eq!(get(&node, &two, "/bump?name=same").await, "same:1");
        assert_eq!(
            json(&node, &one, "/rpc?name=same").await,
            json!({"add":{"sum":5,"name":"same"},"count":2})
        );
        assert_eq!(
            json(&node, &one, "/fail?name=same").await,
            json!({"name":"RangeError","code":7,"remote":true})
        );
        assert_eq!(get(&node, &one, "/f-both?name=same&n=1").await, "1");
        assert_eq!(get(&node, &one, "/f-only?name=same&n=2").await, "2");
        assert_eq!(
            json(&node, &one, "/f-read?name=same").await,
            json!({"root":[1],"facet":[1,2]})
        );
        assert_eq!(
            json(&node, &two, "/f-read?name=same").await,
            json!({"root":[],"facet":[]})
        );
        passed("services-durable-objects-facets-isolation", began);

        let began = Instant::now();
        for host in [&one, &two] {
            assert_eq!(get(&node, host, "/d1-setup").await, "\"ready\"");
        }
        assert_eq!(get(&node, &one, "/d1-next").await, "1");
        assert_eq!(get(&node, &two, "/d1-read").await, "0");
        assert_eq!(get(&node, &two, "/d1-next").await, "1");
        assert_eq!(get(&node, &two, "/d1-next").await, "2");
        assert_eq!(get(&node, &one, "/d1-read").await, "1");
        let batch = json(&node, &one, "/d1-batch").await;
        assert_eq!(batch["changes"], json!([1, 1]));
        assert_eq!(batch["rows"], json!([[1, "one"], [2, "two"]]));
        assert!(
            batch["failed"]["cause"]
                .as_str()
                .unwrap()
                .contains("UNIQUE"),
            "{batch}"
        );
        passed("services-d1-isolation-rollback", began);

        let began = Instant::now();
        assert_eq!(get(&node, &one, "/kv-seed").await, "\"one\"");
        assert_eq!(
            json(&node, &two, "/kv-peek?key=seed").await,
            serde_json::Value::Null
        );
        assert_eq!(
            json(&node, &two, "/kv-put?key=seed&size=16&fill=7").await,
            json!({"ok":true})
        );
        assert_eq!(
            json(&node, &one, "/kv-peek?key=seed").await,
            json!({"length":3,"head":[111,110,101],"tail":[111,110,101],"middle":110})
        );
        assert_eq!(
            json(&node, &two, "/kv-peek?key=seed").await,
            json!({"length":16,"head":[1,2,3,4],"tail":[5,6,7,8],"middle":7})
        );
        let value = json(&node, &one, "/kv").await;
        assert_eq!(value["text"], "hello");
        assert_eq!(
            value["meta"],
            json!({"value":{"a":[1,2]},"metadata":{"tag":"m"},"cacheStatus":null})
        );
        assert_eq!(
            value["pages"],
            json!([["p:one", "p:three"], false, ["p:two"], true])
        );
        assert!(value["missing"].is_null() && value["deleted"].is_null());
        passed("services-kv-bytes-isolation", began);

        let began = Instant::now();
        for (host, label, foreign) in [(&one, "one", &two), (&two, "two", &one)] {
            assert_eq!(json(&node, host, "/r2-peek").await, serde_json::Value::Null);
            let value = json(&node, host, "/r2").await;
            assert_eq!(value["text"], format!("hello {label}"));
            assert_eq!(value["range"], label);
            assert_eq!(value["custom"], json!({"label":label}));
            assert_eq!(value["type"], "text/plain");
            for flag in [
                "unmet",
                "refused",
                "checksumFailed",
                "cancelled",
                "absent",
                "aborted",
                "deleted",
            ] {
                assert_eq!(value[flag], true, "R2 {label} {flag}: {value}");
            }
            assert_eq!(value["stream"], json!([8 * 1024 * 1024 + 1, 7, 7]));
            assert_eq!(value["produced"], 8 * 1024 * 1024 + 1);
            assert_eq!(value["multipart"], "tail");
            assert_eq!(value["pages"], json!([1, true, 2, false]));
            if label == "one" {
                assert_eq!(
                    json(&node, foreign, "/r2-peek").await,
                    serde_json::Value::Null
                );
            }
        }
        assert_eq!(json(&node, &one, "/r2-peek").await, "hello one");
        assert_eq!(json(&node, &two, "/r2-peek").await, "hello two");
        passed("services-r2-streams-isolation", began);

        let began = Instant::now();
        for (host, label) in [(&one, "one"), (&two, "two")] {
            assert_eq!(get(&node, host, "/q-setup").await, "\"ready\"");
            assert_eq!(
                get(&node, host, &format!("/q-produce?label={label}")).await,
                "\"sent\""
            );
        }
        timeout(Duration::from_secs(60), async {
            loop {
                let mut done = true;
                for host in [&one, &two] {
                    let rows = json(&node, host, "/q-log").await;
                    let rows = rows.as_array().unwrap();
                    done &= !deliveries(rows, "dead", 3).is_empty()
                        && !deliveries(rows, "jobs", 5).is_empty()
                        && deliveries(rows, "jobs", 4) == [1, 2];
                }
                if done {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
        .await
        .expect("queues did not deliver and retry");
        tokio::time::sleep(Duration::from_secs(4)).await;
        for (host, label) in [(&one, "one"), (&two, "two")] {
            let rows = json(&node, host, "/q-log").await;
            let rows = rows.as_array().unwrap();
            assert!(
                rows.iter().all(|row| row[6] == label),
                "tenant queue crossed: {rows:?}"
            );
            for n in [1, 2, 5] {
                assert_eq!(deliveries(rows, "jobs", n), [1], "{rows:?}");
            }
            for n in [3, 4] {
                assert_eq!(deliveries(rows, "jobs", n), [1, 2], "{rows:?}");
            }
            assert_eq!(deliveries(rows, "dead", 3).len(), 1);
            assert_eq!(rows.iter().filter(|row| row[0] == "dead").count(), 1);
            let late = rows.iter().find(|row| row[1] == 5).unwrap();
            assert!(
                late[4].as_i64().unwrap() - late[5].as_i64().unwrap() >= 1500,
                "early delayed message: {late}"
            );
        }
        passed("services-queues-delivery-retry-isolation", began);

        let began = Instant::now();
        assert_eq!(
            json(&node, &one, "/wf-create?id=shared").await,
            json!({"id":"shared"})
        );
        let missing = json(&node, &two, "/wf-status?id=shared").await;
        assert!(
            missing["error"]
                .as_str()
                .unwrap()
                .contains("does not exist"),
            "{missing}"
        );
        assert_eq!(
            completed_workflow(&node, &one).await["output"],
            json!({"first":1,"attempts":2,"label":"one"})
        );
        assert_eq!(
            json(&node, &two, "/wf-create?id=shared").await,
            json!({"id":"shared"})
        );
        assert_eq!(
            completed_workflow(&node, &two).await["output"],
            json!({"first":1,"attempts":2,"label":"two"})
        );
        for host in [&one, &two] {
            assert_eq!(
                json(&node, host, "/wf-counts?id=shared").await,
                json!({"first":1,"retry":2,"after":0})
            );
        }
        passed("services-workflows-completion-isolation", began);

        let began = Instant::now();
        assert_eq!(
            get(&node, &one, "/alarm-arm?name=clock&delay=100").await,
            "armed"
        );
        let state = timeout(Duration::from_secs(30), async {
            loop {
                let state = json(&node, &one, "/alarm-state?name=clock").await;
                if state["attempts"] == 1 && state["alarm"].is_null() {
                    break state;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("durable object alarm did not fire");
        assert_eq!(
            state,
            json!({"attempts":1,"hidden":true,"retryCount":0,"isRetry":false,"alarm":null})
        );
        assert_eq!(
            json(&node, &two, "/alarm-state?name=clock").await["attempts"],
            0
        );
        passed("services-alarms-firing-isolation", began);

        let began = Instant::now();
        let mut socket = ws_open(node.public, &one, "/ws?name=same&tag=a").await;
        assert_eq!(ws_next(&mut socket).await.unwrap(), "welcome:1");
        ws_send(&mut socket, "ping").await;
        assert_eq!(ws_next(&mut socket).await.unwrap(), "pong");
        ws_send(&mut socket, "hello").await;
        assert_eq!(ws_next(&mut socket).await.unwrap(), "echo:hello:1:1:a");
        assert_eq!(get(&node, &one, "/ws-peek?name=same").await, "1::1");
        assert_eq!(get(&node, &two, "/ws-peek?name=same").await, "0::0");
        ws_send(&mut socket, "close").await;
        assert_eq!(
            ws_next(&mut socket).await.unwrap_err(),
            (1000, "bye".to_owned())
        );
        passed("services-incoming-websockets-isolation", began);

        let began = Instant::now();
        let port = ws_echo_server().await;
        assert_eq!(
            json(&node, &one, &format!("/dial?url=ws://127.0.0.1:{port}/")).await,
            json!({"protocol":"","seen":["echo:hello",100000],"code":1000})
        );
        passed("services-outgoing-websockets", began);
        node.drain().await;
    })
    .await
    .expect("services scenario exceeded 600 seconds");
    passed("services::t29_services", entire_began);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires real application VMs and native Docker fixture images"]
async fn t29_containers() {
    celld::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    let entire_began = Instant::now();
    timeout(Duration::from_secs(300), async {
        let began = Instant::now();
        let fence = format!("celld-image:{}", "1".repeat(64));
        let image = format!("celld-image:{}", "2".repeat(64));
        assert_eq!(std::env::var("CELLD_APPLICATION_CONTAINER_FENCE_IMAGE").unwrap(), fence,
            "the runner must prepare and select the container fence fixture");
        for image in [&fence, &image] {
            let output = Command::new("docker").args(["image","inspect",image]).kill_on_drop(true).output().await.unwrap();
            assert!(output.status.success(), "missing fixture {image}: {}", String::from_utf8_lossy(&output.stderr));
        }
        let h = Harness::new("containers", true).await;
        let mut hosts = Vec::new();
        for (app, label) in [("e2e-one","one"),("e2e-two","two")] {
            h.create(app).await;
            h.resource(app, "durable_object", "objects", json!({})).await;
            h.resource(app, "container", "box", json!({"image":image})).await;
            let digest = h.source(app, include_str!("../fixtures/t29-services/container-worker.mjs")).await;
            h.stage(app, &json!({
                "manifest_version":1,"application":{"app":app},"id":"one",
                "handlers":[{
                    "id":"javascript","language":"javascript","entrypoint":"main.mjs",
                    "artifact_sha256":digest,"runtime_image_sha256":h.runtime_sha256,
                    "http":true,"environment":{"LABEL":label},
                    "bindings":{"BOX":binding("durable_object","objects"),"BOXED":binding("container","box")},
                    "worker_config":{
                        "do_classes":["Counter"],"durable_objects":{"bindings":[{"name":"BOX","class_name":"Counter"}]},
                        "containers":[{"binding":"BOXED","class_name":"Counter"}],
                        "compatibility_date":"2026-03-01","compatibility_flags":["js_rpc"]
                    }
                }]
            })).await;
            h.activate(app, "one").await;
            hosts.push(h.hostname(app, "javascript").await);
        }
        let node = h.start("containers").await;
        for (host, label) in hosts.iter().zip(["one","two"]) {
            assert_eq!(get(&node, host, "/running").await, "false");
            assert_eq!(get(&node, host, "/start").await, "ok:true");
            assert_eq!(get(&node, host, &format!("/mark?v={label}")).await, "0");
        }
        for (host, label) in hosts.iter().zip(["one","two"]) {
            assert_eq!(get(&node, host, "/who").await, label);
        }
        assert_eq!(json(&node, &hosts[0], "/output").await, json!({"stdout":"out\n","stderr":"err\n","exit":4}));
        assert_eq!(json(&node, &hosts[0], "/redirect").await, json!({"status":302,"location":"/dir/","followed":"nested"}));
        assert_eq!(get(&node, &hosts[0], "/echo").await, "ping|");
        assert_eq!(get(&node, &hosts[0], "/destroy").await, "false");
        assert_eq!(get(&node, &hosts[0], "/running").await, "false");
        assert_eq!(get(&node, &hosts[1], "/running").await, "true");
        assert_eq!(get(&node, &hosts[1], "/who").await, "two");
        assert_eq!(get(&node, &hosts[1], "/destroy").await, "false");
        node.drain().await;
        passed("services-containers-execution-isolation", began);
    }).await.expect("container scenario exceeded 300 seconds");
    passed("services::t29_containers", entire_began);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "qualification: waits for a real minute boundary and trigger withdrawal"]
async fn t29_cron() {
    celld::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    let entire_began = Instant::now();
    timeout(Duration::from_secs(300), async {
        let began = Instant::now();
        let h = Harness::new("cron", true).await;
        let mut hosts = Vec::new();
        for (app, label) in [("e2e-one","one"),("e2e-two","two")] {
            let mut manifest = provision(&h, app, label).await;
            manifest["id"] = json!("cron");
            manifest["triggers"] = json!([{"kind":"cron","id":"tick","expression":"* * * * *","handler":"javascript"}]);
            h.stage(app, &manifest).await;
            hosts.push(h.hostname(app, "javascript").await);
        }
        let node = h.start("cron").await;
        for app in ["e2e-one","e2e-two"] { h.activate(app, "cron").await; }
        timeout(Duration::from_secs(70), async {
            loop {
                let mut done = true;
                for host in &hosts {
                    let rows = json(&node, host, "/cron-log").await;
                    done &= rows.as_array().unwrap().iter().any(|row| row[2] == 2);
                }
                if done { break; }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }).await.expect("cron did not fire and retry at a real minute boundary");
        for (host, label) in hosts.iter().zip(["one","two"]) {
            let rows = json(&node, host, "/cron-log").await;
            let mut ticks = std::collections::BTreeMap::<i64,Vec<i64>>::new();
            for row in rows.as_array().unwrap() {
                assert_eq!(row[0], "* * * * *");
                assert_eq!(row[3], label);
                let at = row[1].as_i64().unwrap();
                assert_eq!(at % 60_000, 0, "cron must record the occurrence time");
                ticks.entry(at).or_default().push(row[2].as_i64().unwrap());
            }
            assert!(!ticks.is_empty());
            assert!(ticks.values().any(|attempts| attempts == &[1,2]));
            // A new minute may start while the other tenant finishes its retry.
            for attempts in ticks.values() {
                assert!(attempts == &[1] || attempts == &[1,2], "repeated cron occurrence: {attempts:?}");
            }
        }
        for app in ["e2e-one","e2e-two"] { h.activate(app, "one").await; }
        tokio::time::sleep(Duration::from_secs(3)).await;
        let mut baseline = Vec::new();
        for host in &hosts { baseline.push(json(&node, host, "/cron-log").await); }
        tokio::time::sleep(Duration::from_secs(65)).await;
        for (host, rows) in hosts.iter().zip(baseline) {
            assert_eq!(json(&node, host, "/cron-log").await, rows, "withdrawn cron fired again");
        }
        node.drain().await;
        passed("services-cron-minute-retry-withdrawal-isolation", began);
    }).await.expect("cron scenario exceeded 300 seconds");
    passed("services::t29_cron", entire_began);
}
