use super::*;

async fn durable_state(node: &Node, host: &str, last: u32) {
    timeout(Duration::from_secs(120), async {
        loop {
            let (status, body) = request(node, host, "/t22-read?name=t29-fault-object").await;
            if status == 200 {
                let state: Value = serde_json::from_str(&body).unwrap();
                assert_eq!(
                    state["rows"],
                    json!((1..=last).collect::<Vec<_>>()),
                    "acknowledged D1 writes changed"
                );
                assert_eq!(state["count"], last, "acknowledged object writes changed");
                let keys: Vec<String> = serde_json::from_value(state["keys"].clone()).unwrap();
                assert_eq!(
                    keys,
                    (1..=last).map(|n| format!("t22:{n}")).collect::<Vec<_>>(),
                    "acknowledged KV writes changed"
                );
                let rows = json(node, host, "/q-log").await;
                if (1..=last).all(|n| {
                    rows.as_array()
                        .unwrap()
                        .iter()
                        .any(|row| row[1] == n && row[6] == "t22")
                }) {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("acknowledged state/queue did not recover through ingress");
}

async fn write(node: &Node, host: &str, n: u32) {
    assert_eq!(
        json(node, host, &format!("/t22?name=t29-fault-object&n={n}")).await,
        n
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires root, KVM, the approved guest image/kernel and real celld nodes"]
async fn t29_node_faults() {
    let began = Instant::now();
    celld::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    let h = Harness::new("faults", true).await;
    let app = "e2e-one";
    h.create(app).await;
    for (kind, id) in [
        ("durable_object", "counter"),
        ("d1", "db"),
        ("kv", "cache"),
        ("queue", "jobs"),
        ("queue", "dead"),
    ] {
        h.resource(app, kind, id, json!({})).await;
    }
    let digest = h.source(app, NODE_SOURCE).await;
    h.stage(app, &json!({
        "manifest_version":1, "application":{"app":app}, "id":"faults",
        "handlers":[{
            "id":"main", "language":"javascript", "entrypoint":"main.mjs",
            "artifact_sha256":digest, "runtime_image_sha256":h.runtime_sha256,
            "http":true, "environment":{"LABEL":"faults"}, "bindings":{
                "COUNTER":{"kind":"resource","resource":{"kind":"durable_object","id":"counter"}},
                "DB":{"kind":"resource","resource":{"kind":"d1","id":"db"}},
                "CACHE":{"kind":"resource","resource":{"kind":"kv","id":"cache"}},
                "JOBS":{"kind":"resource","resource":{"kind":"queue","id":"jobs"}}
            },
            "worker_config":{
                "do_classes":["Counter"], "durable_objects":{"bindings":[{"name":"COUNTER","class_name":"Counter"}]},
                "compatibility_date":"2026-03-01", "compatibility_flags":["js_rpc"],
                "queue_consumers":[
                    {"queue":"jobs","max_batch_size":10,"max_batch_timeout":1,"max_retries":1,"dead_letter_queue":"dead","retry_delay":1},
                    {"queue":"dead","max_batch_size":10,"max_batch_timeout":1,"max_retries":0}
                ]
            }
        }]
    })).await;
    h.activate(app, "faults").await;
    let host = h.hostname(app, "main").await;
    let one = h.start("fault-owner").await;
    assert_eq!(json(&one, &host, "/q-setup").await, "ready");
    for n in 1..=3 {
        write(&one, &host, n).await;
    }
    durable_state(&one, &host, 3).await;

    // Kill the real guest with one committed write and an open transaction.
    let held_host = host.clone();
    let public = one.public;
    let held = tokio::spawn(async move {
        let response = reqwest::Client::builder()
            .timeout(Duration::from_secs(80))
            .build()
            .unwrap()
            .get(format!(
                "http://127.0.0.1:{public}/crash-hold?name=t29-open"
            ))
            .header("host", held_host)
            .send()
            .await
            .ok()?;
        Some(response.status().as_u16())
    });
    timeout(Duration::from_secs(45), async {
        while json(&one, &host, "/rpc?name=t29-open-sig").await["count"] != 1 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("open transaction was not reached");
    let killed = one.vm_pids();
    assert!(!killed.is_empty(), "no Firecracker guest to kill");
    for pid in &killed {
        assert_eq!(unsafe { libc::kill(*pid as i32, libc::SIGKILL) }, 0);
    }
    assert_ne!(
        timeout(Duration::from_secs(85), held)
            .await
            .unwrap()
            .unwrap(),
        Some(200),
        "crashed request was acknowledged"
    );
    durable_state(&one, &host, 3).await;
    assert_eq!(
        json(&one, &host, "/crash-state?name=t29-open").await,
        json!({"acked":1,"uncommitted":0})
    );
    assert_eq!(
        json(&one, &host, "/rpc?name=t29-open-sig").await["count"],
        1
    );
    assert!(
        one.vm_pids().iter().any(|pid| !killed.contains(pid)),
        "guest was not replaced"
    );
    write(&one, &host, 4).await;

    one.drain().await;
    let two = h.start("fault-after-drain").await;
    durable_state(&two, &host, 4).await;
    write(&two, &host, 5).await;
    assert_eq!(
        get(&two, &host, "/alarm-arm?name=t29-loss-alarm&delay=90000").await,
        "armed"
    );
    let pending = json(&two, &host, "/alarm-state?name=t29-loss-alarm").await;
    assert_eq!(pending["attempts"], 0);
    assert!(!pending["alarm"].is_null());
    two.kill().await;

    let successor = h.start("fault-fresh-successor").await;
    durable_state(&successor, &host, 5).await;
    timeout(Duration::from_secs(120), async {
        loop {
            let state = json(&successor, &host, "/alarm-state?name=t29-loss-alarm").await;
            if state["attempts"] == 1 && state["alarm"].is_null() {
                break;
            }
            assert!(
                state["attempts"] == 0 || state["attempts"] == 1,
                "alarm fired more than once: {state}"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("acknowledged alarm did not survive node loss");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        json(&successor, &host, "/alarm-state?name=t29-loss-alarm").await["attempts"],
        1
    );
    write(&successor, &host, 6).await;
    durable_state(&successor, &host, 6).await;
    successor.drain().await;
    passed("faults::t29_node_faults", began);
}
