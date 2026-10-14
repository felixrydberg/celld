use super::*;

#[cfg(test)]
pub(crate) async fn check_application_broker() {
    use crate::bucket::Bucket;
    use crate::r2_store::STREAM_PART;
    use celld_applications::resource_wire::R2Operation as R;
    use celld_applications::{ApplicationId, Name, ResourceAddress, ResourceKind, ResourceRef};
    static DIRECTORY: OnceLock<tempfile::TempDir> = OnceLock::new();
    let directory = DIRECTORY.get_or_init(|| tempfile::tempdir().unwrap());
    set_r2_store(Bucket::open_dev(&directory.path().join("r2.sqlite")).unwrap());
    let name = |value: &str| Name::try_from(value.to_owned()).unwrap();
    let scope = |app: &str| {
        ResourceAddress {
            application: ApplicationId { app: name(app) },
            resource: ResourceRef {
                kind: ResourceKind::R2,
                id: name("bucket"),
            },
        }
        .storage_root()
        .trim_end_matches('/')
        .to_owned()
    };
    let one = scope("one");
    let two = scope("two");
    let call = |scope: String, operation: R, body: Vec<u8>| {
        broker_operation(scope.clone(), scope, operation, body)
    };
    let json = |response: HttpResponse| {
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()
    };
    async fn read(mut response: HttpResponse) -> (serde_json::Value, Vec<u8>) {
        let length = u32::from_be_bytes(response.body[..4].try_into().unwrap()) as usize;
        let record = serde_json::from_slice(&response.body[4..4 + length]).unwrap();
        let mut bytes = response.body.split_off(4 + length);
        if let Some(mut stream) = response.stream {
            while let Some(chunk) = stream.next().await {
                bytes.extend(chunk.unwrap());
            }
        }
        (record, bytes)
    }
    // Local development preserves its private key mapping. Dot segments and
    // already encoded delimiters become
    // literal key segments; leading delimiters never replace the scoped root.
    call(
        two.clone(),
        R::Put {
            key: "sentinel".into(),
            options: "{}".into(),
        },
        b"foreign-kept".to_vec(),
    )
    .await
    .unwrap();
    let unusual = vec![
        format!("{}r2/{two}/sentinel", "../".repeat(20)),
        format!("/r2/{two}/sentinel"),
        format!("%2e%2e/%2F/r2/{two}/sentinel"),
        "./leading//nested/key".into(),
    ];
    let prefix = object_store::path::Path::from(format!("r2/{one}"));
    for key in &unusual {
        let encoded = object_store::path::Path::from(blob_key(&store(&one).unwrap(), &one, key));
        assert!(
            encoded.prefix_matches(&prefix),
            "R2 key escaped application resource: {key}"
        );
        assert_ne!(
            encoded,
            object_store::path::Path::from(blob_key(&store(&two).unwrap(), &two, "sentinel"))
        );
        call(
            one.clone(),
            R::Put {
                key: key.clone(),
                options: "{}".into(),
            },
            b"local".to_vec(),
        )
        .await
        .unwrap();
        let (_, value) = read(
            call(
                one.clone(),
                R::Get {
                    key: key.clone(),
                    options: "{}".into(),
                },
                vec![],
            )
            .await
            .unwrap(),
        )
        .await;
        assert_eq!(value, b"local");
        let (_, foreign) = read(
            call(
                two.clone(),
                R::Get {
                    key: "sentinel".into(),
                    options: "{}".into(),
                },
                vec![],
            )
            .await
            .unwrap(),
        )
        .await;
        assert_eq!(foreign, b"foreign-kept");
    }
    call(one.clone(), R::Delete { keys: unusual }, vec![])
        .await
        .unwrap();
    call(
        two.clone(),
        R::Delete {
            keys: vec!["sentinel".into()],
        },
        vec![],
    )
    .await
    .unwrap();
    let options = r#"{"http":{"contentType":"text/plain"},"custom":{"case":"Kept"}}"#.to_owned();
    assert_eq!(
        json(
            call(
                one.clone(),
                R::Put {
                    key: "record".into(),
                    options: options.clone()
                },
                b"hello".to_vec()
            )
            .await
            .unwrap()
        )["stored"],
        true
    );
    assert_eq!(
        json(
            call(
                two.clone(),
                R::Head {
                    key: "record".into()
                },
                vec![]
            )
            .await
            .unwrap()
        )["state"],
        "miss"
    );
    let head = json(
        call(
            one.clone(),
            R::Head {
                key: "record".into(),
            },
            vec![],
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        json(
            broker_operation(
                one.clone(),
                two.clone(),
                R::Head {
                    key: "record".into()
                },
                vec![]
            )
            .await
            .unwrap()
        )["state"],
        "hit",
        "sharing a bucket shares objects, not upload handles",
    );
    assert_eq!(head["object"]["custom"]["case"], "Kept");
    assert_eq!(head["object"]["http"]["contentType"], "text/plain");
    let (record, bytes) = read(
        call(
            one.clone(),
            R::Get {
                key: "record".into(),
                options: r#"{"range":{"suffix":2}}"#.into(),
            },
            vec![],
        )
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(bytes, b"lo");
    assert_eq!(record["object"]["range"]["length"], 2);
    let (record, bytes) = read(
        call(
            one.clone(),
            R::Get {
                key: "record".into(),
                options: r#"{"onlyIf":{"etagMatches":"wrong"}}"#.into(),
            },
            vec![],
        )
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(record["state"], "unmet");
    assert!(bytes.is_empty());
    assert!(call(
        one.clone(),
        R::Put {
            key: "record".into(),
            options: r#"{"verify":{"sha256":"wrong"}}"#.into()
        },
        b"changed".to_vec()
    )
    .await
    .is_err());
    let (_, bytes) = read(
        call(
            one.clone(),
            R::Get {
                key: "record".into(),
                options: "{}".into(),
            },
            vec![],
        )
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(bytes, b"hello");
    let id = String::from_utf8(
        call(
            one.clone(),
            R::PutBegin {
                key: "stream".into(),
                options: "{}".into(),
            },
            vec![],
        )
        .await
        .unwrap()
        .body,
    )
    .unwrap()
    .parse()
    .unwrap();
    assert!(call(two.clone(), R::PutChunk { id }, vec![8])
        .await
        .is_err());
    assert!(
        broker_operation(one.clone(), two.clone(), R::PutChunk { id }, vec![8])
            .await
            .is_err()
    );
    assert!(broker_operation(
        one.clone(),
        two.clone(),
        R::PutEnd { id, abort: true },
        vec![]
    )
    .await
    .is_err());
    assert!(
        native_put_chunk(id, Some(vec![8]), EgressGate::NoCell, None)
            .await
            .is_err()
    );
    assert!(native_put_end(id, true, EgressGate::NoCell, None)
        .await
        .is_err());
    call(one.clone(), R::PutChunk { id }, vec![7; STREAM_PART + 1])
        .await
        .unwrap();
    assert_eq!(
        json(
            call(one.clone(), R::PutEnd { id, abort: false }, vec![])
                .await
                .unwrap()
        )["stored"],
        true
    );
    let (_, bytes) = read(
        call(
            one.clone(),
            R::Get {
                key: "stream".into(),
                options: "{}".into(),
            },
            vec![],
        )
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(bytes, vec![7; STREAM_PART + 1]);
    let id = String::from_utf8(
        call(
            one.clone(),
            R::MultipartBegin {
                key: "multipart".into(),
                options: options.clone(),
            },
            vec![],
        )
        .await
        .unwrap()
        .body,
    )
    .unwrap();
    assert!(call(
        two.clone(),
        R::MultipartResume {
            key: "multipart".into(),
            id: id.clone()
        },
        vec![]
    )
    .await
    .is_err());
    assert!(broker_operation(
        one.clone(),
        two.clone(),
        R::MultipartResume {
            key: "multipart".into(),
            id: id.clone(),
        },
        vec![]
    )
    .await
    .is_err());
    assert!(
        native_mp_resume(one.clone(), "multipart".into(), id.clone(), None)
            .await
            .is_err()
    );
    assert!(call(
        one.clone(),
        R::MultipartResume {
            key: "other".into(),
            id: id.clone()
        },
        vec![]
    )
    .await
    .is_err());
    call(
        one.clone(),
        R::MultipartResume {
            key: "multipart".into(),
            id: id.clone(),
        },
        vec![],
    )
    .await
    .unwrap();
    let id = id.parse().unwrap();
    call(
        one.clone(),
        R::MultipartPart { id, part: 2 },
        b"second".to_vec(),
    )
    .await
    .unwrap();
    assert!(call(
        two.clone(),
        R::MultipartPart { id, part: 1 },
        b"foreign".to_vec()
    )
    .await
    .is_err());
    assert!(broker_operation(
        one.clone(),
        two.clone(),
        R::MultipartPart { id, part: 1 },
        b"foreign".to_vec()
    )
    .await
    .is_err());
    assert!(
        native_mp_part(id, 1, Some(b"foreign".to_vec()), EgressGate::NoCell, None)
            .await
            .is_err()
    );
    call(
        one.clone(),
        R::MultipartPart { id, part: 1 },
        b"first".to_vec(),
    )
    .await
    .unwrap();
    assert!(call(
        one.clone(),
        R::MultipartPart { id, part: 1 },
        b"rewrite".to_vec()
    )
    .await
    .is_err());
    assert!(call(
        two.clone(),
        R::MultipartComplete {
            id,
            parts: vec![1, 2]
        },
        vec![]
    )
    .await
    .is_err());
    assert!(broker_operation(
        one.clone(),
        two.clone(),
        R::MultipartComplete {
            id,
            parts: vec![1, 2],
        },
        vec![]
    )
    .await
    .is_err());
    assert!(
        native_mp_complete(id, Ok(vec![1, 2]), EgressGate::NoCell, None)
            .await
            .is_err()
    );
    assert_eq!(
        json(
            call(
                one.clone(),
                R::MultipartComplete {
                    id,
                    parts: vec![1, 2]
                },
                vec![]
            )
            .await
            .unwrap()
        )["custom"]["case"],
        "Kept"
    );
    let (_, bytes) = read(
        call(
            one.clone(),
            R::Get {
                key: "multipart".into(),
                options: "{}".into(),
            },
            vec![],
        )
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(bytes, b"firstsecond");
    let id = String::from_utf8(
        call(
            one.clone(),
            R::MultipartBegin {
                key: "aborted".into(),
                options: "{}".into(),
            },
            vec![],
        )
        .await
        .unwrap()
        .body,
    )
    .unwrap()
    .parse()
    .unwrap();
    assert!(call(two.clone(), R::MultipartAbort { id }, vec![])
        .await
        .is_err());
    assert!(
        broker_operation(one.clone(), two.clone(), R::MultipartAbort { id }, vec![])
            .await
            .is_err()
    );
    assert!(native_mp_abort(id, Some(EgressGate::NoCell), None)
        .await
        .is_err());
    call(one.clone(), R::MultipartAbort { id }, vec![])
        .await
        .unwrap();
    call(one.clone(), R::MultipartAbort { id }, vec![])
        .await
        .unwrap();
    let listing = json(
        call(
            one.clone(),
            R::List {
                options: r#"{"limit":1,"include":true}"#.into(),
            },
            vec![],
        )
        .await
        .unwrap(),
    );
    assert_eq!(listing["objects"].as_array().unwrap().len(), 1);
    assert_eq!(listing["truncated"], true);
    let next = json(
        call(
            one.clone(),
            R::List {
                options: serde_json::json!({"cursor":listing["cursor"],"include":true}).to_string(),
            },
            vec![],
        )
        .await
        .unwrap(),
    );
    assert_eq!(next["objects"].as_array().unwrap().len(), 2);
    call(
        one.clone(),
        R::Delete {
            keys: vec![
                "record".into(),
                "stream".into(),
                "multipart".into(),
                "absent".into(),
            ],
        },
        vec![],
    )
    .await
    .unwrap();
    assert!(json(
        call(
            one,
            R::List {
                options: "{}".into()
            },
            vec![]
        )
        .await
        .unwrap()
    )["objects"]
        .as_array()
        .unwrap()
        .is_empty());
    eprintln!(
        "native application R2: original conditional/ranged streaming reads, metadata/checksums, streamed multipart write, out-of-order multipart/resume/complete/abort, cursored list and delete; same-name cross-application scopes, same-bucket cross-owner and legacy handle mutations refused"
    );
}

/// `__r2_head` is swapped for its guest version when a guest isolate is
/// installed (js/guest_ops.rs `override_ops`), and for nothing else. No egress
/// broker exists in-process, so the guest side stops at the broker's refusal;
/// the host half is `broker_operation`, which is compared with the native op.
#[cfg(unix)]
pub(crate) async fn check_guest_head_override() {
    use crate::bucket::Bucket;
    use crate::js::guest_storage::{tests::evaluate_in, FETCH_OUTSIDE_EVENT};
    use celld_applications::resource_wire::R2Operation as R;
    static DIRECTORY: OnceLock<tempfile::TempDir> = OnceLock::new();
    Engine::init();
    let directory = DIRECTORY.get_or_init(|| tempfile::tempdir().unwrap());
    set_r2_store(Bucket::open_dev(&directory.path().join("r2-head.sqlite")).unwrap());
    let put = R::Put {
        key: "record".into(),
        options: r#"{"http":{"contentType":"text/plain"}}"#.into(),
    };
    broker_operation("headcheck".into(), "headcheck".into(), put, b"hello".to_vec())
        .await
        .unwrap();
    let load = |guest: bool| {
        let config = Arc::new(WorkerConfig::new(WorkerConfigOptions {
            src: "export default { fetch() { return new Response('x'); } };".into(),
            script_name: "head-override".into(),
            do_classes: vec![],
            bindings: vec![],
            r2_bindings: vec![],
            d1_bindings: vec![],
            kv_bindings: vec![],
            queue_bindings: vec![],
            queue_consumers: vec![],
            workflow_bindings: vec![],
            vars: vec![],
            node: "check".into(),
            modules: vec![],
            compat: Default::default(),
        }));
        if guest {
            Worker::load_guest_config(config)
        } else {
            Worker::load_config(config)
        }
        .unwrap()
    };
    // Call `op` with `origin` as the continuation and `driver` running the
    // checkpoint; return the one operation it enqueued and who owns it.
    let call = |worker: &Worker, origin: &Arc<IoContext>, driver: &Arc<IoContext>, op: &str| {
        let source = format!("__celld.{op}; return true;");
        evaluate_in(worker, origin, driver, &source).unwrap();
        let mut spawned = asyncrt::drain_spawns();
        assert_eq!(spawned.len(), 1, "{op}");
        let (_, future, _, owner) = spawned.remove(0);
        (future, owner)
    };
    let hit = "__r2_head('headcheck', 'record')";
    let miss = "__r2_head('headcheck', 'absent')";
    let get = "__r2_get('headcheck', 'record', '{}')";

    let native = load(false);
    let (a, b) = (
        IoContext::tracked(&native.runtime_state),
        IoContext::tracked(&native.runtime_state),
    );
    let broker = |key: &str| {
        let key = key.to_owned();
        async move {
            let head = R::Head { key };
            let response = broker_operation("headcheck".into(), "headcheck".into(), head, vec![]);
            serde_json::from_slice::<serde_json::Value>(&response.await.unwrap().body).unwrap()
        }
    };
    for (op, key) in [(hit, "record"), (miss, "absent")] {
        let (future, owner) = call(&native, &a, &b, op);
        assert_eq!(owner, a.continuation_id(), "native {op} owner");
        let Ok(asyncrt::OpOut::Str(json)) = future.await else {
            panic!("native {op} did not resolve");
        };
        let native_json = serde_json::from_str::<serde_json::Value>(&json).unwrap();
        assert_eq!(native_json, broker(key).await, "native and brokered {op}");
    }
    assert!(call(&native, &a, &b, get).0.await.is_ok());

    // A guest isolate never reads the store, though the object is right there
    // under the very name it passes: it asks the broker, and none exists here.
    let guest = load(true);
    let (a, b) = (
        IoContext::tracked(&guest.runtime_state),
        IoContext::tracked(&guest.runtime_state),
    );
    for op in [hit, miss, get] {
        let (future, owner) = call(&guest, &a, &b, op);
        // Attributed to the continuation, not the event running the checkpoint.
        assert_eq!(owner, a.continuation_id(), "guest {op} owner");
        assert_ne!(owner, b.continuation_id(), "guest {op} owner");
        match future.await {
            Err(error) => assert_eq!(error, FETCH_OUTSIDE_EVENT, "guest {op}"),
            Ok(_) => panic!("guest {op} read the store"),
        }
    }
    eprintln!(
        "guest R2 head override: installed in a guest isolate only, attributed to its own continuation under a foreign checkpoint, never reads the store; native head/get unchanged and equal to the broker's head"
    );
}
