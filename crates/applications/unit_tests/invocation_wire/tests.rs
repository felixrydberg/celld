fn sample_stats() -> GuestMemorySample {
    GuestMemorySample {
        rss_bytes: u64::MAX,
        available_bytes: Some(u64::MAX),
        root: HeapMemory {
            used_bytes: u64::MAX,
            physical_bytes: u64::MAX,
            external_bytes: u64::MAX,
        },
        total: HeapMemory {
            used_bytes: u64::MAX,
            physical_bytes: u64::MAX,
            external_bytes: u64::MAX,
        },
        isolates: u32::MAX,
        busy_isolates: u32::MAX,
        loading_isolates: u32::MAX,
        complete: false,
        jemalloc: Some(AllocatorMemory {
            allocated_bytes: u64::MAX,
            active_bytes: u64::MAX,
            resident_bytes: u64::MAX,
            mapped_bytes: u64::MAX,
            retained_bytes: u64::MAX,
        }),
        libc_malloc: Some(LibcMemory {
            in_use_bytes: u64::MAX,
            free_bytes: u64::MAX,
            mmap_bytes: u64::MAX,
            arena_bytes: u64::MAX,
        }),
    }
}

#[test]
fn memory_samples_are_bounded_and_separate_from_invocations() {
    let request = HostControl::MemorySample {
        version: VERSION,
        sample: "a".repeat(64),
    };
    let reply = GuestControl::MemorySample {
        version: VERSION,
        sample: "a".repeat(64),
        stats: sample_stats(),
    };
    assert_eq!(reply.invocation(), None);
    let bytes = request.encode().unwrap();
    assert!(bytes.len() <= MAX_MEMORY_SAMPLE_BYTES);
    assert_eq!(HostControl::decode(&bytes).unwrap(), request);
    let bytes = reply.encode().unwrap();
    assert!(bytes.len() <= MAX_MEMORY_SAMPLE_BYTES);
    assert_eq!(GuestControl::decode(&bytes).unwrap(), reply);
    // Received byte limits include whitespace, not just re-encoded fields.
    let mut padded = bytes.clone();
    padded.resize(MAX_MEMORY_SAMPLE_BYTES, b' ');
    assert_eq!(GuestControl::decode(&padded).unwrap(), reply);
    padded.push(b' ');
    assert!(GuestControl::decode(&padded).is_err());
    let mut padded = request.encode().unwrap();
    padded.resize(MAX_MEMORY_SAMPLE_BYTES + 1, b' ');
    assert!(HostControl::decode(&padded).is_err());
    for sample in ["", "short", &"g".repeat(64)] {
        assert!(HostControl::MemorySample {
            version: VERSION,
            sample: sample.into()
        }
        .encode()
        .is_err());
        assert!(GuestControl::MemorySample {
            version: VERSION,
            sample: sample.into(),
            stats: sample_stats()
        }
        .encode()
        .is_err());
    }
    assert!(HostControl::MemorySample {
        version: VERSION + 1,
        sample: "a".repeat(64)
    }
    .encode()
    .is_err());
    assert!(GuestControl::MemorySample {
        version: VERSION + 1,
        sample: "a".repeat(64),
        stats: sample_stats()
    }
    .encode()
    .is_err());
    let old = br#"{"event":"ready","version":1,"adapter":"javascript","execution_turns":true,"quiescent_terminals":true,"cleanup_turns":true,"callback_turns":true,"response_turns":true}"#;
    assert!(matches!(
        GuestControl::decode(old).unwrap(),
        GuestControl::Ready {
            memory_diagnostics: false,
            idle_lifecycle: false,
            ..
        }
    ));
}

#[test]
fn idle_probe_is_bounded_and_never_an_invocation_terminal() {
    let request = HostControl::IdleProbe {
        version: VERSION,
        probe: "a".repeat(64),
    };
    let reply = GuestControl::IdleStatus {
        version: VERSION,
        probe: "a".repeat(64),
        idle: true,
    };
    assert_eq!(reply.invocation(), None);
    assert!(!reply.is_event_io());
    assert_eq!(
        HostControl::decode(&request.encode().unwrap()).unwrap(),
        request
    );
    assert_eq!(
        GuestControl::decode(&reply.encode().unwrap()).unwrap(),
        reply
    );
    for mut bytes in [request.encode().unwrap(), reply.encode().unwrap()] {
        bytes.resize(MAX_MEMORY_SAMPLE_BYTES + 1, b' ');
        assert!(HostControl::decode(&bytes).is_err());
        assert!(GuestControl::decode(&bytes).is_err());
    }
    for probe in ["", "short", &"g".repeat(64)] {
        assert!(HostControl::IdleProbe {
            version: VERSION,
            probe: probe.into()
        }
        .encode()
        .is_err());
        assert!(GuestControl::IdleStatus {
            version: VERSION,
            probe: probe.into(),
            idle: false
        }
        .encode()
        .is_err());
    }
    assert!(HostControl::IdleProbe {
        version: VERSION + 1,
        probe: "a".repeat(64)
    }
    .encode()
    .is_err());
    assert!(GuestControl::IdleStatus {
        version: VERSION + 1,
        probe: "a".repeat(64),
        idle: true
    }
    .encode()
    .is_err());
    let mut value = serde_json::to_value(&request).unwrap();
    value["invocation"] = serde_json::json!("b".repeat(64));
    assert!(HostControl::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    let mut value = serde_json::to_value(&reply).unwrap();
    value["invocation"] = serde_json::json!("b".repeat(64));
    assert!(GuestControl::decode(&serde_json::to_vec(&value).unwrap()).is_err());
}

#[test]
fn memory_samples_refuse_unknown_fields_at_every_level() {
    let reply = GuestControl::MemorySample {
        version: VERSION,
        sample: "a".repeat(64),
        stats: sample_stats(),
    };
    for path in [
        vec![],
        vec!["stats"],
        vec!["stats", "root"],
        vec!["stats", "total"],
        vec!["stats", "jemalloc"],
        vec!["stats", "libc_malloc"],
    ] {
        let mut value = serde_json::to_value(&reply).unwrap();
        let mut object = &mut value;
        for field in path {
            object = &mut object[field];
        }
        object["unexpected"] = serde_json::json!(true);
        assert!(GuestControl::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    }
    let mut value = serde_json::to_value(HostControl::MemorySample {
        version: VERSION,
        sample: "a".repeat(64),
    })
    .unwrap();
    value["invocation"] = serde_json::json!("b".repeat(64));
    assert!(HostControl::decode(&serde_json::to_vec(&value).unwrap()).is_err());
}

#[test]
fn service_selectors_scope_streams_and_rpc() {
    use super::*;
    let invocation = ServiceInvocation {
        entrypoint: Some("Named".into()),
        props: vec![0, 255],
        rpc: Some(vec!["nested".into(), "value".into()]),
        get: true,
        signals: vec![],
        signal_owner: None,
    };
    let call = ServiceCall {
        binding: "SVC".to_owned().try_into().unwrap(),
        invocation: invocation.clone(),
    };
    let mut request = GuestControl::FetchStart {
        version: VERSION,
        invocation: "a".repeat(64),
        fetch: "b".repeat(64),
        request: HttpRequest {
            method: "POST".into(),
            url: "https://service.invalid/".into(),
            headers: vec![],
            body: vec![],
        },
        redirect: "manual".into(),
        target: None,
        service: Some(Box::new(call)),
        resource: None,
        request_stream: true,
        request_bytes: None,
        dial: None,
    };
    assert_eq!(
        GuestControl::decode(&request.encode().unwrap()).unwrap(),
        request
    );
    let mut forged = serde_json::to_value(&request).unwrap();
    forged["service"]["application"] = serde_json::json!({"app":"other"});
    assert!(GuestControl::decode(&serde_json::to_vec(&forged).unwrap()).is_err());
    let GuestControl::FetchStart { request: body, .. } = &mut request else {
        unreachable!()
    };
    body.body = vec![1];
    assert!(request.encode().is_err());
    let GuestControl::FetchStart {
        request: body,
        target,
        ..
    } = &mut request
    else {
        unreachable!()
    };
    body.body.clear();
    *target = Some(Box::new(ObjectCall {
        object: ObjectTarget {
            class: "Counter".into(),
            id: "c".repeat(64),
            name: None,
        },
        rpc: None,
    }));
    assert!(request.encode().is_err());
    let mut invalid = invocation;
    invalid.rpc = None;
    assert!(invalid.validate().is_err());
}

#[test]
fn alarm_invocation_requires_exclusive_host_object_metadata() {
    let mut invocation = super::InvokeHttp {
        version: super::VERSION,
        invocation: "a".repeat(64),
        storage_grant: "b".repeat(64),
        storage_residency: Some("c".repeat(64)),
        object: Some(super::ObjectTarget {
            class: "Counter".into(),
            id: "d".repeat(64),
            name: None,
        }),
        request: super::HttpRequest {
            method: "POST".into(),
            url: "http://celld.internal/alarm".into(),
            headers: vec![],
            body: vec![],
        },
        request_stream: false,
        rpc: None,
        queue: false,
        service: None,
        alarm: Some(super::AlarmInvocation {
            scheduled_ms: 123,
            retry: 1,
        }),
        socket: None,
    };
    assert!(invocation.encode().is_ok());
    invocation.service = Some(super::ServiceInvocation {
        entrypoint: None,
        props: vec![],
        rpc: None,
        get: false,
        signals: vec![],
        signal_owner: None,
    });
    assert!(
        invocation.encode().is_err(),
        "mixed alarm/service event admitted"
    );
    invocation.service = None;
    invocation.rpc = Some("alarm".into());
    assert!(invocation.encode().is_err());
    invocation.rpc = None;
    invocation.request_stream = true;
    assert!(invocation.encode().is_err());
    invocation.request_stream = false;
    invocation.alarm.as_mut().unwrap().retry = -1;
    assert!(invocation.encode().is_err());
    invocation.alarm.as_mut().unwrap().retry = 1;
    invocation.object = None;
    assert!(invocation.encode().is_err());
    invocation.alarm = None;
    invocation.storage_residency = None;
    assert!(invocation.encode().is_ok());
    let encoded = serde_json::to_value(&invocation).unwrap();
    assert!(
        encoded.get("alarm").is_none(),
        "ordinary invocation emitted alarm metadata"
    );
    invocation.service = Some(super::ServiceInvocation {
        entrypoint: None,
        props: vec![],
        rpc: None,
        get: false,
        signals: vec![],
        signal_owner: None,
    });
    assert!(invocation.encode().is_ok());
    invocation.storage_residency = Some("c".repeat(64));
    assert!(
        invocation.encode().is_err(),
        "service borrowed an object residency"
    );
}
#[test]
fn response_stream_credit_bounds_and_order() {
    use super::*;
    let invocation = "a".repeat(64);
    let mut flow = ResponseFlow::default();
    assert!(flow.chunk(0).is_err() && flow.end(0).is_err());
    assert!(flow.grant(1).is_err());
    flow.grant(0).unwrap();
    assert!(flow.grant(0).is_err() && flow.chunk(1).is_err());
    flow.chunk(0).unwrap();
    assert!(!flow.has_credit() && flow.next_sequence() == 1);
    assert!(flow.chunk(0).is_err() && flow.end(1).is_err());
    flow.grant(1).unwrap();
    flow.end(1).unwrap();
    assert!(flow.is_closed() && flow.grant(1).is_err() && flow.end(1).is_err());
    let mut exhausted = ResponseFlow {
        next: u64::MAX,
        credit: true,
        closed: false,
    };
    assert!(exhausted.chunk(u64::MAX).is_err());
    exhausted.end(u64::MAX).unwrap();
    for message in [
        GuestControl::HttpStreamHead(HttpStreamHead {
            version: VERSION,
            invocation: invocation.clone(),
            status: 201,
            headers: vec![
                ("set-cookie".into(), "one".into()),
                ("set-cookie".into(), "two".into()),
            ],
        }),
        GuestControl::ResponseChunk {
            version: VERSION,
            invocation: invocation.clone(),
            sequence: 0,
            data: vec![255; MAX_CHUNK_BYTES],
        },
        GuestControl::ResponseEnd {
            version: VERSION,
            invocation: invocation.clone(),
            sequence: 1,
            error: None,
        },
        GuestControl::ResponseEnd {
            version: VERSION,
            invocation: invocation.clone(),
            sequence: 1,
            error: Some("producer failed".into()),
        },
    ] {
        assert_eq!(
            GuestControl::decode(&message.encode().unwrap()).unwrap(),
            message
        );
        for field in ["scope", "grant", "write_position", "proof"] {
            let mut spoofed = serde_json::to_value(&message).unwrap();
            spoofed[field] = serde_json::json!("forged");
            assert!(GuestControl::decode(&serde_json::to_vec(&spoofed).unwrap()).is_err());
        }
    }
    let pull = HostControl::ResponsePull {
        version: VERSION,
        invocation: invocation.clone(),
        sequence: 0,
    };
    assert_eq!(HostControl::decode(&pull.encode().unwrap()).unwrap(), pull);
    for data in [vec![], vec![0; MAX_CHUNK_BYTES + 1]] {
        assert!(GuestControl::ResponseChunk {
            version: VERSION,
            invocation: invocation.clone(),
            sequence: 0,
            data
        }
        .encode()
        .is_err());
    }
    assert!(GuestControl::ResponseEnd {
        version: VERSION,
        invocation: invocation.clone(),
        sequence: 0,
        error: Some(String::new())
    }
    .encode()
    .is_err());
    assert!(GuestControl::ResponseEnd {
        version: VERSION,
        invocation,
        sequence: 0,
        error: Some("x".repeat(4097))
    }
    .encode()
    .is_err());
    // Older adapters omit this capability and still decode as buffered.
    let old = br#"{"event":"ready","version":1,"adapter":"javascript","execution_turns":true,"quiescent_terminals":true,"cleanup_turns":true,"callback_turns":true,"response_turns":true}"#;
    assert!(matches!(
        GuestControl::decode(old).unwrap(),
        GuestControl::Ready {
            memory_diagnostics: false,
            idle_lifecycle: false,
            response_streams: false,
            ..
        }
    ));
}
use super::*;

#[test]
fn request_streams_and_brokered_fetch_frames() {
    let invocation = "a".repeat(64);
    let fetch = "f".repeat(64);
    let request = HttpRequest {
        method: "POST".into(),
        url: "https://example.test/upload".into(),
        headers: vec![("x-repeat".into(), "one".into())],
        body: vec![],
    };
    let mut invoke = InvokeHttp {
        version: VERSION,
        invocation: invocation.clone(),
        storage_grant: "b".repeat(64),
        storage_residency: None,
        object: None,
        request: HttpRequest {
            method: "POST".into(),
            url: "https://example.test/upload".into(),
            headers: vec![],
            body: vec![],
        },
        request_stream: true,
        rpc: None,
        queue: false,
        service: None,
        alarm: None,
        socket: None,
    };
    let encoded: serde_json::Value = serde_json::from_slice(&invoke.encode().unwrap()).unwrap();
    assert_eq!(encoded["request_stream"], true);
    invoke.request.body = vec![1];
    assert!(
        invoke.encode().is_err(),
        "a streamed request also carried a body"
    );
    for message in [
        HostControl::RequestChunk {
            version: VERSION,
            invocation: invocation.clone(),
            sequence: 0,
            data: vec![7; MAX_CHUNK_BYTES],
        },
        HostControl::RequestEnd {
            version: VERSION,
            invocation: invocation.clone(),
            sequence: 1,
            error: None,
        },
        HostControl::FetchHead {
            version: VERSION,
            invocation: invocation.clone(),
            fetch: fetch.clone(),
            status: 200,
            headers: vec![
                ("set-cookie".into(), "a".into()),
                ("set-cookie".into(), "b".into()),
            ],
            socket: None,
        },
        HostControl::FetchChunk {
            version: VERSION,
            invocation: invocation.clone(),
            fetch: fetch.clone(),
            sequence: 0,
            data: vec![1],
        },
        HostControl::FetchEnd {
            version: VERSION,
            invocation: invocation.clone(),
            fetch: fetch.clone(),
            sequence: 0,
            error: Some("refused".into()),
        },
    ] {
        assert_eq!(
            HostControl::decode(&message.encode().unwrap()).unwrap(),
            message
        );
    }
    let call = |class: &str, rpc: Option<&str>| GuestControl::FetchStart {
        version: VERSION,
        invocation: invocation.clone(),
        fetch: fetch.clone(),
        request: HttpRequest {
            method: "POST".into(),
            url: "http://object.invalid/".into(),
            headers: vec![],
            body: vec![1, 2],
        },
        redirect: "manual".into(),
        target: Some(Box::new(ObjectCall {
            object: ObjectTarget {
                class: class.into(),
                id: "d".repeat(64),
                name: None,
            },
            rpc: rpc.map(Into::into),
        })),
        service: None,
        resource: None,
        request_stream: false,
        request_bytes: None,
        dial: None,
    };
    for message in [
        GuestControl::RequestPull {
            version: VERSION,
            invocation: invocation.clone(),
            sequence: 0,
        },
        GuestControl::FetchStart {
            version: VERSION,
            invocation: invocation.clone(),
            fetch: fetch.clone(),
            request,
            redirect: "manual".into(),
            target: None,
            service: None,
            resource: None,
            request_stream: false,
            request_bytes: None,
            dial: None,
        },
        call("Counter", None),
        call("Counter", Some("increment")),
        GuestControl::FetchPull {
            version: VERSION,
            invocation: invocation.clone(),
            fetch: fetch.clone(),
            sequence: 0,
        },
        GuestControl::FetchCancel {
            version: VERSION,
            invocation: invocation.clone(),
            fetch: fetch.clone(),
        },
    ] {
        assert!(message.is_event_io());
        assert_eq!(
            GuestControl::decode(&message.encode().unwrap()).unwrap(),
            message
        );
        // A guest names neither the network client nor host authority.
        for field in ["scope", "write_position", "proof", "client", "proxy"] {
            let mut spoofed = serde_json::to_value(&message).unwrap();
            spoofed[field] = serde_json::json!("forged");
            assert!(GuestControl::decode(&serde_json::to_vec(&spoofed).unwrap()).is_err());
        }
    }
    // An object call is a bounded target, never an unvalidated route.
    for refused in [
        call("other:Counter", None),
        call("../Counter", None),
        call("", None),
        call("Counter", Some("")),
        call("Counter", Some(&"m".repeat(1025))),
    ] {
        assert!(refused.encode().is_err());
    }
    for field in ["resource", "grant", "node"] {
        let mut spoofed = serde_json::to_value(call("Counter", None)).unwrap();
        spoofed["target"][field] = serde_json::json!("forged");
        assert!(GuestControl::decode(&serde_json::to_vec(&spoofed).unwrap()).is_err());
    }
    for data in [vec![], vec![0; MAX_CHUNK_BYTES + 1]] {
        assert!(HostControl::RequestChunk {
            version: VERSION,
            invocation: invocation.clone(),
            sequence: 0,
            data: data.clone(),
        }
        .encode()
        .is_err());
        assert!(HostControl::FetchChunk {
            version: VERSION,
            invocation: invocation.clone(),
            fetch: fetch.clone(),
            sequence: 0,
            data,
        }
        .encode()
        .is_err());
    }
    for (url, redirect, fetch) in [
        ("file:///etc/passwd", "follow", fetch.clone()),
        ("https://example.test/", "proxy", fetch.clone()),
        ("https://example.test/", "follow", "bad".to_owned()),
        ("https://exa mple.test/", "follow", fetch.clone()),
    ] {
        assert!(GuestControl::FetchStart {
            version: VERSION,
            invocation: invocation.clone(),
            fetch,
            request: HttpRequest {
                method: "GET".into(),
                url: url.into(),
                headers: vec![],
                body: vec![],
            },
            redirect: redirect.into(),
            target: None,
            service: None,
            resource: None,
            request_stream: false,
            request_bytes: None,
            dial: None,
        }
        .encode()
        .is_err());
    }
    assert!(HostControl::FetchHead {
        version: VERSION,
        invocation: invocation.clone(),
        fetch: fetch.clone(),
        status: 99,
        headers: vec![],
        socket: None,
    }
    .encode()
    .is_err());
    assert!(HostControl::RequestEnd {
        version: VERSION,
        invocation,
        sequence: 0,
        error: Some(String::new()),
    }
    .encode()
    .is_err());
}

#[test]
fn http_boundary_retains_bytes_and_rejects_guest_authority() {
    let mut input = InvokeHttp {
        request_stream: false,
        rpc: None,
        queue: false,
        service: None,
        alarm: None,
        socket: None,
        version: VERSION,
        invocation: "a".repeat(64),
        storage_grant: "b".repeat(64),
        storage_residency: None,
        object: None,
        request: HttpRequest {
            method: "POST".into(),
            url: "https://example.test/counter".into(),
            headers: vec![
                ("x-repeat".into(), "one".into()),
                ("x-repeat".into(), "two".into()),
            ],
            body: vec![0, 255, 128],
        },
    };
    assert_eq!(
        serde_json::from_slice::<InvokeHttp>(&input.encode().unwrap()).unwrap(),
        input
    );
    input.storage_residency = Some("invalid".into());
    assert!(input.encode().is_err());
    input.storage_residency = Some("c".repeat(64));
    assert_eq!(
        serde_json::from_slice::<InvokeHttp>(&input.encode().unwrap()).unwrap(),
        input
    );
    input.object = Some(ObjectTarget {
        class: "Counter".into(),
        id: "d".repeat(64),
        name: Some("named-object".into()),
    });
    assert_eq!(
        serde_json::from_slice::<InvokeHttp>(&input.encode().unwrap()).unwrap(),
        input
    );
    for class in ["", ".", "..", "other:Counter", "../Counter", "λ"] {
        input.object.as_mut().unwrap().class = class.into();
        assert!(input.encode().is_err());
    }
    input.object.as_mut().unwrap().class = "Counter".into();
    // An RPC needs an object; large arguments may stream.
    input.rpc = Some("increment".into());
    assert_eq!(
        serde_json::from_slice::<InvokeHttp>(&input.encode().unwrap()).unwrap(),
        input
    );
    input.request_stream = true;
    input.request.body.clear();
    input.encode().unwrap();
    input.request_stream = false;
    let object = input.object.take();
    assert!(input.encode().is_err());
    input.object = object;
    input.rpc = None;
    input.storage_residency = None;
    assert!(input.encode().is_err());
    input.storage_residency = Some("c".repeat(64));
    // A queue batch is stateless: it never names an object, residency or RPC.
    let object = input.object.take();
    input.queue = true;
    assert!(input.encode().is_err());
    input.storage_residency = None;
    input.request_stream = true;
    input.request.body.clear();
    assert_eq!(
        serde_json::from_slice::<InvokeHttp>(&input.encode().unwrap()).unwrap(),
        input
    );
    input.rpc = Some("increment".into());
    assert!(input.encode().is_err());
    input.rpc = None;
    input.queue = false;
    input.request_stream = false;
    input.request.body = vec![0, 255, 128];
    input.object = object;
    input.storage_residency = Some("c".repeat(64));
    let output = HttpResult {
        version: VERSION,
        invocation: input.invocation.clone(),
        response: HttpResponse {
            status: 200,
            headers: input.request.headers.clone(),
            body: input.request.body.clone(),
        },
        socket: None,
    };
    let bytes = serde_json::to_vec(&output).unwrap();
    assert_eq!(
        HttpResult::decode(&bytes, &input.invocation).unwrap(),
        output
    );
    assert!(HttpResult::decode(&bytes, &"c".repeat(64)).is_err());
    let control = GuestControl::HttpResult(serde_json::from_slice(&bytes).unwrap());
    assert_eq!(
        GuestControl::decode(&control.encode().unwrap()).unwrap(),
        control
    );
    let mut injected = serde_json::to_value(control).unwrap();
    injected["response"]["proof"] = serde_json::json!(true);
    assert!(GuestControl::decode(&serde_json::to_vec(&injected).unwrap()).is_err());
    // Drop the unknown field before testing the `event` tag: `HttpReply` carries
    // the same `HttpResult`, so `http_reply` is accepted, and re-asserting on the
    // poisoned value would pass for the wrong reason. An unrecognized tag fails,
    // and the recorded tag still decodes to the variant it names.
    injected["response"]
        .as_object_mut()
        .unwrap()
        .remove("proof");
    injected["event"] = serde_json::json!("bogus");
    assert!(GuestControl::decode(&serde_json::to_vec(&injected).unwrap()).is_err());
    injected["event"] = serde_json::json!("http_result");
    assert_eq!(
        GuestControl::decode(&serde_json::to_vec(&injected).unwrap()).unwrap(),
        control
    );
    assert!(GuestControl::decode(br#"{"event":"drained","version":1,"invocation":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","write_position":1}"#).is_err());
    let mut malicious = serde_json::to_value(output).unwrap();
    malicious["response"]["write_position"] = serde_json::json!(123);
    assert!(
        HttpResult::decode(&serde_json::to_vec(&malicious).unwrap(), &input.invocation).is_err()
    );
    malicious["response"]
        .as_object_mut()
        .unwrap()
        .remove("write_position");
    malicious["response"]["headers"] = serde_json::json!([["x-test", "a\r\nb"]]);
    assert!(
        HttpResult::decode(&serde_json::to_vec(&malicious).unwrap(), &input.invocation).is_err()
    );
    let invoke = HostControl::InvokeHttp(Box::new(input));
    assert_eq!(
        HostControl::decode(&invoke.encode().unwrap()).unwrap(),
        invoke
    );
    let retirement = HostControl::RetireObject {
        version: VERSION,
        invocation: "a".repeat(64),
        storage_grant: "b".repeat(64),
        storage_residency: "c".repeat(64),
        object: ObjectTarget {
            class: "Counter".into(),
            id: "d".repeat(64),
            name: Some("named".into()),
        },
    };
    assert_eq!(
        HostControl::decode(&retirement.encode().unwrap()).unwrap(),
        retirement
    );
    for field in ["scope", "request", "epoch"] {
        let mut injected = serde_json::to_value(&retirement).unwrap();
        injected[field] = serde_json::json!("other");
        assert!(HostControl::decode(&serde_json::to_vec(&injected).unwrap()).is_err());
    }
    for message in [
        GuestControl::Ready {
            version: VERSION,
            adapter: crate::Language::Javascript,
            execution_turns: true,
            quiescent_terminals: true,
            cleanup_turns: true,
            callback_turns: true,
            response_turns: true,
            reply_drain: false,
            resident_objects: false,
            object_retirement: false,
            memory_diagnostics: false,
            idle_lifecycle: false,
            response_streams: false,
        },
        GuestControl::HttpReply(HttpResult {
            version: VERSION,
            invocation: "a".repeat(64),
            response: HttpResponse {
                status: 200,
                headers: vec![],
                body: vec![0, 255],
            },
            socket: None,
        }),
        GuestControl::Drained {
            version: VERSION,
            invocation: "a".repeat(64),
        },
        GuestControl::ObjectRetired {
            version: VERSION,
            invocation: "a".repeat(64),
        },
        GuestControl::Cancelled {
            version: VERSION,
            invocation: "a".repeat(64),
        },
        GuestControl::Failed {
            version: VERSION,
            invocation: "a".repeat(64),
            message: "handler stopped".into(),
        },
        GuestControl::Condemn {
            version: VERSION,
            reason: "interpreter died".into(),
        },
    ] {
        assert_eq!(
            GuestControl::decode(&message.encode().unwrap()).unwrap(),
            message
        );
    }
    for reason in [String::new(), "x".repeat(4097)] {
        let condemn = GuestControl::Condemn {
            version: VERSION,
            reason,
        };
        assert!(condemn.encode().is_err());
    }
    assert!(HostControl::decode(br#"{"event":"shutdown","version":1,"scope":"other"}"#).is_err());
    assert!(
        GuestControl::decode(br#"{"event":"cancelled","version":1,"invocation":"bad"}"#).is_err()
    );
    assert!(
        GuestControl::decode(br#"{"event":"ready","version":2,"adapter":"javascript"}"#).is_err()
    );
    let hello = StorageConnect {
        version: VERSION,
        invocation: "a".repeat(64),
        grant: "b".repeat(64),
        cleanup: false,
        facet: false,
    };
    assert_eq!(
        StorageConnect::decode(&hello.encode().unwrap()).unwrap(),
        hello
    );
    let mut malicious = serde_json::to_value(hello).unwrap();
    malicious["scope"] = serde_json::json!("other");
    assert!(StorageConnect::decode(&serde_json::to_vec(&malicious).unwrap()).is_err());
}

#[test]
fn service_signal_frames_are_scoped_and_bounded() {
    use super::*;
    let id = "a".repeat(32);
    let call = |signals: Vec<String>, rpc: Option<Vec<String>>| ServiceInvocation {
        entrypoint: None,
        props: vec![],
        rpc,
        get: false,
        signals,
        signal_owner: None,
    };
    let path = Some(vec!["accept".to_owned()]);
    assert!(call(vec![id.clone()], path.clone()).validate().is_ok());
    assert!(
        call(vec![id.clone()], None).validate().is_err(),
        "signal on a fetch"
    );
    assert!(call(vec!["A".repeat(32)], path.clone()).validate().is_err());
    assert!(call(vec!["a".repeat(64)], path.clone()).validate().is_err());
    assert!(call(vec![id.clone(); 33], path.clone()).validate().is_err());
    // Only the host names a signal owner: a guest FetchStart carrying one is
    // refused, while the host's InvokeHttp may.
    let mut owned = call(vec![id.clone()], path);
    owned.signal_owner = Some("c".repeat(64));
    assert!(owned.validate().is_ok());
    owned.signal_owner = Some("c".repeat(32));
    assert!(owned.validate().is_err());
    owned.signal_owner = Some("c".repeat(64));
    let start = GuestControl::FetchStart {
        version: VERSION,
        invocation: "a".repeat(64),
        fetch: "b".repeat(64),
        request: HttpRequest {
            method: "POST".into(),
            url: "https://service.invalid/".into(),
            headers: vec![],
            body: vec![],
        },
        redirect: "manual".into(),
        target: None,
        service: Some(Box::new(ServiceCall {
            binding: "SVC".to_owned().try_into().unwrap(),
            invocation: owned,
        })),
        resource: None,
        request_stream: false,
        request_bytes: None,
        dial: None,
    };
    assert!(start.encode().is_err(), "guest named a signal owner");
    let abort = GuestControl::FetchSignal {
        version: VERSION,
        invocation: "b".repeat(64),
        fetch: "c".repeat(64),
        signal: id.clone(),
        reason: vec![0, 255],
    };
    assert_eq!(
        GuestControl::decode(&abort.encode().unwrap()).unwrap(),
        abort
    );
    assert!(abort.is_event_io());
    let relay = HostControl::SignalAbort {
        version: VERSION,
        invocation: "b".repeat(64),
        signal: id.clone(),
        reason: vec![1],
    };
    assert_eq!(
        HostControl::decode(&relay.encode().unwrap()).unwrap(),
        relay
    );
    let oversized = HostControl::SignalAbort {
        version: VERSION,
        invocation: "b".repeat(64),
        signal: id,
        reason: vec![0; MAX_SIGNAL_REASON_BYTES + 1],
    };
    assert!(oversized.encode().is_err());
}

#[test]
fn socket_frames_events_and_upgrades_are_scoped_and_bounded() {
    use super::*;
    let invoke = |socket: Option<SocketInvocation>, body: Vec<u8>| InvokeHttp {
        version: VERSION,
        invocation: "a".repeat(64),
        storage_grant: "b".repeat(64),
        storage_residency: Some("c".repeat(64)),
        object: Some(ObjectTarget {
            class: "Room".into(),
            id: "d".repeat(64),
            name: None,
        }),
        request: HttpRequest {
            method: "POST".into(),
            url: "http://celld.internal/socket".into(),
            headers: vec![],
            body,
        },
        request_stream: false,
        rpc: None,
        queue: false,
        service: None,
        alarm: None,
        socket,
    };
    let text = SocketInvocation {
        ws: 7,
        event: SocketEvent::Text,
    };
    let mut message = invoke(Some(text.clone()), b"hello".to_vec());
    assert!(message.encode().is_ok());
    let closing = SocketInvocation {
        ws: 7,
        event: SocketEvent::Close {
            code: 1001,
            reason: "bye".into(),
            was_clean: true,
        },
    };
    assert!(invoke(Some(closing.clone()), vec![]).encode().is_ok());
    assert!(
        invoke(Some(closing), b"x".to_vec()).encode().is_err(),
        "a close carries no message body"
    );
    // A socket event is exclusive: no alarm, RPC, queue, stream or service,
    // and only an object with a residency can own a socket.
    message.alarm = Some(AlarmInvocation {
        scheduled_ms: 1,
        retry: 0,
    });
    assert!(message.encode().is_err());
    message.alarm = None;
    message.rpc = Some("method".into());
    assert!(message.encode().is_err());
    message.rpc = None;
    message.object = None;
    assert!(message.encode().is_err());

    let frames = [
        SocketFrame::Text {
            data: "text".into(),
        },
        SocketFrame::Binary {
            data: vec![0, 255, 128],
        },
        SocketFrame::Close {
            code: 1000,
            reason: "done".into(),
        },
    ];
    for frame in frames {
        let sent = GuestControl::SocketFrame {
            version: VERSION,
            invocation: "a".repeat(64),
            ws: 9,
            frame,
            fetch: None,
        };
        assert_eq!(GuestControl::decode(&sent.encode().unwrap()).unwrap(), sent);
        assert!(sent.is_event_io());
        assert_eq!(sent.invocation(), Some("a".repeat(64).as_str()));
    }
    let oversized = GuestControl::SocketFrame {
        version: VERSION,
        invocation: "a".repeat(64),
        ws: 9,
        frame: SocketFrame::Binary {
            data: vec![0; MAX_BODY_BYTES + 1],
        },
        fetch: None,
    };
    assert!(oversized.encode().is_err(), "frame above the message bound");
    let long_reason = GuestControl::SocketFrame {
        version: VERSION,
        invocation: "a".repeat(64),
        ws: 9,
        frame: SocketFrame::Close {
            code: 1000,
            reason: "x".repeat(124),
        },
        fetch: None,
    };
    assert!(long_reason.encode().is_err());
    let mut forged = serde_json::to_value(GuestControl::SocketFrame {
        version: VERSION,
        invocation: "a".repeat(64),
        ws: 9,
        frame: SocketFrame::Text { data: "x".into() },
        fetch: None,
    })
    .unwrap();
    forged["node"] = serde_json::json!("other");
    assert!(GuestControl::decode(&serde_json::to_vec(&forged).unwrap()).is_err());

    for count in [0, SOCKET_WINDOW as u32 + 1] {
        let credit = HostControl::SocketCredit {
            version: VERSION,
            invocation: "a".repeat(64),
            count,
        };
        assert!(
            credit.encode().is_err(),
            "credit {count} outside the window"
        );
    }
    let credit = HostControl::SocketCredit {
        version: VERSION,
        invocation: "a".repeat(64),
        count: SOCKET_WINDOW as u32,
    };
    assert_eq!(
        HostControl::decode(&credit.encode().unwrap()).unwrap(),
        credit
    );

    // A 101 exists only as a socket upgrade, on the reply and the fetch head.
    let upgrade = |status: u16, socket: Option<u64>, body: Vec<u8>| HttpResult {
        version: VERSION,
        invocation: "a".repeat(64),
        response: HttpResponse {
            status,
            headers: vec![],
            body,
        },
        socket,
    };
    assert!(upgrade(101, Some(5), vec![]).validate().is_ok());
    assert!(upgrade(101, None, vec![]).validate().is_err());
    assert!(upgrade(200, Some(5), vec![]).validate().is_err());
    assert!(upgrade(101, Some(5), vec![1]).validate().is_err());
    let head = |status: u16, socket: Option<u64>| HostControl::FetchHead {
        version: VERSION,
        invocation: "a".repeat(64),
        fetch: "b".repeat(64),
        status,
        headers: vec![],
        socket,
    };
    assert!(head(101, Some(5)).encode().is_ok());
    assert!(head(200, Some(5)).encode().is_err());
}

#[test]
fn websocket_dials_are_bodiless_exclusive_and_name_their_fetch() {
    use super::*;
    let start =
        |dial: Option<SocketDial>, method: &str, body: Vec<u8>, target: Option<ObjectCall>| {
            GuestControl::FetchStart {
                version: VERSION,
                invocation: "a".repeat(64),
                fetch: "b".repeat(64),
                request: HttpRequest {
                    method: method.into(),
                    url: "https://peer.invalid/socket".into(),
                    headers: vec![("sec-websocket-protocol".into(), "chat".into())],
                    body,
                },
                redirect: "manual".into(),
                target: target.map(Box::new),
                service: None,
                resource: None,
                request_stream: false,
                request_bytes: None,
                dial,
            }
        };
    let dial = SocketDial {
        protocols: vec!["chat".into(), "v2.json".into()],
        response: true,
    };
    let sent = start(Some(dial.clone()), "GET", vec![], None);
    assert_eq!(GuestControl::decode(&sent.encode().unwrap()).unwrap(), sent);
    assert!(start(Some(dial.clone()), "POST", vec![], None)
        .encode()
        .is_err());
    assert!(start(Some(dial.clone()), "GET", vec![1], None)
        .encode()
        .is_err());
    let object = ObjectCall {
        object: ObjectTarget {
            class: "Room".into(),
            id: "c".repeat(64),
            name: None,
        },
        rpc: None,
    };
    assert!(start(Some(dial.clone()), "GET", vec![], Some(object))
        .encode()
        .is_err());
    let spaced = SocketDial {
        protocols: vec!["not a token".into()],
        response: false,
    };
    assert!(start(Some(spaced), "GET", vec![], None).encode().is_err());
    let frame = |fetch: Option<String>| GuestControl::SocketFrame {
        version: VERSION,
        invocation: "a".repeat(64),
        ws: 0,
        fetch,
        frame: SocketFrame::Text { data: "x".into() },
    };
    assert!(frame(Some("b".repeat(64))).encode().is_ok());
    assert!(frame(Some("not a handle".into())).encode().is_err());
}
