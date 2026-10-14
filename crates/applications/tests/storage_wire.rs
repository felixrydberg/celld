use celld_applications::storage_wire::*;
use serde_json::json;

#[test]
fn storage_wire_preserves_native_types_and_rejects_forged_context_and_frames() {
    let request = Request {
        version: VERSION,
        grant: "a".repeat(64),
        operation: Operation::SqlStart {
            query: "SELECT ?, ?, ?, ?, ?".into(),
            bindings: vec![
                SqlValue::Integer { value: i64::MAX },
                SqlValue::Real {
                    bits: f64::INFINITY.to_bits(),
                },
                SqlValue::Real {
                    bits: (-0.0f64).to_bits(),
                },
                SqlValue::Blob {
                    value: vec![0, 255],
                },
                SqlValue::Null {},
            ],
        },
    };
    let bytes = request.encode().unwrap();
    assert_eq!(Request::decode(&bytes).unwrap(), request);
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        value["operation"]["bindings"][0]["value"],
        i64::MAX.to_string()
    );
    assert_eq!(
        value["operation"]["bindings"][1]["bits"],
        "7ff0000000000000"
    );
    assert_eq!(value["operation"]["bindings"][3]["value"], "AP8=");
    for bad in [
        json!({"type":"integer","value":9223372036854775807i64}),
        json!({"type":"integer","value":"01"}),
        json!({"type":"real","bits":"nan"}),
        json!({"type":"blob","value":"!!!!"}),
        json!({"type":"null","scope":"other"}),
    ] {
        assert!(serde_json::from_value::<SqlValue>(bad).is_err());
    }
    let mut forged = value.clone();
    for field in ["application", "tenant"] {
        forged[field] = json!({"app":"other"});
        assert!(Request::decode(&serde_json::to_vec(&forged).unwrap()).is_err());
        forged = value.clone();
    }
    forged = value.clone();
    forged["version"] = json!(VERSION + 1);
    assert!(Request::decode(&serde_json::to_vec(&forged).unwrap()).is_err());
    forged["version"] = json!(VERSION);
    forged["operation"] = json!({"op":"get_alarm","scope":"other"});
    assert!(Request::decode(&serde_json::to_vec(&forged).unwrap()).is_err());
    forged["operation"] = json!({"op":"turn_finish","turn":"bad"});
    assert!(Request::decode(&serde_json::to_vec(&forged).unwrap()).is_err());
    forged["operation"] = json!({"op":"turn_finish","turn":"b".repeat(64)});
    let turn = Request::decode(&serde_json::to_vec(&forged).unwrap()).unwrap();
    assert!(matches!(turn.operation, Operation::TurnFinish { .. }));
    for bad in [
        json!({"op":"critical_section_finish","critical_section":"bad","failed":true}),
        json!({"op":"critical_section_finish","critical_section":"b".repeat(64),"failed":"true"}),
        json!({"op":"critical_section_start","scope":"other"}),
        json!({"op":"critical_section_start","origin":"bad"}),
        json!({"op":"critical_section_start","origin":null,"scope":"other"}),
        json!({"op":"transaction_start","origin":"bad"}),
        json!({"op":"transaction_start","scope":"other"}),
    ] {
        forged["operation"] = bad;
        assert!(Request::decode(&serde_json::to_vec(&forged).unwrap()).is_err());
    }
    for (operation, expected) in [
        (
            json!({"op":"critical_section_start"}),
            Operation::CriticalSectionStart { origin: None },
        ),
        (
            json!({"op":"critical_section_start","origin":"c".repeat(64)}),
            Operation::CriticalSectionStart {
                origin: Some("c".repeat(64)),
            },
        ),
        (
            json!({"op":"transaction_start"}),
            Operation::TransactionStart { origin: None },
        ),
        (
            json!({"op":"transaction_start","origin":"c".repeat(64)}),
            Operation::TransactionStart {
                origin: Some("c".repeat(64)),
            },
        ),
    ] {
        forged["operation"] = operation.clone();
        let start = Request::decode(&serde_json::to_vec(&forged).unwrap()).unwrap();
        assert_eq!(start.operation, expected);
        let encoded: serde_json::Value = serde_json::from_slice(&start.encode().unwrap()).unwrap();
        assert_eq!(encoded["operation"], operation);
    }
    forged["operation"] =
        json!({"op":"critical_section_finish","critical_section":"b".repeat(64),"failed":true});
    let critical = Request::decode(&serde_json::to_vec(&forged).unwrap()).unwrap();
    assert_eq!(
        Request::decode(&critical.encode().unwrap()).unwrap(),
        critical
    );
    for bad in [
        json!({"op":"abort_object","reason":"a".repeat(4097)}),
        json!({"op":"abort_object","reason":"é".repeat(2049)}),
        json!({"op":"abort_object","reason":null}),
        json!({"op":"abort_object","reason":"stop","scope":"other"}),
        json!({"op":"abort_object","reason":"stop","epoch":7}),
    ] {
        forged["operation"] = bad;
        assert!(Request::decode(&serde_json::to_vec(&forged).unwrap()).is_err());
    }
    forged["operation"] = json!({"op":"abort_object","reason":"é".repeat(2048)});
    let abort = Request::decode(&serde_json::to_vec(&forged).unwrap()).unwrap();
    assert_eq!(Request::decode(&abort.encode().unwrap()).unwrap(), abort);
    for operation in [
        json!({"op":"in_execution_turn","turn":"bad","operation":{"op":"get_alarm"}}),
        json!({"op":"in_execution_turn","turn":"b".repeat(64),"operation":{"op":"transaction_start"}}),
        json!({"op":"in_execution_turn","turn":"b".repeat(64),"operation":{"op":"critical_section_start"}}),
        json!({"op":"in_execution_turn","turn":"b".repeat(64),"operation":{"op":"turn_finish","turn":"b".repeat(64)}}),
        json!({"op":"in_execution_turn","turn":"b".repeat(64),"operation":{"op":"abort_object","reason":"stop"}}),
        json!({"op":"in_execution_turn","turn":"b".repeat(64),"operation":{"op":"in_execution_turn","turn":"b".repeat(64),"operation":{"op":"get_alarm"}}}),
        json!({"op":"in_execution_turn","turn":"b".repeat(64),"operation":{"op":"sql_next","cursor":"bad"}}),
    ] {
        forged["operation"] = operation;
        assert!(Request::decode(&serde_json::to_vec(&forged).unwrap()).is_err());
    }
    forged["operation"] =
        json!({"op":"in_execution_turn","turn":"b".repeat(64),"operation":{"op":"get_alarm"}});
    let borrowed = Request::decode(&serde_json::to_vec(&forged).unwrap()).unwrap();
    assert_eq!(
        Request::decode(&borrowed.encode().unwrap()).unwrap(),
        borrowed
    );
    let reply = Reply {
        version: VERSION,
        outcome: Outcome::Ok {
            value: Value::Entries {
                entries: vec![
                    (
                        "json".into(),
                        StoredValue::Json {
                            value: "null".into(),
                        },
                    ),
                    (
                        "clone".into(),
                        StoredValue::V8 {
                            value: vec![255, 15, 1],
                        },
                    ),
                ],
            },
        },
    };
    assert_eq!(Reply::decode(&reply.encode().unwrap()).unwrap(), reply);
    let mut frames = Vec::new();
    write_frame(&mut frames, &bytes).unwrap();
    write_frame(&mut frames, &reply.encode().unwrap()).unwrap();
    let mut frames = frames.as_slice();
    assert_eq!(read_frame(&mut frames).unwrap().unwrap(), bytes);
    assert_eq!(
        Reply::decode(&read_frame(&mut frames).unwrap().unwrap()).unwrap(),
        reply
    );
    assert!(read_frame(&mut frames).unwrap().is_none());
    for frame in [
        vec![0, 0],
        vec![0, 0, 0, 2, b'{'],
        ((MAX_MESSAGE_BYTES + 1) as u32).to_be_bytes().to_vec(),
        vec![0, 0, 0, 0],
    ] {
        assert!(read_frame(&mut frame.as_slice()).is_err());
    }
}

#[test]
fn socket_registry_operations_are_bounded_and_keep_bytes() {
    let request = |operation| Request {
        version: VERSION,
        grant: "a".repeat(64),
        operation,
    };
    let accept = request(Operation::SocketAccept {
        ws: 3,
        accepted: false,
        tags: vec!["room".into(), "user:7".into()],
    });
    assert_eq!(Request::decode(&accept.encode().unwrap()).unwrap(), accept);
    assert!(Operation::SocketAccept {
        ws: 3,
        accepted: true,
        tags: vec![]
    }
    .is_storage_call());
    assert!(request(Operation::SocketAccept {
        ws: 3,
        accepted: false,
        tags: vec!["t".into(); MAX_SOCKET_TAGS + 1],
    })
    .encode()
    .is_err());
    assert!(request(Operation::SocketList {
        tag: Some("x".repeat(MAX_SOCKET_TAG_BYTES + 1)),
    })
    .encode()
    .is_err());
    let attachment = request(Operation::SocketAttachment {
        ws: 3,
        attachment: vec![0, 255, 1],
    });
    assert_eq!(Request::decode(&attachment.encode().unwrap()).unwrap(), attachment);
    assert!(request(Operation::SocketAttachment {
        ws: 3,
        attachment: vec![0; MAX_SOCKET_ATTACHMENT_BYTES + 1],
    })
    .encode()
    .is_err());
    assert!(request(Operation::SocketAutoResponse {
        pair: Some(("ping".into(), "x".repeat(MAX_SOCKET_AUTO_RESPONSE_BYTES + 1))),
    })
    .encode()
    .is_err());
    let unset = request(Operation::SocketAutoResponse { pair: None });
    assert_eq!(Request::decode(&unset.encode().unwrap()).unwrap(), unset);

    let rows = Reply {
        version: VERSION,
        outcome: Outcome::Ok {
            value: Value::Sockets {
                sockets: vec![
                    SocketRow {
                        ws: 3,
                        tags: vec!["room".into()],
                        attachment: Some(vec![0, 255]),
                    },
                    SocketRow {
                        ws: 4,
                        tags: vec![],
                        attachment: None,
                    },
                ],
            },
        },
    };
    assert_eq!(Reply::decode(&rows.encode().unwrap()).unwrap(), rows);
    let json: serde_json::Value = serde_json::from_slice(&rows.encode().unwrap()).unwrap();
    assert_eq!(
        json["outcome"]["value"]["sockets"][0]["attachment"],
        "AP8=",
        "attachment bytes cross as base64, not a number array"
    );
}
