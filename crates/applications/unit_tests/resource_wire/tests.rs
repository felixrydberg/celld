use super::*;

#[test]
fn container_operations_are_bounded_and_strict() {
    let call = |operation| ResourceCall {
        binding: "BOX".to_owned().try_into().unwrap(),
        operation: ResourceOperation::Container { operation },
    };
    let start = |run, entrypoint: Option<Vec<String>>| ContainerOperation::Start {
        run,
        entrypoint,
        env: vec![("A".into(), "b".into())],
        enable_internet: false,
        labels: vec![],
    };
    let wire = serde_json::to_vec(&call(start(1, Some(vec!["sh".into()])))).unwrap();
    let decoded: ResourceCall = serde_json::from_slice(&wire).unwrap();
    assert!(decoded.validate().is_ok());
    assert!(call(start(0, None)).validate().is_err(), "no run");
    assert!(call(start(1, Some(vec!["x".into(); 1025])))
        .validate()
        .is_err());
    assert!(call(ContainerOperation::Monitor { run: 0 })
        .validate()
        .is_err());
    assert!(call(ContainerOperation::Signal { signal: 0 })
        .validate()
        .is_err());
    assert!(call(ContainerOperation::ExecKill { id: 1, signal: 65 })
        .validate()
        .is_err());
    assert!(call(ContainerOperation::ExecRead { id: 1, which: 3 })
        .validate()
        .is_err());
    let exec = |cmd: Vec<String>, env: Vec<(String, String)>| {
        call(ContainerOperation::Exec {
            cmd,
            env,
            cwd: None,
            user: None,
            combined: false,
        })
    };
    assert!(exec(vec![], vec![]).validate().is_err(), "empty argv");
    assert!(exec(vec!["ls".into()], vec![("".into(), "v".into())])
        .validate()
        .is_err());
    assert!(
        exec(vec!["ls".into()], vec![("A".into(), "x".repeat(2 << 20))])
            .validate()
            .is_err()
    );
    assert!(exec(vec!["ls".into()], vec![]).validate().is_ok());
    assert!(call(ContainerOperation::PortFetch { port: 0 })
        .validate()
        .is_err());
    assert!(call(ContainerOperation::PortConnect { port: 9000 })
        .validate()
        .is_ok());
    let tls = |host: &str| {
        call(ContainerOperation::TcpStartTls {
            id: 1,
            host: host.into(),
        })
    };
    assert!(tls("").validate().is_err() && tls("host").validate().is_ok());
    // Guest-supplied authority has no field to ride in.
    let forged = br#"{"binding":"BOX","operation":{"kind":"container","operation":{"op":"destroy","image":"x"}}}"#;
    assert!(serde_json::from_slice::<ResourceCall>(forged).is_err());
}
