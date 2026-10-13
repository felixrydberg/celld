use super::*;
pub(super) fn app(name: &str) -> ApplicationId {
    ApplicationId {
        app: name.to_owned().try_into().unwrap(),
    }
}
pub(super) fn address(
    application: &ApplicationId,
    kind: ResourceKind,
    id: &str,
) -> ResourceAddress {
    ResourceAddress {
        application: application.clone(),
        resource: celld_applications::ResourceRef {
            kind,
            id: id.to_owned().try_into().unwrap(),
        },
    }
}
#[tokio::test]
async fn effects_are_joined_and_close_on_join() {
    let effects = Effects::default();
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = done.clone();
    // The caller walks away; the effect still ends before join returns.
    drop(effects.spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        flag.store(true, std::sync::atomic::Ordering::SeqCst);
    }));
    effects.join().await;
    assert!(
        done.load(std::sync::atomic::Ordering::SeqCst),
        "join returned early"
    );
    assert!(
        effects.spawn(async {}).is_err(),
        "a settled event took a new effect"
    );
}

#[test]
fn registered_container_spec_and_object_authority_are_exact() {
    let application = app("one");
    let foreign = app("two");
    let container = address(&application, ResourceKind::Container, "image");
    let objects = address(&application, ResourceKind::DurableObject, "objects");
    let image = format!("celld-image:{}", "a".repeat(64));
    let configuration = serde_json::json!({"image":image,"instance_type":"dev","max_instances":2});
    let declaration = serde_json::json!({"binding":"BOX","class_name":"Counter"});
    let mut handler: Handler = serde_json::from_value(serde_json::json!({
            "id":"worker","language":"javascript","entrypoint":"main.mjs",
            "artifact_sha256":"a".repeat(64),"runtime_image_sha256":"b".repeat(64),
            "bindings":{"BOX":{"kind":"resource","resource":container.resource},"OBJECTS":{"kind":"resource","resource":objects.resource}},
            "worker_config":{"do_classes":["Counter"],"containers":[declaration]}
        })).unwrap();
    let mut registered = vec![crate::applications::Resource {
        resource: container.resource.clone(),
        configuration: configuration.clone(),
    }];
    let namespaces = vec![(objects.clone(), "Counter".into())];
    let pinned = bindings(&application, &handler, &registered, &namespaces)
        .unwrap()
        .remove(0);
    assert_eq!(pinned.address, container);
    assert_eq!(
        pinned.spec.class_name,
        container.storage_root(),
        "quota class escaped registered resource"
    );
    // The guest and host namespace resolver accepts the bound Container
    // only with its declaration.
    let resolve = crate::application_javascript_guest::object_namespaces;
    assert!(resolve(&application, &handler).is_ok());
    let mut undeclared = handler.clone();
    undeclared.worker_config["containers"] = serde_json::json!([]);
    assert!(resolve(&application, &undeclared).is_err());
    let scope = objects.cell_scope("Counter", "same").unwrap();
    let object = celld_applications::invocation_wire::ObjectTarget {
        class: "Counter".into(),
        id: scope.split_once(':').unwrap().1.into(),
        name: Some("same".into()),
    };
    assert!(pinned.authorizes(&application, &object));
    assert!(!pinned.authorizes(&foreign, &object));
    let other = address(&foreign, ResourceKind::DurableObject, "objects");
    let mut cross = object.clone();
    cross.id = other
        .cell_scope("Counter", "same")
        .unwrap()
        .split_once(':')
        .unwrap()
        .1
        .into();
    assert!(!pinned.authorizes(&application, &cross));
    let mut forged = object.clone();
    forged.id = "0".repeat(64);
    assert!(!pinned.authorizes(&application, &forged));
    forged = object.clone();
    forged.class = "Another".into();
    assert!(!pinned.authorizes(&application, &forged));
    assert!(bindings(
        &application,
        &handler,
        &registered,
        &[(other, "Counter".into())]
    )
    .is_err());
    assert!(bindings(&application, &handler, &[], &namespaces).is_err());
    assert_eq!(pinned.spec.image, image);
    assert_eq!(pinned.spec.max_instances, Some(2));
    // Only the registered configuration is trusted, and it is strict.
    for (field, value) in [
        ("image", serde_json::json!("ubuntu:latest")),
        ("max_instances", 0.into()),
        ("instance_type", "unlimited".into()),
        ("runtime", "".into()),
        ("privileged", true.into()),
    ] {
        registered[0].configuration = configuration.clone();
        registered[0].configuration[field] = value;
        assert!(
            bindings(&application, &handler, &registered, &namespaces).is_err(),
            "{field}"
        );
    }
    registered[0].configuration = serde_json::json!({});
    assert!(bindings(&application, &handler, &registered, &namespaces).is_err());
    registered[0].configuration = configuration.clone();
    assert!(bindings(&application, &handler, &registered, &[]).is_err());
    let clean = handler.clone();
    handler.worker_config["containers"][0]["binding"] = "FOREIGN".into();
    assert!(declarations(&handler).is_err());
    handler = clean.clone();
    handler.worker_config["containers"] =
        serde_json::json!([declaration.clone(), declaration.clone()]);
    assert!(declarations(&handler).is_err());
    handler = clean.clone();
    handler.worker_config["containers"][0]["class_name"] = "__Workflow.legacy".into();
    assert!(declarations(&handler).is_err());
    // A declaration carries no privileged field and names a declared class.
    handler = clean.clone();
    handler.worker_config["containers"][0]["image"] = image.clone().into();
    assert!(declarations(&handler).is_err());
    handler = clean.clone();
    handler.worker_config["do_classes"] = serde_json::json!([]);
    assert!(declarations(&handler).is_err());
    handler = clean;
    handler.worker_config["containers"] = serde_json::json!([]);
    assert!(declarations(&handler).is_err());
}

#[test]
fn synchronous_container_start_is_event_scoped_and_cancelled() {
    use crate::storage::{self, Cells};
    use celld_applications::storage_wire::{Operation, Outcome, Request, Value, VERSION};
    let directory = tempfile::tempdir().unwrap();
    let cells = Cells::default();
    let _installed = cells.install();
    let scope = format!("Counter:{}", "a".repeat(64));
    storage::open_at_epoch(
        &scope,
        directory.path().join("one.sqlite").to_str().unwrap(),
        7,
        false,
        None,
        false,
    )
    .unwrap();
    let spec = Arc::new(crate::container::ContainerSpec {
        class_name: "scoped-resource".into(),
        image: format!("celld-image:{}", "a".repeat(64)),
        instance_type: None,
        max_instances: None,
        runtime: None,
    });
    let cell = crate::container::CellContainer::test_pending(&scope, spec.clone());
    let wrong = crate::container::CellContainer::test_pending("Counter:another", spec);
    let mut session =
        crate::application_storage::StorageSession::new(&cells, scope.clone(), 7).unwrap();
    let call = |session: &mut crate::application_storage::StorageSession, operation| {
        session
            .execute(
                &cells,
                true,
                Request {
                    version: VERSION,
                    grant: session.grant().into(),
                    operation,
                },
            )
            .outcome
    };
    assert!(matches!(
        call(&mut session, Operation::ContainerState {}),
        Outcome::Error { .. }
    ));
    assert!(
        session.bind_container(wrong).is_err(),
        "native object scope was not authenticated"
    );
    session.bind_container(cell.clone()).unwrap();
    assert!(session.bind_container(cell.clone()).is_err());
    assert!(matches!(
        call(&mut session, Operation::ContainerState {}),
        Outcome::Ok {
            value: Value::ContainerState {
                running: false,
                run: 0
            }
        }
    ));
    let denied = session.execute(
        &cells,
        false,
        Request {
            version: VERSION,
            grant: session.grant().into(),
            operation: Operation::ContainerBeginRun {},
        },
    );
    assert!(matches!(denied.outcome, Outcome::Error { .. }));
    assert!(!cell.running());
    let forged = session.execute(
        &cells,
        true,
        Request {
            version: VERSION,
            grant: "0".repeat(64),
            operation: Operation::ContainerBeginRun {},
        },
    );
    assert!(matches!(forged.outcome, Outcome::Error { .. }));
    assert!(!cell.running());
    assert!(matches!(
        call(&mut session, Operation::ContainerBeginRun {}),
        Outcome::Ok {
            value: Value::ContainerState {
                running: true,
                run: 1
            }
        }
    ));
    assert!(matches!(
        call(&mut session, Operation::ContainerState {}),
        Outcome::Ok {
            value: Value::ContainerState {
                running: true,
                run: 1
            }
        }
    ));
    assert!(matches!(
        call(&mut session, Operation::ContainerBeginRun {}),
        Outcome::Error { .. }
    ));
    session.revoke(&cells).unwrap();
    assert!(
        !cell.running(),
        "cancelled queued start orphaned the running flag"
    );
    assert!(matches!(
        call(&mut session, Operation::ContainerBeginRun {}),
        Outcome::Error { .. }
    ));
    let mut successor =
        crate::application_storage::StorageSession::new(&cells, scope.clone(), 7).unwrap();
    successor.bind_container(cell.clone()).unwrap();
    assert!(matches!(
        call(&mut successor, Operation::ContainerBeginRun {}),
        Outcome::Ok {
            value: Value::ContainerState {
                running: true,
                run: 2
            }
        }
    ));
    cell.cancel_pending_run(1);
    session.revoke(&cells).unwrap();
    assert!(
        cell.running(),
        "stale cancellation closed the successor's run"
    );
    successor.revoke(&cells).unwrap();
    assert!(!cell.running());
    // A taken start belongs to its effect: revoke leaves it running.
    let mut taker =
        crate::application_storage::StorageSession::new(&cells, scope.clone(), 7).unwrap();
    assert!(taker.container().is_err() && taker.take_container_run(3).is_err());
    taker.bind_container(cell.clone()).unwrap();
    assert!(matches!(
        call(&mut taker, Operation::ContainerBeginRun {}),
        Outcome::Ok {
            value: Value::ContainerState {
                running: true,
                run: 3
            }
        }
    ));
    assert!(taker.take_container_run(2).is_err());
    assert!(Arc::ptr_eq(&taker.take_container_run(3).unwrap(), &cell));
    assert!(taker.take_container_run(3).is_err());
    taker.revoke(&cells).unwrap();
    assert!(cell.running(), "revoke closed a start its effect owns");
    cell.cancel_pending_run(3);
    storage::close(&scope);
}
