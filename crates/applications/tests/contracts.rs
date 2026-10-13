use celld_applications::{
    ApplicationId, BindingTarget, Deployment, Name, ResourceAddress, ResourceKind, ResourceRef,
    RouteId, RouteTable,
};

fn name(value: &str) -> Name {
    value.to_owned().try_into().unwrap()
}

fn deployment(app: &str) -> Deployment {
    serde_json::from_value(serde_json::json!({
        "manifest_version": 1,
        "application": { "app": app },
        "id": "v1",
        "handlers": [{
            "id": "main", "language": "javascript", "entrypoint": "index.js", "http": true,
            "artifact_sha256": "a".repeat(64), "runtime_image_sha256": "b".repeat(64),
            "bindings": { "DB": { "kind": "resource", "resource": { "kind": "d1", "id": "db" } } },
            "worker_config": { "compatibility_flags": ["js_rpc"], "durable_objects": {
                "bindings": [{ "name": "COUNTER", "class_name": "Counter" }]
            } }
        }]
    }))
    .unwrap()
}

#[test]
fn identities_cannot_escape_the_application_root() {
    for invalid in [
        "", "..", "../other", "a/b", "a\\b", "a:b", "a%2fb", "a\0b", "å",
    ] {
        assert!(
            Name::try_from(invalid.to_owned()).is_err(),
            "accepted {invalid:?}"
        );
        assert!(serde_json::from_value::<Name>(serde_json::json!(invalid)).is_err());
    }
    assert!(Name::try_from("a".repeat(129)).is_err());
    let first = ApplicationId { app: name("one") };
    let second = ApplicationId { app: name("two") };
    assert_eq!(first.storage_root(), "applications/v2/apps/one/");
    assert_ne!(first.storage_root(), second.storage_root());
    for invalid in [
        serde_json::json!({"tenant":"one","app":"one"}),
        serde_json::json!({"app":"one","root":"../../"}),
        serde_json::json!({"tenant":"one"}),
    ] {
        assert!(serde_json::from_value::<ApplicationId>(invalid).is_err());
    }
    assert_eq!(
        serde_json::to_value(&first).unwrap(),
        serde_json::json!({"app":"one"})
    );
}

#[test]
fn resource_identity_survives_redeployment_without_cross_application_aliases() {
    let first = deployment("one");
    let resource = ResourceRef {
        kind: ResourceKind::DurableObject,
        id: name("counter"),
    };
    let address = ResourceAddress {
        application: first.application.clone(),
        resource: resource.clone(),
    };
    let scope = address.cell_scope("Counter", "room:with/slashes").unwrap();
    // Fixed original two-round HMAC vector, independently calculated from
    // the stable resource namespace. Changing it renames persistent objects.
    assert_eq!(
        scope,
        "Counter:f04d8d08ce66b4833ea7a4f3da9d7198bb9af9925be20d3163a9c49e838dc7f1"
    );
    assert!(celld_logic::cell::valid_cell_scope(&scope));
    // The same address rebuilt from the same inputs derives the same id, so a
    // redeployment cannot rename a persistent object.
    let reloaded = address.clone();
    assert_eq!(
        address.cell_scope("Counter", "room:with/slashes").unwrap(),
        reloaded.cell_scope("Counter", "room:with/slashes").unwrap()
    );
    for changed in [
        ResourceAddress {
            application: deployment("two").application,
            resource: address.resource.clone(),
        },
        ResourceAddress {
            application: address.application.clone(),
            resource: ResourceRef {
                kind: ResourceKind::DurableObject,
                id: name("sibling"),
            },
        },
        ResourceAddress {
            application: address.application.clone(),
            resource: ResourceRef {
                kind: ResourceKind::Kv,
                id: name("counter"),
            },
        },
    ] {
        assert_ne!(address.storage_root(), changed.storage_root());
        assert_ne!(
            scope,
            changed.cell_scope("Counter", "room:with/slashes").unwrap()
        );
    }
    assert!(address.cell_scope("../Counter", "room").is_err());
    assert!(address.cell_scope("Counter:other", "room").is_err());
    assert!(address.cell_scope(&"a".repeat(200), "room").is_err());
}

#[test]
fn object_ids_authenticate_under_one_resource_namespace() {
    use celld_applications::invocation_wire::ObjectTarget;
    let address = |app: &str, id: &str| ResourceAddress {
        application: ApplicationId { app: name(app) },
        resource: ResourceRef {
            kind: ResourceKind::DurableObject,
            id: name(id),
        },
    };
    let resource = address("one", "counter");
    let target = |resource: &ResourceAddress, class: &str, instance: &str| ObjectTarget {
        class: class.into(),
        id: resource.cell_scope(class, instance).unwrap()[class.len() + 1..].into(),
        name: Some(instance.into()),
    };
    let object = target(&resource, "Counter", "room");
    assert!(resource.owns(&object));
    // A unique id carries no name; it authenticates the same way.
    let unique = ObjectTarget {
        name: None,
        ..object.clone()
    };
    assert!(resource.owns(&unique));
    // The same name in another application, resource or class derives
    // a different id, which none of the others accept.
    for other in [
        address("two", "counter"),
        address("another", "counter"),
        address("one", "sibling"),
    ] {
        assert!(!other.owns(&object) && !resource.owns(&target(&other, "Counter", "room")));
    }
    assert!(!resource.owns(&ObjectTarget {
        class: "Other".into(),
        ..object.clone()
    }));
    assert!(!resource.owns(&ObjectTarget {
        name: Some("lobby".into()),
        ..object.clone()
    }));
    let mut forged = object.id.clone().into_bytes();
    forged[40] = if forged[40] == b'0' { b'1' } else { b'0' };
    assert!(!resource.owns(&ObjectTarget {
        id: String::from_utf8(forged).unwrap(),
        ..unique
    }));
    assert!(!resource.owns(&ObjectTarget {
        id: object.id.to_uppercase(),
        ..object
    }));
}

#[test]
fn publication_rejects_invalid_targets_and_retains_original_worker_configuration() {
    let original = deployment("one");
    original.validate().unwrap();
    let roundtrip: Deployment =
        serde_json::from_slice(&serde_json::to_vec(&original).unwrap()).unwrap();
    assert_eq!(roundtrip, original);
    let mut unsupported = serde_json::to_value(&original).unwrap();
    unsupported["handlers"][0]["language"] = serde_json::json!("python");
    assert!(serde_json::from_value::<Deployment>(unsupported).is_err());
    let mut javascript = original.clone();
    javascript.handlers[0].entrypoint = "handlers/main.mjs".into();
    javascript.validate().unwrap();
    for path in [
        "../main.mjs",
        "/main.mjs",
        "a/../main.mjs",
        "C:/main.mjs",
        "a\\main.mjs",
    ] {
        javascript.handlers[0].entrypoint = path.into();
        assert!(javascript.validate().is_err(), "accepted {path}");
    }
    let mut bad;
    // Tenant-chosen hostnames are gone; an old manifest is refused by name.
    let old: Deployment = serde_json::from_value(serde_json::json!({
        "manifest_version": 1, "application": original.application, "id": "v1",
        "handlers": original.handlers,
        "http_routes": [{ "hostname": "example.com", "path_prefix": "/", "handler": "main" }]
    }))
    .unwrap();
    assert!(old.validate().unwrap_err().to_string().contains("http_routes is no longer supported"));
    let mut handler = serde_json::to_value(&original.handlers[0]).unwrap();
    handler["route_id"] = "0".repeat(32).into();
    assert!(serde_json::from_value::<celld_applications::Handler>(handler).is_err());
    bad = original.clone();
    bad.handlers.push(original.handlers[0].clone());
    assert!(bad.validate().is_err());
    bad = original.clone();
    bad.handlers[0].bindings.insert(
        name("SERVICE"),
        BindingTarget::Handler {
            handler: name("missing"),
        },
    );
    assert!(bad.validate().is_err());
    bad = original.clone();
    bad.handlers[0]
        .environment
        .insert(name("DB"), "shadow".into());
    assert!(bad.validate().is_err());
    bad = original;
    bad.triggers = serde_json::from_value(serde_json::json!([
        { "kind": "cron", "id": "nightly", "expression": "bad cron", "handler": "main" }
    ]))
    .unwrap();
    assert!(bad.validate().is_err());
}

#[test]
fn route_ids_are_unique_unguessable_and_never_application_selected() {
    let ids: std::collections::BTreeSet<_> =
        (0..256).map(|_| RouteId::generate().unwrap()).collect();
    assert_eq!(ids.len(), 256);
    for id in &ids {
        assert!(id.as_str().len() == 32 && celld_applications::valid_hostname(id.as_str()));
        assert_eq!(RouteId::try_from(id.as_str().to_owned()).unwrap(), *id);
    }
    // Only the host's own format parses, so a manifest or API body cannot
    // carry a chosen label, a hostname or an uppercase or short lookalike.
    for invalid in ["", "example.com", "0".repeat(31).as_str(), "0".repeat(33).as_str(), "A".repeat(32).as_str(), "g".repeat(32).as_str()] {
        assert!(RouteId::try_from(invalid.to_owned()).is_err(), "accepted {invalid:?}");
    }
}

#[test]
fn routes_never_reach_another_applications_handler() {
    let host = |id: &RouteId| format!("{}.apps.test", id.as_str());
    let first = deployment("one");
    let second = deployment("two");
    let ids = |id: &RouteId| std::collections::BTreeMap::from([(name("main"), id.clone())]);
    let (one, two) = (RouteId::generate().unwrap(), RouteId::generate().unwrap());
    let routes =
        RouteTable::from_deployments([(&first, &ids(&one), None), (&second, &ids(&two), None)]).unwrap();
    // The same handler name and manifest in two applications: each id selects only
    // its own application, whatever the case or port of the Host.
    assert_eq!(routes.resolve(&host(&one), "apps.test").unwrap().application, first.application);
    assert_eq!(routes.resolve(&host(&two), "apps.test").unwrap().application, second.application);
    assert_eq!(
        routes
            .resolve(&host(&one).to_ascii_uppercase(), "apps.test")
            .map(|target| &target.application),
        Some(&first.application)
    );
    assert!(routes.resolve(&format!("{}:8443", host(&two)), "apps.test").is_some());
    // No other host, label, domain or port reaches a handler.
    let other = RouteId::generate().unwrap();
    for hostname in [
        host(&other),
        format!("{}.example.test", one.as_str()),
        format!("{}.evil.apps.test", one.as_str()),
        format!("x.{}", host(&one)),
        format!("{}:0", host(&one)),
        format!("{}:garbage", host(&one)),
        "apps.test".into(),
        ".apps.test".into(),
        "example.com".into(),
    ] {
        assert!(routes.resolve(&hostname, "apps.test").is_none(), "{hostname}");
    }
    // An id can be assigned to one handler only, and a served handler needs one.
    assert!(RouteTable::from_deployments([(&first, &ids(&one), None), (&second, &ids(&one), None)]).is_err());
    assert!(RouteTable::from_deployments([(&first, &Default::default(), None)]).is_err());
    // A replacement keeps the id; a handler that is not `http` is not served;
    // another application's replacement with the first's ids cannot take its place.
    let mut replacement = first.clone();
    replacement.id = name("v2");
    let routes = RouteTable::from_deployments([(&replacement, &ids(&one), None), (&second, &ids(&two), None)]).unwrap();
    assert_eq!(routes.resolve(&host(&one), "apps.test").unwrap().deployment, name("v2"));
    replacement.handlers[0].http = false;
    let routes = RouteTable::from_deployments([(&replacement, &ids(&one), None), (&second, &ids(&two), None)]).unwrap();
    assert!(routes.resolve(&host(&one), "apps.test").is_none());
    assert_eq!(routes.resolve(&host(&two), "apps.test").unwrap().application, second.application);
}

/// Names may hold `-` and `_`, so these pairs flatten to one spelling; none may
/// share a storage root, a cell id or an object's ownership.
#[test]
fn application_and_resource_boundary_never_aliases_for_lookalike_names() {
    use celld_applications::invocation_wire::ObjectTarget;
    let pairs = [
        ("a-b", "c"),
        ("a", "b-c"),
        ("a_b", "c"),
        ("a", "b_c"),
        ("ab", "c"),
        ("a", "bc"),
        ("a-b-c", "c"),
        ("a", "b-c-c"),
    ];
    let addresses: Vec<_> = pairs
        .iter()
        .map(|(app, resource)| ResourceAddress {
            application: ApplicationId { app: name(app) },
            resource: ResourceRef {
                kind: ResourceKind::DurableObject,
                id: name(resource),
            },
        })
        .collect();
    let object = |address: &ResourceAddress| {
        let scope = address.cell_scope("Counter", "room").unwrap();
        ObjectTarget {
            class: "Counter".into(),
            id: scope.split_once(':').unwrap().1.into(),
            name: Some("room".into()),
        }
    };
    for (index, address) in addresses.iter().enumerate() {
        for (other, foreign) in addresses.iter().enumerate() {
            assert_eq!(
                index == other,
                address.storage_root() == foreign.storage_root(),
                "{pairs:?}"
            );
            assert_eq!(index == other, object(address).id == object(foreign).id);
            assert_eq!(index == other, foreign.owns(&object(address)));
        }
    }
}

#[test]
fn static_deployments_need_no_handler_but_cannot_execute_triggers() {
    let mut manifest = serde_json::to_value(deployment("one")).unwrap();
    manifest.as_object_mut().unwrap().remove("handlers");
    manifest["assets"] = serde_json::json!({"index":"assets.json", "sha256":"a".repeat(64), "file_count":1, "total_bytes":5});
    manifest["triggers"] = serde_json::json!([]);
    let static_only: Deployment = serde_json::from_value(manifest.clone()).unwrap();
    static_only.validate().unwrap();
    let id = RouteId::generate().unwrap();
    let routes = RouteTable::from_deployments([(&static_only, &Default::default(), Some(&id))]).unwrap();
    assert!(routes.resolve(&format!("{}.apps.test", id.as_str()), "apps.test").unwrap().handler().is_err());
    for trigger in [
        serde_json::json!({"kind":"cron", "id":"tick", "expression":"* * * * *", "handler":"main"}),
        serde_json::json!({"kind":"queue", "queue":"jobs", "handler":"main"}),
    ] {
        let mut invalid = manifest.clone(); invalid["triggers"] = serde_json::json!([trigger]);
        assert!(serde_json::from_value::<Deployment>(invalid).unwrap().validate().is_err());
    }
    manifest.as_object_mut().unwrap().remove("assets");
    assert!(serde_json::from_value::<Deployment>(manifest).unwrap().validate().is_err());
}
