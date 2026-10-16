use super::*;

#[cfg(test)]
fn pending_cron_arms(
    crons: &HashMap<String, Arc<CronEntry>>,
    armed: &mut HashMap<String, CronSchedule>,
) -> Vec<(String, CronSchedule)> {
    // A removed trigger forgets its record so a later one arms afresh.
    armed.retain(|scope, _| {
        crons
            .get(scope)
            .is_some_and(|entry| entry.schedule.is_some())
    });
    crons
        .iter()
        .filter_map(|(scope, entry)| {
            let schedule = entry.schedule.clone()?;
            (armed.get(scope) != Some(&schedule)).then(|| (scope.clone(), schedule))
        })
        .collect()
}

#[cfg(test)]
#[test]
fn scope_cache_bounds_bytes_expiration_and_live_pins() {
    let mut cache = ScopeCache {
        max_entries: 3,
        max_bytes: 20,
        ..Default::default()
    };
    let address = ResourceAddress {
        application: ApplicationId {
            app: "cache".to_owned().try_into().unwrap(),
        },
        resource: ResourceRef {
            kind: ResourceKind::Kv,
            id: "cache".to_owned().try_into().unwrap(),
        },
    };
    let pinned = Arc::new(ScopeMetadata {
        scope: "cell-0".into(),
        address,
        compat: Default::default(),
        queue: None,
        cron: None,
        object: None,
    });
    for index in 0..100 {
        cache.insert(format!("cell-{index}"), 6, pinned.clone());
        assert!(cache.entries.len() <= 3 && cache.bytes <= 20);
    }
    assert!(cache.get("cell-99").is_some());
    assert!(cache.get("cell-0").is_none());
    assert_eq!(
        pinned.scope, "cell-0",
        "eviction must not revoke a live event pin"
    );
    cache.insert("oversized".into(), 21, pinned.clone());
    assert!(cache.get("oversized").is_none());
    for (at, _, _) in cache.entries.values_mut() {
        *at -= RECONCILE_PERIOD;
    }
    assert!(cache.get("cell-99").is_none());
    assert_eq!(cache.bytes, 0);
}

#[cfg(test)]
#[test]
fn metadata_cache_limits_are_validated() {
    let config: VmHostConfig = serde_json::from_value(serde_json::json!({
        "jailer":"/opt/celld/jailer", "firecracker":"/opt/celld/firecracker",
        "kernel":{"path":"/opt/celld/vmlinux", "sha256":"a".repeat(64)},
        "runtimes":[{"path":"/opt/celld/runtime.img", "sha256":"b".repeat(64)}],
        "uid":1000,"gid":1000,"max_vms":2,"max_memory_mib":12288,
        "idle_ttl_secs":1,"platform_domain":"apps.test"
    }))
    .unwrap();
    config.validate().unwrap();
    for (entries, bytes) in [(0, 1), (4097, 1), (1, 0), (1, 64 * 1024 * 1024 + 1)] {
        let mut invalid = config.clone();
        invalid.metadata_cache_entries = Some(entries);
        invalid.metadata_cache_bytes = Some(bytes);
        assert!(invalid.validate().is_err());
    }
}

#[cfg(test)]
pub(crate) async fn check_inactive_cron_candidates_keep_ownership_without_arming() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(ApplicationStore::new(
        crate::dev::open_local_bucket(&directory.path().join("bucket.sqlite")).unwrap(),
    ));
    let application = ApplicationId {
        app: "dormant-cron".to_owned().try_into().unwrap(),
    };
    let resource = ResourceRef {
        kind: ResourceKind::Cron,
        id: "tick".to_owned().try_into().unwrap(),
    };
    let mut snapshot = store.read(&application).await.unwrap();
    snapshot.create(application.clone()).unwrap();
    snapshot
        .register_resource(
            &application,
            crate::applications::Resource {
                resource: resource.clone(),
                configuration: serde_json::json!({}),
            },
        )
        .unwrap();
    store.commit(snapshot).await.unwrap();
    let scope = ResourceAddress {
        application,
        resource,
    }
    .cell_scope(celld_logic::cron::RESERVED_CLASS, "tick")
    .unwrap();
    assert_eq!(
        store
            .cron_candidates(1, None)
            .await
            .unwrap()
            .candidates
            .len(),
        1
    );
    let config = serde_json::from_value(serde_json::json!({
        "jailer":"/unavailable/jailer", "firecracker":"/unavailable/firecracker",
        "kernel":{"path":"/unavailable/kernel", "sha256":"a".repeat(64)},
        "runtimes":[{"path":"/unavailable/runtime", "sha256":"b".repeat(64)}],
        "uid":1000,"gid":1000,"max_vms":2,"max_memory_mib":12288,
        "idle_ttl_secs":1,"platform_domain":"apps.test"
    }))
    .unwrap();
    let dispatcher = RouteDispatcher::new(config, store, "cron-test".into())
        .await
        .unwrap();
    assert!(dispatcher.owns_storage_cell(&scope).await.unwrap());
    assert!(dispatcher
        .cron_entry(&scope)
        .await
        .unwrap()
        .unwrap()
        .schedule
        .is_none());
    assert!(!dispatcher.cron_candidate_ready(&scope).await.unwrap());
    assert_eq!(
        dispatcher.routes.pool.as_ref().unwrap().snapshot(),
        VmPoolSnapshot::default()
    );
    assert!(dispatcher
        .routes
        .pool
        .as_ref()
        .unwrap()
        .state
        .lock()
        .unwrap()
        .slots
        .is_empty());
    dispatcher.shutdown().await.unwrap();
}

#[cfg(test)]
#[test]
fn alarm_catalog_authenticates_scope_and_active_deployment() {
    let application = |tenant: &str| -> ApplicationId {
        serde_json::from_value(serde_json::json!({"app": tenant})).unwrap()
    };
    let resource = ResourceRef {
        kind: ResourceKind::DurableObject,
        id: "counter".to_owned().try_into().unwrap(),
    };
    let deployment = |application: &ApplicationId, id: &str| -> Deployment {
        serde_json::from_value(serde_json::json!({
            "manifest_version": 1, "application": application, "id": id,
            "handlers": [{"id": "main", "language": "javascript", "entrypoint": "main.mjs",
                "artifact_sha256": "a".repeat(64), "runtime_image_sha256": "b".repeat(64),
                "bindings": {"COUNTER": {"kind": "resource", "resource": resource}},
                "worker_config": {"do_classes": ["Counter"]}}],
        }))
        .unwrap()
    };
    let one = application("one");
    let two = application("two");
    let scope = |app: &ApplicationId| {
        ResourceAddress {
            application: app.clone(),
            resource: resource.clone(),
        }
        .cell_scope("Counter", "same-name")
        .unwrap()
    };
    let mut active = HashMap::from([(one.clone(), deployment(&one, "v1"))]);
    assert_eq!(
        alarm_target(&active, &scope(&one)).unwrap().0.application,
        one
    );
    assert!(
        alarm_target(&active, &scope(&two)).is_none(),
        "cross-application alarm admitted"
    );
    assert!(alarm_target(&active, &format!("Counter:{}", "f".repeat(64))).is_none());
    assert!(alarm_target(&active, &scope(&one).replacen("Counter:", "Other:", 1)).is_none());
    assert!(alarm_target(
        &active,
        &scope(&one).replacen("Counter:", "__Workflow.counter:", 1)
    )
    .is_none());
    active.insert(two.clone(), deployment(&two, "v2"));
    assert_eq!(
        alarm_target(&active, &scope(&two))
            .unwrap()
            .0
            .deployment
            .as_str(),
        "v2"
    );
    active.insert(one.clone(), deployment(&one, "replacement"));
    assert_eq!(
        alarm_target(&active, &scope(&one))
            .unwrap()
            .0
            .deployment
            .as_str(),
        "replacement"
    );
    active.remove(&one);
    assert!(
        alarm_target(&active, &scope(&one)).is_none(),
        "inactive deployment admitted"
    );
}

#[cfg(test)]
#[test]
fn alarm_catalog_authenticates_bound_workflow() {
    let application = |tenant: &str| -> ApplicationId {
        serde_json::from_value(serde_json::json!({"app": tenant})).unwrap()
    };
    let one = application("one");
    let two = application("two");
    let workflow = ResourceRef {
        kind: ResourceKind::Workflow,
        id: "flow".to_owned().try_into().unwrap(),
    };
    let class = crate::deploy::workflow_class(workflow.id.as_str());
    let scope = |app: &ApplicationId| {
        ResourceAddress {
            application: app.clone(),
            resource: workflow.clone(),
        }
        .cell_scope(&class, "flow/instance")
        .unwrap()
    };
    let deployment: Deployment = serde_json::from_value(serde_json::json!({
        "manifest_version": 1, "application": one, "id": "workflow",
        "handlers": [{"id": "main", "language": "javascript", "entrypoint": "main.mjs",
            "artifact_sha256": "a".repeat(64), "runtime_image_sha256": "b".repeat(64),
            "bindings": {"FLOW": {"kind": "resource", "resource": workflow}},
            "worker_config": {"workflows": [{"binding":"FLOW","name":"flow","class_name":"Flow"}]}}]
    }))
    .unwrap();
    let mut active = HashMap::from([(one.clone(), deployment)]);
    let (target, object) =
        alarm_target(&active, &scope(&one)).expect("bound workflow alarm was refused");
    assert_eq!(target.application, one);
    assert_eq!(target.deployment.as_str(), "workflow");
    assert!(
        object.name.is_none(),
        "native alarm must not need a guest-supplied instance name"
    );
    let address = ResourceAddress {
        application: one.clone(),
        resource: workflow.clone(),
    };
    assert!(
        native_namespace_owns_object(&address, &class, &object),
        "native Workflow activation refused"
    );
    assert!(
        !crate::application_javascript_guest::namespace_owns_object(&address, &class, &object),
        "guest Workflow call bypassed its instance name"
    );
    let foreign = ResourceAddress {
        application: two.clone(),
        resource: workflow.clone(),
    };
    assert!(
        !native_namespace_owns_object(&foreign, &class, &object),
        "foreign native Workflow activation admitted"
    );
    assert!(!native_namespace_owns_object(
        &address,
        "__Workflow.legacy",
        &object
    ));
    assert!(
        alarm_target(&active, &scope(&two)).is_none(),
        "cross-application workflow alarm admitted"
    );
    assert!(alarm_target(
        &active,
        &scope(&one).replacen(&format!("{class}:"), "__Workflow.legacy:", 1)
    )
    .is_none());
    assert!(alarm_target(&active, &format!("{class}:{}", "f".repeat(64))).is_none());
    active.get_mut(&one).unwrap().handlers[0].bindings.clear();
    assert!(
        alarm_target(&active, &scope(&one)).is_none(),
        "unbound reserved workflow alarm admitted"
    );
}

#[cfg(test)]
#[test]
fn catalog_authenticates_objects_for_guest_peer_and_storage_paths() {
    use celld_applications::invocation_wire::ObjectTarget;
    let application = |tenant: &str| -> ApplicationId {
        serde_json::from_value(serde_json::json!({"app": tenant})).unwrap()
    };
    let bindings = serde_json::json!({
        "COUNTER": {"kind": "resource", "resource": {"kind": "durable_object", "id": "counter"}},
        "DB": {"kind": "resource", "resource": {"kind": "d1", "id": "db"}},
        "CACHE": {"kind": "resource", "resource": {"kind": "kv", "id": "cache"}},
        "JOBS": {"kind": "resource", "resource": {"kind": "queue", "id": "jobs"}},
    });
    let deployment = |application: &ApplicationId,
                      id: &str,
                      bindings: &serde_json::Value,
                      classes: &[&str]|
     -> Deployment {
        serde_json::from_value(serde_json::json!({
            "manifest_version": 1, "application": application, "id": id,
            "handlers": [{"id": "main", "language": "javascript", "entrypoint": "main.mjs",
                "artifact_sha256": "a".repeat(64), "runtime_image_sha256": "b".repeat(64),
                "environment": {"WHO": application.app.as_str()},
                "bindings": bindings, "worker_config": {"do_classes": classes}}],
        }))
        .unwrap()
    };
    let (one, two) = (application("one"), application("two"));
    let address = |application: &ApplicationId, kind, id: &str| ResourceAddress {
        application: application.clone(),
        resource: ResourceRef {
            kind,
            id: id.to_owned().try_into().unwrap(),
        },
    };
    let (kv_name, queue_name) = (
        celld_logic::kv::cell_name("namespace", 0),
        celld_logic::queue::cell_name("jobs"),
    );
    // Each service's cell as its owner mints it: named, and the unique form (no
    // name) that a peer-forwarded call or the storage pool sees.
    let objects = |application: &ApplicationId| -> Vec<ObjectTarget> {
        [
            (
                address(application, ResourceKind::DurableObject, "counter"),
                "Counter",
                "room",
            ),
            (
                address(application, ResourceKind::D1, "db"),
                crate::deploy::D1_CLASS,
                "main",
            ),
            (
                address(application, ResourceKind::Kv, "cache"),
                celld_logic::kv::RESERVED_CLASS,
                kv_name.as_str(),
            ),
            (
                address(application, ResourceKind::Queue, "jobs"),
                celld_logic::queue::RESERVED_CLASS,
                queue_name,
            ),
        ]
        .into_iter()
        .map(|(resource, class, name)| ObjectTarget {
            class: class.into(),
            id: resource
                .cell_scope(class, name)
                .unwrap()
                .split_once(':')
                .unwrap()
                .1
                .into(),
            name: Some(name.into()),
        })
        .collect()
    };
    let unique = |object: &ObjectTarget| ObjectTarget {
        name: None,
        ..object.clone()
    };
    let cell = |object: &ObjectTarget| format!("{}:{}", object.class, object.id);
    let mut active = HashMap::from([
        (one.clone(), deployment(&one, "v1", &bindings, &["Counter"])),
        (two.clone(), deployment(&two, "v1", &bindings, &["Counter"])),
    ]);
    let handler = |active: &HashMap<ApplicationId, Deployment>, application: &ApplicationId| {
        active[application].handlers[0].clone()
    };
    let (theirs, mine) = (objects(&two), objects(&one));
    for (own, other) in mine.iter().zip(&theirs) {
        assert_ne!(
            own.id, other.id,
            "same names in two tenants share a cell id"
        );
        for form in [own.clone(), unique(own)] {
            // Guest and peer path: the application's own active handler, never the other's.
            assert!(
                handler_owns_object(&one, &handler(&active, &one), &form),
                "{form:?}"
            );
            assert!(
                !handler_owns_object(&two, &handler(&active, &two), &form),
                "forged peer application served {form:?}"
            );
            assert!(!handler_owns_object(
                &one,
                &handler(&active, &one),
                &unique(other)
            ));
            // Storage path: the catalog finds the owner by the cell alone.
            assert_eq!(
                storage_handler(&active, &cell(&form))
                    .unwrap()
                    .environment
                    .values()
                    .next()
                    .unwrap(),
                "one"
            );
        }
        // A forged id, class and name resolve to nothing.
        let mut forged = own.id.clone().into_bytes();
        forged[40] = if forged[40] == b'0' { b'1' } else { b'0' };
        for bad in [
            ObjectTarget {
                id: String::from_utf8(forged).unwrap(),
                ..unique(own)
            },
            ObjectTarget {
                id: "0".repeat(64),
                ..unique(own)
            },
            ObjectTarget {
                class: "Counter".into(),
                ..unique(&mine[1])
            },
        ] {
            assert!(
                !handler_owns_object(&one, &handler(&active, &one), &bad),
                "{bad:?}"
            );
            assert!(storage_handler(&active, &cell(&bad)).is_none(), "{bad:?}");
        }
        // The cell string carries no name, so only a call can name the wrong one.
        let renamed = ObjectTarget {
            name: Some("other".into()),
            ..own.clone()
        };
        assert!(
            !handler_owns_object(&one, &handler(&active, &one), &renamed),
            "{renamed:?}"
        );
    }
    assert!(storage_handler(&active, &format!("Counter:{}", "f".repeat(64))).is_none());
    assert!(storage_handler(&active, "Counter").is_none());
    // Replacement dropping a binding: its cells stop resolving at once, the other
    // tenant's keep resolving, and a replacement that keeps it adopts the same cells.
    let mut fewer = bindings.clone();
    fewer
        .as_object_mut()
        .unwrap()
        .retain(|alias, _| alias == "DB");
    active.insert(one.clone(), deployment(&one, "v2", &fewer, &[]));
    for (index, expect) in [false, true, false, false].into_iter().enumerate() {
        assert_eq!(
            storage_handler(&active, &cell(&mine[index])).is_some(),
            expect,
            "replaced {:?}",
            mine[index]
        );
        assert!(storage_handler(&active, &cell(&theirs[index])).is_some());
    }
    active.insert(one.clone(), deployment(&one, "v3", &bindings, &["Counter"]));
    assert!(storage_handler(&active, &cell(&mine[0])).is_some());
    // Deactivation: nothing of the application activates, nothing of the other changes.
    active.remove(&one);
    assert!(mine
        .iter()
        .all(|object| storage_handler(&active, &cell(object)).is_none()));
    assert!(storage_handler(&active, &cell(&theirs[0])).is_some());
    // A guest class named like the trusted cron cell is refused, so no object of
    // that class can start in the trusted pool.
    let cron = deployment(
        &one,
        "cron",
        &bindings,
        &[celld_logic::cron::RESERVED_CLASS],
    );
    assert!(
        crate::application_javascript_guest::object_namespaces(&one, &cron.handlers[0]).is_err()
    );
}

#[cfg(test)]
const NODE: &str = "node-pool";

#[cfg(test)]
pub(super) const TEST_DOMAIN: &str = "apps.test";

#[cfg(test)]
pub(super) async fn assigned_host(
    store: &ApplicationStore,
    application: &ApplicationId,
    handler: &str,
) -> String {
    let snapshot = store.read(application).await.unwrap();
    let id = &snapshot.application(application).unwrap().route_ids
        [&Name::try_from(handler.to_owned()).unwrap()];
    format!("{}.{TEST_DOMAIN}", id.as_str())
}

#[cfg(test)]
async fn check_vm_pool(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    directory: &Path,
    first: &ApplicationId,
    owned_vm_pids: fn() -> HashSet<u32>,
) {
    use celld_applications::invocation_wire::HttpRequest;
    let previous_active = store
        .read(first)
        .await
        .unwrap()
        .application(first)
        .unwrap()
        .active
        .clone();
    const SOURCE: &str = r#"import { env } from 'cloudflare:workers';
export default {
  async fetch(request) {
    if (new URL(request.url).pathname === '/slow') {
      await new Promise((resolve) => setTimeout(resolve, 400));
    }
    return new Response(env.POOL_LABEL);
  }
};
"#;
    let source = directory.join("pool-source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("main.mjs"), SOURCE).unwrap();
    let image = directory.join("pool.bundle");
    let built = timeout(
        Duration::from_secs(30),
        Command::new("/usr/bin/python3")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../applications/guest/build-application-image.py"),
            )
            .arg(&source)
            .arg(&image)
            .env_clear()
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let artifact = std::fs::read(image).unwrap();
    let digest = content_digest(&artifact);
    let other = ApplicationId {
        app: "pool-other".to_owned().try_into().unwrap(),
    };
    let mut snapshot = store.read(&other).await.unwrap();
    snapshot.create(other.clone()).unwrap();
    store.commit(snapshot).await.unwrap();
    let stage = |application: ApplicationId, id: &'static str, runtime: String| {
        let (artifact, digest) = (artifact.clone(), digest.clone());
        async move {
            store
                .upload_artifact(&application, &digest, artifact.into())
                .await
                .unwrap();
            let deployment: Deployment = serde_json::from_value(serde_json::json!({
                "manifest_version": 1,
                "application": application,
                "id": id,
                "handlers": [{
                    "id": "javascript",
                    "language": "javascript",
                    "entrypoint": "main.mjs",
                    "artifact_sha256": digest,
                    "runtime_image_sha256": runtime,
                    "environment": {"POOL_LABEL": id},
                    "secrets": {},
                    "bindings": {},
                    "worker_config": {"compatibility_date": "2026-03-01"}
                }]
            }))
            .unwrap();
            store.stage(&deployment).await.unwrap();
            let mut snapshot = store.read(&application).await.unwrap();
            store.activate(&mut snapshot, deployment).await.unwrap();
            store.commit(snapshot).await.unwrap();
        }
    };
    let approved = options.runtime.sha256.clone();
    for id in ["pool-a", "pool-b", "pool-c"] {
        stage(first.clone(), id, approved.clone()).await;
    }
    stage(other.clone(), "pool-other", approved.clone()).await;
    stage(first.clone(), "pool-unapproved", "e".repeat(64)).await;
    let name = |value: &str| -> Name { value.to_owned().try_into().unwrap() };
    let handler = name("javascript");
    let mut inactive = store
        .deployment(first, &name("pool-a"))
        .await
        .unwrap()
        .unwrap();
    inactive.id = name("pool-inactive");
    store.stage(&inactive).await.unwrap();
    let config = VmHostConfig {
        jailer: options.jailer.clone(),
        firecracker: options.firecracker.clone(),
        kernel: options.kernel.clone(),
        runtimes: vec![options.runtime.clone()],
        uid: options.uid,
        gid: options.gid,
        max_vms: 2,
        max_memory_mib: 12_288,
        idle_ttl_secs: 1,
        platform_domain: TEST_DOMAIN.into(),
        container_fence_image: None,
        artifact_cache_ttl_secs: None,
        metadata_cache_entries: None,
        metadata_cache_bytes: None,
        startup_pool: 0,
        memory_guard: None,
    };
    // Untrusted or unbounded configuration refuses before any pool exists.
    for broken in [
        VmHostConfig {
            jailer: "jailer".into(),
            ..config.clone()
        },
        VmHostConfig {
            runtimes: vec![options.runtime.clone(), options.runtime.clone()],
            ..config.clone()
        },
        VmHostConfig {
            runtimes: vec![],
            ..config.clone()
        },
        VmHostConfig {
            container_fence_image: Some("ubuntu:latest".into()),
            ..config.clone()
        },
        VmHostConfig {
            max_vms: 0,
            ..config.clone()
        },
        VmHostConfig {
            artifact_cache_ttl_secs: Some(0),
            metadata_cache_entries: None,
            metadata_cache_bytes: None,
            ..config.clone()
        },
        VmHostConfig {
            max_memory_mib: u64::from(options.memory_mib) - 1,
            ..config.clone()
        },
    ] {
        assert!(VmPool::new(broken, store.clone(), NODE.into()).is_err());
    }
    let text = |result: Result<crate::js::HttpResponse>| match result {
        Ok(response) => String::from_utf8(response.body).unwrap(),
        Err(error) => panic!("pooled handler failed: {error:#}"),
    };
    let call = |lease: &VmLease, path: &str| {
        let (client, app) = (lease.client.clone(), app.clone());
        let url = format!("http://pool{path}");
        async move {
            client
                .invoke_stateless(
                    &app,
                    Duration::from_secs(10),
                    HttpRequest {
                        method: "GET".into(),
                        url,
                        headers: vec![],
                        body: vec![],
                    },
                    None,
                )
                .await
        }
    };
    let settled = |pool: &VmPool, vms: u32| {
        let pool_snapshot = pool.snapshot();
        assert_eq!(pool_snapshot.quarantined, 0);
        pool_snapshot.vms == vms && owned_vm_pids().len() == vms as usize
    };
    async fn until(mut done: impl FnMut() -> bool, what: &str) {
        timeout(Duration::from_secs(15), async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what}"));
    }
    // What the pool observed for a deployment, as the management API reads it.
    let observed = |deployment: &str| {
        let deployment = name(deployment);
        async move {
            let status = store.status(first).await.unwrap().pop()?;
            let mut handlers = status.handlers.into_iter();
            handlers
                .find(|handler| handler.deployment == deployment)
                .map(|handler| handler.state)
        }
    };
    let failed = |reason: &str| {
        Some(HandlerState::Failed {
            reason: reason.into(),
        })
    };
    assert!(observed("pool-a").await.is_none(), "nothing launched yet");
    // Mismatched, symlinked and unapproved images refuse launch and release
    // their reservation without leaving a VMM.
    let mut mismatched = options.runtime.clone();
    mismatched.path = options.kernel.path.clone();
    let linked = directory.join("pool-runtime-link.ext4");
    std::os::unix::fs::symlink(&options.runtime.path, &linked).unwrap();
    for (runtime, refusal) in [
        (mismatched, "digest mismatch"),
        (
            ApprovedFile {
                path: linked,
                sha256: approved.clone(),
            },
            "",
        ),
    ] {
        let pool = VmPool::new(
            VmHostConfig {
                runtimes: vec![runtime],
                ..config.clone()
            },
            store.clone(),
            NODE.into(),
        )
        .unwrap();
        let error = pool
            .acquire_inner(app, first, &name("pool-a"), &handler, true)
            .await
            .err()
            .expect("an unapproved image launched");
        assert!(format!("{error:#}").contains(refusal), "{error:#}");
        assert_eq!(observed("pool-a").await, failed("launch failed"));
        assert!(settled(&pool, 0), "refused image kept capacity or a VMM");
        pool.shutdown().await.unwrap();
    }
    let pool = VmPool::new(config.clone(), store.clone(), NODE.into()).unwrap();
    let error = pool
        .acquire(app, first, &name("pool-unapproved"), &handler)
        .await
        .err()
        .unwrap();
    assert!(format!("{error:#}").contains("not approved"));
    assert_eq!(
        observed("pool-unapproved").await,
        failed("runtime image is not approved")
    );
    assert!(settled(&pool, 0));
    // Fresh admission refuses both a never-active manifest and a replaced
    // generation before reserving capacity. The capacity exercises below use
    // retained generations that the fixture activated above.
    for deployment in ["pool-inactive", "pool-a"] {
        assert!(pool
            .acquire(app, first, &name(deployment), &handler)
            .await
            .err()
            .unwrap()
            .is::<DeploymentRetiring>());
        assert!(settled(&pool, 0));
    }
    // Concurrent cold acquires share one startup; a later acquire is warm.
    let (pool_a, pool_c) = (name("pool-a"), name("pool-c"));
    let starts = futures_util::future::join_all(
        (0..4).map(|_| pool.acquire_inner(app, first, &pool_a, &handler, true)),
    )
    .await;
    let a: Vec<VmLease> = starts.into_iter().map(Result::unwrap).collect();
    assert!(a.iter().all(|lease| Arc::ptr_eq(&lease.pin, &a[0].pin)));
    let warm = pool
        .acquire_inner(app, first, &name("pool-a"), &handler, true)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&warm.pin, &a[0].pin));
    assert_eq!(text(call(&warm, "/").await), "pool-a");
    assert!(settled(&pool, 1));
    assert_eq!(observed("pool-a").await, Some(HandlerState::Serving));
    // Another deployment of the same application gets its own VM.
    let b = pool
        .acquire_inner(app, first, &name("pool-b"), &handler, true)
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&b.pin, &a[0].pin));
    assert_eq!(text(call(&b, "/").await), "pool-b");
    assert!(settled(&pool, 2));
    // Busy VMs are never evicted: a third identity is refused at capacity.
    let busy = call(&b, "/slow");
    let refused = pool.acquire_inner(app, first, &pool_c, &handler, true);
    let (busy, refused) = tokio::join!(busy, refused);
    assert_eq!(text(busy), "pool-b");
    assert!(refused
        .err()
        .unwrap()
        .downcast_ref::<CapacityExhausted>()
        .is_some());
    let refused = pool
        .acquire(app, &other, &name("pool-other"), &handler)
        .await;
    assert!(refused
        .err()
        .unwrap()
        .downcast_ref::<CapacityExhausted>()
        .is_some());
    assert!(settled(&pool, 2));
    // Releasing a's leases makes it the idle victim; c launches only after a's
    // VMM is reaped, so capacity never exceeds two VMs.
    let evicted = warm.client.clone();
    drop(a);
    drop(warm);
    until(|| evicted.transport.pending() == 0, "a did not settle").await;
    let c = pool
        .acquire_inner(app, first, &name("pool-c"), &handler, true)
        .await
        .unwrap();
    assert!(evicted.stop.is_cancelled(), "idle VM was not evicted");
    assert_eq!(text(call(&c, "/").await), "pool-c");
    assert!(settled(&pool, 2));
    // Warm reuse never crosses applications: the other application's VM
    // replaces idle b and serves its own pinned environment.
    drop(b);
    let foreign = pool
        .acquire(app, &other, &name("pool-other"), &handler)
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&foreign.pin, &c.pin));
    assert_eq!(foreign.pin.application, other);
    assert_eq!(text(call(&foreign, "/").await), "pool-other");
    assert!(settled(&pool, 2));
    // Replacement: a retired deployment refuses new leases, its live lease
    // keeps serving, and the VM is reaped once that lease settles.
    assert_eq!(observed("pool-c").await, Some(HandlerState::Serving));
    let mut snapshot = store.read(first).await.unwrap();
    let current = store
        .deployment(first, &name("pool-c"))
        .await
        .unwrap()
        .unwrap();
    store.activate(&mut snapshot, current).await.unwrap();
    store.commit(snapshot).await.unwrap();
    pool.retire_deployment(first, &name("pool-c")).await;
    assert_eq!(observed("pool-c").await, Some(HandlerState::Draining));
    assert!(format!(
        "{:#}",
        pool.acquire(app, first, &name("pool-c"), &handler)
            .await
            .err()
            .unwrap()
    )
    .contains("retiring"));
    assert_eq!(text(call(&c, "/").await), "pool-c");
    let replaced = c.client.clone();
    drop(c);
    until(
        || replaced.stop.is_cancelled(),
        "retired deployment kept its VM",
    )
    .await;
    until(|| settled(&pool, 1), "retired VMM was not reaped").await;
    assert!(
        observed("pool-c").await.is_none(),
        "reaped VM still reported"
    );
    // A caller cancelled during startup cannot abandon the VMM: the pool owns
    // startup, keeps the VM within capacity and still reaps it. A restore takes
    // about 250 ms, so the cancel comes well before it.
    let cancelled = {
        let pool = &pool;
        let a = name("pool-a");
        let handler = handler.clone();
        timeout(Duration::from_millis(80), async move {
            pool.acquire_inner(app, first, &a, &handler, true).await
        })
        .await
    };
    assert!(cancelled.is_err(), "startup finished before the cancel");
    until(
        || pool.snapshot().starting == 0,
        "cancelled startup stalled",
    )
    .await;
    assert!(pool.snapshot().vms <= 2 && owned_vm_pids().len() == pool.snapshot().vms as usize);
    // Idle VMs retire after the TTL.
    drop(foreign);
    until(|| settled(&pool, 0), "idle VMs outlived their TTL").await;
    // A draining node launches nothing.
    app.draining
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let drained = pool
        .acquire_inner(app, first, &name("pool-a"), &handler, true)
        .await;
    app.draining
        .store(false, std::sync::atomic::Ordering::SeqCst);
    assert!(format!("{:#}", drained.err().unwrap()).contains("draining"));
    assert!(settled(&pool, 0));
    // Shutdown stops live VMs, proves every reap and refuses later leases.
    let last = pool
        .acquire_inner(app, first, &name("pool-a"), &handler, true)
        .await
        .unwrap();
    assert_eq!(text(call(&last, "/").await), "pool-a");
    drop(last);
    timeout(Duration::from_secs(30), pool.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(settled(&pool, 0));
    assert!(store.status(first).await.unwrap().is_empty());
    assert!(pool
        .acquire_inner(app, first, &name("pool-a"), &handler, true)
        .await
        .is_err());
    // Status is per node: two nodes serving one application keep two records,
    // and one node's shutdown removes only its own.
    let [node_a, node_b] = ["node-a", "node-b"]
        .map(|node| VmPool::new(config.clone(), store.clone(), node.into()).unwrap());
    let (a, b) = tokio::join!(
        node_a.acquire_inner(app, first, &pool_a, &handler, true),
        node_b.acquire_inner(app, first, &pool_a, &handler, true)
    );
    let leases = (a.unwrap(), b.unwrap());
    let nodes = || async {
        let records = store.status(first).await.unwrap();
        let mut nodes: Vec<_> = records.into_iter().map(|record| record.node).collect();
        nodes.sort();
        nodes
    };
    assert_eq!(nodes().await, ["node-a", "node-b"]);
    drop(leases);
    node_a.shutdown().await.unwrap();
    assert_eq!(nodes().await, ["node-b"]);
    node_b.shutdown().await.unwrap();
    assert!(nodes().await.is_empty());
    // Leave the shared fixture app's activation as this check found it.
    let mut snapshot = store.read(first).await.unwrap();
    if let Some(active) = previous_active {
        store.activate(&mut snapshot, active).await.unwrap();
    } else {
        snapshot.deactivate(first).unwrap();
    }
    store.commit(snapshot).await.unwrap();
    eprintln!("owned JavaScript VM capacity: digest-selected approved runtime images; mismatched, symlinked and unapproved images refused launch without a VMM or held capacity; concurrent cold acquires shared one startup and later acquires reused the warm VM; deployments and applications never shared a VM; busy VMs refused a third identity at capacity, an idle VM was evicted and reaped before its replacement launched; a retired deployment kept serving its live lease then was reaped; a cancelled cold acquire left no abandoned VMM; idle TTL, drain refusal and shutdown reaped every VM; the published handler status read failed (unapproved image), serving, draining and gone after the reap, kept per node across two pools; actual VM, no independent-host qualification");
}

#[cfg(test)]
pub(crate) fn check_queue_catalog() {
    use crate::applications::Resource;
    let queue = |id: &str| Resource {
        resource: ResourceRef {
            kind: ResourceKind::Queue,
            id: id.to_owned().try_into().unwrap(),
        },
        configuration: serde_json::json!({}),
    };
    let consumer = |queue: &str, dead_letter: Option<&str>, retries: u16| {
        let mut value = serde_json::json!({
            "queue": queue, "max_batch_size": 10, "max_batch_timeout": 5, "max_retries": retries,
        });
        if let Some(dead_letter) = dead_letter {
            value["dead_letter_queue"] = dead_letter.into();
        }
        value
    };
    // One handler per consumer list; a handler list may hold several handlers.
    let application = |tenant: &str, handlers: Vec<serde_json::Value>| {
        let id: ApplicationId = serde_json::from_value(serde_json::json!({"app": tenant})).unwrap();
        let handlers: Vec<_> = handlers
            .into_iter()
            .enumerate()
            .map(|(index, consumers)| serde_json::json!({
                "id": format!("handler-{index}"), "language": "javascript", "entrypoint": "main.mjs",
                "artifact_sha256": "a".repeat(64), "runtime_image_sha256": "b".repeat(64),
                "worker_config": {"queue_consumers": consumers},
            }))
            .collect();
        let active = serde_json::from_value(serde_json::json!({
            "manifest_version": 1, "application": id, "id": "v1", "handlers": handlers,
        }))
        .unwrap();
        Application {
            id,
            resources: vec![queue("jobs"), queue("dead")],
            active: Some(active),
            secrets: BTreeMap::new(),
            limits: None,
            route_ids: BTreeMap::new(),
            asset_route_id: None,
        }
    };
    let scope = |application: &Application, id: &str| {
        let resource = ResourceAddress {
            application: application.id.clone(),
            resource: queue(id).resource,
        };
        resource.cell_scope("__Queue", id).unwrap()
    };
    let key = |application: &Application, id: &str| {
        ResourceAddress {
            application: application.id.clone(),
            resource: queue(id).resource,
        }
        .namespace_key("__Queue")
    };
    // The same queue names in two applications; one consumes with a dead-letter queue.
    let one = application(
        "one",
        vec![serde_json::json!([consumer("jobs", Some("dead"), 7)])],
    );
    let two = application("two", vec![serde_json::json!([consumer("jobs", None, 2)])]);
    // Invalid (an unregistered queue) and contested (two handlers) consumers consume nothing.
    let bad = application(
        "bad",
        vec![serde_json::json!([consumer("absent", None, 1)])],
    );
    let contested = application(
        "contested",
        vec![
            serde_json::json!([consumer("jobs", None, 1)]),
            serde_json::json!([consumer("jobs", None, 1)]),
        ],
    );
    let mut inactive = application("inactive", vec![serde_json::json!([])]);
    inactive.active = None;
    let catalog = queue_catalog([&one, &two, &bad, &contested, &inactive].into_iter());
    assert_eq!(
        catalog.len(),
        8,
        "every queue of each active application, none of the inactive one"
    );
    assert!(!catalog.contains_key(&scope(&inactive, "jobs")));
    let (first, second) = (
        &catalog[&scope(&one, "jobs")],
        &catalog[&scope(&two, "jobs")],
    );
    assert_ne!(scope(&one, "jobs"), scope(&two, "jobs"));
    assert!(first.application == one.id && second.application == two.id);
    let (handler, config) = first.consumer.as_ref().unwrap();
    assert_eq!((handler.as_str(), config.max_retries), ("handler-0", 7));
    assert_eq!(
        first.dead_letter_key.as_deref(),
        Some(key(&one, "dead").as_str())
    );
    assert_ne!(first.dead_letter_key, Some(key(&two, "dead")));
    assert_eq!(second.consumer.as_ref().unwrap().1.max_retries, 2);
    assert!(second.dead_letter_key.is_none());
    // What a cell reads is only its own scope's: settings and dead-letter key.
    let wire = first.consumer_wire().unwrap();
    assert_eq!(wire["maxRetries"], 7);
    assert_eq!(wire["deadLetterQueue"], "dead");
    assert_eq!(wire["deadLetterNamespaceKey"], key(&one, "dead"));
    assert_eq!(
        wire["maxConcurrency"],
        crate::application_guest_transport::QUEUE_MAX_CONCURRENCY
    );
    let wire = second.consumer_wire().unwrap();
    assert_eq!(wire["maxRetries"], 2);
    assert!(wire.get("deadLetterQueue").is_none() && wire.get("deadLetterNamespaceKey").is_none());
    // A dead-letter-only queue has an entry and no consumer.
    let dead = &catalog[&scope(&one, "dead")];
    assert!(dead.consumer.is_none() && dead.consumer_wire().is_none());
    for refused in [&bad, &contested] {
        assert!(catalog[&scope(refused, "jobs")].consumer.is_none());
    }
    assert!(!catalog.contains_key(&format!("__Queue:{}", "0".repeat(64))));
    // The entry pins the deployment it was built from.
    assert_eq!(first.deployment.as_str(), "v1");
    // A queue trigger picks the consuming handler; its settings are that
    // handler's own entry, else the original defaults.
    let triggered = |handlers: Vec<serde_json::Value>, handler: &str| {
        let mut app = application("trigger", handlers);
        app.active.as_mut().unwrap().triggers = serde_json::from_value(serde_json::json!([
            {"kind": "queue", "queue": "jobs", "handler": handler}
        ]))
        .unwrap();
        let catalog = queue_catalog(std::iter::once(&app));
        catalog[&scope(&app, "jobs")].clone()
    };
    let (handler, config) = triggered(vec![serde_json::json!([])], "handler-0")
        .consumer
        .clone()
        .unwrap();
    assert_eq!(handler.as_str(), "handler-0");
    assert_eq!(
        (
            config.max_batch_size,
            config.max_batch_timeout,
            config.max_retries
        ),
        (10, 5, 3)
    );
    let own = triggered(
        vec![serde_json::json!([consumer("jobs", Some("dead"), 7)])],
        "handler-0",
    );
    assert_eq!(own.consumer.as_ref().unwrap().1.max_retries, 7);
    assert!(own.dead_letter_key.is_some());
    // Another handler's own claim on the queue contests it.
    let contested = triggered(
        vec![
            serde_json::json!([consumer("jobs", None, 1)]),
            serde_json::json!([]),
        ],
        "handler-1",
    );
    assert!(contested.consumer.is_none());
}

#[cfg(test)]
pub(crate) fn check_cron_catalog() {
    use crate::applications::Resource;
    let cron = |id: &str| Resource {
        resource: ResourceRef {
            kind: ResourceKind::Cron,
            id: id.to_owned().try_into().unwrap(),
        },
        configuration: serde_json::json!({}),
    };
    let application = |tenant: &str, deployment: &str, expression: &str| {
        let id: ApplicationId = serde_json::from_value(serde_json::json!({"app": tenant})).unwrap();
        let active = serde_json::from_value(serde_json::json!({
            "manifest_version": 1, "application": id, "id": deployment,
            "handlers": [{
                "id": "main", "language": "javascript", "entrypoint": "main.mjs",
                "artifact_sha256": "a".repeat(64), "runtime_image_sha256": "b".repeat(64),
            }],
            "triggers": [{"kind": "cron", "id": "tick", "expression": expression, "handler": "main"}],
        }))
        .unwrap();
        Application {
            id,
            resources: vec![cron("tick"), cron("idle")],
            active: Some(active),
            secrets: BTreeMap::new(),
            limits: None,
            route_ids: BTreeMap::new(),
            asset_route_id: None,
        }
    };
    let scope = |application: &Application, id: &str| {
        ResourceAddress {
            application: application.id.clone(),
            resource: cron(id).resource,
        }
        .cell_scope(".cron", id)
        .unwrap()
    };
    // The same trigger id in two applications, and a trigger replaced.
    let one = application("one", "v1", "* * * * *");
    let two = application("two", "v1", "0 * * * *");
    let catalog = cron_catalog([&one, &two].into_iter());
    assert_eq!(catalog.len(), 4);
    assert_ne!(scope(&one, "tick"), scope(&two, "tick"));
    let (first, second) = (
        &catalog[&scope(&one, "tick")],
        &catalog[&scope(&two, "tick")],
    );
    assert!(first.application == one.id && second.application == two.id);
    let schedule = first.schedule.as_ref().unwrap();
    assert_eq!(
        (
            schedule.deployment.as_str(),
            schedule.handler.as_str(),
            schedule.expression.as_str()
        ),
        ("v1", "main", "* * * * *")
    );
    assert_eq!(second.schedule.as_ref().unwrap().expression, "0 * * * *");
    // A registered resource nothing triggers still has a cell, with no schedule.
    assert!(catalog[&scope(&one, "idle")].schedule.is_none());
    // Arming: once per schedule; a replacement arms again; a removal forgets.
    let mut armed = HashMap::new();
    let mut pending = pending_cron_arms(&catalog, &mut armed);
    pending.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(pending.len(), 2);
    for (cell, schedule) in pending {
        armed.insert(cell, schedule);
    }
    assert!(pending_cron_arms(&catalog, &mut armed).is_empty());
    let replaced = application("one", "v2", "*/5 * * * *");
    let after = cron_catalog([&replaced, &two].into_iter());
    let pending = pending_cron_arms(&after, &mut armed);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].0, scope(&one, "tick"));
    assert_eq!(pending[0].1.expression, "*/5 * * * *");
    // Deactivation keeps the cell but drops its schedule.
    let mut inactive = application("one", "v2", "*/5 * * * *");
    inactive.active = None;
    let after = cron_catalog([&inactive, &two].into_iter());
    assert!(after[&scope(&one, "tick")].schedule.is_none());
    assert!(pending_cron_arms(&after, &mut armed).is_empty());
    assert!(!armed.contains_key(&scope(&one, "tick")));
}

#[cfg(test)]
async fn check_route_dispatch(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    directory: &Path,
    first: &ApplicationId,
    owned_vm_pids: fn() -> HashSet<u32>,
) {
    use celld_applications::invocation_wire::HttpRequest;
    let source = directory.join("route-source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(
        source.join("main.mjs"),
        r#"import { env } from 'cloudflare:workers';
export default {
  async fetch(request) {
    return new Response(env.LABEL + ' ' + new URL(request.url).pathname);
  },
  // The scheduled time picks the outcome: 2 succeeds, 1 throws, 3 opts out of retry.
  async scheduled(controller) {
    const where = [env.LABEL, controller.cron, controller.scheduledTime].join(' ');
    if (controller.scheduledTime === 3) controller.noRetry();
    if (controller.scheduledTime !== 2) throw new Error(where);
  }
};
"#,
    )
    .unwrap();
    let image = directory.join("route.bundle");
    let built = timeout(
        Duration::from_secs(30),
        Command::new("/usr/bin/python3")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../applications/guest/build-application-image.py"),
            )
            .arg(&source)
            .arg(&image)
            .env_clear()
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let artifact = std::fs::read(image).unwrap();
    let digest = content_digest(&artifact);
    store
        .upload_artifact(first, &digest, artifact.into())
        .await
        .unwrap();
    for id in ["route-a", "route-b", "route-c"] {
        let deployment: Deployment = serde_json::from_value(serde_json::json!({
            "manifest_version": 1,
            "application": first,
            "id": id,
            "handlers": [{
                "id": "javascript",
                "language": "javascript",
                "entrypoint": "main.mjs",
                "artifact_sha256": digest,
                "runtime_image_sha256": options.runtime.sha256,
                "http": true,
                "environment": {"LABEL": id},
                "worker_config": {"compatibility_date": "2026-03-01"}
            }]
        }))
        .unwrap();
        store.stage(&deployment).await.unwrap();
    }
    let config = VmHostConfig {
        jailer: options.jailer.clone(),
        firecracker: options.firecracker.clone(),
        kernel: options.kernel.clone(),
        runtimes: vec![options.runtime.clone()],
        uid: options.uid,
        gid: options.gid,
        max_vms: 2,
        max_memory_mib: 12_288,
        // Far longer than this check: only reconciliation retires a VM.
        idle_ttl_secs: 600,
        platform_domain: TEST_DOMAIN.into(),
        container_fence_image: None,
        artifact_cache_ttl_secs: None,
        metadata_cache_entries: None,
        metadata_cache_bytes: None,
        startup_pool: 0,
        memory_guard: None,
    };
    async fn get(
        app: &crate::actor::AppHandle,
        dispatcher: &RouteDispatcher,
        host: &str,
        path: &str,
    ) -> Option<String> {
        let request = HttpRequest {
            method: "GET".into(),
            url: format!("http://{host}{path}"),
            headers: vec![],
            body: vec![],
        };
        dispatcher
            .dispatch(app, Duration::from_secs(10), request, None)
            .await
            .unwrap_or_else(|error| panic!("dispatch failed: {error:#}"))
            .map(|response| String::from_utf8(response.body).unwrap())
    }
    let staged = |id: &str| {
        let id: Name = id.to_owned().try_into().unwrap();
        async move { store.deployment(first, &id).await.unwrap().unwrap() }
    };
    let activate = |id: &str| {
        let id = id.to_owned();
        async move {
            let mut snapshot = store.read(first).await.unwrap();
            store
                .activate(&mut snapshot, staged(&id).await)
                .await
                .unwrap();
            store.commit(snapshot).await.unwrap();
        }
    };
    let vms = |dispatcher: &RouteDispatcher, count: u32| {
        let pool = dispatcher.routes.pool.as_ref().unwrap().snapshot();
        assert_eq!(pool.quarantined, 0);
        pool.vms == count && owned_vm_pids().len() == count as usize
    };
    async fn until(mut done: impl FnMut() -> bool, what: &str) {
        timeout(Duration::from_secs(15), async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what}"));
    }
    // Metadata-only inactive applications do not reserve execution or cache
    // space. Keep the fleet large enough to expose an eager catalog walk.
    for index in 0..64 {
        let application = ApplicationId {
            app: format!("route-dormant-{index}").try_into().unwrap(),
        };
        let mut snapshot = store.read(&application).await.unwrap();
        snapshot.create(application).unwrap();
        store.commit(snapshot).await.unwrap();
    }
    // Staged is not published: no route, and no VM launches for it.
    let dispatcher = RouteDispatcher::new(config.clone(), store.clone(), NODE.into())
        .await
        .unwrap();
    let shared_templates = dispatcher.routes.pool.as_ref().unwrap().templates.status();
    dispatcher.reconcile().await.unwrap();
    assert_eq!(
        dispatcher.routes.pool.as_ref().unwrap().snapshot(),
        VmPoolSnapshot::default()
    );
    assert!(dispatcher
        .routes
        .pool
        .as_ref()
        .unwrap()
        .state
        .lock()
        .unwrap()
        .slots
        .is_empty());
    assert!(dispatcher
        .routes
        .pool
        .as_ref()
        .unwrap()
        .observed
        .handlers
        .lock()
        .await
        .is_empty());
    assert!(dispatcher.routes.scopes.lock().unwrap().entries.is_empty());
    assert_eq!(
        dispatcher.routes.pool.as_ref().unwrap().templates.status(),
        shared_templates
    );
    // Not even an id exists yet; ids come only from activation.
    let unknown = format!("{}.{TEST_DOMAIN}", "0".repeat(32));
    assert_eq!(get(app, &dispatcher, &unknown, "/").await, None);
    assert!(vms(&dispatcher, 0));
    // An object call selects its VM from the active deployment only: with none,
    // or when no active handler declares and binds the object, it is refused
    // before any VM launches.
    let call = || {
        let object = celld_applications::invocation_wire::ObjectTarget {
            class: "Counter".into(),
            id: "0".repeat(64),
            name: None,
        };
        let request = HttpRequest {
            method: "GET".into(),
            url: "http://object/".into(),
            headers: vec![],
            body: vec![],
        };
        let dispatcher = &dispatcher;
        async move {
            dispatcher
                .call_object(app, first, object, None, request)
                .await
        }
    };
    assert!(call()
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("no active deployment"));
    activate("route-a").await;
    dispatcher.reconcile().await.unwrap();
    let host = assigned_host(store, first, "javascript").await;
    assert_eq!(
        get(app, &dispatcher, &host, "/x").await.as_deref(),
        Some("route-a /x")
    );
    assert!(call()
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("declares and binds"));
    assert_eq!(get(app, &dispatcher, &unknown, "/").await, None);
    assert_eq!(
        get(app, &dispatcher, "route-a.example.test", "/").await,
        None
    );
    assert!(vms(&dispatcher, 1));
    assert!(dispatcher
        .routes
        .pool
        .as_ref()
        .unwrap()
        .state
        .lock()
        .unwrap()
        .slots
        .keys()
        .all(|(application, _, _)| application == first));
    assert!(dispatcher
        .routes
        .pool
        .as_ref()
        .unwrap()
        .observed
        .handlers
        .lock()
        .await
        .keys()
        .all(|(application, _, _)| application == first));
    assert!(dispatcher
        .routes
        .scopes
        .lock()
        .unwrap()
        .entries
        .values()
        .all(|(_, _, metadata)| &metadata.address.application == first));
    // Two writers on one revision: one activation wins, routing follows it, and
    // the replaced deployment's VM is retired by reconciliation.
    let (mut b, mut c) = (
        store.read(first).await.unwrap(),
        store.read(first).await.unwrap(),
    );
    store
        .activate(&mut b, staged("route-b").await)
        .await
        .unwrap();
    store
        .activate(&mut c, staged("route-c").await)
        .await
        .unwrap();
    let (b, c) = tokio::join!(store.commit(b), store.commit(c));
    dispatcher.reconcile().await.unwrap();
    let (winner, loser) = match (b, c) {
        (Ok(_), Err(error)) => ("route-b", error),
        (Err(error), Ok(_)) => ("route-c", error),
        _ => panic!("racing activations must have exactly one winner"),
    };
    assert!(loser.is::<crate::applications::Conflict>());
    assert_eq!(
        get(app, &dispatcher, &host, "/").await.as_deref(),
        Some(format!("{winner} /").as_str())
    );
    until(|| vms(&dispatcher, 1), "replaced VM was not retired").await;
    // Rollback re-admits a deployment that was retired.
    activate("route-a").await;
    dispatcher.reconcile().await.unwrap();
    assert_eq!(
        get(app, &dispatcher, &host, "/").await.as_deref(),
        Some("route-a /")
    );
    until(|| vms(&dispatcher, 1), "rolled-back VM was not replaced").await;
    // A restarted dispatcher serves the committed catalog from a cold pool.
    dispatcher.shutdown().await.unwrap();
    assert!(vms(&dispatcher, 0));
    let dispatcher = RouteDispatcher::new(config, store.clone(), NODE.into())
        .await
        .unwrap();
    assert_eq!(
        get(app, &dispatcher, &host, "/").await.as_deref(),
        Some("route-a /")
    );
    // Deactivation withdraws the route and retires the VM.
    let mut snapshot = store.read(first).await.unwrap();
    snapshot.deactivate(first).unwrap();
    store.commit(snapshot).await.unwrap();
    dispatcher.reconcile().await.unwrap();
    assert_eq!(get(app, &dispatcher, &host, "/").await, None);
    until(|| vms(&dispatcher, 0), "deactivated VM was not retired").await;
    // T17: a cron trigger of the active deployment runs its pinned handler's
    // original scheduled event; replacement and deactivation stop the old one.
    for (id, expression) in [("cron-a", "* * * * *"), ("cron-b", "*/5 * * * *")] {
        let deployment: Deployment = serde_json::from_value(serde_json::json!({
            "manifest_version": 1, "application": first, "id": id,
            "handlers": [{
                "id": "javascript", "language": "javascript", "entrypoint": "main.mjs",
                "artifact_sha256": digest, "runtime_image_sha256": options.runtime.sha256,
                "environment": {"LABEL": id},
                "worker_config": {"compatibility_date": "2026-03-01"}
            }],
            "triggers": [{"kind": "cron", "id": "tick", "expression": expression, "handler": "javascript"}]
        }))
        .unwrap();
        store.stage(&deployment).await.unwrap();
    }
    let tick = ResourceRef {
        kind: ResourceKind::Cron,
        id: "tick".to_owned().try_into().unwrap(),
    };
    let mut snapshot = store.read(first).await.unwrap();
    snapshot
        .register_resource(
            first,
            crate::applications::Resource {
                resource: tick.clone(),
                configuration: serde_json::json!({}),
            },
        )
        .unwrap();
    store.commit(snapshot).await.unwrap();
    dispatcher.install_alarm_host(app.clone()).unwrap();
    // The registered Cron resource enters the catalog, inactive until a trigger.
    dispatcher.reconcile().await.unwrap();
    let scope = ResourceAddress {
        application: first.clone(),
        resource: tick,
    }
    .cell_scope(celld_logic::cron::RESERVED_CLASS, "tick")
    .unwrap();
    assert!(
        dispatcher
            .cron_entry(&scope)
            .await
            .unwrap()
            .unwrap()
            .schedule
            .is_none(),
        "no trigger is active yet"
    );
    let run = |cron: &'static str, ms: i64| {
        let (dispatcher, scope) = (&dispatcher, scope.clone());
        async move {
            dispatcher
                .dispatch_scheduled(&scope, cron, ms)
                .await
                .map(|body| serde_json::from_str::<serde_json::Value>(&body).unwrap())
        }
    };
    assert!(run("* * * * *", 2)
        .await
        .unwrap_err()
        .to_string()
        .contains("not active"));
    activate("cron-a").await;
    dispatcher.reconcile().await.unwrap();
    assert!(dispatcher
        .cron_entry(&scope)
        .await
        .unwrap()
        .unwrap()
        .schedule
        .is_some());
    let ok = run("* * * * *", 2).await.unwrap();
    assert_eq!(
        (ok["outcome"].as_str(), ok["noRetry"].as_bool()),
        (Some("ok"), Some(false))
    );
    let failed = run("* * * * *", 1).await.unwrap();
    assert_eq!(failed["outcome"], "exception");
    assert_eq!(failed["noRetry"], false);
    assert!(
        failed["error"]
            .as_str()
            .unwrap()
            .contains("cron-a * * * * * 1"),
        "{failed}"
    );
    let opted_out = run("* * * * *", 3).await.unwrap();
    assert_eq!(
        (
            opted_out["outcome"].as_str(),
            opted_out["noRetry"].as_bool()
        ),
        (Some("exception"), Some(true))
    );
    // A cell can neither name another expression nor another application's cell.
    assert!(run("0 0 * * *", 2)
        .await
        .unwrap_err()
        .to_string()
        .contains("changed"));
    let foreign = format!("{}:{}", celld_logic::cron::RESERVED_CLASS, "0".repeat(64));
    assert!(dispatcher
        .dispatch_scheduled(&foreign, "* * * * *", 2)
        .await
        .unwrap_err()
        .to_string()
        .contains("not an application cron"));
    // Replacement: the new deployment runs, and the old expression is refused.
    activate("cron-b").await;
    dispatcher.reconcile().await.unwrap();
    assert!(run("* * * * *", 1)
        .await
        .unwrap_err()
        .to_string()
        .contains("changed"));
    let replaced = run("*/5 * * * *", 1).await.unwrap();
    assert!(
        replaced["error"]
            .as_str()
            .unwrap()
            .contains("cron-b */5 * * * * 1"),
        "{replaced}"
    );
    // Deactivation: nothing runs, and the cell stays resolvable to retire itself.
    let mut snapshot = store.read(first).await.unwrap();
    snapshot.deactivate(first).unwrap();
    store.commit(snapshot).await.unwrap();
    dispatcher.reconcile().await.unwrap();
    assert!(run("*/5 * * * *", 2)
        .await
        .unwrap_err()
        .to_string()
        .contains("not active"));
    assert!(dispatcher.owns_storage_cell(&scope).await.unwrap());
    until(|| vms(&dispatcher, 0), "cron VM was not retired").await;
    dispatcher.shutdown().await.unwrap();
    eprintln!("route dispatch: a staged deployment was unreachable and launched nothing; an activated route reached its pinned handler in a VM; racing activations had one CAS winner and routing followed it; replacement retired the old VM, rollback re-admitted it, a restarted dispatcher served the committed catalog and deactivation withdrew the route and reaped the VM; actual VM, no independent-host qualification");
}

#[cfg(test)]
async fn check_replacement_under_load(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    directory: &Path,
    first: &ApplicationId,
    owned_vm_pids: fn() -> HashSet<u32>,
) {
    use celld_applications::invocation_wire::HttpRequest;
    use std::sync::atomic::{AtomicBool, Ordering};
    let source = directory.join("load-source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(
        source.join("main.mjs"),
        r#"import { env } from 'cloudflare:workers';
export default {
  async fetch(request, _env, ctx) {
    const path = new URL(request.url).pathname;
    if (path === '/slow') await new Promise((resolve) => setTimeout(resolve, 1000));
    if (path === '/background') ctx.waitUntil(new Promise((resolve) => setTimeout(resolve, 4000)));
    return new Response(env.LABEL + ' ' + path);
  }
};
"#,
    )
    .unwrap();
    let image = directory.join("load.bundle");
    let built = timeout(
        Duration::from_secs(30),
        Command::new("/usr/bin/python3")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../applications/guest/build-application-image.py"),
            )
            .arg(&source)
            .arg(&image)
            .env_clear()
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let artifact = std::fs::read(image).unwrap();
    let digest = content_digest(&artifact);
    store
        .upload_artifact(first, &digest, artifact.into())
        .await
        .unwrap();
    for id in ["load-a", "load-b"] {
        let deployment: Deployment = serde_json::from_value(serde_json::json!({
            "manifest_version": 1,
            "application": first,
            "id": id,
            "handlers": [{
                "id": "javascript",
                "language": "javascript",
                "entrypoint": "main.mjs",
                "artifact_sha256": digest,
                "runtime_image_sha256": options.runtime.sha256,
                "http": true,
                "environment": {"LABEL": id},
                "worker_config": {"compatibility_date": "2026-03-01"}
            }]
        }))
        .unwrap();
        store.stage(&deployment).await.unwrap();
    }
    let config = VmHostConfig {
        jailer: options.jailer.clone(),
        firecracker: options.firecracker.clone(),
        kernel: options.kernel.clone(),
        runtimes: vec![options.runtime.clone()],
        uid: options.uid,
        gid: options.gid,
        // The old VM, busy until its work settles, and its successor.
        max_vms: 2,
        max_memory_mib: 12_288,
        // Far longer than this check: only replacement retires a VM, and it
        // must not wait for this.
        idle_ttl_secs: 600,
        platform_domain: TEST_DOMAIN.into(),
        container_fence_image: None,
        artifact_cache_ttl_secs: None,
        metadata_cache_entries: None,
        metadata_cache_bytes: None,
        startup_pool: 0,
        memory_guard: None,
    };
    async fn fetch(
        app: crate::actor::AppHandle,
        dispatcher: Arc<RouteDispatcher>,
        host: String,
        path: &'static str,
    ) -> Result<String, String> {
        let request = HttpRequest {
            method: "GET".into(),
            url: format!("http://{host}{path}"),
            headers: vec![],
            body: vec![],
        };
        match dispatcher
            .dispatch(&app, Duration::from_secs(20), request, None)
            .await
        {
            Ok(Some(response)) => Ok(String::from_utf8(response.body).unwrap()),
            Ok(None) => Err("no route".into()),
            Err(error) => Err(format!("{error:#}")),
        }
    }
    async fn until(mut done: impl FnMut() -> bool, what: &str) {
        timeout(Duration::from_secs(15), async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what}"));
    }
    let name = |value: &str| -> Name { value.to_owned().try_into().unwrap() };
    let handler = name("javascript");
    let observed = |deployment: &str| {
        let deployment = name(deployment);
        async move {
            store
                .status(first)
                .await
                .unwrap()
                .into_iter()
                .flat_map(|record| record.handlers)
                .find(|handler| handler.deployment == deployment)
                .map(|handler| handler.state)
        }
    };
    let activate = |id: &str| {
        let id = name(id);
        async move {
            let mut snapshot = store.read(first).await.unwrap();
            let deployment = store.deployment(first, &id).await.unwrap().unwrap();
            store.activate(&mut snapshot, deployment).await.unwrap();
            store.commit(snapshot).await.unwrap();
        }
    };
    let dispatcher = Arc::new(
        RouteDispatcher::new(config, store.clone(), NODE.into())
            .await
            .unwrap(),
    );
    let vms = |count: u32| {
        let pool = dispatcher.routes.pool.as_ref().unwrap().snapshot();
        assert_eq!(pool.quarantined, 0);
        pool.vms == count && owned_vm_pids().len() == count as usize
    };
    activate("load-a").await;
    dispatcher.reconcile().await.unwrap();
    let fetch = {
        let (app, dispatcher) = (app.clone(), dispatcher.clone());
        let host = assigned_host(store, first, "javascript").await;
        move |path: &'static str| fetch(app.clone(), dispatcher.clone(), host.clone(), path)
    };
    assert_eq!(fetch("/warm").await.unwrap(), "load-a /warm");
    let old = dispatcher
        .routes
        .pool
        .as_ref()
        .unwrap()
        .acquire(app, first, &name("load-a"), &handler)
        .await
        .unwrap()
        .client
        .clone();
    // Accepted by the old deployment: slow requests and a reply whose
    // `waitUntil` work goes on after it.
    let slow: Vec<_> = (0..3).map(|_| tokio::spawn(fetch("/slow"))).collect();
    let replied = Instant::now();
    assert_eq!(fetch("/background").await.unwrap(), "load-a /background");
    until(
        || old.transport.pending() >= 4,
        "old VM did not hold the accepted work",
    )
    .await;
    // Load that continues across the swap. Each client sends one request at a
    // time, so what it sees is ordered.
    let stop = Arc::new(AtomicBool::new(false));
    let loaders: Vec<_> = (0..3)
        .map(|_| {
            let (fetch, stop) = (fetch.clone(), stop.clone());
            tokio::spawn(async move {
                let mut seen = Vec::new();
                while !stop.load(Ordering::SeqCst) {
                    seen.push(fetch("/x").await);
                }
                seen
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(300)).await;
    activate("load-b").await;
    dispatcher.reconcile().await.unwrap();
    assert!(
        slow.iter().all(|slow| !slow.is_finished()),
        "the replacement did not happen under in-flight work"
    );
    assert_eq!(observed("load-a").await, Some(HandlerState::Draining));
    // In-flight requests finish on the pinned old deployment.
    for slow in slow {
        assert_eq!(slow.await.unwrap().unwrap(), "load-a /slow");
    }
    // Only the retained background work holds the old VM now.
    // The slow requests' drains settle just after their replies.
    timeout(Duration::from_secs(5), async {
        while old.transport.pending() != 1 {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("only the retained waitUntil should hold the old VM");
    assert!(!old.stop.is_cancelled(), "old VM left under retained work");
    timeout(Duration::from_secs(30), async {
        while fetch("/probe").await.unwrap() != "load-b /probe" {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the new deployment was not reached");
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::SeqCst);
    let (mut before, mut after) = (0, 0);
    for seen in loaders {
        let mut swapped = false;
        for result in seen.await.unwrap() {
            match result
                .unwrap_or_else(|error| panic!("a request failed across the replacement: {error}"))
                .as_str()
            {
                "load-a /x" => {
                    assert!(
                        !swapped,
                        "a request reached the old deployment after the new"
                    );
                    before += 1;
                }
                "load-b /x" => {
                    swapped = true;
                    after += 1;
                }
                other => panic!("unexpected reply {other:?}"),
            }
        }
    }
    assert!(
        before > 0 && after > 0,
        "load did not span the swap: {before}/{after}"
    );
    // The drained VM is reaped once the background work ends: not before, and
    // not at the idle TTL.
    until(|| old.stop.is_cancelled(), "drained old VM was not retired").await;
    assert!(
        replied.elapsed() >= Duration::from_millis(3500),
        "old VM was retired before its waitUntil work settled: {:?}",
        replied.elapsed()
    );
    until(|| vms(1), "old VMM was not reaped").await;
    assert!(
        observed("load-a").await.is_none(),
        "reaped VM still reported"
    );
    assert!(dispatcher
        .routes
        .pool
        .as_ref()
        .unwrap()
        .acquire(app, first, &name("load-a"), &handler)
        .await
        .err()
        .unwrap()
        .is::<DeploymentRetiring>());
    assert_eq!(fetch("/after").await.unwrap(), "load-b /after");
    dispatcher.shutdown().await.unwrap();
    assert!(vms(0));
    eprintln!("replacement under load: requests in flight and a retained waitUntil finished on the pinned old deployment, three clients saw the old deployment then the new with no failed request, the old VM stayed until its work settled and was then reaped well before the idle TTL; actual VM, no independent-host qualification");
}

#[cfg(test)]
async fn check_stateless_handlers(
    app: &crate::actor::AppHandle,
    store: &ApplicationStore,
    options: &LaunchOptions,
    directory: &Path,
    first: &ApplicationId,
) {
    use celld_applications::invocation_wire::HttpRequest;
    // Emitted by `tsc` (target ES2022, module ES2022) from a module using an
    // enum, a private class field and Worker module types.
    const COMPILED: &str = r#"import { env } from 'cloudflare:workers';
var Mode;
(function (Mode) {
    Mode["Background"] = "/background";
    Mode["Count"] = "/count";
    Mode["Error"] = "/error";
    Mode["Slow"] = "/slow";
})(Mode || (Mode = {}));
class Greeter {
    #label;
    constructor(label) {
        this.#label = label;
    }
    greet(path) {
        return `${this.#label}:${path}`;
    }
}
let background = 0;
export default {
    async fetch(request, _env, ctx) {
        const path = new URL(request.url).pathname;
        const greeter = new Greeter(env.APP_LABEL);
        switch (path) {
            case Mode.Background:
                ctx.waitUntil((async () => {
                    await new Promise((resolve) => setTimeout(resolve, 50));
                    background++;
                })());
                return new Response(greeter.greet(path));
            case Mode.Count:
                return new Response(String(background));
            case Mode.Error:
                throw new Error('stateless handler failure');
            case Mode.Slow:
                await new Promise((resolve) => setTimeout(resolve, 5000));
                return new Response('late');
            default:
                return new Response(greeter.greet(path));
        }
    },
};
//# sourceMappingURL=main.js.map
"#;
    let second = ApplicationId {
        app: "stateless-other".to_owned().try_into().unwrap(),
    };
    let mut snapshot = store.read(&second).await.unwrap();
    snapshot.create(second.clone()).unwrap();
    store.commit(snapshot).await.unwrap();
    let source = directory.join("stateless-source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("main.mjs"), COMPILED).unwrap();
    let image = directory.join("stateless.bundle");
    let built = timeout(
        Duration::from_secs(30),
        Command::new("/usr/bin/python3")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../applications/guest/build-application-image.py"),
            )
            .arg(&source)
            .arg(&image)
            .env_clear()
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let artifact = std::fs::read(image).unwrap();
    let digest = content_digest(&artifact);
    let launch = |application: ApplicationId, label: &'static str| {
        let (artifact, digest) = (artifact.clone(), digest.clone());
        let options = options.clone();
        async move {
            store
                .upload_artifact(&application, &digest, artifact.into())
                .await
                .unwrap();
            let deployment: Deployment = serde_json::from_value(serde_json::json!({
                "manifest_version": 1,
                "application": application,
                "id": label.to_lowercase(),
                "handlers": [{
                    "id": "javascript",
                    "language": "javascript",
                    "entrypoint": "main.mjs",
                    "artifact_sha256": digest,
                    "runtime_image_sha256": options.runtime.sha256,
                    "environment": {"APP_LABEL": label},
                    "secrets": {},
                    "bindings": {},
                    "worker_config": {"compatibility_date": "2026-03-01"}
                }]
            }))
            .unwrap();
            store.stage(&deployment).await.unwrap();
            timeout(
                Duration::from_secs(30),
                Vm::launch(
                    options,
                    store,
                    &application,
                    &deployment.id,
                    &"javascript".to_owned().try_into().unwrap(),
                ),
            )
            .await
            .unwrap()
            .unwrap_or_else(|error| panic!("stateless launch failed: {error:#}"))
        }
    };
    let call = |vm: &Vm, path: &str, deadline: Duration| {
        let (client, app) = (vm.client(), app.clone());
        let url = format!("http://stateless{path}");
        async move {
            client
                .invoke_stateless(
                    &app,
                    deadline,
                    HttpRequest {
                        method: "GET".into(),
                        url,
                        headers: vec![],
                        body: vec![],
                    },
                    None,
                )
                .await
        }
    };
    let text = |result: Result<crate::js::HttpResponse>| match result {
        Ok(response) => String::from_utf8(response.body).unwrap(),
        Err(error) => panic!("stateless handler failed: {error:#}"),
    };
    let idle = || async {
        timeout(Duration::from_secs(10), async {
            while app.public_in_flight() != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("stateless work did not settle");
    };
    // The same handler name in two applications serves distinct replies.
    let first_v1 = launch(first.clone(), "First-V1").await;
    let second_v1 = launch(second.clone(), "Second-V1").await;
    let seconds = Duration::from_secs(5);
    assert_eq!(
        text(call(&first_v1, "/echo", seconds).await),
        "First-V1:/echo"
    );
    assert_eq!(
        text(call(&second_v1, "/echo", seconds).await),
        "Second-V1:/echo"
    );
    // waitUntil continues after the reply under the public admission.
    assert_eq!(
        text(call(&first_v1, "/background", seconds).await),
        "First-V1:/background"
    );
    idle().await;
    assert_eq!(text(call(&first_v1, "/count", seconds).await), "1");
    assert_eq!(text(call(&second_v1, "/count", seconds).await), "0");
    // Errors and a deadline cancellation fail only their own invocation.
    let failure = call(&first_v1, "/error", seconds).await.err().unwrap();
    assert!(format!("{failure:#}").contains("stateless handler failure"));
    let expired = call(&first_v1, "/slow", Duration::from_millis(200))
        .await
        .err()
        .unwrap();
    assert!(format!("{expired:#}").contains("deadline"));
    idle().await;
    assert_eq!(
        text(call(&first_v1, "/echo", seconds).await),
        "First-V1:/echo"
    );
    // Replacement: the new deployment serves its environment while the old
    // VM keeps its pin until shutdown.
    let first_v2 = launch(first.clone(), "First-V2").await;
    assert_eq!(
        text(call(&first_v2, "/echo", seconds).await),
        "First-V2:/echo"
    );
    assert_eq!(
        text(call(&first_v1, "/echo", seconds).await),
        "First-V1:/echo"
    );
    idle().await;
    for vm in [first_v1, second_v1, first_v2] {
        assert_eq!(vm.shutdown().await.unwrap().reason, "shutdown");
    }
    eprintln!("owned JavaScript stateless handlers: two applications deployed the same handler name from compiled TypeScript and served distinct pinned environments in actual VMs; waitUntil finished after the reply; an error and a deadline cancellation failed only their invocation; a replacement deployment served beside the pinned old VM until shutdown; no routing/activation or independent-host qualification");
}

#[cfg(test)]
async fn check_restored_vms(
    app: &crate::actor::AppHandle,
    store: &ApplicationStore,
    options: &LaunchOptions,
    directory: &Path,
    application: &ApplicationId,
) {
    use celld_applications::invocation_wire::HttpRequest;
    const SOURCE: &str = r#"export default {
  fetch() {
    return new Response(JSON.stringify({
      random: Math.random(),
      uuid: crypto.randomUUID(),
      bytes: Array.from(crypto.getRandomValues(new Uint8Array(16))),
      now: Date.now(),
    }));
  },
};
"#;
    let source = directory.join("restore-source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("main.mjs"), SOURCE).unwrap();
    let image = directory.join("restore.bundle");
    let built = timeout(
        Duration::from_secs(30),
        Command::new("/usr/bin/python3")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../applications/guest/build-application-image.py"),
            )
            .arg(&source)
            .arg(&image)
            .env_clear()
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let artifact = std::fs::read(image).unwrap();
    let digest = content_digest(&artifact);
    store
        .upload_artifact(application, &digest, artifact.into())
        .await
        .unwrap();
    let deployment: Deployment = serde_json::from_value(serde_json::json!({
        "manifest_version": 1,
        "application": application,
        "id": "restored",
        "handlers": [{
            "id": "javascript",
            "language": "javascript",
            "entrypoint": "main.mjs",
            "artifact_sha256": digest,
            "runtime_image_sha256": options.runtime.sha256,
            "worker_config": {"compatibility_date": "2026-03-01"}
        }]
    }))
    .unwrap();
    store.stage(&deployment).await.unwrap();
    // Templates of their own, so the corruption below touches no other check.
    let options = LaunchOptions {
        templates: Arc::new(Templates::new().unwrap()),
        ..options.clone()
    };
    let launch = || async {
        timeout(
            Duration::from_secs(60),
            Vm::launch(
                options.clone(),
                store,
                application,
                &deployment.id,
                &"javascript".to_owned().try_into().unwrap(),
            ),
        )
        .await
        .unwrap()
        .unwrap_or_else(|error| panic!("restored launch failed: {error:#}"))
    };
    let draw = |vm: &Vm| {
        let (client, app) = (vm.client(), app.clone());
        async move {
            let response = client
                .invoke_stateless(
                    &app,
                    Duration::from_secs(5),
                    HttpRequest {
                        method: "GET".into(),
                        url: "http://restored/".into(),
                        headers: vec![],
                        body: vec![],
                    },
                    None,
                )
                .await
                .unwrap();
            serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()
        }
    };
    // The first launch builds the template; both VMs restore from it.
    let (one, two) = (launch().await, launch().await);
    assert_eq!((one.path, two.path), ("restore", "restore"));
    let (first, second) = (draw(&one).await, draw(&two).await);
    for field in ["random", "uuid", "bytes"] {
        assert_ne!(first[field], second[field], "restored VMs share {field}");
    }
    let host = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    for drawn in [&first, &second] {
        let skew = drawn["now"].as_i64().unwrap() - host;
        assert!(skew.abs() < 1000, "restored guest clock is {skew} ms off");
    }
    // A template that no longer restores: the failed VMM is reaped and the
    // launch boots cold.
    for state in options.templates.state_files() {
        std::fs::remove_file(&state).unwrap();
        std::fs::write(state, b"not a snapshot").unwrap();
    }
    let cold = launch().await;
    assert_eq!(cold.path, "cold");
    assert!(draw(&cold).await["random"].is_f64());
    for vm in [one, two, cold] {
        assert_eq!(vm.shutdown().await.unwrap().reason, "shutdown");
    }
    eprintln!("restored VMs: two VMs restored from one template drew different Math.random, randomUUID and getRandomValues in V8 and had the host clock within a second; a template with a corrupt snapshot failed its restore, was reaped and fell back to a cold boot; actual VMs, no independent-host qualification");
}

#[cfg(test)]
async fn check_startup_classes(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    directory: &Path,
    owned_vm_pids: fn() -> HashSet<u32>,
) {
    let pool = VmPool::new(
        VmHostConfig {
            jailer: options.jailer.clone(),
            firecracker: options.firecracker.clone(),
            kernel: options.kernel.clone(),
            runtimes: vec![options.runtime.clone()],
            uid: options.uid,
            gid: options.gid,
            max_vms: 1,
            max_memory_mib: 12288,
            idle_ttl_secs: 1,
            platform_domain: TEST_DOMAIN.into(),
            container_fence_image: None,
            artifact_cache_ttl_secs: None,
            metadata_cache_entries: None,
            metadata_cache_bytes: None,
            startup_pool: 0,
            memory_guard: None,
        },
        store.clone(),
        "startup-classes".into(),
    )
    .unwrap();
    let cancelled = Arc::new(
        VmPool::new(
            pool.config.clone(),
            store.clone(),
            "startup-cancelled".into(),
        )
        .unwrap(),
    );
    let owner = cancelled.clone();
    let initialization_app = app.clone();
    let preparing = tokio::spawn(async move { owner.prepare(&initialization_app).await });
    timeout(Duration::from_secs(30), async {
        while !owned_vm_pids().into_iter().any(|pid| {
            std::fs::read_to_string(format!("/proc/{pid}/cgroup"))
                .is_ok_and(|membership| membership.contains("/celld-vms-"))
        }) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("cancel test did not start a capture VM");
    preparing.abort();
    assert!(preparing.await.unwrap_err().is_cancelled());
    timeout(Duration::from_secs(180), cancelled.shutdown())
        .await
        .expect("cancelled startup did not wake shutdown after its owned reap")
        .unwrap();
    assert!(!cancelled.templates.ready());
    assert_eq!(cancelled.snapshot().memory_mib, 0);
    assert!(owned_vm_pids().is_empty());
    let began = Instant::now();
    pool.prepare(app)
        .await
        .unwrap_or_else(|error| panic!("startup preparation failed: {error:#}"));
    let startup_ms = began.elapsed().as_millis();
    assert!(pool.templates.ready() && owned_vm_pids().is_empty());
    assert_eq!((pool.snapshot().vms, pool.snapshot().memory_mib), (0, 0));
    let states = pool.templates.state_files();
    assert_eq!(states.len(), 6);
    let original: Vec<_> = states
        .iter()
        .map(|path| std::fs::metadata(path).unwrap().modified().unwrap())
        .collect();
    let image = directory.join("startup-classes.img");
    std::fs::write(
        &image,
        crate::application_image::build(&[(
            "main.mjs".into(),
            b"export default { fetch() { return new Response('class ready'); } };".to_vec(),
        )])
        .unwrap(),
    )
    .unwrap();
    // Admission reserved Basic, then an administrator changed the application
    // to Lite before its owned launch task read the catalog again.
    let application: ApplicationId =
        serde_json::from_value(serde_json::json!({"app":"classes"})).unwrap();
    let mut catalog = store.read(&application).await.unwrap();
    catalog.create(application.clone()).unwrap();
    catalog
        .set_limits(
            &application,
            Some(ApplicationLimits {
                size_class: ApplicationSizeClass::Basic,
                invocation_timeout_ms: 3000,
            }),
        )
        .unwrap();
    store.commit(catalog).await.unwrap();
    let artifact = std::fs::read(&image).unwrap();
    let digest = content_digest(&artifact);
    store
        .upload_artifact(&application, &digest, artifact.into())
        .await
        .unwrap();
    let deployment: Deployment = serde_json::from_value(serde_json::json!({
        "manifest_version":1, "application":application, "id":"race",
        "handlers":[{"id":"class", "language":"javascript", "entrypoint":"main.mjs",
        "artifact_sha256":digest, "runtime_image_sha256":options.runtime.sha256,
        "worker_config":{"compatibility_date":"2026-03-01"}}]
    }))
    .unwrap();
    store.stage(&deployment).await.unwrap();
    let key = (
        application.clone(),
        deployment.id.clone(),
        deployment.handlers[0].id.clone(),
    );
    let mut catalog = store.read(&application).await.unwrap();
    store
        .activate(&mut catalog, deployment.clone())
        .await
        .unwrap();
    store.commit(catalog).await.unwrap();
    let (publish, startup) = tokio::sync::watch::channel(None);
    {
        let mut state = pool.state.lock().unwrap();
        state.snapshot.vms = 1;
        state.snapshot.memory_mib = u64::from(ApplicationSizeClass::Basic.resources().1);
        state.snapshot.starting = 1;
        state
            .slots
            .insert(key.clone(), Slot::Starting(startup.clone()));
    }
    let mut catalog = store.read(&application).await.unwrap();
    catalog
        .set_limits(
            &application,
            Some(ApplicationLimits {
                size_class: ApplicationSizeClass::Lite,
                invocation_timeout_ms: 3000,
            }),
        )
        .unwrap();
    store.commit(catalog).await.unwrap();
    VmPool::start(
        app.clone(),
        pool.config.clone(),
        store.clone(),
        pool.state.clone(),
        pool.observed.clone(),
        pool.changed.clone(),
        pool.templates.clone(),
        pool.pressure.clone(),
        key,
        publish,
        Default::default(),
        false,
        ApplicationSizeClass::Basic,
    )
    .await;
    assert!(
        matches!(startup.borrow().as_ref(), Some(Err(error)) if error.contains("size changed during admission"))
    );
    assert_eq!(
        (
            pool.snapshot().vms,
            pool.snapshot().memory_mib,
            pool.snapshot().starting
        ),
        (0, 0, 0)
    );
    assert!(
        owned_vm_pids().is_empty(),
        "changed class booted a VMM before refusing admission"
    );
    for class in ApplicationSizeClass::ALL {
        let mut selected = options.clone();
        selected.templates = pool.templates.clone();
        (selected.vcpus, selected.memory_mib, selected.cpu_quota) = class.resources();
        let handler = serde_json::from_value(serde_json::json!({
            "id": "class", "language": "javascript", "entrypoint": "main.mjs",
            "artifact_sha256": "0".repeat(64), "runtime_image_sha256": selected.runtime.sha256,
            "worker_config": {"compatibility_date": "2026-03-01"},
        }))
        .unwrap();
        let vm = Vm::launch_builtin(selected.clone(), handler, &image)
            .await
            .unwrap();
        assert_eq!(vm.path, "restore");
        let pids = owned_vm_pids();
        assert_eq!(pids.len(), 1);
        let pid = pids.into_iter().next().unwrap();
        let membership = std::fs::read_to_string(format!("/proc/{pid}/cgroup")).unwrap();
        let path = membership
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .expect("cgroup v2 membership");
        let quota = std::fs::read_to_string(
            Path::new("/sys/fs/cgroup")
                .join(path.trim_start_matches('/'))
                .join("cpu.max"),
        )
        .unwrap();
        assert_eq!(quota.trim(), format!("{} 100000", selected.cpu_quota));
        let response = vm
            .client()
            .invoke_stateless(
                app,
                Duration::from_secs(15),
                celld_applications::invocation_wire::HttpRequest {
                    method: "GET".into(),
                    url: "http://class/".into(),
                    headers: vec![],
                    body: vec![],
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(response.body, b"class ready");
        vm.shutdown().await.unwrap();
        assert!(owned_vm_pids().is_empty());
    }
    assert_eq!(
        original,
        states
            .iter()
            .map(|path| std::fs::metadata(path).unwrap().modified().unwrap())
            .collect::<Vec<_>>(),
        "request rebuilt a startup template"
    );
    pool.shutdown().await.unwrap();
    eprintln!("startup classes: all six captured serially in {startup_ms}ms; first launch of every class restored with exact per-VM cpu.max; no request-path builds, no VMM left");
}

#[cfg(test)]
async fn check_startup_pool(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    directory: &Path,
    first: &ApplicationId,
    owned_vm_pids: fn() -> HashSet<u32>,
) {
    use celld_applications::invocation_wire::HttpRequest;
    let previous_active = store
        .read(first)
        .await
        .unwrap()
        .application(first)
        .unwrap()
        .active
        .clone();
    const SOURCE: &str = r#"import { env } from 'cloudflare:workers';
let served = 0;
export default {
  fetch() {
    served += 1;
    return new Response(JSON.stringify({
      label: env.LABEL,
      served,
      random: Math.random(),
      uuid: crypto.randomUUID(),
      bytes: Array.from(crypto.getRandomValues(new Uint8Array(16))),
    }));
  },
};
"#;
    let source = directory.join("startup-pool-source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("main.mjs"), SOURCE).unwrap();
    let image = directory.join("startup-pool.bundle");
    let built = timeout(
        Duration::from_secs(30),
        Command::new("/usr/bin/python3")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../applications/guest/build-application-image.py"),
            )
            .arg(&source)
            .arg(&image)
            .env_clear()
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let artifact = std::fs::read(image).unwrap();
    let digest = content_digest(&artifact);
    // A second application with the first's limits (the check application
    // runs 1 vCPU VMs), and a third whose limits give its VMs another size, so
    // another template and another pool.
    let name = |value: &str| -> Name { value.to_owned().try_into().unwrap() };
    let application = |tenant: &str| ApplicationId {
        app: tenant.to_owned().try_into().unwrap(),
    };
    let (other, small) = (application("warm-other"), application("warm-small"));
    for (application, size_class) in [
        (&other, ApplicationSizeClass::Basic),
        (&small, ApplicationSizeClass::Lite),
    ] {
        let mut snapshot = store.read(application).await.unwrap();
        snapshot.create(application.clone()).unwrap();
        let limits = ApplicationLimits {
            size_class,
            invocation_timeout_ms: 3000,
        };
        snapshot.set_limits(application, Some(limits)).unwrap();
        store.commit(snapshot).await.unwrap();
    }
    for (application, id) in [
        (first, "warm-a"),
        (&other, "warm-b"),
        (first, "warm-c"),
        (&small, "warm-d"),
        (&small, "warm-e"),
    ] {
        store
            .upload_artifact(application, &digest, artifact.clone().into())
            .await
            .unwrap();
        let deployment: Deployment = serde_json::from_value(serde_json::json!({
            "manifest_version": 1,
            "application": application,
            "id": id,
            "handlers": [{
                "id": "javascript",
                "language": "javascript",
                "entrypoint": "main.mjs",
                "artifact_sha256": digest,
                "runtime_image_sha256": options.runtime.sha256,
                "environment": {"LABEL": id},
                "worker_config": {"compatibility_date": "2026-03-01"}
            }]
        }))
        .unwrap();
        store.stage(&deployment).await.unwrap();
        let mut snapshot = store.read(application).await.unwrap();
        store.activate(&mut snapshot, deployment).await.unwrap();
        store.commit(snapshot).await.unwrap();
    }
    let handler = name("javascript");
    let config = VmHostConfig {
        jailer: options.jailer.clone(),
        firecracker: options.firecracker.clone(),
        kernel: options.kernel.clone(),
        runtimes: vec![options.runtime.clone()],
        uid: options.uid,
        gid: options.gid,
        max_vms: 5,
        max_memory_mib: 12_288,
        idle_ttl_secs: 1,
        platform_domain: TEST_DOMAIN.into(),
        container_fence_image: None,
        artifact_cache_ttl_secs: None,
        metadata_cache_entries: None,
        metadata_cache_bytes: None,
        startup_pool: 2,
        memory_guard: None,
    };
    // The pool must leave room for a launch.
    for size in [5, 6] {
        let broken = VmHostConfig {
            startup_pool: size,
            ..config.clone()
        };
        assert!(VmPool::new(broken, store.clone(), NODE.into()).is_err());
    }
    let pool = VmPool::new(config.clone(), store.clone(), NODE.into()).unwrap();
    let pooled = |what: &str| pool.templates.status()[what].as_u64().unwrap();
    async fn until(mut done: impl FnMut() -> bool, what: &str) {
        timeout(Duration::from_secs(60), async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what}"));
    }
    let draw = |lease: &VmLease| {
        let (client, app) = (lease.client.clone(), app.clone());
        async move {
            let response = client
                .invoke_stateless(
                    &app,
                    Duration::from_secs(5),
                    HttpRequest {
                        method: "GET".into(),
                        url: "http://warm/".into(),
                        headers: vec![],
                        body: vec![],
                    },
                    None,
                )
                .await
                .unwrap();
            serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()
        }
    };
    // warm-a and warm-d were active before warm-c and warm-e replaced
    // them. This fixture measures capacity for already accepted generations.
    assert!(pool
        .acquire(app, first, &name("warm-a"), &handler)
        .await
        .err()
        .unwrap()
        .is::<DeploymentRetiring>());
    assert_eq!(pool.snapshot().vms, 0);
    let acquire = |application: &ApplicationId, deployment: &str| {
        let (application, deployment) = (application.clone(), name(deployment));
        let (pool, handler) = (&pool, handler.clone());
        async move {
            let began = Instant::now();
            let lease = pool
                .acquire_inner(app, &application, &deployment, &handler, true)
                .await
                .unwrap();
            (lease, began.elapsed())
        }
    };
    // The first launch builds the template and restores: a miss. The pool then
    // fills to two VMs without being asked, keeping a slot free.
    let (a, first_ms) = acquire(first, "warm-a").await;
    assert_eq!((pooled("hits"), pooled("misses")), (0, 1));
    until(|| pooled("idle") == 2, "the pool did not fill").await;
    assert_eq!(pool.snapshot().vms, 1);
    assert_eq!(owned_vm_pids().len(), 3);
    // Two applications claim the two waiting VMs: both hits.
    let (b, b_ms) = acquire(&other, "warm-b").await;
    until(
        || pooled("idle") == 2,
        "the pool did not refill after one claim",
    )
    .await;
    let (c, c_ms) = acquire(first, "warm-c").await;
    assert_eq!((pooled("hits"), pooled("misses")), (2, 1));
    let (drawn_a, drawn_b, drawn_c) = (draw(&a).await, draw(&b).await, draw(&c).await);
    assert_eq!(
        [&drawn_a, &drawn_b, &drawn_c].map(|drawn| drawn["label"].as_str().unwrap().to_owned()),
        ["warm-a", "warm-b", "warm-c"]
    );
    for field in ["random", "uuid", "bytes"] {
        assert!(
            drawn_a[field] != drawn_b[field]
                && drawn_b[field] != drawn_c[field]
                && drawn_a[field] != drawn_c[field],
            "pooled VMs share {field}"
        );
    }
    // State lives in the VM that served it: b's second request is its second,
    // c has served one, and a claimed VM is not waiting any more.
    assert_eq!(draw(&b).await["served"], 2);
    assert_eq!(
        (drawn_c["served"].clone(), draw(&c).await["served"].clone()),
        (1.into(), 2.into())
    );
    // The claimed VMs are replaced, but never into the last free slot.
    until(|| pooled("idle") == 1, "the pool did not refill").await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        (pooled("idle"), pooled("preparing"), pool.snapshot().vms),
        (1, 0, 3)
    );
    assert_eq!(owned_vm_pids().len(), 4);
    // Another VM size has no pool VM of its own: restores, and fills the node.
    let (d, d_ms) = acquire(&small, "warm-d").await;
    assert_eq!((pooled("hits"), pooled("misses")), (2, 2));
    assert_eq!(owned_vm_pids().len(), 5);
    // The node is full: the idle pool VM goes, not a refusal.
    let (e, e_ms) = acquire(&small, "warm-e").await;
    assert_eq!(pooled("idle"), 0);
    assert_eq!((pool.snapshot().vms, owned_vm_pids().len()), (5, 5));
    assert_eq!(draw(&e).await["label"], "warm-e");
    let status = pool.status();
    assert_eq!(status["startupPool"]["size"], 2);
    assert_eq!(
        status["startupPool"]["pool"]["templates"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert!(status["startupPool"]["pool"]["templates"]
        .as_array()
        .unwrap()
        .iter()
        .all(|template| template["state"] == "ready"));
    // With the node idle each template refills to two VMs, four of the five
    // slots, and a VM that breaks is replaced.
    drop((a, b, c, d, e));
    until(
        || pool.snapshot().vms == 0 && pooled("idle") == 4,
        "the idle node did not refill the pool",
    )
    .await;
    assert_eq!(owned_vm_pids().len(), 4);
    let victim = *owned_vm_pids().iter().next().unwrap();
    // SAFETY: SIGKILL to a VMM this test process owns.
    unsafe { libc::kill(victim as i32, libc::SIGKILL) };
    until(
        || !owned_vm_pids().contains(&victim) && pooled("idle") == 4 && owned_vm_pids().len() == 4,
        "a broken pool VM was not replaced",
    )
    .await;
    // Shutdown reaps the waiting VMs and stops the refill.
    timeout(Duration::from_secs(30), pool.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(owned_vm_pids().is_empty(), "pool VMs outlived the shutdown");
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        owned_vm_pids().is_empty(),
        "the pool refilled after the shutdown"
    );
    assert_eq!((pooled("idle"), pooled("preparing")), (0, 0));
    // Leave the shared fixture app's activation as this check found it.
    let mut snapshot = store.read(first).await.unwrap();
    if let Some(active) = previous_active {
        store.activate(&mut snapshot, active).await.unwrap();
    } else {
        snapshot.deactivate(first).unwrap();
    }
    store.commit(snapshot).await.unwrap();
    eprintln!(
        "startup pool: acquire ms: first {:.0} (builds the template), claimed from the pool {:.0} and {:.0}, another VM size (builds its template) {:.0}, evicting an idle pool VM to restore {:.0}; two applications' pooled VMs drew different randomness and kept their own state; the pool refilled without taking the last slot, replaced a killed VM, and shutdown left no VMM; actual VMs, no independent-host qualification",
        first_ms.as_secs_f64() * 1000.0,
        b_ms.as_secs_f64() * 1000.0,
        c_ms.as_secs_f64() * 1000.0,
        d_ms.as_secs_f64() * 1000.0,
        e_ms.as_secs_f64() * 1000.0,
    );
}

#[cfg(test)]
async fn check_template_retry(options: &LaunchOptions, directory: &Path) {
    let real = options.runtime.path.clone();
    let late = directory.join("late-runtime.ext4");
    std::fs::write(&late, b"not the approved image").unwrap();
    let options = LaunchOptions {
        runtime: ApprovedFile {
            path: late.clone(),
            sha256: options.runtime.sha256.clone(),
        },
        templates: Arc::new(Templates::new().unwrap()),
        ..options.clone()
    };
    let state = || options.templates.status()["templates"][0]["state"].clone();
    assert!(options.templates.get(&options).await.unwrap().is_none());
    assert_eq!(state(), "failed");
    // The image is repaired, but the backoff holds and launches boot cold.
    std::fs::remove_file(&late).unwrap();
    std::fs::copy(real, &late).unwrap();
    let began = Instant::now();
    assert!(options.templates.get(&options).await.unwrap().is_none());
    assert!(
        began.elapsed() < Duration::from_secs(1),
        "a retry came before its backoff"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(options.templates.get(&options).await.unwrap().is_some());
    assert_eq!(state(), "ready");
    options.templates.discard().await.unwrap();
    eprintln!("template retry: a failed build kept launches cold through its backoff and was rebuilt once the image was repaired; actual VM, no independent-host qualification");
}

#[cfg(test)]
async fn check_streams_and_egress(
    app: &crate::actor::AppHandle,
    store: &ApplicationStore,
    options: &LaunchOptions,
    directory: &Path,
    resource: &ResourceAddress,
) {
    use celld_applications::invocation_wire::HttpRequest;
    use futures_util::StreamExt;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    const SOURCE: &str = r#"import { env } from 'cloudflare:workers';
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function drain(body) {
  let total = 0;
  for await (const chunk of body) total += chunk.byteLength;
  return total;
}
export class Counter {
  constructor(ctx) { this.ctx = ctx; }
  async fetch(request) {
    // The fetch waits for this event's own held transaction to settle.
    this.ctx.waitUntil(this.ctx.storage.transaction(async (tx) => {
      await tx.put('egress-held', true);
      await sleep(300);
    }));
    const response = await fetch(env.EGRESS_URL + '/observe');
    return new Response(await response.text());
  }
}
export default {
  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (path === '/upload') return new Response(String(await drain(request.body)));
    if (path === '/upload-slow') {
      const reader = request.body.getReader();
      let total = (await reader.read()).value.byteLength;
      await sleep(400);
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        total += value.byteLength;
      }
      return new Response(String(total));
    }
    if (path === '/fetch-big') {
      const response = await fetch(env.EGRESS_URL + '/big');
      return new Response(response.status + ':' + await drain(response.body));
    }
    if (path === '/fetch-echo') {
      const response = await fetch(env.EGRESS_URL + '/echo', {
        method: 'POST', headers: { 'x-test': 'brokered' }, body: 'payload',
      });
      return new Response(await response.text());
    }
    if (path === '/fetch-abandon') {
      const reader = (await fetch(env.EGRESS_URL + '/endless')).body.getReader();
      await reader.read();
      await reader.cancel();
      return new Response('abandoned');
    }
    return new Response('egress');
  }
};
"#;
    // The host test network: what the guest reaches only through the broker.
    let observed = Arc::new(Mutex::new(Vec::new()));
    let endless_dropped = Arc::new(AtomicBool::new(false));
    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let router = {
        use axum::routing::{get, post};
        let observed = observed.clone();
        let endless_dropped = endless_dropped.clone();
        axum::Router::new()
            .route(
                "/big",
                get(|| async {
                    axum::body::Body::from_stream(futures_util::stream::iter(
                        (0..160)
                            .map(|_| Ok::<_, std::io::Error>(bytes::Bytes::from(vec![9u8; 65536]))),
                    ))
                }),
            )
            .route(
                "/echo",
                post(
                    |headers: axum::http::HeaderMap, body: bytes::Bytes| async move {
                        format!(
                            "{}:{}",
                            headers
                                .get("x-test")
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or(""),
                            String::from_utf8_lossy(&body)
                        )
                    },
                ),
            )
            .route(
                "/endless",
                get(move || {
                    let guard = Dropped(endless_dropped.clone());
                    async move {
                        axum::body::Body::from_stream(futures_util::stream::unfold(
                            guard,
                            |guard| async move {
                                tokio::time::sleep(Duration::from_millis(5)).await;
                                Some((
                                    Ok::<_, std::io::Error>(bytes::Bytes::from(vec![1u8; 1024])),
                                    guard,
                                ))
                            },
                        ))
                    }
                }),
            )
            .route(
                "/observe",
                get(move || {
                    let observed = observed.clone();
                    async move {
                        observed.lock().unwrap().push(Instant::now());
                        "observed"
                    }
                }),
            )
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let source = directory.join("egress-source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("main.mjs"), SOURCE).unwrap();
    let image = directory.join("egress.bundle");
    let built = timeout(
        Duration::from_secs(30),
        Command::new("/usr/bin/python3")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../applications/guest/build-application-image.py"),
            )
            .arg(&source)
            .arg(&image)
            .env_clear()
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let artifact = std::fs::read(image).unwrap();
    let digest = content_digest(&artifact);
    store
        .upload_artifact(&resource.application, &digest, artifact.into())
        .await
        .unwrap();
    let deployment: Deployment = serde_json::from_value(serde_json::json!({
        "manifest_version": 1,
        "application": resource.application,
        "id": "egress",
        "handlers": [{
            "id": "javascript",
            "language": "javascript",
            "entrypoint": "main.mjs",
            "artifact_sha256": digest,
            "runtime_image_sha256": options.runtime.sha256,
            "environment": {"EGRESS_URL": format!("http://{address}")},
            "secrets": {},
            "bindings": {"COUNTER": {"kind": "resource", "resource": resource.resource}},
            "worker_config": {"do_classes": ["Counter"], "compatibility_date": "2026-03-01"}
        }]
    }))
    .unwrap();
    store.stage(&deployment).await.unwrap();
    let vm = timeout(
        Duration::from_secs(30),
        Vm::launch(
            options.clone(),
            store,
            &resource.application,
            &deployment.id,
            &"javascript".to_owned().try_into().unwrap(),
        ),
    )
    .await
    .unwrap()
    .unwrap_or_else(|error| panic!("egress launch failed: {error:#}"));
    let client = vm.client();
    let request = |path: &str, method: &str| HttpRequest {
        method: method.into(),
        url: format!("http://egress{path}"),
        headers: vec![],
        body: vec![],
    };
    // 20 MiB upload in 64 KiB items, produced only on demand.
    let upload = |produced: Arc<AtomicUsize>| -> crate::js::HttpChunkStream {
        Box::pin(futures_util::stream::iter(0..320).then(move |_| {
            let produced = produced.clone();
            async move {
                produced.fetch_add(1, Ordering::AcqRel);
                Ok(vec![7u8; 65536])
            }
        }))
    };
    let text = |result: Result<crate::js::HttpResponse>| match result {
        Ok(response) => String::from_utf8(response.body).unwrap(),
        Err(error) => panic!("egress handler failed: {error:#}"),
    };
    let seconds = Duration::from_secs(20);
    let produced = Arc::new(AtomicUsize::new(0));
    let total = client
        .invoke_stateless(
            app,
            seconds,
            request("/upload", "POST"),
            Some(upload(produced.clone())),
        )
        .await;
    assert_eq!(text(total), (320 * 65536).to_string());
    assert_eq!(produced.load(Ordering::Acquire), 320);
    // Backpressure: while the handler sleeps after one item, the host
    // producer cannot run ahead beyond a few credited items.
    let produced = Arc::new(AtomicUsize::new(0));
    let slow = {
        let (client, app, body) = (client.clone(), app.clone(), upload(produced.clone()));
        tokio::spawn(async move {
            client
                .invoke_stateless(
                    &app,
                    Duration::from_secs(20),
                    request("/upload-slow", "POST"),
                    Some(body),
                )
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(250)).await;
    let ahead = produced.load(Ordering::Acquire);
    assert!(
        ahead < 8,
        "upload ran {ahead} items ahead of a paused handler"
    );
    assert_eq!(text(slow.await.unwrap()), (320 * 65536).to_string());
    // A client disconnect cancels the guest handler and stops the producer.
    let produced = Arc::new(AtomicUsize::new(0));
    let cancelled = client
        .invoke_stateless(
            app,
            Duration::from_millis(150),
            request("/upload-slow", "POST"),
            Some(upload(produced.clone())),
        )
        .await;
    assert!(format!("{:#}", cancelled.err().unwrap()).contains("deadline"));
    tokio::time::sleep(Duration::from_millis(600)).await;
    let stopped = produced.load(Ordering::Acquire);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(stopped < 8 && produced.load(Ordering::Acquire) == stopped);
    // Brokered download above the frame limit and request/headers forwarding.
    let none = || None::<crate::js::HttpChunkStream>;
    assert_eq!(
        text(
            client
                .invoke_stateless(app, seconds, request("/fetch-big", "GET"), none())
                .await
        ),
        format!("200:{}", 160 * 65536)
    );
    assert_eq!(
        text(
            client
                .invoke_stateless(app, seconds, request("/fetch-echo", "GET"), none())
                .await
        ),
        "brokered:payload"
    );
    // A guest that cancels a fetch body stops the host's outbound request.
    assert_eq!(
        text(
            client
                .invoke_stateless(app, seconds, request("/fetch-abandon", "GET"), none())
                .await
        ),
        "abandoned"
    );
    timeout(Duration::from_secs(5), async {
        while !endless_dropped.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("abandoned fetch kept its outbound request open");
    // Unproved object state cannot escape: the outbound request waits for the
    // event's own held transaction, as its response would.
    let started = Instant::now();
    let completion = client
        .invoke(
            app.clone(),
            resource.clone(),
            "Counter".into(),
            "egress-object".into(),
            seconds,
            request("/", "GET"),
        )
        .await
        .unwrap();
    assert_eq!(
        String::from_utf8(completion.result.as_ref().unwrap().body.clone()).unwrap(),
        "observed"
    );
    drop(completion);
    let arrival = observed.lock().unwrap()[0];
    assert!(
        arrival.duration_since(started) >= Duration::from_millis(280),
        "outbound request left before the event's transaction settled"
    );
    timeout(Duration::from_secs(10), async {
        while app.public_in_flight() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(vm.shutdown().await.unwrap().reason, "shutdown");
    server.abort();
    eprintln!("owned JavaScript streams and egress: 20 MiB uploads streamed in credited 64 KiB items with the host producer held to a few items ahead of a paused handler; a client disconnect stopped both the guest handler and the producer; a 10 MiB brokered download, forwarded request body/headers and a guest-cancelled fetch body (closing the outbound request) worked; an object event's outbound request waited for its own held transaction; actual VM, host test network, no independent-host qualification");
}

#[cfg(test)]
async fn check_object_bindings(
    app: &crate::actor::AppHandle,
    store: &ApplicationStore,
    options: &LaunchOptions,
    directory: &Path,
    resource: &ResourceAddress,
) {
    use celld_applications::invocation_wire::HttpRequest;
    const SOURCE: &str = r#"import { DurableObject, env } from 'cloudflare:workers';
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
export class Counter extends DurableObject {
  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (path === '/stamp') return new Response(String(Date.now()));
    if (path === '/id') return new Response(this.ctx.id.toString());
    if (path === '/relay') {
      // Unproved state: the call to the other object waits for this write.
      const to = new URL(request.url).searchParams.get('to');
      const started = Date.now();
      this.ctx.waitUntil(this.ctx.storage.transaction(async tx => { await tx.put('held', true); await sleep(300); }));
      const stamp = await (await env.COUNTER.getByName(to).fetch('http://object/stamp')).text();
      return new Response(String(Number(stamp) - started));
    }
    const count = (await this.ctx.storage.get('count') ?? 0) + 1;
    await this.ctx.storage.put('count', count);
    return new Response(`${this.ctx.id.name ?? 'unique'}:${count}:${env.APP_LABEL}`);
  }
  async add(a, b) { return { sum: a + b, name: this.ctx.id.name ?? null }; }
  async count() { return (await this.ctx.storage.get('count')) ?? 0; }
  async fail() { const error = new RangeError('rpc failed'); error.code = 7; throw error; }
}
export class Alternate extends Counter {}
export default {
  async fetch(request) {
    const url = new URL(request.url);
    const name = url.searchParams.get('name');
    const stub = env.COUNTER.getByName(name);
    const text = async (stub) => (await stub.fetch('http://object/bump')).text();
    switch (url.pathname) {
      case '/bump': return new Response(await text(stub));
      case '/id': return new Response(env.COUNTER.idFromName(name).toString());
      case '/alternate': return new Response(await text(env.SECOND.getByName(name)));
      case '/alternate-id': return new Response(env.SECOND.idFromName(name).toString());
      case '/wrong-class':
        try { env.SECOND.idFromString(url.searchParams.get('id')); return new Response('accepted'); }
        catch (error) { return new Response(error.name + ':' + error.message); }
      case '/foreign':
        try { env.COUNTER.idFromString(url.searchParams.get('id')); return new Response('accepted'); }
        catch (error) { return new Response(error.name + ':' + error.message); }
      case '/rpc': return Response.json({ add: await stub.add(2, 3), count: await stub.count() });
      case '/fail':
        try { await stub.fail(); return new Response('returned'); }
        catch (error) {
          return Response.json({ name: error.name, message: error.message, code: error.code, remote: error.remote, range: error instanceof RangeError });
        }
      case '/missing':
        try { await stub.nothing(); return new Response('returned'); }
        catch (error) { return new Response(error.name + ':' + error.message); }
      case '/unique': {
        const id = env.COUNTER.newUniqueId();
        const first = await text(env.COUNTER.get(id));
        const again = env.COUNTER.get(env.COUNTER.idFromString(id.toString()));
        return Response.json({ first, second: await text(again), add: await again.add(1, 1),
          same: await (await again.fetch('http://object/id')).text() === id.toString() });
      }
    }
    return new Response('objects');
  }
};
"#;
    let source = directory.join("objects-source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("main.mjs"), SOURCE).unwrap();
    let image = directory.join("objects.bundle");
    let built = timeout(
        Duration::from_secs(30),
        Command::new("/usr/bin/python3")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../applications/guest/build-application-image.py"),
            )
            .arg(&source)
            .arg(&image)
            .env_clear()
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let artifact = std::fs::read(image).unwrap();
    let digest = content_digest(&artifact);
    // The same resource and object names in a second tenant.
    let other = ApplicationId {
        app: "objects-other".to_owned().try_into().unwrap(),
    };
    let mut snapshot = store.read(&other).await.unwrap();
    snapshot.create(other.clone()).unwrap();
    snapshot
        .register_resource(
            &other,
            crate::applications::Resource {
                resource: resource.resource.clone(),
                configuration: serde_json::json!({}),
            },
        )
        .unwrap();
    store.commit(snapshot).await.unwrap();
    let alternate = ResourceRef {
        kind: celld_applications::ResourceKind::DurableObject,
        id: "alternate".to_owned().try_into().unwrap(),
    };
    for application in [&resource.application, &other] {
        let mut snapshot = store.read(application).await.unwrap();
        snapshot
            .register_resource(
                application,
                crate::applications::Resource {
                    resource: alternate.clone(),
                    configuration: serde_json::json!({}),
                },
            )
            .unwrap();
        store.commit(snapshot).await.unwrap();
    }
    let launch = |application: ApplicationId, label: &'static str| {
        let (artifact, digest, options) = (artifact.clone(), digest.clone(), options.clone());
        let resource = resource.resource.clone();
        let alternate = alternate.clone();
        async move {
            store
                .upload_artifact(&application, &digest, artifact.into())
                .await
                .unwrap();
            let deployment: Deployment = serde_json::from_value(serde_json::json!({
                "manifest_version": 1,
                "application": application,
                "id": format!("objects-{}", label.to_lowercase()),
                "handlers": [{
                    "id": "javascript",
                    "language": "javascript",
                    "entrypoint": "main.mjs",
                    "artifact_sha256": digest,
                    "runtime_image_sha256": options.runtime.sha256,
                    "environment": {"APP_LABEL": label},
                    "secrets": {},
                    "bindings": {"COUNTER": {"kind": "resource", "resource": resource}, "SECOND": {"kind": "resource", "resource": alternate}},
                    "worker_config": {"do_classes": ["Counter", "Alternate"], "durable_objects": {"bindings": [{"name":"COUNTER","class_name":"Counter"},{"name":"SECOND","class_name":"Alternate"}]}, "compatibility_date": "2026-03-01", "compatibility_flags": ["js_rpc"]}
                }]
            }))
            .unwrap();
            store
                .stage(&deployment)
                .await
                .unwrap_or_else(|error| panic!("staging {label} failed: {error:#}"));
            timeout(
                Duration::from_secs(30),
                Vm::launch(
                    options,
                    store,
                    &application,
                    &deployment.id,
                    &"javascript".to_owned().try_into().unwrap(),
                ),
            )
            .await
            .unwrap()
            .unwrap_or_else(|error| panic!("object binding launch failed: {error:#}"))
        }
    };
    let call = |vm: &Vm, path: &str| {
        let (client, app) = (vm.client(), app.clone());
        let url = format!("http://objects{path}");
        async move {
            let response = client
                .invoke_stateless(
                    &app,
                    Duration::from_secs(20),
                    HttpRequest {
                        method: "GET".into(),
                        url,
                        headers: vec![],
                        body: vec![],
                    },
                    None,
                )
                .await
                .unwrap_or_else(|error| panic!("object call failed: {error:#}"));
            String::from_utf8(response.body).unwrap()
        }
    };
    let idle = || async {
        timeout(Duration::from_secs(10), async {
            while app.public_in_flight() != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("object work did not settle");
    };
    let first = launch(resource.application.clone(), "First-V1").await;
    let second = launch(other.clone(), "Other-V1").await;
    // Named objects, by fetch; the same name in two tenants is two objects.
    assert_eq!(call(&first, "/bump?name=shared").await, "shared:1:First-V1");
    assert_eq!(call(&first, "/bump?name=shared").await, "shared:2:First-V1");
    assert_eq!(
        call(&second, "/bump?name=shared").await,
        "shared:1:Other-V1"
    );
    // Neither the id nor a name aliases across tenants.
    let (id, foreign) = (
        call(&first, "/id?name=shared").await,
        call(&second, "/id?name=shared").await,
    );
    assert_ne!(id, foreign);
    // Explicit class bindings keep same-named objects in two namespaces distinct.
    assert_ne!(id, call(&first, "/alternate-id?name=shared").await);
    assert_eq!(
        call(&first, "/alternate?name=shared").await,
        "shared:1:First-V1"
    );
    assert_eq!(
        call(&second, "/alternate?name=shared").await,
        "shared:1:Other-V1"
    );
    let wrong_class = call(&first, &format!("/wrong-class?id={id}")).await;
    assert!(
        wrong_class.starts_with("TypeError:")
            && wrong_class.contains("not valid for this namespace"),
        "{wrong_class}"
    );
    let refused = call(&second, &format!("/foreign?id={id}")).await;
    assert!(
        refused.starts_with("TypeError:") && refused.contains("not valid for this namespace"),
        "{refused}"
    );
    // RPC values, and a thrown error that keeps its class and properties.
    let rpc: serde_json::Value =
        serde_json::from_str(&call(&first, "/rpc?name=shared").await).unwrap();
    assert_eq!(
        rpc,
        serde_json::json!({"add": {"sum": 5, "name": "shared"}, "count": 2})
    );
    let failure: serde_json::Value =
        serde_json::from_str(&call(&first, "/fail?name=shared").await).unwrap();
    assert_eq!(
        failure,
        serde_json::json!({"name": "RangeError", "message": "rpc failed", "code": 7, "remote": true, "range": true})
    );
    assert_eq!(
        call(&first, "/missing?name=shared").await,
        "TypeError:nothing is not a function"
    );
    // A unique id reaches one nameless object, whose own id is the caller's.
    let unique: serde_json::Value = serde_json::from_str(&call(&first, "/unique").await).unwrap();
    assert_eq!(
        unique,
        serde_json::json!({"first": "unique:1:First-V1", "second": "unique:2:First-V1", "add": {"sum": 2, "name": null}, "same": true})
    );
    // An object event calls another object only after its own held write.
    let relayed = first
        .client()
        .invoke(
            app.clone(),
            resource.clone(),
            "Counter".into(),
            "caller".into(),
            Duration::from_secs(10),
            HttpRequest {
                method: "GET".into(),
                url: "http://objects/relay?to=callee".into(),
                headers: vec![],
                body: vec![],
            },
        )
        .await
        .unwrap();
    let waited: i64 = String::from_utf8(relayed.result.as_ref().unwrap().body.clone())
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        waited >= 280,
        "call left after {waited} ms, before the held write"
    );
    drop(relayed);
    idle().await;
    // Replacement keeps the objects: the new deployment serves the same data.
    assert_eq!(first.shutdown().await.unwrap().reason, "shutdown");
    let replacement = launch(resource.application.clone(), "First-V2").await;
    assert_eq!(
        call(&replacement, "/alternate?name=shared").await,
        "shared:2:First-V2"
    );
    assert_eq!(
        call(&replacement, "/bump?name=shared").await,
        "shared:3:First-V2"
    );
    assert_eq!(
        call(&second, "/bump?name=shared").await,
        "shared:2:Other-V1"
    );
    idle().await;
    for vm in [replacement, second] {
        assert_eq!(vm.shutdown().await.unwrap().reason, "shutdown");
    }
    eprintln!("owned JavaScript object bindings: stateless and object events called named and unique Durable Objects by fetch and RPC through the host's namespace grants in actual VMs; RPC values, thrown errors, unique ids and ctx.id matched original Celld; the same names in two tenants stayed distinct and a foreign id was refused; an object event's outbound call waited for its held write; a replacement deployment served the retained objects; local ownership only, no remote peer or independent-host qualification");
}

#[cfg(test)]
/// The class is `Counter`: this test host installs no hosted dispatcher, so object
/// activation needs a class the host check Worker exports (as `check_container_bindings`).
async fn check_hibernating_sockets(
    app: &crate::actor::AppHandle,
    store: &ApplicationStore,
    options: &LaunchOptions,
    directory: &Path,
    resource: &ResourceAddress,
) {
    use celld_applications::invocation_wire::{HttpRequest, ObjectTarget, SocketEvent};
    const SOURCE: &str = r#"import { DurableObject, env } from 'cloudflare:workers';
// An outbound socket the host dials: text and a binary frame above one credit's
// chunk bound echoed back, then a close handshake.
const dialPeer = async (label) => {
  const ws = new WebSocket('wss://peer.invalid/' + label, ['chat']);
  const seen = [];
  const code = await new Promise((resolve, reject) => {
    ws.addEventListener('open', () => ws.send('hello'));
    ws.addEventListener('message', (event) => {
      seen.push(typeof event.data === 'string' ? event.data : event.data.byteLength);
      if (seen.length === 1) ws.send(new Uint8Array(100000)); else ws.close(1000, 'done');
    });
    ws.addEventListener('close', (event) => resolve(event.code));
    ws.addEventListener('error', () => reject(new Error('socket error')));
  });
  return { protocol: ws.protocol, seen, code };
};
export class Counter extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping', 'pong'));
  }
  async fetch(request) {
    const url = new URL(request.url);
    if (url.searchParams.has('dial')) return Response.json(await dialPeer('object'));
    if (url.searchParams.has('peek')) {
      return new Response(`${(await this.ctx.storage.get('messages')) ?? 0}:${(await this.ctx.storage.get('closed')) ?? ''}:${this.ctx.getWebSockets().length}`);
    }
    if (request.headers.get('upgrade') !== 'websocket') return new Response('no upgrade', { status: 426 });
    const [client, server] = Object.values(new WebSocketPair());
    const tag = url.searchParams.get('tag') ?? 'room';
    this.ctx.acceptWebSocket(server, [tag]);
    server.serializeAttachment({ joined: tag, n: 0 });
    // Frames sent before the 101 leaves wait for the client's connection.
    server.send('welcome:' + this.ctx.getWebSockets().length);
    // An accepted socket no upgrade returns is released with its event.
    if (url.searchParams.has('abandon')) return new Response(String(server._id));
    return new Response(null, { status: 101, webSocket: client });
  }
  async webSocketMessage(ws, message) {
    const attachment = ws.deserializeAttachment();
    attachment.n += 1;
    ws.serializeAttachment(attachment);
    const count = ((await this.ctx.storage.get('messages')) ?? 0) + 1;
    await this.ctx.storage.put('messages', count);
    if (message === 'all') {
      for (const peer of this.ctx.getWebSockets(attachment.joined)) peer.send('broadcast:' + attachment.joined);
    } else if (message === 'close') {
      ws.close(1000, 'bye');
    } else {
      ws.send('echo:' + message + ':' + attachment.n + ':' + count + ':' + this.ctx.getTags(ws).join(','));
    }
  }
  async webSocketClose(ws, code, reason, wasClean) {
    await this.ctx.storage.put('closed', code + ':' + reason + ':' + wasClean);
  }
}
export default {
  async fetch(request) {
    if (new URL(request.url).pathname === '/dial') return Response.json(await dialPeer('stateless'));
    const name = new URL(request.url).searchParams.get('name') ?? 'room';
    return env.ROOM.getByName(name).fetch(request);
  }
};
"#;
    let source = directory.join("sockets-source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("main.mjs"), SOURCE).unwrap();
    let image = directory.join("sockets.bundle");
    let built = timeout(
        Duration::from_secs(30),
        Command::new("/usr/bin/python3")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../applications/guest/build-application-image.py"),
            )
            .arg(&source)
            .arg(&image)
            .env_clear()
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let artifact = std::fs::read(image).unwrap();
    let digest = content_digest(&artifact);
    let application = resource.application.clone();
    store
        .upload_artifact(&application, &digest, artifact.into())
        .await
        .unwrap();
    let deployment: Deployment = serde_json::from_value(serde_json::json!({
        "manifest_version": 1,
        "application": application,
        "id": "sockets",
        "handlers": [{
            "id": "javascript",
            "language": "javascript",
            "entrypoint": "main.mjs",
            "artifact_sha256": digest,
            "runtime_image_sha256": options.runtime.sha256,
            "environment": {},
            "secrets": {},
            "bindings": {"ROOM": {"kind": "resource", "resource": resource.resource}},
            "worker_config": {"do_classes": ["Counter"], "durable_objects": {"bindings": [{"name":"ROOM","class_name":"Counter"}]}, "compatibility_date": "2026-03-01", "compatibility_flags": ["js_rpc"]}
        }]
    }))
    .unwrap();
    store.stage(&deployment).await.unwrap();
    let launch = || async {
        timeout(
            Duration::from_secs(30),
            Vm::launch(
                options.clone(),
                store,
                &application,
                &deployment.id,
                &"javascript".to_owned().try_into().unwrap(),
            ),
        )
        .await
        .unwrap()
        .unwrap_or_else(|error| panic!("socket launch failed: {error:#}"))
    };
    let request = |query: &str, upgrade: bool| HttpRequest {
        method: "GET".into(),
        url: format!("http://sockets/?{query}"),
        headers: if upgrade {
            vec![("upgrade".into(), "websocket".into())]
        } else {
            vec![]
        },
        body: vec![],
    };
    let upgrade = |vm: &Vm, query: &'static str| {
        let (client, app) = (vm.client(), app.clone());
        let request = request(query, true);
        async move {
            let response = client
                .invoke_stateless(&app, Duration::from_secs(20), request, None)
                .await
                .unwrap_or_else(|error| panic!("upgrade failed: {error:#}"));
            assert_eq!(response.status, 101);
            let Some(crate::js::HttpResponseWebSocket::Guest(socket)) = response.websocket else {
                panic!("upgrade carried no host socket");
            };
            let target = socket.into_target();
            // What the node's socket task does with the connection.
            let (frames, receiver) = tokio::sync::mpsc::unbounded_channel();
            crate::js::ws_register(target.id, frames);
            (target, receiver)
        }
    };
    async fn next(
        step: &str,
        receiver: &mut tokio::sync::mpsc::UnboundedReceiver<crate::js::WsOut>,
    ) -> crate::js::WsOut {
        timeout(Duration::from_secs(10), receiver.recv())
            .await
            .unwrap_or_else(|_| panic!("no frame arrived: {step}"))
            .unwrap_or_else(|| panic!("socket closed: {step}"))
    }
    // A response relayed from an object by the stateless handler arrives as a stream.
    async fn body_of(response: crate::js::HttpResponse) -> String {
        let mut body = response.body;
        if let Some(mut stream) = response.stream {
            use futures_util::StreamExt;
            while let Some(chunk) = stream.next().await {
                body.extend(chunk.unwrap());
            }
        }
        String::from_utf8(body).unwrap()
    }
    let text = |out: crate::js::WsOut| match out {
        crate::js::WsOut::Text(text) => text,
        _ => panic!("expected a text frame"),
    };
    let object = |scope: &str| {
        let (class, id) = scope.split_once(':').unwrap();
        ObjectTarget {
            class: class.into(),
            id: id.into(),
            name: None,
        }
    };
    let event = |vm: &Vm, target: &crate::js::WsTarget, event: SocketEvent, body: &str| {
        let (client, app) = (vm.client(), app.clone());
        let (object, ws) = (object(&target.scope), target.id);
        let body = body.as_bytes().to_vec();
        async move {
            let completion = client
                .transport
                .invoke_socket(app, object, ws, event, body)
                .await
                .unwrap_or_else(|error| panic!("socket event failed: {error:#}"));
            completion.result.unwrap();
        }
    };
    let peek = |vm: &Vm| {
        let (client, app) = (vm.client(), app.clone());
        let request = request("peek=1", false);
        async move {
            let response = client
                .invoke_stateless(&app, Duration::from_secs(20), request, None)
                .await
                .unwrap();
            body_of(response).await
        }
    };
    let vm = launch().await;
    // Outbound sockets: the node's connector dials; a scripted peer stands in for it.
    let peer = crate::application_guest_transport::tests::install_scripted_peer();
    for (path, label) in [
        ("dial=1".to_owned(), "object"),
        ("dial".to_owned(), "stateless"),
    ] {
        let (client, app) = (vm.client(), app.clone());
        let request = HttpRequest {
            method: "GET".into(),
            url: if label == "object" {
                format!("http://sockets/?{path}&name=dialer")
            } else {
                "http://sockets/dial".into()
            },
            headers: vec![],
            body: vec![],
        };
        let response = client
            .invoke_stateless(&app, Duration::from_secs(30), request, None)
            .await
            .unwrap_or_else(|error| panic!("{label} dial failed: {error:#}"));
        let mut body = response.body;
        if let Some(mut stream) = response.stream {
            use futures_util::StreamExt;
            while let Some(chunk) = stream.next().await {
                body.extend(chunk.unwrap());
            }
        }
        let dialed: serde_json::Value = serde_json::from_slice(&body)
            .unwrap_or_else(|_| panic!("{label} dial reply {:?}", String::from_utf8_lossy(&body)));
        assert_eq!(
            dialed,
            serde_json::json!({"protocol": "chat", "seen": ["echo:hello", 100000], "code": 1000}),
            "{label}"
        );
    }
    assert!(
        peer.closed
            .lock()
            .unwrap()
            .iter()
            .all(|(_, code)| *code == 1000 || *code == 1001),
        "a dialed socket closed abnormally"
    );
    let (first, mut first_frames) = upgrade(&vm, "tag=a").await;
    // The frame sent before the upgrade left is delivered once the client connects.
    assert_eq!(
        text(next("first socket welcome", &mut first_frames).await),
        "welcome:1"
    );
    let (second, mut second_frames) = upgrade(&vm, "tag=b").await;
    assert_eq!(first.scope, second.scope);
    assert_eq!(
        text(next("second socket welcome", &mut second_frames).await),
        "welcome:2"
    );

    // A message reaches `webSocketMessage` on the pinned object; the reply, the
    // attachment, the stored count and the tag all come from the original API.
    event(&vm, &first, SocketEvent::Text, "hi").await;
    assert_eq!(
        text(next("echo of the first message", &mut first_frames).await),
        "echo:hi:1:1:a"
    );
    // A tagged lookup broadcasts to its own tag only.
    event(&vm, &second, SocketEvent::Text, "all").await;
    assert_eq!(
        text(next("tagged broadcast", &mut second_frames).await),
        "broadcast:b"
    );
    assert!(first_frames.try_recv().is_err(), "broadcast crossed tags");
    // The auto-response is the shell's: the guest never wakes for it.
    let before = vm.client().transport.pending();
    assert_eq!(
        crate::js::ws_auto_response(&first.scope, first.id, "ping").as_deref(),
        Some("pong")
    );
    assert_eq!(vm.client().transport.pending(), before);
    assert_eq!(peek(&vm).await, "2::2");

    // Guest eviction: the sockets stay with the host, and the next message
    // wakes a cold VM that rebuilds the socket from the registry's tags and
    // attachment (the count it kept in the first VM continues).
    assert_eq!(vm.shutdown().await.unwrap().reason, "shutdown");
    assert_eq!(
        crate::js::ws_scope_of(first.id).as_deref(),
        Some(first.scope.as_str())
    );
    let vm = launch().await;
    event(&vm, &first, SocketEvent::Text, "again").await;
    assert_eq!(
        text(next("echo after the cold VM rebind", &mut first_frames).await),
        "echo:again:2:3:a"
    );

    // A frame is released only after the event's own writes are proved; the
    // close frame is the object's `ws.close()`.
    event(&vm, &first, SocketEvent::Text, "close").await;
    assert!(matches!(
        next("object close frame", &mut first_frames).await,
        crate::js::WsOut::Close(1000, reason) if reason == "bye"
    ));
    event(
        &vm,
        &first,
        SocketEvent::Close {
            code: 1000,
            reason: "bye".into(),
            was_clean: true,
        },
        "",
    )
    .await;
    assert_eq!(peek(&vm).await, "4:1000:bye:true:2");

    // An accepted socket whose upgrade is never returned is released with its event.
    let abandoned = {
        let response = vm
            .client()
            .invoke_stateless(
                app,
                Duration::from_secs(20),
                request("abandon=1", true),
                None,
            )
            .await
            .unwrap();
        assert_eq!(response.status, 200);
        body_of(response).await.parse::<u64>().unwrap()
    };
    assert!(
        crate::js::ws_scope_of(abandoned).is_none(),
        "an unclaimed socket outlived its event"
    );
    for socket in [first, second] {
        app.websocket_closed(socket.scope.clone(), socket.id);
        crate::js::ws_unregister(socket.id);
    }
    assert_eq!(vm.shutdown().await.unwrap().reason, "shutdown");
    eprintln!("owned JavaScript hibernatable sockets: an upgrade accepted by an object in an actual VM became a host-registry socket; messages, tags, attachments and the original auto-response (without waking the guest) worked; frames were proved and released in order; a cold VM reattached the socket after eviction; an unclaimed accept was released; outbound sockets dialed through the host's connector worked from a handler and an object event; no WebSocket client, peer node or independent-host qualification");
}

#[cfg(test)]
async fn check_d1_bindings(
    app: &crate::actor::AppHandle,
    store: &ApplicationStore,
    options: &LaunchOptions,
    directory: &Path,
    _first: &ApplicationId,
) {
    use celld_applications::invocation_wire::HttpRequest;
    const SOURCE: &str = r#"import { env } from 'cloudflare:workers';
const caught = async (work) => {
  try { return { returned: await work() }; }
  catch (error) { return { name: error.name, message: error.message, cause: error.cause && error.cause.message }; }
};
const insert = (id, label) => env.DB.prepare('INSERT INTO t (id, label) VALUES (?, ?)').bind(id, label);
const next = () => env.DB.prepare('UPDATE c SET n = n + 1 RETURNING n').first('n');
export default {
  async fetch(request) {
    const url = new URL(request.url);
    const db = env.DB;
    switch (url.pathname) {
      case '/setup':
        await db.exec('CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, label TEXT)');
        await db.exec('CREATE TABLE IF NOT EXISTS c (n INTEGER)');
        await db.exec('INSERT INTO c (n) SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM c)');
        return Response.json('ready');
      case '/types': {
        await db.exec('CREATE TABLE ty (n INTEGER, r REAL, s TEXT, b BLOB, z)');
        const written = await db.prepare('INSERT INTO ty VALUES (?, ?, ?, ?, ?)')
          .bind(9007199254740, 1.5, 'café', new Uint8Array([0, 1, 255]), null).run();
        const read = db.prepare('SELECT * FROM ty');
        return Response.json({
          changes: written.meta.changes,
          first: await read.first(),
          all: (await read.all()).results,
          raw: await read.raw({ columnNames: true }),
          column: await read.first('s'),
          kinds: await db.prepare('SELECT typeof(n), typeof(r), typeof(s), typeof(b), typeof(z) FROM ty').raw(),
          none: await db.prepare('SELECT * FROM ty WHERE n = ?').bind(1).first(),
          noColumn: await caught(() => read.first('nope')),
          unsupported: await caught(async () => db.prepare('SELECT ?').bind({})),
          syntax: await caught(() => db.prepare('SELEC 1').run()),
        });
      }
      case '/batch-ok': {
        const results = await db.batch([insert(1, 'one'), insert(2, 'two'), db.prepare('SELECT count(*) AS n FROM t')]);
        return Response.json(results.map((result) => [result.success, result.meta.changes, result.results]));
      }
      case '/batch-fail': {
        const failed = await caught(() => db.batch([insert(3, 'three'), insert(1, 'duplicate')]));
        return Response.json({ failed, rows: await db.prepare('SELECT id FROM t ORDER BY id').raw() });
      }
      case '/session': {
        const session = db.withSession();
        const before = session.getBookmark();
        await session.prepare('SELECT 1').first();
        const batch = db.withSession('first-primary');
        await batch.batch([db.prepare('SELECT 1')]);
        return Response.json({ before, after: session.getBookmark(), batch: batch.getBookmark(),
          invalid: await caught(async () => db.withSession(' ')) });
      }
      case '/next': return Response.json(await next());
      case '/large-input': {
        const values = await db.batch(Array.from({ length: 5 }, (_, index) =>
          db.prepare('SELECT length(?) AS n').bind(String(index).repeat(1800000))));
        const larger = await db.batch(Array.from({ length: 10 }, (_, index) =>
          db.prepare('SELECT length(?) AS n').bind(String(index).repeat(1800000))));
        return Response.json({ values: values.map(value => value.results[0].n), larger: larger.map(value => value.results[0].n) });
      }
      case '/large': {
        const { results } = await db.prepare("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<20) SELECT i, replace(hex(zeroblob(900000)), '00', 'x') AS value FROM n ORDER BY i").all();
        return Response.json(results.map(({ i, value }) => [i, value.length, value.slice(0, 4), value.slice(-4)]));
      }
      case '/burst': return Response.json(await Promise.all(Array.from({ length: 6 }, next)));
      case '/rows': return Response.json(await db.prepare('SELECT id, label FROM t ORDER BY id').raw());
      case '/own': await insert(1, env.APP_LABEL).run(); return Response.json('ok');
    }
    return new Response('d1');
  }
};
"#;
    let source = directory.join("d1-source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("main.mjs"), SOURCE).unwrap();
    let image = directory.join("d1.bundle");
    let built = timeout(
        Duration::from_secs(30),
        Command::new("/usr/bin/python3")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../applications/guest/build-application-image.py"),
            )
            .arg(&source)
            .arg(&image)
            .env_clear()
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let artifact = std::fs::read(image).unwrap();
    let digest = content_digest(&artifact);
    // A second tenant with a same-named database.
    // Keep D1 qualification independent of the deliberately short limit used
    // by the preceding VM-limit check on its application.
    let first = ApplicationId {
        app: "databases".to_owned().try_into().unwrap(),
    };
    let other = ApplicationId {
        app: "d1-other".to_owned().try_into().unwrap(),
    };
    let database = ResourceRef {
        kind: celld_applications::ResourceKind::D1,
        id: "db".to_owned().try_into().unwrap(),
    };
    for application in [&first, &other] {
        let mut snapshot = store.read(application).await.unwrap();
        snapshot.create(application.clone()).unwrap();
        snapshot
            .register_resource(
                application,
                crate::applications::Resource {
                    resource: database.clone(),
                    configuration: serde_json::json!({}),
                },
            )
            .unwrap();
        store.commit(snapshot).await.unwrap();
    }
    let launch = |application: ApplicationId, label: &'static str| {
        let (artifact, digest, options) = (artifact.clone(), digest.clone(), options.clone());
        let database = database.clone();
        async move {
            store
                .upload_artifact(&application, &digest, artifact.into())
                .await
                .unwrap();
            let deployment: Deployment = serde_json::from_value(serde_json::json!({
                "manifest_version": 1,
                "application": application,
                "id": format!("d1-{}", label.to_lowercase()),
                "handlers": [{
                    "id": "javascript",
                    "language": "javascript",
                    "entrypoint": "main.mjs",
                    "artifact_sha256": digest,
                    "runtime_image_sha256": options.runtime.sha256,
                    "environment": {"APP_LABEL": label},
                    "secrets": {},
                    "bindings": {"DB": {"kind": "resource", "resource": database}},
                    "worker_config": {"compatibility_date": "2026-03-01"}
                }]
            }))
            .unwrap();
            store
                .stage(&deployment)
                .await
                .unwrap_or_else(|error| panic!("staging {label} failed: {error:#}"));
            timeout(
                Duration::from_secs(30),
                Vm::launch(
                    options,
                    store,
                    &application,
                    &deployment.id,
                    &"javascript".to_owned().try_into().unwrap(),
                ),
            )
            .await
            .unwrap()
            .unwrap_or_else(|error| panic!("D1 binding launch failed: {error:#}"))
        }
    };
    let call = |vm: &Vm, path: &str| {
        let (client, app) = (vm.client(), app.clone());
        let url = format!("http://d1{path}");
        async move {
            let response = client
                .invoke_stateless(
                    &app,
                    Duration::from_secs(20),
                    HttpRequest {
                        method: "GET".into(),
                        url: url.clone(),
                        headers: vec![],
                        body: vec![],
                    },
                    None,
                )
                .await
                .unwrap_or_else(|error| panic!("D1 call {url} failed: {error:#}"));
            serde_json::from_slice::<serde_json::Value>(&response.body).unwrap_or_else(|_| {
                panic!("D1 reply {:?}", String::from_utf8_lossy(&response.body))
            })
        }
    };
    let idle = || async {
        timeout(Duration::from_secs(10), async {
            while app.public_in_flight() != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("D1 work did not settle");
    };
    let one = launch(first.clone(), "One-V1").await;
    let two = launch(other.clone(), "Two-V1").await;
    for vm in [&one, &two] {
        assert_eq!(call(vm, "/setup").await, "ready");
    }
    // SQL types, results, `first(column)` and original errors.
    let types = call(&one, "/types").await;
    let row = serde_json::json!({"n":9007199254740i64,"r":1.5,"s":"café","b":[0,1,255],"z":null});
    assert_eq!(types["changes"], 1);
    assert_eq!(types["first"], row);
    assert_eq!(types["all"], serde_json::json!([row]));
    assert_eq!(
        types["raw"],
        serde_json::json!([
            ["n", "r", "s", "b", "z"],
            [9007199254740i64, 1.5, "café", [0, 1, 255], null]
        ])
    );
    assert_eq!(types["column"], "café");
    assert_eq!(
        types["kinds"],
        serde_json::json!([["integer", "real", "text", "blob", "null"]])
    );
    assert!(types["none"].is_null());
    assert_eq!(types["noColumn"]["name"], "Error");
    assert_eq!(
        types["noColumn"]["message"],
        "D1_COLUMN_NOTFOUND: Column not found (nope)"
    );
    assert_eq!(types["noColumn"]["cause"], "Column not found");
    assert_eq!(types["unsupported"]["name"], "Error");
    assert!(types["unsupported"]["message"]
        .as_str()
        .unwrap()
        .starts_with("D1_TYPE_ERROR: "));
    assert!(types["unsupported"]["cause"]
        .as_str()
        .unwrap()
        .contains("not supported"));
    assert_eq!(types["syntax"]["name"], "Error");
    let syntax = types["syntax"]["message"].as_str().unwrap();
    assert!(syntax.starts_with("D1_ERROR: ") && syntax.contains("syntax error"));
    assert_eq!(
        syntax.strip_prefix("D1_ERROR: ").unwrap(),
        types["syntax"]["cause"].as_str().unwrap()
    );
    // Batches: success, and a failing one that rolls back whole.
    let batch = call(&one, "/batch-ok").await;
    assert_eq!(
        batch,
        serde_json::json!([[true,1,[]],[true,1,[]],[true,0,[{"n":2}]]])
    );
    let failed = call(&one, "/batch-fail").await;
    assert_eq!(failed["failed"]["name"], "Error");
    let message = failed["failed"]["message"].as_str().unwrap();
    assert!(message.starts_with("D1_ERROR: ") && message.contains("UNIQUE constraint failed"));
    assert_eq!(
        message.strip_prefix("D1_ERROR: ").unwrap(),
        failed["failed"]["cause"].as_str().unwrap()
    );
    assert_eq!(failed["rows"], serde_json::json!([[1], [2]]));
    let session = call(&one, "/session").await;
    assert_eq!(
        session,
        serde_json::json!({"before":null,"after":"celld:primary","batch":"celld:primary",
        "invalid":{"name":"Error","message":"D1_SESSION_ERROR: invalid bookmark or constraint"}})
    );
    // An aggregate 18 MB crosses both transport caps while every row and
    // SQL intermediate remains below SQLite's original 2,200,000-byte limit.
    assert_eq!(
        call(&one, "/large").await,
        serde_json::json!((1..=20)
            .map(|i| serde_json::json!([i, 900000, "xxxx", "xxxx"]))
            .collect::<Vec<_>>())
    );
    assert_eq!(
        call(&one, "/large-input").await,
        serde_json::json!({"values":vec![1800000;5],"larger":vec![1800000;10]})
    );
    // Concurrent requests and in-handler concurrency serialize on one database.
    // Include caller events in the existing 32-event VM admission budget.
    // Two concurrent waves retain all 48 writes without testing overload as
    // though it were an accepted invocation.
    let mut counts = Vec::new();
    for _ in 0..2 {
        counts.extend(futures_util::future::join_all((0..4).map(|_| call(&one, "/burst"))).await);
        idle().await;
    }
    let mut seen: Vec<i64> = counts
        .iter()
        .flat_map(|burst| {
            burst
                .as_array()
                .unwrap()
                .iter()
                .map(|n| n.as_i64().unwrap())
        })
        .collect();
    seen.sort();
    assert_eq!(seen, (1..=48).collect::<Vec<_>>());
    assert_eq!(call(&one, "/next").await, 49);
    // The same name in another tenant is another database.
    assert_eq!(call(&two, "/next").await, 1);
    assert_eq!(call(&two, "/rows").await, serde_json::json!([]));
    assert_eq!(call(&two, "/own").await, "ok");
    assert_eq!(
        call(&two, "/rows").await,
        serde_json::json!([[1, "Two-V1"]])
    );
    assert_eq!(
        call(&one, "/rows").await,
        serde_json::json!([[1, "one"], [2, "two"]])
    );
    idle().await;
    // A replacement deployment keeps the acknowledged data.
    assert_eq!(one.shutdown().await.unwrap().reason, "shutdown");
    let replacement = launch(first.clone(), "One-V2").await;
    assert_eq!(call(&replacement, "/next").await, 50);
    assert_eq!(
        call(&replacement, "/rows").await,
        serde_json::json!([[1, "one"], [2, "two"]])
    );
    idle().await;
    for vm in [replacement, two] {
        assert_eq!(vm.shutdown().await.unwrap().reason, "shutdown");
    }
}

#[cfg(test)]
async fn check_python_handlers(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    _first: &ApplicationId,
    owned_vm_pids: fn() -> HashSet<u32>,
) {
    use celld_applications::invocation_wire::HttpRequest;
    let Some(project) = std::env::var_os("CELLD_APPLICATION_PYTHON_TEST_PROJECT") else {
        eprintln!("Python handler check skipped: CELLD_APPLICATION_PYTHON_TEST_PROJECT is unset");
        return;
    };
    let config = PathBuf::from(project).join("wrangler.jsonc");
    let building = Instant::now();
    let (bundle, worker_config) =
        crate::asyncrt::blocking(move || crate::application_cli::python_artifact(config))
            .await
            .unwrap()
            .unwrap_or_else(|error| panic!("Python build failed: {error:#}"));
    let build_ms = building.elapsed().as_millis();
    let bundle_bytes = bundle.len();
    // The shared runtime drive, built once on the node the first time a Python
    // handler needs it: the interpreter's files and the base snapshot. While the
    // runtime image is bad the build fails, and a retry waits out its backoff.
    let (scratch, real) = (tempfile::tempdir().unwrap(), options.runtime.path.clone());
    let late = scratch.path().join("late-runtime.ext4");
    std::fs::write(&late, b"not the approved image").unwrap();
    let mut options = LaunchOptions {
        runtime: ApprovedFile {
            path: late.clone(),
            sha256: options.runtime.sha256.clone(),
        },
        templates: Arc::new(Templates::new().unwrap()),
        ..options.clone()
    };
    assert!(options.templates.pyodide(&options, app).await.is_err());
    std::fs::remove_file(&late).unwrap();
    std::fs::copy(real, &late).unwrap();
    let began = Instant::now();
    assert!(options.templates.pyodide(&options, app).await.is_err());
    assert!(
        began.elapsed() < Duration::from_secs(1),
        "a retry came before its backoff"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    let building = Instant::now();
    // Concurrent first launches share the one build.
    let (one, two) = tokio::join!(
        options.templates.pyodide(&options, app),
        options.templates.pyodide(&options, app)
    );
    let drive = one.unwrap_or_else(|error| panic!("Pyodide runtime build failed: {error:#}"));
    assert_eq!(drive, two.unwrap());
    options.shared = Some(drive);
    let (runtime_ms, runtime_bytes) = (
        building.elapsed().as_millis(),
        std::fs::metadata(options.shared.as_ref().unwrap())
            .unwrap()
            .len(),
    );
    let options = &options;
    let application = ApplicationId {
        app: "python".to_owned().try_into().unwrap(),
    };
    let resource = |kind, id: &str| ResourceRef {
        kind,
        id: id.to_owned().try_into().unwrap(),
    };
    let database = resource(ResourceKind::D1, "db");
    let cache = resource(ResourceKind::Kv, "cache");
    let mut snapshot = store.read(&application).await.unwrap();
    snapshot.create(application.clone()).unwrap();
    if std::env::var_os("CELLD_APPLICATION_PYTHON_LITE_CHECK_ONLY").is_some() {
        snapshot
            .set_limits(
                &application,
                Some(ApplicationLimits {
                    size_class: ApplicationSizeClass::Lite,
                    invocation_timeout_ms: 30_000,
                }),
            )
            .unwrap();
    }
    for registered in [&database, &cache] {
        snapshot
            .register_resource(
                &application,
                crate::applications::Resource {
                    resource: registered.clone(),
                    configuration: serde_json::json!({}),
                },
            )
            .unwrap();
    }
    store.commit(snapshot).await.unwrap();
    let name = |value: &str| -> Name { value.to_owned().try_into().unwrap() };
    let stage = |id: &'static str,
                 entrypoint: &'static str,
                 bundle: Vec<u8>,
                 worker_config: serde_json::Value| {
        let (application, database, cache) = (application.clone(), database.clone(), cache.clone());
        let runtime = options.runtime.sha256.clone();
        async move {
            let digest = content_digest(&bundle);
            store
                .upload_artifact(&application, &digest, bundle.into())
                .await
                .unwrap();
            let deployment: Deployment = serde_json::from_value(serde_json::json!({
                "manifest_version": 1,
                "application": application,
                "id": id,
                "handlers": [{
                    "id": id,
                    "language": "javascript",
                    "entrypoint": entrypoint,
                    "artifact_sha256": digest,
                    "runtime_image_sha256": runtime,
                    "bindings": {
                        "DB": {"kind": "resource", "resource": database},
                        "CACHE": {"kind": "resource", "resource": cache},
                    },
                    "worker_config": worker_config,
                }]
            }))
            .unwrap();
            store.stage(&deployment).await.unwrap();
        }
    };
    let javascript = crate::application_image::build(&[(
        "main.mjs".into(),
        b"export default { fetch() { return new Response('ok'); } };".to_vec(),
    )])
    .unwrap();
    stage(
        "javascript",
        "main.mjs",
        javascript,
        serde_json::json!({"compatibility_date": "2026-03-01"}),
    )
    .await;
    stage("python", "index.js", bundle, worker_config).await;
    let launch = |id: &'static str| {
        let (options, application) = (options.clone(), application.clone());
        async move {
            let began = Instant::now();
            let vm = timeout(
                Duration::from_secs(120),
                Vm::launch(options, store, &application, &name(id), &name(id)),
            )
            .await
            .unwrap()
            .unwrap_or_else(|error| panic!("{id} launch failed: {error:#}"));
            (vm, began.elapsed().as_millis())
        }
    };
    let call = |client: VmClient, path: &'static str| {
        let app = app.clone();
        async move {
            client
                .invoke_stateless(
                    &app,
                    Duration::from_secs(120),
                    HttpRequest {
                        method: "GET".into(),
                        url: format!("http://python{path}"),
                        headers: vec![],
                        body: vec![],
                    },
                    None,
                )
                .await
        }
    };
    let json = |result: Result<crate::js::HttpResponse>| match result {
        Ok(response) => {
            serde_json::from_slice::<serde_json::Value>(&response.body).unwrap_or_else(|_| {
                panic!("Python reply {:?}", String::from_utf8_lossy(&response.body))
            })
        }
        Err(error) => panic!("Python handler failed: {error:#}"),
    };
    // Per-VM cost. Both launches include the guest's load of its modules, so
    // the 8.6 MB interpreter's Wasm compile is in the launch; Pyodide's own
    // boot is in the first request.
    // The first launch of an image builds its template; measure the next ones.
    let (warm, _) = launch("javascript").await;
    assert_eq!(warm.shutdown().await.unwrap().reason, "shutdown");
    let (script, js_launch) = launch("javascript").await;
    let began = Instant::now();
    assert_eq!(call(script.client(), "/").await.unwrap().body, b"ok");
    let js_first = began.elapsed().as_millis();
    let js_path = script.path;
    // The handler's files are read from its image (the claim fails unless the
    // drive mounts read-only), not copied into memory.
    let mounts = script.console();
    assert!(
        mounts.contains(r#"CELLD_APPLICATION_DRIVE_MOUNTED {"device":"/dev/vdb","filesystem":"erofs","read_only":true,"#)
            && !mounts.contains(r#"CELLD_APPLICATION_DRIVE_MOUNTED {"device":"/dev/vdc""#),
        "JavaScript VM mounts: {mounts}"
    );
    assert_eq!(script.shutdown().await.unwrap().reason, "shutdown");
    let (vm, py_launch) = launch("python").await;
    let began = Instant::now();
    let hello = json(call(vm.client(), "/hello").await);
    let py_first = began.elapsed().as_millis();
    assert_eq!(hello, serde_json::json!({"hello": "python", "served": 1}));
    let began = Instant::now();
    assert_eq!(json(call(vm.client(), "/hello").await)["served"], 2);
    let py_second = began.elapsed().as_millis();
    // KV and D1 through the Python Workers SDK; D1 rows persist across requests.
    assert_eq!(
        json(call(vm.client(), "/kv").await),
        serde_json::json!({"value": "kv-from-python"})
    );
    for count in [1, 2] {
        assert_eq!(
            json(call(vm.client(), "/d1").await),
            serde_json::json!({"count": count})
        );
    }
    // The handler image and the shared runtime, both read-only drives.
    let mounts = vm.console();
    assert!(
        mounts.contains(r#"CELLD_APPLICATION_DRIVE_MOUNTED {"device":"/dev/vdb","filesystem":"erofs","read_only":true,"#)
            && mounts.contains(r#"CELLD_APPLICATION_DRIVE_MOUNTED {"device":"/dev/vdc","filesystem":"erofs","read_only":true,"#),
        "Python VM mounts: {mounts}"
    );
    // Both VMs restore one base snapshot: Python's random state must differ.
    let random = json(call(vm.client(), "/random").await);
    let (other, _) = launch("python").await;
    let other_random = json(call(other.client(), "/random").await);
    assert_ne!(
        random["random"], other_random["random"],
        "{random} {other_random}"
    );
    assert_ne!(
        random["temp"], other_random["temp"],
        "{random} {other_random}"
    );
    assert_eq!(other.shutdown().await.unwrap().reason, "shutdown");
    let py_path = vm.path;
    assert_eq!(vm.shutdown().await.unwrap().reason, "shutdown");
    assert!(owned_vm_pids().is_empty(), "Python VMM was not reaped");
    eprintln!(
        "Python handler cost: image {bundle_bytes} bytes, built in {build_ms} ms; shared runtime drive \
         {runtime_bytes} bytes, built on the node in {runtime_ms} ms; \
         JS handler ({js_path}) launch {js_launch} ms, first request {js_first} ms; \
         Python handler ({py_path}) launch {py_launch} ms, first request {py_first} ms, second {py_second} ms"
    );
    // A fatal interpreter error condemns the guest: it fails its request, the
    // pool stops leasing the VM and retires it once idle, and the next request
    // gets a fresh VM, which is told apart by its request counter.
    let config = VmHostConfig {
        jailer: options.jailer.clone(),
        firecracker: options.firecracker.clone(),
        kernel: options.kernel.clone(),
        runtimes: vec![options.runtime.clone()],
        uid: options.uid,
        gid: options.gid,
        max_vms: 2,
        max_memory_mib: 12_288,
        idle_ttl_secs: 60,
        platform_domain: TEST_DOMAIN.into(),
        container_fence_image: None,
        artifact_cache_ttl_secs: None,
        metadata_cache_entries: None,
        metadata_cache_bytes: None,
        startup_pool: 0,
        memory_guard: None,
    };
    let pool = VmPool::new(config, store.clone(), NODE.into()).unwrap();
    let (deployment, handler) = (name("python"), name("python"));
    let mut snapshot = store.read(&application).await.unwrap();
    let manifest = store
        .deployment(&application, &deployment)
        .await
        .unwrap()
        .unwrap();
    store.activate(&mut snapshot, manifest).await.unwrap();
    store.commit(snapshot).await.unwrap();
    let lease = pool
        .acquire(app, &application, &deployment, &handler)
        .await
        .unwrap();
    let condemned = owned_vm_pids();
    assert_eq!(
        json(call(lease.client.clone(), "/hello").await)["served"],
        1
    );
    let failure = call(lease.client.clone(), "/fatal").await.err();
    let failure = failure.expect("a fatal interpreter error must fail its request");
    // The guest writes its condemnation ahead of the failed reply, so the next
    // acquire must already see it.
    assert!(lease.client.transport.is_condemned());
    drop(lease);
    let began = Instant::now();
    let lease = pool
        .acquire(app, &application, &deployment, &handler)
        .await
        .unwrap();
    let fresh = json(call(lease.client.clone(), "/hello").await);
    assert_eq!(fresh["served"], 1, "the condemned VM was leased again");
    let replaced_ms = began.elapsed().as_millis();
    assert!(
        owned_vm_pids().is_disjoint(&condemned),
        "the condemned VM outlived its replacement"
    );
    drop(lease);
    pool.shutdown().await.unwrap();
    assert!(
        owned_vm_pids().is_empty(),
        "pooled Python VMM was not reaped"
    );
    eprintln!(
        "Python handler condemnation: the fatal error failed its request ({failure:#}); the next request \
         got a fresh VM {replaced_ms} ms after the failure"
    );
    eprintln!("owned Python handler: a Python Worker built into JS and Wasm modules by the publish CLI served HTTP with D1 and KV bindings in actual VMs; a fatal interpreter error retired its VM and the next request started a fresh one; no routing or independent-host qualification");
}

#[cfg(test)]
async fn check_dynamic_loader(
    app: &crate::actor::AppHandle,
    store: &ApplicationStore,
    options: &LaunchOptions,
    directory: &Path,
    _first: &ApplicationId,
) {
    use celld_applications::invocation_wire::HttpRequest;
    const SOURCE: &str = r#"export default {
  async fetch(request, env) {
    const L = env.LOADER, out = {};
    const main = (body, extra = {}) => ({ compatibilityDate: '2026-03-01', mainModule: 'main.js', modules: { 'main.js': body, ...extra } });
    const fail = async (f) => { try { await f(); return 'resolved'; } catch (e) { return String((e && e.message) || e); } };
    const text = async (stub) => (await stub.getEntrypoint().fetch('http://child/path')).text();
    const plain = 'export default { fetch() { return new Response("plain"); } }';
    let calls = 0;
    const named = () => L.get('named', () => { calls++; return { ...main(`
      import { WorkerEntrypoint } from 'cloudflare:workers';
      import { x } from './dep.js';
      import wasm from './empty.wasm';
      export class Api extends WorkerEntrypoint {
        add(a, b) { return a + b + x + (this.ctx.props?.bonus ?? 0); }
        spin() { for (;;) {} }
      }
      export default { fetch(req, env) { return new Response(new URL(req.url).pathname + ':' + env.plain + ':' + (wasm instanceof WebAssembly.Module)); } };
    `, { 'dep.js': 'export const x = 7;', 'empty.wasm': { wasm: new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]) } }), env: { plain: 'yes' } }; });
    out.label = env.APP_LABEL;
    out.fetch = await text(named());
    out.rpc = await named().getEntrypoint('Api', { props: { bonus: 10 } }).add(1, 2);
    out.limit = await fail(() => named().getEntrypoint('Api', { limits: { cpuMs: 20 } }).spin());
    out.after = await named().getEntrypoint('Api').add(1, 1);
    out.calls = calls;
    const anonymous = L.load(main('export default { fetch() { return new Response("anon"); } }'));
    out.load = await text(anonymous);
    anonymous.dispose();
    out.disposed = await fail(() => text(anonymous));
    out.egress = await text(L.load(main('export default { async fetch() { try { await fetch("http://example.invalid/"); return new Response("reached"); } catch (e) { return new Response(String(e.message)); } } }')));
    out.nullOutbound = await text(L.load({ ...main(plain), globalOutbound: null }));
    out.textModule = await fail(() => text(L.load(main(plain, { 't.txt': { text: 'x' } }))));
    out.py = await fail(() => text(L.load(main(plain, { 'p.py': { py: 'x = 1' } }))));
    return Response.json(out);
  }
};
"#;
    let source = directory.join("loader-source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("main.mjs"), SOURCE).unwrap();
    let image = directory.join("loader.bundle");
    let built = timeout(
        Duration::from_secs(30),
        Command::new("/usr/bin/python3")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../applications/guest/build-application-image.py"),
            )
            .arg(&source)
            .arg(&image)
            .env_clear()
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let artifact = std::fs::read(image).unwrap();
    let digest = content_digest(&artifact);
    let first = ApplicationId {
        app: "loaders".to_owned().try_into().unwrap(),
    };
    let other = ApplicationId {
        app: "loader-other".to_owned().try_into().unwrap(),
    };
    let loader = ResourceRef {
        kind: celld_applications::ResourceKind::WorkerLoader,
        id: "loader".to_owned().try_into().unwrap(),
    };
    for application in [&first, &other] {
        let mut snapshot = store.read(application).await.unwrap();
        snapshot.create(application.clone()).unwrap();
        snapshot
            .register_resource(
                application,
                crate::applications::Resource {
                    resource: loader.clone(),
                    configuration: serde_json::json!({}),
                },
            )
            .unwrap();
        store.commit(snapshot).await.unwrap();
    }
    let launch = |application: ApplicationId, label: &'static str, declared: bool| {
        let (artifact, digest, options) = (artifact.clone(), digest.clone(), options.clone());
        let loader = loader.clone();
        async move {
            store
                .upload_artifact(&application, &digest, artifact.into())
                .await
                .unwrap();
            let worker_config = if declared {
                serde_json::json!({"compatibility_date": "2026-03-01",
                    "worker_loaders": [{"binding": "LOADER"}]})
            } else {
                serde_json::json!({"compatibility_date": "2026-03-01"})
            };
            let deployment: Deployment = serde_json::from_value(serde_json::json!({
                "manifest_version": 1,
                "application": application,
                "id": format!("loader-{}", label.to_lowercase()),
                "handlers": [{
                    "id": "javascript",
                    "language": "javascript",
                    "entrypoint": "main.mjs",
                    "artifact_sha256": digest,
                    "runtime_image_sha256": options.runtime.sha256,
                    "environment": {"APP_LABEL": label},
                    "secrets": {},
                    "bindings": {"LOADER": {"kind": "resource", "resource": loader}},
                    "worker_config": worker_config
                }]
            }))
            .unwrap();
            // Declaring and binding must agree, before any VMM exists.
            let staged = store.stage(&deployment).await;
            if declared {
                staged.unwrap_or_else(|error| panic!("staging {label} failed: {error:#}"));
                Some(
                    timeout(
                        Duration::from_secs(30),
                        Vm::launch(
                            options,
                            store,
                            &application,
                            &deployment.id,
                            &"javascript".to_owned().try_into().unwrap(),
                        ),
                    )
                    .await
                    .unwrap()
                    .unwrap_or_else(|error| panic!("loader launch failed: {error:#}")),
                )
            } else {
                let refused = match staged {
                    Err(_) => true,
                    Ok(_) => timeout(
                        Duration::from_secs(30),
                        Vm::launch(
                            options,
                            store,
                            &application,
                            &deployment.id,
                            &"javascript".to_owned().try_into().unwrap(),
                        ),
                    )
                    .await
                    .unwrap()
                    .is_err(),
                };
                assert!(refused, "an undeclared worker loader binding was accepted");
                None
            }
        }
    };
    let call = |vm: &Vm| {
        let (client, app) = (vm.client(), app.clone());
        async move {
            let response = client
                .invoke_stateless(
                    &app,
                    Duration::from_secs(30),
                    HttpRequest {
                        method: "GET".into(),
                        url: "http://loader/run".into(),
                        headers: vec![],
                        body: vec![],
                    },
                    None,
                )
                .await
                .unwrap_or_else(|error| panic!("loader call failed: {error:#}"));
            serde_json::from_slice::<serde_json::Value>(&response.body).unwrap_or_else(|_| {
                panic!("loader reply {:?}", String::from_utf8_lossy(&response.body))
            })
        }
    };
    assert!(launch(first.clone(), "Undeclared", false).await.is_none());
    let one = launch(first.clone(), "One", true).await.unwrap();
    let two = launch(other.clone(), "Two", true).await.unwrap();
    for (vm, label) in [(&one, "One"), (&two, "Two")] {
        let out = call(vm).await;
        let said = |key: &str| out[key].as_str().unwrap_or_default().to_owned();
        assert_eq!(said("label"), label, "{out}");
        assert_eq!(said("fetch"), "/path:yes:true", "{out}");
        assert_eq!(out["rpc"], 20, "{out}");
        assert_ne!(said("limit"), "resolved", "{out}");
        assert_eq!(out["after"], 9, "{out}");
        assert_eq!(out["calls"], 1, "{out}");
        assert_eq!(said("load"), "anon", "{out}");
        assert!(said("disposed").contains("unknown worker"), "{out}");
        assert!(
            said("egress").contains("not permitted to access the internet"),
            "{out}"
        );
        assert_eq!(said("nullOutbound"), "plain", "{out}");
        for key in ["textModule", "py"] {
            assert!(
                said(key).contains("must be a string or wasm bytes"),
                "{key}: {out}"
            );
        }
    }
    // Named workers are memoized for the VM's life: the next request loads none.
    let again = call(&one).await;
    assert_eq!(again["calls"], 0, "{again}");
    assert_eq!(again["fetch"], "/path:yes:true", "{again}");
    timeout(Duration::from_secs(10), async {
        while app.public_in_flight() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("loader work did not settle");
    for vm in [one, two] {
        assert_eq!(vm.shutdown().await.unwrap().reason, "shutdown");
    }
    eprintln!("owned JavaScript Worker Loader: two applications declared a worker_loader binding and ran dynamic modules in actual VMs (get/load, entrypoint fetch/RPC, props, sibling/Wasm modules, limits, dispose); the child had no egress; text/py modules refused; an undeclared loader binding refused; no facets, Fetcher/env-service/tails support or independent-host qualification");
}

#[cfg(test)]
async fn check_facets(
    app: &crate::actor::AppHandle,
    store: &ApplicationStore,
    options: &LaunchOptions,
    directory: &Path,
    resource: &ResourceAddress,
) {
    use celld_applications::invocation_wire::HttpRequest;
    const SOURCE: &str = r#"import { DurableObject } from 'cloudflare:workers';
const facetCode = `
  import { DurableObject } from 'cloudflare:workers';
  export class F extends DurableObject {
    constructor(ctx, env) { super(ctx, env); this.n = 0; }
    add(k) {
      const sql = this.ctx.storage.sql;
      sql.exec('CREATE TABLE IF NOT EXISTS t(v INTEGER)');
      const cur = sql.exec('SELECT COALESCE(SUM(v), 0) AS s FROM t').one().s;
      sql.exec('INSERT INTO t VALUES(?)', k);
      return { n: ++this.n, sum: cur + k };
    }
    async egress() {
      try { await fetch('http://example.invalid/'); return 'reached'; } catch (e) { return String(e.message); }
    }
    async alarm() {
      try { await this.ctx.storage.setAlarm(Date.now() + 1000); return 'set'; } catch (e) { return e.message; }
    }
  }
  export default { fetch() { return new Response('f'); } };
`;
export class Counter extends DurableObject {
  facet() {
    const cls = this.env.LOADER
      .get('code', () => ({ compatibilityDate: '2026-03-01', mainModule: 'main.js', modules: { 'main.js': facetCode } }))
      .getDurableObjectClass('F');
    return this.ctx.facets.get('child', () => ({ class: cls }));
  }
  async fetch(request) {
    const url = new URL(request.url);
    const json = async (work) => Response.json(await work());
    switch (url.pathname) {
      case '/add': return json(() => this.facet().add(Number(url.searchParams.get('n'))));
      case '/rollback':
        await this.ctx.storage.transaction(async (txn) => { await this.facet().add(100); txn.rollback(); });
        return json(() => this.facet().add(0));
      case '/commit':
        await this.ctx.storage.transaction(async () => { await this.facet().add(1000); });
        return json(() => this.facet().add(0));
      case '/abort': this.ctx.facets.abort('child'); return json(() => this.facet().add(0));
      case '/delete': this.ctx.facets.delete('child'); return json(() => this.facet().add(0));
      case '/egress': return json(() => this.facet().egress());
      case '/alarm': return json(() => this.facet().alarm());
    }
    return new Response('counter');
  }
}
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    return env.COUNTER.getByName(url.searchParams.get('name')).fetch('http://object' + url.pathname + url.search);
  }
};
"#;
    let source = directory.join("facets-source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("main.mjs"), SOURCE).unwrap();
    let image = directory.join("facets.bundle");
    let built = timeout(
        Duration::from_secs(30),
        Command::new("/usr/bin/python3")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../applications/guest/build-application-image.py"),
            )
            .arg(&source)
            .arg(&image)
            .env_clear()
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let artifact = std::fs::read(image).unwrap();
    let digest = content_digest(&artifact);
    let other = ApplicationId {
        app: "facets-other".to_owned().try_into().unwrap(),
    };
    let loader = ResourceRef {
        kind: celld_applications::ResourceKind::WorkerLoader,
        id: "loader".to_owned().try_into().unwrap(),
    };
    let mut snapshot = store.read(&other).await.unwrap();
    snapshot.create(other.clone()).unwrap();
    snapshot
        .register_resource(
            &other,
            crate::applications::Resource {
                resource: resource.resource.clone(),
                configuration: serde_json::json!({}),
            },
        )
        .unwrap();
    store.commit(snapshot).await.unwrap();
    for application in [&resource.application, &other] {
        let mut snapshot = store.read(application).await.unwrap();
        snapshot
            .register_resource(
                application,
                crate::applications::Resource {
                    resource: loader.clone(),
                    configuration: serde_json::json!({}),
                },
            )
            .unwrap();
        store.commit(snapshot).await.unwrap();
    }
    let launch = |application: ApplicationId, label: &'static str| {
        let (artifact, digest, options) = (artifact.clone(), digest.clone(), options.clone());
        let (counter, loader) = (resource.resource.clone(), loader.clone());
        async move {
            store
                .upload_artifact(&application, &digest, artifact.into())
                .await
                .unwrap();
            let deployment: Deployment = serde_json::from_value(serde_json::json!({
                "manifest_version": 1,
                "application": application,
                "id": format!("facets-{}", label.to_lowercase()),
                "handlers": [{
                    "id": "javascript",
                    "language": "javascript",
                    "entrypoint": "main.mjs",
                    "artifact_sha256": digest,
                    "runtime_image_sha256": options.runtime.sha256,
                    "environment": {"APP_LABEL": label},
                    "secrets": {},
                    "bindings": {
                        "COUNTER": {"kind": "resource", "resource": counter},
                        "LOADER": {"kind": "resource", "resource": loader}
                    },
                    "worker_config": {
                        "do_classes": ["Counter"],
                        "durable_objects": {"bindings": [{"name": "COUNTER", "class_name": "Counter"}]},
                        "worker_loaders": [{"binding": "LOADER"}],
                        "compatibility_date": "2026-03-01",
                        "compatibility_flags": ["js_rpc"]
                    }
                }]
            }))
            .unwrap();
            store
                .stage(&deployment)
                .await
                .unwrap_or_else(|error| panic!("staging {label} failed: {error:#}"));
            timeout(
                Duration::from_secs(30),
                Vm::launch(
                    options,
                    store,
                    &application,
                    &deployment.id,
                    &"javascript".to_owned().try_into().unwrap(),
                ),
            )
            .await
            .unwrap()
            .unwrap_or_else(|error| panic!("facet launch failed: {error:#}"))
        }
    };
    let call = |vm: &Vm, path: &str| {
        let (client, app) = (vm.client(), app.clone());
        let url = format!("http://facets{path}");
        async move {
            let response = client
                .invoke_stateless(
                    &app,
                    Duration::from_secs(30),
                    HttpRequest {
                        method: "GET".into(),
                        url: url.clone(),
                        headers: vec![],
                        body: vec![],
                    },
                    None,
                )
                .await
                .unwrap_or_else(|error| panic!("facet call {url} failed: {error:#}"));
            let status = response.status;
            let mut body = response.body;
            if let Some(mut stream) = response.stream {
                use futures_util::StreamExt;
                while let Some(chunk) = stream.next().await {
                    body.extend(chunk.unwrap());
                }
            }
            serde_json::from_slice::<serde_json::Value>(&body).unwrap_or_else(|_| {
                panic!("facet reply {status} {:?}", String::from_utf8_lossy(&body))
            })
        }
    };
    let step =
        |value: serde_json::Value| (value["n"].as_i64().unwrap(), value["sum"].as_i64().unwrap());
    let idle = || async {
        timeout(Duration::from_secs(10), async {
            while app.public_in_flight() != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("facet work did not settle");
    };
    // The production wiring: the node's host loop serves the facet streams the
    // isolates ask for, as `dispatch_facet` does, so the guests' facets open,
    // prove and delete their streams through it.
    let (facet_requests, mut requested) = tokio::sync::mpsc::unbounded_channel();
    crate::js::set_facet_tx(facet_requests);
    let host = app.clone();
    tokio::spawn(async move {
        use crate::js::FacetReq;
        while let Some(request) = requested.recv().await {
            let runtime = host.runtime.as_ref().expect("a cell runtime");
            match request {
                FacetReq::Open {
                    root,
                    epoch,
                    names,
                    reply,
                } => drop(reply.send(runtime.open_facet(&root, epoch, &names).await)),
                FacetReq::Delete {
                    root,
                    epoch,
                    names,
                    reply,
                } => drop(reply.send(runtime.delete_facet(&root, epoch, &names).await)),
                FacetReq::Prove {
                    stream,
                    epoch,
                    reply,
                } => drop(reply.send(runtime.prove_facet(&stream, epoch).await)),
            }
        }
    });
    let one = launch(resource.application.clone(), "One-V1").await;
    let two = launch(other.clone(), "Two-V1").await;
    // The facet's instance serves later events of its object; its data is its
    // own file, and the same names in another application are separate.
    assert_eq!(step(call(&one, "/add?name=a&n=5").await), (1, 5));
    assert_eq!(step(call(&one, "/add?name=a&n=7").await), (2, 12));
    assert_eq!(step(call(&two, "/add?name=a&n=3").await), (1, 3));
    assert_eq!(step(call(&one, "/add?name=b&n=1").await), (1, 1));
    // A facet is a database of its own: the root's transaction takes neither
    // its write nor its instance with it, rolled back or committed.
    assert_eq!(step(call(&one, "/rollback?name=a").await), (4, 112));
    assert_eq!(step(call(&one, "/commit?name=a").await), (6, 1112));
    // No egress and no alarms for a facet.
    let egress = call(&one, "/egress?name=a").await;
    assert!(
        egress
            .as_str()
            .unwrap()
            .contains("not permitted to access the internet"),
        "{egress}"
    );
    assert_eq!(
        call(&one, "/alarm?name=a").await,
        serde_json::json!("Facets currently cannot set alarms.")
    );
    // Abort drops the instance and keeps the data; delete drops both.
    assert_eq!(step(call(&one, "/abort?name=a").await), (1, 1112));
    assert_eq!(step(call(&one, "/delete?name=a").await), (1, 0));
    assert_eq!(step(call(&one, "/add?name=a&n=9").await), (2, 9));
    idle().await;
    // A replacement deployment keeps the facet's data.
    assert_eq!(one.shutdown().await.unwrap().reason, "shutdown");
    let replacement = launch(resource.application.clone(), "One-V2").await;
    assert_eq!(step(call(&replacement, "/add?name=a&n=1").await), (1, 10));
    assert_eq!(step(call(&two, "/add?name=a&n=1").await), (2, 4));
    idle().await;
    for vm in [replacement, two] {
        assert_eq!(vm.shutdown().await.unwrap().reason, "shutdown");
    }
    eprintln!("owned JavaScript facets: dynamically loaded facet classes ran in actual VMs on a database file and replication stream of their own through the host's session; one instance served later events, a root rollback left it alone, abort and delete were original, egress and alarms were refused, the same names in two tenants were separate and a replacement deployment kept the data; no remote peer or independent-host qualification");
}

#[cfg(test)]
async fn check_service_bindings(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    directory: &Path,
) {
    use celld_applications::invocation_wire::{HttpRequest, ServiceInvocation};
    use futures_util::StreamExt;
    const SOURCE: &str = r#"
import {WorkerEntrypoint, RpcTarget} from 'cloudflare:workers';
let cancelled = 0;
export class Named extends WorkerEntrypoint {
  get value() { return 'named:' + this.env.LABEL; }
  get props() { return this.ctx.props; }
  fetch() { return new Response(this.env.LABEL + ':' + this.ctx.props); }
}
export default class Main extends WorkerEntrypoint {
  get value() { return this.env.LABEL; }
  get nested() { return { sum(a, b) { return a + b; } }; }
  target() { return new RpcTarget(); }
  async accept(signal) {
    if (signal.aborted) return 'pre:' + signal.reason.message;
    await new Promise(resolve => signal.addEventListener('abort', resolve, {once:true}));
    return signal.reason.message;
  }
  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (this.env.ROLE === 'caller') {
      if (path === '/rpc') return Response.json({value: await this.env.SVC.value,
        sum: await this.env.SVC.nested.sum(2, 3), named: await this.env.NAMED.value});
      if (path === '/capability') {
        try { await this.env.SVC.target(); return new Response('unexpected success'); }
        catch (error) { return new Response(error.name + ':' + error.message); }
      }
      if (path === '/signal' || path === '/signal-pre' || path === '/signal-many') {
        const controller = new AbortController();
        if (path === '/signal-pre') controller.abort(new Error('caller aborted early'));
        else setTimeout(() => controller.abort(new Error('caller aborted')), 100);
        const call = () => this.env.SVC.accept(controller.signal);
        return new Response((path === '/signal-many' ? await Promise.all([call(), call()]) : [await call()]).join(','));
      }
      if (path === '/cancel') {
        const controller = new AbortController();
        const pending = this.env.SVC.fetch('http://service/hang', {signal: controller.signal});
        setTimeout(() => controller.abort(new Error('caller cancelled')), 100);
        try { await pending; return new Response('unexpected success'); }
        catch (error) { return new Response(error.message); }
      }
      if (path === '/asset') return this.env.ASSETS.fetch('https://assets.local/' + (new URL(request.url).searchParams.get('path') ?? 'a.txt'));
      if (path === '/late-cold') { await request.arrayBuffer(); return this.env.COLD.fetch('http://service/'); }
      if (path === '/late') { await request.arrayBuffer(); return this.env.SVC.fetch('http://service/'); }
      return this.env.SVC.fetch(request);
    }
    if (path === '/echo') return Response.json({label: this.env.LABEL, bytes: (await request.arrayBuffer()).byteLength});
    if (path === '/hang') {
      await new Promise(resolve => request.signal.addEventListener('abort', () => {cancelled++; resolve();}, {once:true}));
      return new Response('cancelled');
    }
    if (path === '/cancelled') return new Response(String(cancelled));
    if (path === '/big') {
      let n = 0;
      return new Response(new ReadableStream({pull(controller) {
        if (n++ === 160) controller.close(); else controller.enqueue(new Uint8Array(65536).fill(7));
      }}));
    }
    return new Response(this.env.LABEL);
  }
}
"#;
    let source = directory.join("services-source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("main.mjs"), SOURCE).unwrap();
    let image = directory.join("services.bundle");
    let built = Command::new("/usr/bin/python3")
        .arg(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../applications/guest/build-application-image.py"),
        )
        .arg(&source)
        .arg(&image)
        .env_clear()
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .kill_on_drop(true)
        .output()
        .await
        .unwrap();
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let artifact = std::fs::read(image).unwrap();
    let digest = content_digest(&artifact);
    let apps: Vec<ApplicationId> = ["services-one", "services-two"]
        .into_iter()
        .map(|tenant| ApplicationId {
            app: tenant.to_owned().try_into().unwrap(),
        })
        .collect();
    // Static assets as `celld application assets` publishes them: the bodies
    // and the original index naming them, as the application's artifacts.
    let site = ResourceRef {
        kind: celld_applications::ResourceKind::StaticAssets,
        id: "site".to_owned().try_into().unwrap(),
    };
    let site_files = |tenant: &str| {
        vec![
            ("/a.txt".to_owned(), format!("asset {tenant}")),
            (format!("/only-{tenant}.txt"), format!("private {tenant}")),
        ]
    };
    let site_index = |tenant: &str| {
        let entries: serde_json::Map<_, _> = site_files(tenant)
            .into_iter()
            .map(|(path, body)| {
                (path, serde_json::json!({"sha256": content_digest(body.as_bytes()), "bytes": body.len(), "content_type": "text/plain"}))
            })
            .collect();
        serde_json::json!({"schema_version": 1, "entries": entries, "config": {"binding": "ASSETS"}})
            .to_string()
            .into_bytes()
    };
    for id in &apps {
        let mut snapshot = store.read(id).await.unwrap();
        snapshot.create(id.clone()).unwrap();
        snapshot
            .register_resource(
                id,
                crate::applications::Resource {
                    resource: site.clone(),
                    configuration: serde_json::json!({}),
                },
            )
            .unwrap();
        store.commit(snapshot).await.unwrap();
    }
    let manifest = |application: &ApplicationId, version: &str| -> Deployment {
        let label = format!("{}-{version}", application.app.as_str());
        serde_json::from_value(serde_json::json!({"manifest_version":1,"application":application,"id":version,
            "handlers":[
                {"id":"caller","language":"javascript","entrypoint":"main.mjs","artifact_sha256":digest,"runtime_image_sha256":options.runtime.sha256,
                 "http":true,"environment":{"ROLE":"caller","LABEL":label},
                 "bindings":{"SVC":{"kind":"handler","handler":"backend"},"NAMED":{"kind":"handler","handler":"backend"},"COLD":{"kind":"handler","handler":"cold"},"ASSETS":{"kind":"resource","resource":site}},
                 "worker_config":{"assets":{"index":"assets.json","sha256":content_digest(&site_index(application.app.as_str())),"file_count":2,"total_bytes":site_files(application.app.as_str()).iter().map(|(_, body)| body.len()).sum::<usize>()},"compatibility_date":"2026-03-01","compatibility_flags":["js_rpc"],"services":[{"binding":"NAMED","service":"backend","entrypoint":"Named"}]}},
                {"id":"backend","language":"javascript","entrypoint":"main.mjs","artifact_sha256":digest,"runtime_image_sha256":options.runtime.sha256,
                 "environment":{"ROLE":"backend","LABEL":label},"worker_config":{"compatibility_date":"2026-03-01","compatibility_flags":["js_rpc"]}},
                {"id":"cold","language":"javascript","entrypoint":"main.mjs","artifact_sha256":digest,"runtime_image_sha256":options.runtime.sha256,
                 "environment":{"ROLE":"backend","LABEL":format!("{label}:cold")},"worker_config":{"compatibility_date":"2026-03-01","compatibility_flags":["js_rpc"]}}]
        })).unwrap()
    };
    for id in &apps {
        store
            .upload_artifact(id, &digest, artifact.clone().into())
            .await
            .unwrap();
        let tenant = id.app.as_str();
        for (_, body) in site_files(tenant) {
            store
                .upload_artifact(
                    id,
                    &content_digest(body.as_bytes()),
                    body.into_bytes().into(),
                )
                .await
                .unwrap();
        }
        let index = site_index(tenant);
        store
            .upload_artifact(id, &content_digest(&index), index.into())
            .await
            .unwrap();
        let deployment = manifest(id, "v1");
        store.stage(&deployment).await.unwrap();
        let mut snapshot = store.read(&deployment.application).await.unwrap();
        store.activate(&mut snapshot, deployment).await.unwrap();
        store.commit(snapshot).await.unwrap();
    }
    let dispatcher = Arc::new(
        RouteDispatcher::new(
            VmHostConfig {
                jailer: options.jailer.clone(),
                firecracker: options.firecracker.clone(),
                kernel: options.kernel.clone(),
                runtimes: vec![options.runtime.clone()],
                uid: options.uid,
                gid: options.gid,
                max_vms: 6,
                max_memory_mib: 12_288,
                idle_ttl_secs: 600,
                platform_domain: TEST_DOMAIN.into(),
                container_fence_image: None,
                artifact_cache_ttl_secs: None,
                metadata_cache_entries: None,
                metadata_cache_bytes: None,
                startup_pool: 0,
                memory_guard: None,
            },
            store.clone(),
            "services-test".into(),
        )
        .await
        .unwrap(),
    );
    let mut hosts = HashMap::new();
    for id in &apps {
        hosts.insert(id.clone(), assigned_host(store, id, "caller").await);
    }
    let request = |id: &ApplicationId, path: &str| HttpRequest {
        method: "POST".into(),
        url: format!("http://{}{path}", hosts[id]),
        headers: vec![],
        body: vec![],
    };
    async fn bytes(mut response: crate::js::HttpResponse) -> Vec<u8> {
        if let Some(mut stream) = response.stream.take() {
            while let Some(chunk) = stream.next().await {
                response.body.extend(chunk.unwrap());
            }
        }
        response.body
    }
    for id in &apps {
        let reply = dispatcher
            .dispatch(app, Duration::from_secs(30), request(id, "/rpc"), None)
            .await
            .unwrap()
            .unwrap();
        let result: serde_json::Value = serde_json::from_slice(&bytes(reply).await).unwrap();
        let label = format!("{}-v1", id.app.as_str());
        assert_eq!(
            result,
            serde_json::json!({"value":label,"sum":5,"named":format!("named:{label}")})
        );
    }
    // Static assets: the guest has the original ASSETS binding over its own
    // application's assets only, and ingress resolves the same assets first.
    for (id, foreign) in [(&apps[0], &apps[1]), (&apps[1], &apps[0])] {
        let (tenant, other) = (id.app.as_str(), foreign.app.as_str());
        for (path, status, body) in [
            ("/asset", 200, format!("asset {tenant}")),
            (
                &format!("/asset?path=only-{tenant}.txt"),
                200,
                format!("private {tenant}"),
            ),
            (
                &format!("/asset?path=only-{other}.txt"),
                404,
                "Not found".to_owned(),
            ),
        ] {
            let reply = dispatcher
                .dispatch(app, Duration::from_secs(30), request(id, path), None)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(reply.status, status, "{tenant} {path}");
            assert_eq!(
                String::from_utf8(bytes(reply).await).unwrap(),
                body,
                "{tenant} {path}"
            );
        }
        let resolver = dispatcher
            .asset_resolver_for(&format!("http://{}/a.txt", hosts[id]))
            .await
            .unwrap()
            .expect("the route's handler declares assets");
        let served = resolver
            .ingress_response("/a.txt", None, false, &axum::http::HeaderMap::new())
            .await
            .unwrap()
            .expect("ingress serves the asset");
        let served = axum::body::to_bytes(served.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(served, format!("asset {tenant}").as_bytes());
        assert!(resolver
            .ingress_response(
                &format!("/only-{other}.txt"),
                None,
                false,
                &axum::http::HeaderMap::new()
            )
            .await
            .unwrap()
            .is_none());
    }
    let reply = dispatcher
        .dispatch(
            app,
            Duration::from_secs(30),
            request(&apps[0], "/capability"),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(String::from_utf8(bytes(reply).await)
        .unwrap()
        .starts_with("DataCloneError:"));
    let reply = dispatcher
        .dispatch(
            app,
            Duration::from_secs(30),
            request(&apps[0], "/signal"),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        String::from_utf8(bytes(reply).await).unwrap(),
        "caller aborted"
    );
    for (path, expected) in [
        ("/signal-pre", "pre:caller aborted early"),
        ("/signal-many", "caller aborted,caller aborted"),
    ] {
        let reply = dispatcher
            .dispatch(app, Duration::from_secs(30), request(&apps[0], path), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(String::from_utf8(bytes(reply).await).unwrap(), expected);
    }
    // Native incoming body source transfers through caller and callee VMs with
    // bounded credits; its total deliberately exceeds the buffered wire limit.
    let body = Box::pin(futures_util::stream::iter(
        (0..160).map(|_| Ok::<_, String>(vec![3; 65536])),
    ));
    let reply = dispatcher
        .dispatch(
            app,
            Duration::from_secs(30),
            request(&apps[0], "/echo"),
            Some(body),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes(reply).await).unwrap(),
        serde_json::json!({"label":"services-one-v1","bytes":160 * 65536})
    );
    let reply = dispatcher
        .dispatch(
            app,
            Duration::from_secs(30),
            request(&apps[0], "/big"),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let body = bytes(reply).await;
    assert_eq!(body.len(), 160 * 65536);
    assert!(body.iter().all(|byte| *byte == 7));
    let reply = dispatcher
        .dispatch(
            app,
            Duration::from_secs(30),
            request(&apps[0], "/cancel"),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        String::from_utf8(bytes(reply).await).unwrap(),
        "caller cancelled"
    );
    timeout(Duration::from_secs(5), async {
        loop {
            let reply = dispatcher
                .dispatch(
                    app,
                    Duration::from_secs(30),
                    request(&apps[0], "/cancelled"),
                    None,
                )
                .await
                .unwrap()
                .unwrap();
            if bytes(reply).await == b"1" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("service callee did not receive request cancellation");
    // The original named entrypoint receives structured-clone props unchanged.
    let callee = dispatcher
        .routes
        .pool
        .as_ref()
        .unwrap()
        .acquire(
            app,
            &apps[0],
            &"v1".to_owned().try_into().unwrap(),
            &"backend".to_owned().try_into().unwrap(),
        )
        .await
        .unwrap();
    let selected = |entrypoint: &str, path: &str, props: Vec<u8>| ServiceInvocation {
        entrypoint: Some(entrypoint.into()),
        props,
        rpc: Some(vec![path.into()]),
        get: true,
        signals: vec![],
        signal_owner: None,
    };
    let props = bytes(
        callee
            .invoke_service(
                app,
                request(&apps[0], "/"),
                selected("default", "value", vec![]),
                None,
                None,
            )
            .await
            .unwrap(),
    )
    .await;
    let result = bytes(
        callee
            .invoke_service(
                app,
                request(&apps[0], "/"),
                selected("Named", "props", props.clone()),
                None,
                None,
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(result, props);
    drop(callee);
    let (late_release, late_body) = tokio::sync::mpsc::channel::<Vec<u8>>(1);
    let late_body = Box::pin(futures_util::stream::unfold(
        late_body,
        |mut receiver| async {
            receiver
                .recv()
                .await
                .map(|bytes| (Ok::<_, String>(bytes), receiver))
        },
    ));
    let late = {
        let (dispatcher, app, request) =
            (dispatcher.clone(), app.clone(), request(&apps[0], "/late"));
        tokio::spawn(async move {
            dispatcher
                .dispatch(&app, Duration::from_secs(30), request, Some(late_body))
                .await
                .unwrap()
                .unwrap()
        })
    };
    assert!(
        !dispatcher
            .routes
            .pool
            .as_ref()
            .unwrap()
            .state
            .lock()
            .unwrap()
            .slots
            .contains_key(&(
                apps[0].clone(),
                "v1".to_owned().try_into().unwrap(),
                "cold".to_owned().try_into().unwrap()
            )),
        "cold callee was prewarmed"
    );
    let (cold_release, cold_body) = tokio::sync::mpsc::channel::<Vec<u8>>(1);
    let cold_body = Box::pin(futures_util::stream::unfold(
        cold_body,
        |mut receiver| async {
            receiver
                .recv()
                .await
                .map(|bytes| (Ok::<_, String>(bytes), receiver))
        },
    ));
    let late_cold = {
        let (dispatcher, app, request) = (
            dispatcher.clone(),
            app.clone(),
            request(&apps[0], "/late-cold"),
        );
        tokio::spawn(async move {
            dispatcher
                .dispatch(&app, Duration::from_secs(30), request, Some(cold_body))
                .await
                .unwrap()
                .unwrap()
        })
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let pending = {
                let state = dispatcher.routes.pool.as_ref().unwrap().state.lock().unwrap();
                matches!(state.slots.get(&(apps[0].clone(), "v1".to_owned().try_into().unwrap(), "caller".to_owned().try_into().unwrap())), Some(Slot::Ready(ready)) if ready.client.transport.pending() >= 2)
            };
            if pending { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("old caller event did not enter before replacement");
    let replacement = manifest(&apps[0], "v2");
    store.stage(&replacement).await.unwrap();
    let mut snapshot = store.read(&replacement.application).await.unwrap();
    store.activate(&mut snapshot, replacement).await.unwrap();
    store.commit(snapshot).await.unwrap();
    dispatcher.reconcile().await.unwrap();
    late_release.send(vec![1]).await.unwrap();
    cold_release.send(vec![1]).await.unwrap();
    drop(late_release);
    drop(cold_release);
    assert_eq!(bytes(late.await.unwrap()).await, b"services-one-v1");
    assert_eq!(
        bytes(late_cold.await.unwrap()).await,
        b"services-one-v1:cold"
    );
    let reply = dispatcher
        .dispatch(app, Duration::from_secs(30), request(&apps[0], "/"), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bytes(reply).await, b"services-one-v2");
    assert!(dispatcher
        .routes
        .pool
        .as_ref()
        .unwrap()
        .acquire(
            app,
            &apps[0],
            &"v1".to_owned().try_into().unwrap(),
            &"caller".to_owned().try_into().unwrap()
        )
        .await
        .is_err());
    assert!(dispatcher
        .routes
        .pool
        .as_ref()
        .unwrap()
        .acquire(
            app,
            &apps[0],
            &"v1".to_owned().try_into().unwrap(),
            &"cold".to_owned().try_into().unwrap()
        )
        .await
        .is_err());
    dispatcher.shutdown().await.unwrap();
    eprintln!("application service VMs: default/named entrypoints, RPC Get/deep Call, structured-clone props, original cross-script capability refusal, two same-name applications, credited 10MiB request/response, AbortSignal fetch cancellation, live/pre-aborted/concurrent RPC AbortSignal relay and pinned-old service calls through deployment replacement");
}

#[cfg(test)]
async fn check_container_bindings(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    directory: &Path,
) {
    use celld_applications::invocation_wire::HttpRequest;
    use futures_util::StreamExt;
    if std::env::var_os("DOCKER_HOST").is_none() {
        eprintln!("skipping the Containers VM check: DOCKER_HOST must name a native Docker daemon whose bridge addresses this host can reach");
        return;
    }
    const SOURCE: &str = r#"
import { DurableObject } from 'cloudflare:workers';
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const text = (bytes) => new TextDecoder().decode(bytes);
export class Counter extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
    const c = this.ctx.container;
    switch (url.pathname) {
      case '/running': return new Response(String(c.running));
      case '/start': {
        c.start({ env: { WHO: this.env.LABEL } });
        this.exited = c.monitor().then(() => 'exited', (error) => 'error:' + error.message);
        for (let i = 0; i < 100; i++) {
          try {
            const reply = await c.getTcpPort(8080).fetch('http://app.test/index.html');
            return new Response(await reply.text() + ':' + c.running);
          } catch { await sleep(100); }
        }
        return new Response('never listened', { status: 500 });
      }
      case '/mark': {
        const process = await c.exec(['sh', '-c', 'cat > /www/who'], { stdin: 'pipe' });
        const writer = process.stdin.getWriter();
        await writer.write(new TextEncoder().encode(url.searchParams.get('v')));
        await writer.close();
        return new Response(String(await process.exitCode));
      }
      case '/who': return new Response(await (await c.getTcpPort(8080).fetch('http://app.test/who')).text());
      case '/output': {
        const process = await c.exec(['sh', '-c', 'echo out; echo err >&2; exit 4']);
        const output = await process.output();
        return Response.json({ stdout: text(output.stdout), stderr: text(output.stderr), exit: output.exitCode });
      }
      case '/redirect': {
        const manual = await c.getTcpPort(8080).fetch('http://app.test/dir', { redirect: 'manual' });
        const followed = await c.getTcpPort(8080).fetch('http://app.test/dir');
        return Response.json({ status: manual.status, location: manual.headers.get('location'), followed: await followed.text() });
      }
      case '/echo': {
        const socket = c.getTcpPort(9000).connect('ignored:9000');
        const writer = socket.writable.getWriter();
        await writer.write(new TextEncoder().encode('ping'));
        const reader = socket.readable.getReader();
        let got = '';
        while (got.length < 4) {
          const { value, done } = await reader.read();
          if (done) break;
          got += text(value);
        }
        await writer.close();
        let rest = '';
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          rest += text(value);
        }
        return new Response(got + '|' + rest);
      }
      case '/tls':
        try { c.getTcpPort(9000).connect('ignored:9000', { secureTransport: 'on' }); return new Response('connected'); }
        catch (error) { return new Response(error.message); }
      case '/destroy': await c.destroy(); return new Response(String(c.running));
      case '/slow-start':
        c.start({ entrypoint: ['sleep', '3600'] });
        await new Promise(() => {});
        return new Response('unreachable');
    }
    return new Response('unknown', { status: 404 });
  }
}
export default { fetch(request, env) { return env.BOX.getByName('same').fetch(request); } };
"#;
    let (fence, workload) = crate::application_container::docker::images();
    let source = directory.join("containers-source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("main.mjs"), SOURCE).unwrap();
    let image = directory.join("containers.bundle");
    let built = Command::new("/usr/bin/python3")
        .arg(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../applications/guest/build-application-image.py"),
        )
        .arg(&source)
        .arg(&image)
        .env_clear()
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .kill_on_drop(true)
        .output()
        .await
        .unwrap();
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let artifact = std::fs::read(image).unwrap();
    let digest = content_digest(&artifact);
    let apps: Vec<ApplicationId> = ["containers-one", "containers-two"]
        .into_iter()
        .map(|tenant| ApplicationId {
            app: tenant.to_owned().try_into().unwrap(),
        })
        .collect();
    let resource = |kind, id: &str| ResourceRef {
        kind,
        id: id.to_owned().try_into().unwrap(),
    };
    let objects = resource(celld_applications::ResourceKind::DurableObject, "objects");
    let boxed = resource(celld_applications::ResourceKind::Container, "box");
    for id in &apps {
        let mut snapshot = store.read(id).await.unwrap();
        snapshot.create(id.clone()).unwrap();
        for (resource, configuration) in [
            (&objects, serde_json::json!({})),
            (&boxed, serde_json::json!({ "image": workload })),
        ] {
            snapshot
                .register_resource(
                    id,
                    crate::applications::Resource {
                        resource: resource.clone(),
                        configuration,
                    },
                )
                .unwrap();
        }
        store.commit(snapshot).await.unwrap();
    }
    for id in &apps {
        store
            .upload_artifact(id, &digest, artifact.clone().into())
            .await
            .unwrap();
        let label = id.app.as_str();
        let deployment: Deployment = serde_json::from_value(serde_json::json!({
            "manifest_version": 1, "application": id, "id": "v1",
            "handlers": [{
                "id": "javascript", "language": "javascript", "entrypoint": "main.mjs",
                "artifact_sha256": digest, "runtime_image_sha256": options.runtime.sha256,
                "http": true, "environment": {"LABEL": label},
                "bindings": {"BOX": {"kind": "resource", "resource": objects}, "BOXED": {"kind": "resource", "resource": boxed}},
                "worker_config": {
                    "do_classes": ["Counter"],
                    "durable_objects": {"bindings": [{"name": "BOX", "class_name": "Counter"}]},
                    "containers": [{"binding": "BOXED", "class_name": "Counter"}],
                    "compatibility_date": "2026-03-01", "compatibility_flags": ["js_rpc"]
                }
            }]
        }))
        .unwrap();
        store.stage(&deployment).await.unwrap();
        let mut snapshot = store.read(&deployment.application).await.unwrap();
        store.activate(&mut snapshot, deployment).await.unwrap();
        store.commit(snapshot).await.unwrap();
    }
    let dispatcher = Arc::new(
        RouteDispatcher::new(
            VmHostConfig {
                jailer: options.jailer.clone(),
                firecracker: options.firecracker.clone(),
                kernel: options.kernel.clone(),
                runtimes: vec![options.runtime.clone()],
                uid: options.uid,
                gid: options.gid,
                max_vms: 4,
                max_memory_mib: 12_288,
                idle_ttl_secs: 600,
                platform_domain: TEST_DOMAIN.into(),
                container_fence_image: Some(fence),
                artifact_cache_ttl_secs: None,
                metadata_cache_entries: None,
                metadata_cache_bytes: None,
                startup_pool: 0,
                memory_guard: None,
            },
            store.clone(),
            "containers-test".into(),
        )
        .await
        .unwrap(),
    );
    let mut hosts = HashMap::new();
    for id in &apps {
        hosts.insert(id.clone(), assigned_host(store, id, "javascript").await);
    }
    let request = |id: &ApplicationId, path: &str| HttpRequest {
        method: "GET".into(),
        url: format!("http://{}{path}", hosts[id]),
        headers: vec![],
        body: vec![],
    };
    let call = |id: &ApplicationId, path: &str| {
        let (dispatcher, app, request) = (dispatcher.clone(), app.clone(), request(id, path));
        async move {
            let mut reply = dispatcher
                .dispatch(&app, Duration::from_secs(60), request, None)
                .await
                .unwrap()
                .unwrap();
            if let Some(mut stream) = reply.stream.take() {
                while let Some(chunk) = stream.next().await {
                    reply.body.extend(chunk.unwrap());
                }
            }
            (reply.status, String::from_utf8(reply.body).unwrap())
        }
    };
    let scope = |id: &ApplicationId| {
        let objects = ResourceAddress {
            application: id.clone(),
            resource: objects.clone(),
        };
        objects.cell_scope("Counter", "same").unwrap()
    };
    // A container exists on the engine, whatever the node calls itself.
    let exists = |id: &ApplicationId| {
        let filter = format!("label=celld.cell={}", scope(id));
        !crate::application_container::docker::cli(&["ps", "-aq", "--filter", &filter], None)
            .is_empty()
    };
    let (one, two) = (&apps[0], &apps[1]);
    // start(), an immediate monitor() and a port fetch, in two applications.
    for id in &apps {
        let (status, body) = call(id, "/start").await;
        assert_eq!(
            (status, body.as_str()),
            (200, "ok:true"),
            "{}",
            id.app.as_str()
        );
        assert!(exists(id));
    }
    // One class, alias and object name, two containers: each keeps its own files.
    for id in &apps {
        let mark = format!("/mark?v={}", id.app.as_str());
        assert_eq!(call(id, &mark).await.1, "0");
    }
    for id in &apps {
        assert_eq!(call(id, "/who").await.1, id.app.as_str());
    }
    // exec output and exit code; redirect handling; raw TCP with a half-close.
    let output: serde_json::Value = serde_json::from_str(&call(one, "/output").await.1).unwrap();
    assert_eq!(
        output,
        serde_json::json!({"stdout": "out\n", "stderr": "err\n", "exit": 4})
    );
    let redirect: serde_json::Value =
        serde_json::from_str(&call(one, "/redirect").await.1).unwrap();
    assert_eq!(
        redirect,
        serde_json::json!({"status": 302, "location": "/dir/", "followed": "nested"})
    );
    assert_eq!(call(one, "/echo").await.1, "ping|");
    assert!(call(one, "/tls")
        .await
        .1
        .contains("TLS is not currently supported"));
    // destroy ends one application's container and only that one.
    assert_eq!(call(one, "/destroy").await.1, "false");
    assert!(!exists(one) && exists(two));
    assert_eq!(call(one, "/running").await.1, "false");
    // An aborted request while a start is in flight leaves no orphan: the
    // container exists exactly when its owner's native record says it runs.
    assert_eq!(call(two, "/destroy").await.1, "false");
    let aborted = tokio::time::timeout(Duration::from_millis(700), call(two, "/slow-start")).await;
    assert!(aborted.is_err(), "the request was meant to hang");
    tokio::time::timeout(Duration::from_secs(30), async {
        while app.public_in_flight() != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the aborted request did not settle");
    let engine = crate::container::engine().await.unwrap();
    let recorded = |id: &ApplicationId| engine.cell(&scope(id)).is_some_and(|cell| cell.running());
    assert_eq!(
        exists(two),
        recorded(two),
        "a container without an owner record"
    );
    // VM retirement keeps the containers, which belong to their native
    // objects; retiring an owner removes only its own.
    if !recorded(two) {
        assert_eq!(call(two, "/start").await.1, "ok:true");
    }
    dispatcher.shutdown().await.unwrap();
    assert!(
        exists(two) && recorded(two),
        "retiring the VM removed the container"
    );
    let runtime = app.runtime.as_ref().expect("the checks run on a runtime");
    for id in &apps {
        let cell = scope(id);
        if let Some(epoch) = app.resident_epoch(cell.clone()).await {
            runtime
                .stop_cell(&cell, epoch, crate::runtime::StopMode::CloseInPlace)
                .await
                .unwrap();
        }
        assert!(!exists(id), "owner retirement left {cell}'s container");
    }
    eprintln!("application containers: two same-name applications each started, monitored, exec'd, fetched and connected to their own container from guest JS; destroy and an aborted in-flight start left no orphan; VM retirement kept the containers and owner retirement removed them; native Docker, no independent-host qualification");
}

#[cfg(test)]
pub(crate) async fn check_supervised_handler_bridge(
    app: &crate::actor::AppHandle,
    runtime: &crate::runtime::RuntimeManager,
    resource: &ResourceAddress,
    _cell: &str,
) {
    use crate::applications::Resource;
    fn manifest(resource: &ResourceAddress, id: &str, artifact: &str, image: &str) -> Deployment {
        serde_json::from_value(serde_json::json!({"manifest_version":1,"application":resource.application,"id":id,"handlers":[{"id":"javascript","language":"javascript","entrypoint":"main.mjs","artifact_sha256":artifact,"runtime_image_sha256":image,"environment":{"APP_LABEL":id},"secrets":{"PRIVATE_TOKEN":"api-token"},"bindings":{"COUNTER":{"kind":"resource","resource":resource.resource}},"worker_config":{"do_classes":["Counter"]}}]})).unwrap()
    }
    let options = LaunchOptions {
        jailer: "/usr/local/bin/jailer".into(),
        firecracker: "/usr/local/bin/firecracker".into(),
        kernel: ApprovedFile {
            path: std::env::var_os("CELLD_APPLICATION_JAVASCRIPT_TEST_KERNEL")
                .map(PathBuf::from)
                .unwrap_or_else(|| "/opt/celld/vmlinux".into()),
            sha256: std::env::var("CELLD_APPLICATION_JAVASCRIPT_TEST_KERNEL_SHA256")
                .unwrap_or_else(|_| "a".repeat(64)),
        },
        runtime: ApprovedFile {
            path: std::env::var_os("CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE")
                .map(PathBuf::from)
                .unwrap_or_else(|| "/unavailable.ext4".into()),
            sha256: "b".repeat(64),
        },
        adapter: Language::Javascript,
        uid: 1000,
        gid: 1000,
        vcpus: 1,
        memory_mib: 1024,
        cpu_quota: 25_000,
        templates: Arc::new(Templates::new().unwrap()),
        shared: None,
    };
    let registered = vec![crate::applications::Resource {
        resource: resource.resource.clone(),
        configuration: serde_json::json!({}),
    }];
    let valid = manifest(resource, "one", &"c".repeat(64), &options.runtime.sha256);
    assert!(prepare_pin(
        &options,
        valid.clone(),
        &"javascript".to_owned().try_into().unwrap(),
        &registered
    )
    .is_ok());
    let mut invalid = valid.clone();
    invalid.handlers[0].runtime_image_sha256 = "d".repeat(64);
    assert!(prepare_pin(
        &options,
        invalid,
        &"javascript".to_owned().try_into().unwrap(),
        &registered
    )
    .is_err());
    let mut invalid = valid;
    invalid.handlers[0].secrets.insert(
        "SECRET".to_owned().try_into().unwrap(),
        "missing".to_owned().try_into().unwrap(),
    );
    assert!(prepare_pin(
        &options,
        invalid,
        &"javascript".to_owned().try_into().unwrap(),
        &registered
    )
    .is_ok());
    let files = tempfile::tempdir().unwrap();
    let approved = files.path().join("approved.img");
    let bytes = b"immutable approved guest image";
    std::fs::write(&approved, bytes).unwrap();
    let source = ApprovedFile {
        path: approved.clone(),
        sha256: content_digest(bytes),
    };
    let copied = files.path().join("copied.img");
    copy_pinned(source.clone(), copied.clone(), 1024)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&copied).unwrap(), bytes);
    assert_eq!(std::fs::metadata(&copied).unwrap().mode() & 0o777, 0o444);
    assert!(
        copy_pinned(source.clone(), files.path().join("small.img"), 4)
            .await
            .is_err()
    );
    let mut mismatch = source.clone();
    mismatch.sha256 = "0".repeat(64);
    assert!(
        copy_pinned(mismatch, files.path().join("mismatch.img"), 1024)
            .await
            .is_err()
    );
    let link = files.path().join("linked.img");
    std::os::unix::fs::symlink(&approved, &link).unwrap();
    assert!(copy_pinned(
        ApprovedFile {
            path: link,
            ..source
        },
        files.path().join("linked-copy.img"),
        1024,
    )
    .await
    .is_err());
    let failed_store = Arc::new(ApplicationStore::new(
        crate::bucket::Bucket::open_dev(&files.path().join("startup.sqlite")).unwrap(),
    ));
    let failed = VmPool::new(
        VmHostConfig {
            jailer: options.jailer.clone(),
            firecracker: options.firecracker.clone(),
            kernel: options.kernel.clone(),
            runtimes: vec![options.runtime.clone()],
            uid: options.uid,
            gid: options.gid,
            max_vms: 1,
            max_memory_mib: 12288,
            idle_ttl_secs: 1,
            platform_domain: TEST_DOMAIN.into(),
            container_fence_image: None,
            artifact_cache_ttl_secs: None,
            metadata_cache_entries: None,
            metadata_cache_bytes: None,
            startup_pool: 0,
            memory_guard: None,
        },
        failed_store,
        "startup-failed".into(),
    )
    .unwrap();
    assert!(failed.prepare(app).await.is_err());
    assert!(!failed.templates.ready());
    assert_eq!(
        (failed.snapshot().vms, failed.snapshot().memory_mib),
        (0, 0)
    );
    let lookup = LaunchOptions {
        templates: failed.templates.clone(),
        ..options.clone()
    };
    assert!(failed.templates.get(&lookup).await.ok().flatten().is_none());
    assert!(failed.templates.pyodide(&lookup, app).await.is_err());
    assert!(
        failed.templates.state_files().is_empty(),
        "a missing startup cache was built on lookup"
    );
    failed.shutdown().await.unwrap();
    let panicked = VmPool::new(
        failed.config.clone(),
        failed.store.clone(),
        "startup-panicked".into(),
    )
    .unwrap();
    let charge = panicked.charge_startup(256).await.unwrap();
    let error = tokio::spawn(async move {
        let _charge = charge;
        panic!("injected startup task panic");
    })
    .await
    .context(ReapUnproved)
    .unwrap_err();
    assert!(error.is::<ReapUnproved>());
    let snapshot = panicked.snapshot();
    assert_eq!(
        (
            snapshot.vms,
            snapshot.memory_mib,
            snapshot.starting,
            snapshot.quarantined
        ),
        (1, 256, 0, 1)
    );
    assert!(timeout(Duration::from_secs(1), panicked.shutdown())
        .await
        .unwrap()
        .is_err());
    if std::env::var_os("CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE").is_none() {
        return;
    }
    let mut options = options;
    options.runtime.sha256 = std::fs::read_to_string(options.runtime.path.with_extension("sha256"))
        .unwrap()
        .trim()
        .into();
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(ApplicationStore::new(
        crate::bucket::Bucket::open_dev(&directory.path().join("management.sqlite")).unwrap(),
    ));
    // The images these launches link into their jails.
    store.enable_artifact_cache(
        crate::applications::ArtifactCache::new(Duration::from_secs(600)).unwrap(),
    );
    let mut snapshot = store.read(&resource.application).await.unwrap();
    snapshot.create(resource.application.clone()).unwrap();
    snapshot
        .register_resource(
            &resource.application,
            Resource {
                resource: resource.resource.clone(),
                configuration: serde_json::json!({}),
            },
        )
        .unwrap();
    let secret_name: Name = "api-token".to_owned().try_into().unwrap();
    let secret_version = store
        .stage_secret(
            &resource.application,
            &secret_name,
            "application-one".into(),
        )
        .await
        .unwrap();
    snapshot
        .set_secret(&resource.application, secret_name.clone(), secret_version)
        .unwrap();
    snapshot
        .set_limits(
            &resource.application,
            Some(ApplicationLimits {
                size_class: ApplicationSizeClass::Basic,
                invocation_timeout_ms: 3000,
            }),
        )
        .unwrap();
    store.commit(snapshot).await.unwrap();
    fn owned_vm_pids() -> HashSet<u32> {
        // Observe only this test process's direct jailer/VMM children. This
        // check neither kills nor changes any externally managed VM.
        let parent = format!("PPid:\t{}", std::process::id());
        std::fs::read_dir("/proc")
            .unwrap()
            .flatten()
            .filter_map(|item| {
                let pid = item.file_name().to_str()?.parse::<u32>().ok()?;
                let status = std::fs::read_to_string(item.path().join("status")).ok()?;
                let command = std::fs::read_to_string(item.path().join("comm")).ok()?;
                (status.lines().any(|line| line == parent)
                    && matches!(command.trim(), "jailer" | "firecracker"))
                .then_some(pid)
            })
            .collect()
    }
    assert!(owned_vm_pids().is_empty());
    // T05: staged JavaScript cold-launches through Vm::launch and its
    // production Dispatcher; the original Actor admits every event and gates
    // every response byte. Each VM keeps the secret version it launched with.
    if std::env::var_os("CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE").is_some() {
        use celld_applications::invocation_wire::HttpRequest;
        use futures_util::StreamExt;
        let mut javascript_options = options.clone();
        javascript_options.adapter = Language::Javascript;
        javascript_options.runtime.path =
            std::env::var_os("CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE")
                .unwrap()
                .into();
        javascript_options.runtime.sha256 =
            std::fs::read_to_string(javascript_options.runtime.path.with_extension("sha256"))
                .unwrap()
                .trim()
                .into();
        if std::env::var_os("CELLD_APPLICATION_MEMORY_CHECK_ONLY").is_some() {
            // Restored and pooled VMs alone: their randomness and lifecycle.
            if std::env::var_os("CELLD_APPLICATION_POOL_CHECK_ONLY").is_some() {
                check_restored_vms(
                    app,
                    &store,
                    &javascript_options,
                    directory.path(),
                    &resource.application,
                )
                .await;
                assert!(owned_vm_pids().is_empty(), "restored VMM was not reaped");
                check_startup_pool(
                    app,
                    &store,
                    &javascript_options,
                    directory.path(),
                    &resource.application,
                    owned_vm_pids,
                )
                .await;
                assert!(
                    owned_vm_pids().is_empty(),
                    "startup pool VMM was not reaped"
                );
                return;
            }
            if std::env::var_os("CELLD_APPLICATION_MEMORY_LOADER_BACKGROUND_CHECK_ONLY").is_some() {
                loader_background_baseline::run(
                    app,
                    &store,
                    &javascript_options,
                    directory.path(),
                    owned_vm_pids,
                )
                .await;
                return;
            }
            if std::env::var_os("CELLD_APPLICATION_MEMORY_WORKING_SET_CHECK_ONLY").is_some() {
                working_set_baseline::run(
                    app,
                    &store,
                    &javascript_options,
                    directory.path(),
                    owned_vm_pids,
                )
                .await;
                return;
            }
            if std::env::var_os("CELLD_APPLICATION_MEMORY_QUARANTINE_CHECK_ONLY").is_some() {
                quarantine_baseline::run(
                    app,
                    &store,
                    &javascript_options,
                    directory.path(),
                    owned_vm_pids,
                )
                .await;
                return;
            }
            if std::env::var_os("CELLD_APPLICATION_MEMORY_DENSITY_CHECK_ONLY").is_some() {
                density_baseline::run(
                    app,
                    &store,
                    &javascript_options,
                    directory.path(),
                    owned_vm_pids,
                )
                .await;
                return;
            }
            if std::env::var_os("CELLD_APPLICATION_MEMORY_CACHE_FAILURE_CHECK_ONLY").is_some() {
                cache_failure_baseline::run(
                    app,
                    &store,
                    &javascript_options,
                    directory.path(),
                    owned_vm_pids,
                )
                .await;
                return;
            }
            if std::env::var_os("CELLD_APPLICATION_CPU_CHECK_ONLY").is_some() {
                cpu_baseline::run(
                    app,
                    &store,
                    &javascript_options,
                    directory.path(),
                    owned_vm_pids,
                )
                .await;
                return;
            }
            if std::env::var_os("CELLD_APPLICATION_MEMORY_MIXED_WAKE_CHECK_ONLY").is_some() {
                mixed_wake_baseline::run(
                    app,
                    &store,
                    &javascript_options,
                    directory.path(),
                    owned_vm_pids,
                )
                .await;
                return;
            }
            if std::env::var_os("CELLD_APPLICATION_MEMORY_CACHE_CHECK_ONLY").is_some() {
                cache_baseline::run(
                    app,
                    &store,
                    &javascript_options,
                    directory.path(),
                    owned_vm_pids,
                )
                .await;
                return;
            }
            if std::env::var_os("CELLD_APPLICATION_MEMORY_SERVICE_WAKE_CHECK_ONLY").is_some() {
                service_wake_baseline::run(
                    app,
                    &store,
                    &javascript_options,
                    directory.path(),
                    owned_vm_pids,
                )
                .await;
                return;
            }
            if std::env::var_os("CELLD_APPLICATION_MEMORY_PRESSURE_CHECK_ONLY").is_some() {
                pressure_baseline::run(
                    app,
                    &store,
                    &javascript_options,
                    directory.path(),
                    owned_vm_pids,
                )
                .await;
                return;
            }
            if std::env::var_os("CELLD_APPLICATION_MEMORY_GUARD_CHECK_ONLY").is_some() {
                guard_baseline::run(
                    app,
                    &store,
                    &javascript_options,
                    directory.path(),
                    owned_vm_pids,
                )
                .await;
                return;
            }
            if std::env::var_os("CELLD_APPLICATION_MEMORY_LIFECYCLE_CHECK_ONLY").is_some() {
                lifecycle_baseline::run(
                    app,
                    &store,
                    &javascript_options,
                    directory.path(),
                    owned_vm_pids,
                )
                .await;
                return;
            }
            if std::env::var_os("CELLD_APPLICATION_MEMORY_REPORTING_CHECK_ONLY").is_some() {
                reporting_baseline::run(
                    app,
                    &store,
                    &javascript_options,
                    directory.path(),
                    owned_vm_pids,
                )
                .await;
                return;
            }
            if std::env::var_os("CELLD_APPLICATION_MEMORY_FREEZE_ONLY").is_some() {
                freeze_baseline::run(
                    app,
                    &store,
                    &javascript_options,
                    directory.path(),
                    owned_vm_pids,
                )
                .await;
                return;
            }
            memory_baseline::run(
                app,
                &store,
                &javascript_options,
                directory.path(),
                owned_vm_pids,
            )
            .await;
            return;
        }
        if std::env::var_os("CELLD_APPLICATION_CLASS_CHECK_ONLY").is_some() {
            check_startup_classes(
                app,
                &store,
                &javascript_options,
                directory.path(),
                owned_vm_pids,
            )
            .await;
            return;
        }
        if std::env::var_os("CELLD_APPLICATION_PYTHON_LITE_CHECK_ONLY").is_some() {
            (
                javascript_options.vcpus,
                javascript_options.memory_mib,
                javascript_options.cpu_quota,
            ) = ApplicationSizeClass::Lite.resources();
            check_python_handlers(
                app,
                &store,
                &javascript_options,
                &resource.application,
                owned_vm_pids,
            )
            .await;
            return;
        }
        let javascript_source = directory.path().join("javascript-source");
        std::fs::create_dir(&javascript_source).unwrap();
        std::fs::write(javascript_source.join("main.mjs"), r#"
import {VERSION} from './helper.mjs';
import label from './message.txt';
import wasm from './empty.wasm';
import {env} from 'cloudflare:workers';
if (label !== 'staged text' || !(wasm instanceof WebAssembly.Module)) throw new Error('staged module mismatch');
export class Counter {
  constructor(ctx) { this.ctx = ctx; }
  async fetch(request) {
    const path = new URL(request.url).pathname;
    const ctx = this.ctx;
    if (path === '/token') return new Response(env.PRIVATE_TOKEN);
    if (path === '/count') {
      let count = 0;
      await ctx.storage.transaction(async tx => {
        count = (await tx.get('staged-count') ?? 0) + 1;
        await tx.put('staged-count', count);
      });
      return new Response(String(count));
    }
    if (path === '/held') {
      ctx.waitUntil(ctx.storage.transaction(async tx => {
        await tx.put('staged-held', true);
        await new Promise(resolve => setTimeout(resolve, 400));
      }));
      return new Response('held');
    }
    if (path === '/stream') {
      let sent = 0;
      return new Response(new ReadableStream({ async pull(controller) {
        if (sent === 64) { controller.close(); return; }
        sent++;
        await ctx.storage.put('staged-streamed', sent);
        await new Promise(resolve => setTimeout(resolve, 2));
        controller.enqueue(new TextEncoder().encode('chunk' + sent + ';'));
      }}), {headers: [['set-cookie', 'one'], ['set-cookie', 'two']]});
    }
    if (path === '/error') throw new Error('staged handler error');
    if (path === '/abort') {
      await ctx.storage.put('staged-abort-prefix', true);
      ctx.abort('staged abort');
    }
    return new Response('staged object');
  }
}
export default { get fetch() {
    if (env.PRIVATE_TOKEN !== 'application-' + VERSION || env.APP_LABEL !== VERSION || 'AWS_SECRET_ACCESS_KEY' in env || 'S3_SECRET_KEY' in env) throw new Error('staged environment mismatch');
    console.log('CELLD_JS_STAGED_' + VERSION + '_MODULES_AND_ENV_LOADED');
    return () => new Response('staged handler');
}};
"#).unwrap();
        std::fs::write(javascript_source.join("message.txt"), "staged text").unwrap();
        std::fs::write(
            javascript_source.join("empty.wasm"),
            [0, 97, 115, 109, 1, 0, 0, 0],
        )
        .unwrap();
        let mut vms = Vec::new();
        for version in ["one", "two"] {
            let next = store
                .stage_secret(
                    &resource.application,
                    &secret_name,
                    format!("application-{version}"),
                )
                .await
                .unwrap();
            let mut snapshot = store.read(&resource.application).await.unwrap();
            snapshot
                .set_secret(&resource.application, secret_name.clone(), next)
                .unwrap();
            store.commit(snapshot).await.unwrap();
            std::fs::write(
                javascript_source.join("helper.mjs"),
                format!("export const VERSION = '{version}';"),
            )
            .unwrap();
            let image = directory
                .path()
                .join(format!("javascript-{version}.bundle"));
            let built = timeout(
                Duration::from_secs(30),
                Command::new("/usr/bin/python3")
                    .arg(
                        Path::new(env!("CARGO_MANIFEST_DIR"))
                            .join("../applications/guest/build-application-image.py"),
                    )
                    .arg(&javascript_source)
                    .arg(&image)
                    .env_clear()
                    .env("PYTHONDONTWRITEBYTECODE", "1")
                    .kill_on_drop(true)
                    .output(),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(
                built.status.success(),
                "JavaScript artifact build failed: {}",
                String::from_utf8_lossy(&built.stderr)
            );
            let body = std::fs::read(image).unwrap();
            let digest = content_digest(&body);
            store
                .upload_artifact(&resource.application, &digest, body.into())
                .await
                .unwrap();
            let mut staged = manifest(
                resource,
                version,
                &digest,
                &javascript_options.runtime.sha256,
            );
            staged.id = format!("javascript-{version}").try_into().unwrap();
            staged.handlers[0].id = "javascript".to_owned().try_into().unwrap();
            staged.handlers[0].language = Language::Javascript;
            staged.handlers[0].entrypoint = "main.mjs".into();
            staged.handlers[0].worker_config = serde_json::json!({"do_classes":["Counter"],"compatibility_date":"2026-03-01","modules":[
                {"name":"helper.mjs","path":"helper.mjs","kind":"es_module"},
                    {"name":"./message.txt","path":"message.txt","kind":"text"},
                {"name":"empty.wasm","path":"empty.wasm","kind":"wasm"}]});
            store.stage(&staged).await.unwrap();
            let vm = timeout(
                Duration::from_secs(30),
                Vm::launch(
                    javascript_options.clone(),
                    &store,
                    &resource.application,
                    &staged.id,
                    &staged.handlers[0].id,
                ),
            )
            .await
            .unwrap()
            .unwrap_or_else(|error| panic!("staged JavaScript launch failed: {error:#}"));
            assert_eq!(
                store
                    .deployment(&resource.application, &staged.id)
                    .await
                    .unwrap()
                    .unwrap(),
                staged
            );
            vms.push(vm);
        }
        let call = |vm: &Vm, name: &str, path: &str, deadline: Duration| {
            let (client, app, resource) = (vm.client(), app.clone(), resource.clone());
            let (name, url) = (name.to_owned(), format!("http://staged{path}"));
            async move {
                client
                    .invoke(
                        app,
                        resource,
                        "Counter".into(),
                        name,
                        deadline,
                        HttpRequest {
                            method: "GET".into(),
                            url,
                            headers: vec![],
                            body: vec![],
                        },
                    )
                    .await
                    .unwrap_or_else(|error| {
                        panic!("staged invocation failed: {error:#}; {}", client.console())
                    })
            }
        };
        let text =
            |completion: &crate::application_guest::GuestCompletion<crate::js::HttpResponse>| {
                match &completion.result {
                    Ok(response) => String::from_utf8_lossy(&response.body).into_owned(),
                    Err(error) => panic!("staged handler failed: {error:#}"),
                }
            };
        let idle = || async {
            timeout(Duration::from_secs(10), async {
                while app.public_in_flight() != 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("staged event did not settle");
        };
        // One object is served by one VM at a time: pinning uses its own names.
        let stored = |name: &str, key: &'static str| {
            let cell = resource.cell_scope("Counter", name).unwrap();
            let (app, runtime) = (app.clone(), runtime.clone());
            async move {
                let epoch = app.resident_epoch(cell.clone()).await.unwrap();
                let admission = runtime.admit_storage_request(&cell, epoch).unwrap();
                runtime
                    .admitted_storage_turn(&admission, |_| {
                        (
                            crate::storage::get_stored(&cell, key).unwrap().is_some(),
                            crate::storage::sync_sample(&cell)
                                .unwrap()
                                .unwrap()
                                .in_transaction,
                        )
                    })
                    .await
                    .unwrap()
            }
        };
        // Secret rotation stays pinned per VM: one launched before rotating.
        for (index, version) in ["one", "two"].into_iter().enumerate() {
            let client = vms[index].client();
            let token = match client
                .invoke(
                    app.clone(),
                    resource.clone(),
                    "Counter".into(),
                    format!("pin-{version}"),
                    Duration::from_secs(5),
                    HttpRequest {
                        method: "GET".into(),
                        url: "http://staged/token".into(),
                        headers: vec![],
                        body: vec![],
                    },
                )
                .await
            {
                Ok(token) => token,
                Err(error) => {
                    let exit = vms.remove(index).wait().await;
                    panic!(
                        "staged {version} invocation failed: {error:#}; {}",
                        exit.map(|exit| exit.console).unwrap_or_default()
                    );
                }
            };
            assert_eq!(text(&token), format!("application-{version}"));
        }
        idle().await;
        let two = &vms[1];
        // Real handlers update native storage; gated bytes reach the caller.
        for expected in ["1", "2"] {
            let count = call(two, "named-object", "/count", Duration::from_secs(5)).await;
            assert_eq!(text(&count), expected);
        }
        idle().await;
        assert_eq!(stored("named-object", "staged-count").await, (true, false));
        // A held transaction delays its own reply while another object's
        // event completes through the same VM and Actor.
        let started = Instant::now();
        let held = tokio::spawn(call(two, "named-object", "/held", Duration::from_secs(10)));
        tokio::time::sleep(Duration::from_millis(50)).await;
        let other = call(two, "other", "/count", Duration::from_secs(5)).await;
        assert_eq!(text(&other), "1");
        assert!(!held.is_finished(), "held reply escaped its transaction");
        let held = held.await.unwrap();
        assert_eq!(text(&held), "held");
        assert!(started.elapsed() >= Duration::from_millis(350));
        drop((held, other));
        idle().await;
        assert_eq!(stored("named-object", "staged-held").await, (true, false));
        // Streams: duplicate headers survive and every chunk is proof-gated.
        let mut streamed = call(two, "named-object", "/stream", Duration::from_secs(10)).await;
        let response = streamed.result.as_mut().unwrap();
        let cookies: Vec<_> = response
            .headers
            .iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case("set-cookie"))
            .map(|(_, value)| value.as_str())
            .collect();
        assert_eq!(cookies, ["one", "two"]);
        let mut body = response.stream.take().expect("staged stream body");
        let mut received = String::new();
        while let Some(chunk) = timeout(Duration::from_secs(5), body.next()).await.unwrap() {
            received.push_str(&String::from_utf8(chunk.unwrap()).unwrap());
        }
        assert!(received.starts_with("chunk1;") && received.ends_with("chunk64;"));
        drop((body, streamed));
        idle().await;
        // Dropping a body mid-stream joins the event's cleanup.
        let mut dropped = call(two, "other", "/stream", Duration::from_secs(10)).await;
        let mut body = dropped.result.as_mut().unwrap().stream.take().unwrap();
        assert!(timeout(Duration::from_secs(5), body.next())
            .await
            .unwrap()
            .is_some());
        drop((body, dropped));
        idle().await;
        let failed = call(two, "named-object", "/error", Duration::from_secs(5)).await;
        assert!(
            format!("{:#}", failed.result.as_ref().err().unwrap()).contains("staged handler error")
        );
        drop(failed);
        idle().await;
        // Shutdown reaps the VMM after its events drained.
        let mut vms = vms.into_iter();
        let (one, two) = (vms.next().unwrap(), vms.next().unwrap());
        let exit = two.shutdown().await.unwrap();
        assert_eq!(exit.reason, "shutdown");
        assert!(exit
            .console
            .contains("CELLD_JS_STAGED_two_MODULES_AND_ENV_LOADED"));
        // A failed object (abort) retires its VM, which is reaped before settling.
        let aborted = call(&one, "pin-one", "/abort", Duration::from_secs(10)).await;
        assert!(aborted.result.is_err());
        drop(aborted);
        idle().await;
        assert_eq!(
            stored("pin-one", "staged-abort-prefix").await,
            (true, false)
        );
        let exit = timeout(Duration::from_secs(10), one.wait())
            .await
            .unwrap()
            .unwrap();
        assert_ne!(exit.reason, "shutdown");
        assert!(exit
            .console
            .contains("CELLD_JS_STAGED_one_MODULES_AND_ENV_LOADED"));
        assert!(owned_vm_pids().is_empty(), "staged VMM was not reaped");
        check_stateless_handlers(
            app,
            &store,
            &javascript_options,
            directory.path(),
            &resource.application,
        )
        .await;
        assert!(owned_vm_pids().is_empty(), "stateless VMM was not reaped");
        check_restored_vms(
            app,
            &store,
            &javascript_options,
            directory.path(),
            &resource.application,
        )
        .await;
        assert!(owned_vm_pids().is_empty(), "restored VMM was not reaped");
        check_template_retry(&javascript_options, directory.path()).await;
        assert!(
            owned_vm_pids().is_empty(),
            "retried template VMM was not reaped"
        );
        check_startup_classes(
            app,
            &store,
            &javascript_options,
            directory.path(),
            owned_vm_pids,
        )
        .await;
        check_startup_pool(
            app,
            &store,
            &javascript_options,
            directory.path(),
            &resource.application,
            owned_vm_pids,
        )
        .await;
        assert!(
            owned_vm_pids().is_empty(),
            "startup pool VMM was not reaped"
        );
        check_streams_and_egress(app, &store, &javascript_options, directory.path(), resource)
            .await;
        assert!(owned_vm_pids().is_empty(), "egress VMM was not reaped");
        check_object_bindings(app, &store, &javascript_options, directory.path(), resource).await;
        assert!(
            owned_vm_pids().is_empty(),
            "object binding VMM was not reaped"
        );
        check_hibernating_sockets(app, &store, &javascript_options, directory.path(), resource)
            .await;
        assert!(owned_vm_pids().is_empty(), "socket VMM was not reaped");
        check_d1_bindings(
            app,
            &store,
            &javascript_options,
            directory.path(),
            &resource.application,
        )
        .await;
        assert!(owned_vm_pids().is_empty(), "D1 VMM was not reaped");
        check_python_handlers(
            app,
            &store,
            &javascript_options,
            &resource.application,
            owned_vm_pids,
        )
        .await;
        assert!(owned_vm_pids().is_empty(), "Python VMM was not reaped");
        check_dynamic_loader(
            app,
            &store,
            &javascript_options,
            directory.path(),
            &resource.application,
        )
        .await;
        assert!(owned_vm_pids().is_empty(), "loader VMM was not reaped");
        check_facets(app, &store, &javascript_options, directory.path(), resource).await;
        assert!(owned_vm_pids().is_empty(), "facet VMM was not reaped");
        check_vm_pool(
            app,
            &store,
            &javascript_options,
            directory.path(),
            &resource.application,
            owned_vm_pids,
        )
        .await;
        assert!(owned_vm_pids().is_empty(), "pooled VMM was not reaped");
        check_route_dispatch(
            app,
            &store,
            &javascript_options,
            directory.path(),
            &resource.application,
            owned_vm_pids,
        )
        .await;
        assert!(owned_vm_pids().is_empty(), "routed VMM was not reaped");
        check_replacement_under_load(
            app,
            &store,
            &javascript_options,
            directory.path(),
            &resource.application,
            owned_vm_pids,
        )
        .await;
        assert!(owned_vm_pids().is_empty(), "replacement VMM was not reaped");
        check_service_bindings(app, &store, &javascript_options, directory.path()).await;
        assert!(owned_vm_pids().is_empty(), "service VMM was not reaped");
        check_container_bindings(app, &store, &javascript_options, directory.path()).await;
        assert!(owned_vm_pids().is_empty(), "container VMM was not reaped");
        eprintln!("owned JavaScript runtime: staged ES/text/Wasm artifacts cold-launched through Vm::launch and the production Dispatcher; each VM kept its launch-time secret after rotation; real handlers updated native storage and returned proof-gated bytes; a held transaction delayed its reply while another object completed; duplicate headers and 64 credited stream chunks survived; a mid-stream body drop and a handler error joined cleanup; shutdown and object abort reaped the VMMs; no independent-host qualification");
    }
    let mut snapshot = store.read(&resource.application).await.unwrap();
    snapshot
        .remove_secret(&resource.application, &secret_name)
        .unwrap();
    store.commit(snapshot).await.unwrap();
    let before = owned_vm_pids();
    assert!(Vm::launch(
        options,
        &store,
        &resource.application,
        &"javascript-two".to_owned().try_into().unwrap(),
        &"javascript".to_owned().try_into().unwrap()
    )
    .await
    .err()
    .unwrap()
    .to_string()
    .contains("secret is not registered"));
    assert_eq!(owned_vm_pids(), before, "missing secret started a VM");
    eprintln!("owned JavaScript bootstrap: application secret rotation, immutable manifests, private environment and missing-secret cold-launch refusal checked");
}
