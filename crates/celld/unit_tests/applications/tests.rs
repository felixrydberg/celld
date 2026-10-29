use super::*;
use celld_applications::Name;

#[test]
fn application_sizes_are_exact_and_custom_limits_are_refused() {
    let names = [
        "lite",
        "basic",
        "standard-1",
        "standard-2",
        "standard-3",
        "standard-4",
    ];
    let resources = [
        (1, 256, 6250),
        (1, 1024, 25000),
        (1, 4096, 50000),
        (1, 6144, 100000),
        (2, 8192, 200000),
        (4, 12288, 400000),
    ];
    for ((class, name), expected) in ApplicationSizeClass::ALL
        .into_iter()
        .zip(names)
        .zip(resources)
    {
        assert_eq!(class.resources(), expected);
        assert_eq!(serde_json::to_value(class).unwrap(), name);
        let limits: ApplicationLimits = serde_json::from_value(
            serde_json::json!({"size_class": name, "invocation_timeout_ms": 1000}),
        )
        .unwrap();
        assert_eq!(limits.size_class, class);
        limits.validate().unwrap();
    }
    for name in ["dev", "standard", "custom", "Lite"] {
        assert!(serde_json::from_value::<ApplicationLimits>(
            serde_json::json!({"size_class": name, "invocation_timeout_ms": 1000})
        )
        .is_err());
    }
    for field in ["vcpus", "memory_mib"] {
        let mut value = serde_json::json!({"size_class": "basic", "invocation_timeout_ms": 1000});
        value[field] = 1.into();
        assert!(serde_json::from_value::<ApplicationLimits>(value).is_err());
        let mut staged = serde_json::to_value(deployment(app("one"), "v1")).unwrap();
        staged["handlers"][0][field] = 1.into();
        assert!(serde_json::from_value::<Deployment>(staged).is_err());
    }
}

fn name(value: &str) -> Name {
    value.to_owned().try_into().unwrap()
}
fn app(id: &str) -> ApplicationId {
    ApplicationId { app: name(id) }
}
fn deployment(application: ApplicationId, version: &str) -> Deployment {
    serde_json::from_value(serde_json::json!({
        "manifest_version": 1, "application": application, "id": version,
        "handlers": [{"id":"main", "language":"javascript", "entrypoint":"index.js",
            "artifact_sha256":"a".repeat(64), "runtime_image_sha256":"b".repeat(64),
            "http":true,
            "bindings":{"DB":{"kind":"resource", "resource":{"kind":"d1", "id":"db"}}}}]
    }))
    .unwrap()
}

pub(crate) async fn application_artifact_cache_hits_refetch_eviction_and_isolation() {
    let directory = tempfile::tempdir().unwrap();
    let store = ApplicationStore::new(
        Bucket::open_dev(&directory.path().join("artifacts.sqlite")).unwrap(),
    );
    let ttl = Duration::from_secs(1);
    store.enable_artifact_cache(ArtifactCache::at(
        directory.path().join("cache"),
        ttl,
        1 << 20,
    ));
    let body = Bytes::from("artifact bytes");
    let digest = content_digest(&body);
    let (one, two) = (app("one"), app("two"));
    for id in [&one, &two] {
        store
            .upload_artifact(id, &digest, body.clone())
            .await
            .unwrap();
    }
    let cached = |id: &ApplicationId| {
        let (store, digest) = (&store, &digest);
        let id = id.clone();
        async move {
            let path = store.cached_artifact(&id, digest).await.unwrap()?;
            Some(Bytes::from(std::fs::read(path).unwrap()))
        }
    };
    assert_eq!(cached(&one).await, Some(body.clone()));
    // Warm: the bucket object is gone, so only the cache can answer.
    store
        .bucket
        .delete(&artifact_key(&one, &digest).unwrap())
        .await
        .unwrap();
    assert_eq!(cached(&one).await, Some(body.clone()));
    // The other application's identical digest is a separate entry, so it misses.
    let cache = store.cache.get().unwrap();
    let entry = |id: &ApplicationId| {
        cache
            .root
            .join(content_digest(id.storage_root().as_bytes()))
            .join(&digest)
    };
    assert!(entry(&one).exists() && !entry(&two).exists());
    assert_eq!(cached(&two).await, Some(body.clone()));
    assert!(entry(&two).exists());
    // A missing entry is refetched: with no bucket copy the load fails...
    std::fs::remove_file(entry(&one)).unwrap();
    assert!(cached(&one).await.is_none());
    // ...and with one the entry is restored.
    std::fs::remove_file(entry(&two)).unwrap();
    assert_eq!(cached(&two).await, Some(body.clone()));
    assert_eq!(std::fs::read(entry(&two)).unwrap(), body);
    // Expiry uses filesystem wall time and sweep throttling uses std Instant;
    // a Tokio sleep alone does not establish either precondition.
    age_cache_entry(cache, &entry(&two));
    store.sweep_artifact_cache().await;
    assert!(!entry(&two).exists());
}

fn age_cache_entry(cache: &ArtifactCache, path: &Path) {
    let age = cache.ttl * 2;
    std::fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(std::time::SystemTime::UNIX_EPOCH))
        .unwrap();
    *cache.swept.lock().unwrap() = Some(Instant::now() - age);
}

#[tokio::test]
async fn artifact_cache_expiry_sweeps_only_unused_entries() {
    // This check needs only the cache's Tokio blocking task, never the
    // process-wide Actor/runtime facade used by the aggregate fixture.
    let directory = tempfile::tempdir().unwrap();
    let cache = ArtifactCache::at(
        directory.path().to_owned(),
        Duration::from_secs(60),
        1 << 20,
    );
    let scope = directory.path().join("scope");
    std::fs::create_dir(&scope).unwrap();
    let expired = scope.join(content_digest(b"expired"));
    let fresh = scope.join(content_digest(b"fresh"));
    std::fs::write(&expired, b"expired").unwrap();
    std::fs::write(&fresh, b"fresh").unwrap();
    age_cache_entry(&cache, &expired);
    cache.sweep().await;
    assert!(!expired.exists());
    assert_eq!(std::fs::read(fresh).unwrap(), b"fresh");
}

pub(crate) async fn application_secret_rotation_isolation_and_limits() {
    let directory = tempfile::tempdir().unwrap();
    let store =
        ApplicationStore::new(Bucket::open_dev(&directory.path().join("secrets.sqlite")).unwrap());
    let secret = name("api-token");
    let mut versions = Vec::new();
    for (id, value) in [(app("one"), "scope-one"), (app("two"), "scope-two")] {
        let mut snapshot = store.read(&id).await.unwrap();
        snapshot.create(id.clone()).unwrap();
        snapshot
            .register_resource(
                &id,
                Resource {
                    resource: ResourceRef {
                        kind: ResourceKind::D1,
                        id: name("db"),
                    },
                    configuration: empty_object(),
                },
            )
            .unwrap();
        let version = store
            .stage_secret(&id, &secret, value.into())
            .await
            .unwrap();
        versions.push(version.clone());
        snapshot.set_secret(&id, secret.clone(), version).unwrap();
        store.commit(snapshot).await.unwrap();
    }
    let mut deployment = deployment(app("one"), "secret-version");
    deployment.handlers[0]
        .secrets
        .insert(name("PRIVATE_TOKEN"), secret.clone());
    let original = store.read(&app("one")).await.unwrap();
    let env = store
        .resolve_environment(
            original.application(&app("one")).unwrap(),
            &deployment.handlers[0],
        )
        .await
        .unwrap();
    assert_eq!(env[&name("PRIVATE_TOKEN")], "scope-one");
    let env = store
        .resolve_environment(
            store
                .read(&app("two"))
                .await
                .unwrap()
                .application(&app("two"))
                .unwrap(),
            &deployment.handlers[0],
        )
        .await
        .unwrap();
    assert_eq!(env[&name("PRIVATE_TOKEN")], "scope-two");
    let encoded = serde_json::to_string(original.application(&app("one")).unwrap()).unwrap();
    assert!(!encoded.contains("scope-one"));
    assert_ne!(versions[0], versions[1]);
    let mut forged = store.read(&app("one")).await.unwrap();
    forged
        .set_secret(&app("one"), secret.clone(), versions[1].clone())
        .unwrap();
    assert!(store
        .resolve_environment(
            forged.application(&app("one")).unwrap(),
            &deployment.handlers[0]
        )
        .await
        .is_err());

    let mut winner = store.read(&app("one")).await.unwrap();
    let mut loser = store.read(&app("one")).await.unwrap();
    let new = store
        .stage_secret(&app("one"), &secret, "rotation-winner".into())
        .await
        .unwrap();
    let uncommitted = store
        .stage_secret(&app("one"), &secret, "rotation-loser".into())
        .await
        .unwrap();
    winner.set_secret(&app("one"), secret.clone(), new).unwrap();
    loser
        .set_secret(&app("one"), secret.clone(), uncommitted)
        .unwrap();
    store.commit(winner).await.unwrap();
    assert!(store.commit(loser).await.unwrap_err().is::<Conflict>());
    let current = store.read(&app("one")).await.unwrap();
    assert_eq!(
        store
            .resolve_environment(
                current.application(&app("one")).unwrap(),
                &deployment.handlers[0]
            )
            .await
            .unwrap()[&name("PRIVATE_TOKEN")],
        "rotation-winner"
    );
    // A cold launch already holding an old snapshot reads its immutable
    // version, even after a concurrent rotation publishes another version.
    assert_eq!(
        store
            .resolve_environment(
                original.application(&app("one")).unwrap(),
                &deployment.handlers[0]
            )
            .await
            .unwrap()[&name("PRIVATE_TOKEN")],
        "scope-one"
    );

    let mut snapshot = store.read(&app("one")).await.unwrap();
    snapshot
        .set_limits(
            &app("one"),
            Some(ApplicationLimits {
                size_class: ApplicationSizeClass::Lite,
                invocation_timeout_ms: 5000,
            }),
        )
        .unwrap();
    assert!(snapshot
        .set_limits(
            &app("one"),
            Some(ApplicationLimits {
                size_class: ApplicationSizeClass::Lite,
                invocation_timeout_ms: 0
            })
        )
        .is_err());
    store.stage(&deployment).await.unwrap();
    store
        .activate(&mut snapshot, deployment.clone())
        .await
        .unwrap();
    assert!(snapshot.remove_secret(&app("one"), &secret).is_err());
    store.commit(snapshot).await.unwrap();
    let mut snapshot = store.read(&app("one")).await.unwrap();
    assert_eq!(
        snapshot
            .application(&app("one"))
            .unwrap()
            .limits
            .unwrap()
            .invocation_timeout_ms,
        5000
    );
    snapshot.deactivate(&app("one")).unwrap();
    snapshot.remove_secret(&app("one"), &secret).unwrap();
    snapshot.set_limits(&app("one"), None).unwrap();
    store.commit(snapshot).await.unwrap();
    let missing = store.read(&app("one")).await.unwrap();
    assert!(store
        .resolve_environment(
            missing.application(&app("one")).unwrap(),
            &deployment.handlers[0]
        )
        .await
        .is_err());
    assert!(missing.application(&app("one")).unwrap().limits.is_none());
    assert!(store
        .stage_secret(&app("one"), &secret, "\0".into())
        .await
        .is_err());
    assert!(store
        .stage_secret(&app("one"), &secret, "x".repeat(MAX_SECRET_BYTES + 1))
        .await
        .is_err());
    // A value copied under another application's key, at a version the
    // catalog trusts, still names its own application and is refused.
    let (copied, _) = store
        .bucket
        .get(&secret_key(&app("two"), &secret, &versions[1]).unwrap())
        .await
        .unwrap()
        .unwrap();
    store
        .bucket
        .put(
            &secret_key(&app("one"), &secret, &versions[1]).unwrap(),
            copied,
        )
        .await
        .unwrap();
    let mut tampered = store.read(&app("one")).await.unwrap();
    tampered
        .set_secret(&app("one"), secret.clone(), versions[1].clone())
        .unwrap();
    let error = store
        .resolve_environment(
            tampered.application(&app("one")).unwrap(),
            &deployment.handlers[0],
        )
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("identity"), "{error:#}");
    eprintln!("application secrets/limits: application separation, opaque metadata without values, foreign-version refusal, immutable old snapshot, rotation CAS winner/loser, active-reference deletion refusal, missing-secret refusal and validated/reset policy over original local bucket; no independent-provider qualification");
}

pub(crate) mod metadata_fault_store {
    use super::*;
    use futures_util::stream::BoxStream;
    use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
    use object_store::path::Path as ObjectPath;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Default)]
    pub(crate) struct Store {
        inner: object_store::memory::InMemory,
        pub(crate) calls: Mutex<Vec<(String, String)>>,
        pub(super) fail: Mutex<Option<(String, bool)>>,
        /// Written to its key just before the next put there: a rival wins the race.
        pub(super) race: Mutex<Option<(String, Bytes)>>,
    }
    impl std::fmt::Display for Store {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("T30 fault store")
        }
    }
    impl Store {
        fn call(&self, operation: &str, key: &str) {
            self.calls
                .lock()
                .unwrap()
                .push((operation.into(), key.into()));
        }
        fn error() -> object_store::Error {
            object_store::Error::Generic {
                store: "T30 fault store",
                source: std::io::Error::other("injected lost reply").into(),
            }
        }
        pub(crate) fn bucket(self: &Arc<Self>) -> Bucket {
            Bucket::with_paginated_store_for_test(self.clone(), self.clone(), String::new())
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for Store {
        async fn put_opts(
            &self,
            key: &ObjectPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.call("put", key.as_ref());
            let rival = {
                let mut race = self.race.lock().unwrap();
                if race
                    .as_ref()
                    .is_some_and(|(wanted, _)| wanted == key.as_ref())
                {
                    race.take()
                } else {
                    None
                }
            };
            if let Some((_, body)) = rival {
                self.inner.put(key, body.into()).await?;
            }
            let failure = {
                let mut fail = self.fail.lock().unwrap();
                if fail
                    .as_ref()
                    .is_some_and(|(wanted, _)| wanted == key.as_ref())
                {
                    fail.take()
                } else {
                    None
                }
            };
            if failure.as_ref().is_some_and(|(_, after)| !after) {
                return Err(Self::error());
            }
            let result = self.inner.put_opts(key, payload, options).await?;
            if failure.is_some() {
                return Err(Self::error());
            }
            Ok(result)
        }
        async fn get_opts(
            &self,
            key: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.call(if options.head { "head" } else { "get" }, key.as_ref());
            // A real read waits on the network; without this yield concurrent
            // readers could never overlap, and request collapsing goes untested.
            tokio::task::yield_now().await;
            self.inner.get_opts(key, options).await
        }
        async fn put_multipart_opts(
            &self,
            key: &ObjectPath,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(key, options).await
        }
        async fn delete(&self, key: &ObjectPath) -> object_store::Result<()> {
            self.inner.delete(key).await
        }
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.call("unbounded-list", prefix.map_or("", |p| p.as_ref()));
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.call("unbounded-list", prefix.map_or("", |p| p.as_ref()));
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
        ) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }
    #[async_trait::async_trait]
    impl PaginatedListStore for Store {
        async fn list_paginated(
            &self,
            prefix: Option<&str>,
            options: PaginatedListOptions,
        ) -> object_store::Result<PaginatedListResult> {
            use futures_util::TryStreamExt;
            self.call("page", prefix.unwrap_or_default());
            let prefix = prefix.unwrap_or_default();
            let objects = self
                .inner
                .list(Some(&ObjectPath::from(prefix)))
                .try_collect::<Vec<_>>()
                .await?;
            let mut objects = objects
                .into_iter()
                .filter(|object| {
                    options
                        .page_token
                        .as_ref()
                        .is_none_or(|token| object.location.as_ref() > token.as_str())
                })
                .collect::<Vec<_>>();
            objects.sort_by(|a, b| a.location.cmp(&b.location));
            let limit = options.max_keys.unwrap();
            let more = objects.len() > limit;
            objects.truncate(limit);
            let page_token = more.then(|| objects.last().unwrap().location.to_string());
            Ok(PaginatedListResult {
                result: ListResult {
                    common_prefixes: Vec::new(),
                    objects,
                },
                page_token,
            })
        }
    }
}

async fn metadata_faults_and_bounded_calls() {
    let backend = std::sync::Arc::new(metadata_fault_store::Store::default());
    let store = ApplicationStore::new(backend.bucket());
    for id in [app("one"), app("two")] {
        let mut snapshot = store.read(&id).await.unwrap();
        snapshot.create(id).unwrap();
        store.commit(snapshot).await.unwrap();
    }
    backend.calls.lock().unwrap().clear();
    let one = app("one");
    let mut snapshot = store.read(&one).await.unwrap();
    snapshot
        .set_limits(
            &one,
            Some(ApplicationLimits {
                size_class: ApplicationSizeClass::Basic,
                invocation_timeout_ms: 1000,
            }),
        )
        .unwrap();
    store.commit(snapshot).await.unwrap();
    let calls = backend.calls.lock().unwrap().clone();
    assert!(
        calls.len() <= 6,
        "one-app management did unbounded work: {calls:?}"
    );
    // The application's own record, plus the epoch log's read and conditional
    // write that tell other nodes.
    assert!(calls
        .iter()
        .all(|(_, key)| key == &metadata_key(&one) || key == EPOCH_KEY));
    let epoch_calls: Vec<_> = calls
        .iter()
        .filter(|(_, key)| key == EPOCH_KEY)
        .map(|(operation, _)| operation.as_str())
        .collect();
    assert_eq!(epoch_calls, ["get", "put"]);
    assert!(!calls
        .iter()
        .any(|(operation, _)| operation.contains("list") || operation == "page"));
    backend.calls.lock().unwrap().clear();
    let page = store.list(1, None).await.unwrap();
    assert_eq!(page.applications.len(), 1);
    assert_eq!(
        backend
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(op, _)| op == "page")
            .count(),
        1
    );
    assert_eq!(
        backend
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(op, _)| op == "get")
            .count(),
        1
    );
    assert!(backend
        .calls
        .lock()
        .unwrap()
        .iter()
        .all(|(op, _)| op != "unbounded-list"));

    // Lost metadata reply reconciles exactly; failed-before-write leaves old state.
    let mut snapshot = store.read(&one).await.unwrap();
    snapshot.set_limits(&one, None).unwrap();
    *backend.fail.lock().unwrap() = Some((metadata_key(&one), true));
    store.commit(snapshot).await.unwrap();
    assert!(store
        .read(&one)
        .await
        .unwrap()
        .application(&one)
        .unwrap()
        .limits
        .is_none());
    let mut snapshot = store.read(&one).await.unwrap();
    snapshot
        .set_limits(
            &one,
            Some(ApplicationLimits {
                size_class: ApplicationSizeClass::Lite,
                invocation_timeout_ms: 1000,
            }),
        )
        .unwrap();
    *backend.fail.lock().unwrap() = Some((metadata_key(&one), false));
    assert!(store.commit(snapshot).await.is_err());
    assert!(store
        .read(&one)
        .await
        .unwrap()
        .application(&one)
        .unwrap()
        .limits
        .is_none());

    let mut snapshot = store.read(&one).await.unwrap();
    snapshot
        .register_resource(
            &one,
            Resource {
                resource: ResourceRef {
                    kind: ResourceKind::D1,
                    id: name("db"),
                },
                configuration: empty_object(),
            },
        )
        .unwrap();
    store.commit(snapshot).await.unwrap();
    let route = RouteId::generate().unwrap();
    *backend.fail.lock().unwrap() = Some((route_index_key(&route), false));
    assert!(store
        .allocate_route(&one, Some(&name("main")), || Ok(route.clone()))
        .await
        .is_err());
    assert!(store.resolve_route(&route).await.unwrap().is_none());
    assert!(store
        .read(&one)
        .await
        .unwrap()
        .application(&one)
        .unwrap()
        .active
        .is_none());
    *backend.fail.lock().unwrap() = Some((route_index_key(&route), true));
    assert_eq!(
        store
            .allocate_route(&one, Some(&name("main")), || Ok(route.clone()))
            .await
            .unwrap(),
        route
    );
    assert!(store.resolve_route(&route).await.unwrap().is_none());
    let deployment = deployment(one.clone(), "fault-v1");
    store.stage(&deployment).await.unwrap();
    let mut snapshot = store.read(&one).await.unwrap();
    store.activate(&mut snapshot, deployment).await.unwrap();
    let active_route = snapshot.application(&one).unwrap().route_ids[&name("main")].clone();
    store.commit(snapshot).await.unwrap();
    assert!(store.resolve_route(&active_route).await.unwrap().is_some());

    // Every locator boundary may be interrupted without publishing partial active metadata.
    let id = app("locator-faults");
    let mut initial = store.read(&id).await.unwrap();
    initial.create(id.clone()).unwrap();
    store.commit(initial).await.unwrap();
    for (kind, name_text) in [(ResourceKind::Queue, "jobs"), (ResourceKind::Cron, "tick")] {
        let address = ResourceAddress {
            application: id.clone(),
            resource: ResourceRef {
                kind,
                id: name(name_text),
            },
        };
        let class = if kind == ResourceKind::Queue {
            celld_logic::queue::RESERVED_CLASS
        } else {
            celld_logic::cron::RESERVED_CLASS
        };
        let scope = address.cell_scope(class, name_text).unwrap();
        let mut failures = vec![cell_index_key(&scope)];
        if kind == ResourceKind::Cron {
            failures.push(format!(
                "{CRON_PREFIX}/{}.json",
                content_digest(scope.as_bytes())
            ));
        }
        failures.push(metadata_key(&id));
        for key in failures {
            let mut snapshot = store.read(&id).await.unwrap();
            snapshot
                .register_resource(
                    &id,
                    Resource {
                        resource: address.resource.clone(),
                        configuration: empty_object(),
                    },
                )
                .unwrap();
            *backend.fail.lock().unwrap() = Some((key, false));
            assert!(store.commit(snapshot).await.is_err());
            assert!(store
                .read(&id)
                .await
                .unwrap()
                .application(&id)
                .unwrap()
                .resources
                .is_empty());
        }
    }
}

/// Commits `id` (created on first use) with a new timeout, so every commit
/// produces a revision of its own.
async fn touch(store: &ApplicationStore, id: &ApplicationId, timeout_ms: u64) -> String {
    let mut snapshot = store.read(id).await.unwrap();
    if snapshot.application(id).is_none() {
        snapshot.create(id.clone()).unwrap();
    }
    let limits = ApplicationLimits {
        size_class: ApplicationSizeClass::Basic,
        invocation_timeout_ms: timeout_ms,
    };
    snapshot.set_limits(id, Some(limits)).unwrap();
    store.commit(snapshot).await.unwrap()
}

async fn epoch_log_trims_and_restarts_over_unreadable_bodies() {
    let backend = std::sync::Arc::new(metadata_fault_store::Store::default());
    let store = ApplicationStore::new(backend.bucket());
    let id = app("one");
    // An older binary's body is not a log, and not an error.
    assert!(store.epoch().await.unwrap().is_none());
    store.bucket.put(EPOCH_KEY, "tok\n123").await.unwrap();
    assert!(store.epoch().await.unwrap().is_none());

    let mut revisions = vec![touch(&store, &id, 1).await];
    let first = store.epoch().await.unwrap().unwrap();
    assert_eq!((first.seq, first.changes.len()), (1, 1));
    for timeout in 2..=70 {
        revisions.push(touch(&store, &id, timeout).await);
    }
    // The 64 newest of 70, consecutive, newest last.
    let log = store.epoch().await.unwrap().unwrap();
    assert_eq!(log.log, first.log);
    assert_eq!(log.seq, 70);
    let expected: Vec<_> = (7..=70)
        .zip(&revisions[6..])
        .map(|(seq, revision)| EpochChange {
            seq,
            application: id.clone(),
            revision: revision.clone(),
        })
        .collect();
    assert_eq!(log.changes, expected);

    // An older binary overwrites the log: the next commit starts another one.
    store.bucket.put(EPOCH_KEY, "tok\n123").await.unwrap();
    assert!(store.epoch().await.unwrap().is_none());
    touch(&store, &id, 71).await;
    let restarted = store.epoch().await.unwrap().unwrap();
    assert_eq!((restarted.seq, restarted.changes.len()), (1, 1));
    assert_ne!(restarted.log, log.log);
    assert!(restarted.since(&log.log, 70).is_none());
}

async fn epoch_log_keeps_every_committers_change() {
    let backend = std::sync::Arc::new(metadata_fault_store::Store::default());
    let (a, b) = (
        ApplicationStore::new(backend.bucket()),
        ApplicationStore::new(backend.bucket()),
    );
    let (one, two) = (app("one"), app("two"));
    let first = touch(&a, &one, 1).await;
    let second = touch(&b, &two, 1).await;
    let third = touch(&a, &one, 2).await;
    let log = b.epoch().await.unwrap().unwrap();
    let changes: Vec<_> = log
        .changes
        .iter()
        .map(|change| (change.seq, &change.application, &change.revision))
        .collect();
    assert_eq!(
        changes,
        [(1, &one, &first), (2, &two, &second), (3, &one, &third)]
    );

    // A rival append lands between this one's read and write: the conditional
    // write loses and is redone on top of the rival's log.
    let mut rival = log.clone();
    rival.seq += 1;
    rival.changes.push(EpochChange {
        seq: rival.seq,
        application: two.clone(),
        revision: "rival".into(),
    });
    *backend.race.lock().unwrap() = Some((
        EPOCH_KEY.into(),
        Bytes::from(serde_json::to_vec(&rival).unwrap()),
    ));
    backend.calls.lock().unwrap().clear();
    let fourth = touch(&a, &one, 3).await;
    let epoch_calls: Vec<_> = backend
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, key)| key == EPOCH_KEY)
        .map(|(operation, _)| operation.clone())
        .collect();
    assert_eq!(epoch_calls, ["get", "put", "get", "put"]);
    let log = a.epoch().await.unwrap().unwrap();
    assert_eq!(log.seq, 5);
    assert_eq!(
        log.changes[3..],
        [
            EpochChange {
                seq: 4,
                application: two,
                revision: "rival".into()
            },
            EpochChange {
                seq: 5,
                application: one.clone(),
                revision: fourth
            },
        ]
    );

    // A failed append never fails the commit; the log just misses it.
    *backend.fail.lock().unwrap() = Some((EPOCH_KEY.into(), false));
    let fifth = touch(&a, &one, 4).await;
    assert_eq!(a.read(&one).await.unwrap().revision(), Some(&*fifth));
    assert_eq!(a.epoch().await.unwrap().unwrap().seq, 5);
}

/// The sweep reads the catalog from LIST pages instead of a HEAD per
/// application, and appends to the epoch log only the commits it lost.
async fn catalog_sweep_repairs_lost_epoch_entries() {
    let backend = std::sync::Arc::new(metadata_fault_store::Store::default());
    let store = ApplicationStore::new(backend.bucket());
    let (one, two) = (app("one"), app("two"));
    let first = touch(&store, &one, 1).await;
    let second = touch(&store, &two, 1).await;
    // The listing carries the revisions a read answers.
    let tokens = store.metadata_tokens().await.unwrap();
    assert_eq!(
        tokens,
        BTreeMap::from([(one.clone(), first), (two.clone(), second)])
    );
    for (id, token) in &tokens {
        assert_eq!(
            store.read(id).await.unwrap().revision(),
            Some(token.as_str())
        );
    }

    // Everything is new to a node with no baseline, yet each commit's entry is
    // in the log already.
    let seq = store.epoch().await.unwrap().unwrap().seq;
    let (swept, changed) = store.sweep(&BTreeMap::new()).await.unwrap();
    assert_eq!(swept, tokens);
    assert_eq!(changed, BTreeSet::from([one.clone(), two.clone()]));
    assert_eq!(
        store.epoch().await.unwrap().unwrap().seq,
        seq,
        "nothing to repair"
    );

    // An unchanged catalog costs one list page and one epoch GET.
    backend.calls.lock().unwrap().clear();
    let (unchanged, changed) = store.sweep(&swept).await.unwrap();
    assert_eq!(unchanged, swept);
    assert!(changed.is_empty());
    let calls = backend.calls.lock().unwrap().clone();
    let operations: Vec<_> = calls
        .iter()
        .map(|(operation, _)| operation.as_str())
        .collect();
    assert_eq!(operations, ["page", "get"]);
    assert_eq!(calls[1].1, EPOCH_KEY);

    // A commit whose append failed is in the listing alone: the next sweep
    // appends it, once.
    let newest = |log: &EpochLog| {
        let change = log.changes.last().unwrap();
        (change.application.clone(), change.revision.clone())
    };
    *backend.fail.lock().unwrap() = Some((EPOCH_KEY.into(), false));
    let third = touch(&store, &one, 2).await;
    assert_eq!(
        store.epoch().await.unwrap().unwrap().seq,
        seq,
        "the append was lost"
    );
    let (swept, changed) = store.sweep(&swept).await.unwrap();
    assert_eq!(changed, BTreeSet::from([one.clone()]));
    let log = store.epoch().await.unwrap().unwrap();
    assert_eq!(log.seq, seq + 1);
    assert_eq!(newest(&log), (one.clone(), third));
    assert!(store.sweep(&swept).await.unwrap().1.is_empty());
    assert_eq!(store.epoch().await.unwrap().unwrap().seq, seq + 1);

    // A repair that fails too is retried: the application stays changed until
    // its entry lands.
    *backend.fail.lock().unwrap() = Some((EPOCH_KEY.into(), false));
    let fourth = touch(&store, &two, 2).await;
    *backend.fail.lock().unwrap() = Some((EPOCH_KEY.into(), false));
    let (swept, changed) = store.sweep(&swept).await.unwrap();
    assert_eq!(changed, BTreeSet::from([two.clone()]));
    assert_eq!(store.epoch().await.unwrap().unwrap().seq, seq + 1);
    let (swept, changed) = store.sweep(&swept).await.unwrap();
    assert_eq!(changed, BTreeSet::from([two.clone()]));
    let log = store.epoch().await.unwrap().unwrap();
    assert_eq!(log.seq, seq + 2);
    assert_eq!(newest(&log), (two.clone(), fourth));
    assert!(store.sweep(&swept).await.unwrap().1.is_empty());

    // An application that disappeared is named by an empty revision, once.
    let mut before = swept.clone();
    before.insert(app("gone"), "token".into());
    let (_, changed) = store.sweep(&before).await.unwrap();
    assert_eq!(changed, BTreeSet::from([app("gone")]));
    let log = store.epoch().await.unwrap().unwrap();
    assert_eq!(
        (log.seq, newest(&log)),
        (seq + 3, (app("gone"), String::new()))
    );
    store.sweep(&before).await.unwrap();
    assert_eq!(store.epoch().await.unwrap().unwrap().seq, seq + 3);

    // A thousand and one applications take two list pages.
    let backend = std::sync::Arc::new(metadata_fault_store::Store::default());
    let store = ApplicationStore::new(backend.bucket());
    for n in 0..=TOKEN_PAGE {
        let key = metadata_key(&app(&format!("bulk-{n}")));
        store.bucket.put(&key, "{}").await.unwrap();
    }
    backend.calls.lock().unwrap().clear();
    assert_eq!(store.metadata_tokens().await.unwrap().len(), TOKEN_PAGE + 1);
    let pages = backend
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(op, _)| op == "page")
        .count();
    assert_eq!(pages, 2);
}

#[test]
fn epoch_log_since_names_only_continuous_changes() {
    let change = |seq| EpochChange {
        seq,
        application: app("one"),
        revision: format!("r{seq}"),
    };
    let log = EpochLog {
        version: EPOCH_VERSION,
        log: "a".into(),
        seq: 10,
        changes: (7..=10).map(change).collect(),
    };
    let since = |name: &str, seq| {
        log.since(name, seq)
            .map(|changes| changes.iter().map(|change| change.seq).collect::<Vec<_>>())
    };
    assert_eq!(since("a", 10), Some(vec![]), "caught up");
    assert_eq!(since("a", 9), Some(vec![10]));
    assert_eq!(
        since("a", 6),
        Some(vec![7, 8, 9, 10]),
        "7 directly follows 6"
    );
    assert_eq!(since("a", 5), None, "change 6 was trimmed");
    assert_eq!(since("b", 9), None, "another log");
    assert_eq!(since("b", 10), None, "another log, even when caught up");
}

pub(crate) async fn application_publication_cas_route_ids_and_resource_preservation() {
    let directory = tempfile::tempdir().unwrap();
    let store =
        ApplicationStore::new(Bucket::open_dev(&directory.path().join("bucket.sqlite")).unwrap());
    let (one, two) = (app("one"), app("two"));
    let mut first = store.read(&one).await.unwrap();
    let mut second = store.read(&two).await.unwrap();
    let mut stale_create = store.read(&one).await.unwrap();
    first.create(one.clone()).unwrap();
    second.create(two.clone()).unwrap();
    stale_create.create(one.clone()).unwrap();
    let (a, b) = tokio::join!(store.commit(first), store.commit(second));
    assert!(
        a.is_ok() && b.is_ok(),
        "unrelated application updates conflicted"
    );
    assert!(store
        .commit(stale_create)
        .await
        .unwrap_err()
        .is::<Conflict>());
    for id in [&one, &two] {
        let mut snapshot = store.read(id).await.unwrap();
        assert!(snapshot
            .set_limits(if id == &one { &two } else { &one }, None)
            .is_err());
        snapshot
            .register_resource(
                id,
                Resource {
                    resource: ResourceRef {
                        kind: ResourceKind::D1,
                        id: name("db"),
                    },
                    configuration: empty_object(),
                },
            )
            .unwrap();
        store.commit(snapshot).await.unwrap();
    }
    let first = deployment(one.clone(), "v1");
    store.stage(&first).await.unwrap();
    store.stage(&first).await.unwrap();
    let mut changed = first.clone();
    changed.handlers[0].artifact_sha256 = "c".repeat(64);
    assert!(store
        .stage(&changed)
        .await
        .unwrap_err()
        .is::<ImmutableConflict>());
    let mut snapshot = store.read(&one).await.unwrap();
    store.activate(&mut snapshot, first).await.unwrap();
    let one_id = snapshot.application(&one).unwrap().route_ids[&name("main")].clone();
    // Crash before metadata CAS: a prepared route candidate does not admit a handler.
    store
        .publish_indexes(snapshot.application(&one).unwrap())
        .await
        .unwrap();
    assert!(store.resolve_route(&one_id).await.unwrap().is_none());
    store.commit(snapshot).await.unwrap();
    assert_eq!(
        store
            .resolve_route(&one_id)
            .await
            .unwrap()
            .unwrap()
            .application,
        one
    );
    let second = deployment(two.clone(), "v1");
    store.stage(&second).await.unwrap();
    let mut snapshot = store.read(&two).await.unwrap();
    store.activate(&mut snapshot, second).await.unwrap();
    let two_id = snapshot.application(&two).unwrap().route_ids[&name("main")].clone();
    assert_ne!(one_id, two_id);
    store.commit(snapshot).await.unwrap();
    let replacement_id = RouteId::generate().unwrap();
    let mut attempts = [one_id.clone(), replacement_id.clone()].into_iter();
    assert_eq!(
        store
            .allocate_route(&two, Some(&name("extra")), || Ok(attempts.next().unwrap()))
            .await
            .unwrap(),
        replacement_id
    );
    assert!(store
        .resolve_route(&replacement_id)
        .await
        .unwrap()
        .is_none());
    let mut count = 0;
    assert!(store
        .allocate_route(&two, Some(&name("extra")), || {
            count += 1;
            Ok(one_id.clone())
        })
        .await
        .is_err());
    assert_eq!(count, 8);

    let mut forged = store.read(&two).await.unwrap();
    forged
        .application_mut(&two)
        .unwrap()
        .route_ids
        .insert(name("main"), one_id.clone());
    assert!(store
        .commit(forged)
        .await
        .unwrap_err()
        .is::<ImmutableConflict>());
    assert_eq!(
        store
            .resolve_route(&one_id)
            .await
            .unwrap()
            .unwrap()
            .application,
        one
    );
    let mut snapshot = store.read(&one).await.unwrap();
    let stale = store.read(&one).await.unwrap();
    let replacement = deployment(one.clone(), "v2");
    store.stage(&replacement).await.unwrap();
    store.activate(&mut snapshot, replacement).await.unwrap();
    store.commit(snapshot).await.unwrap();
    assert!(store.commit(stale).await.unwrap_err().is::<Conflict>());
    let mut snapshot = store.read(&one).await.unwrap();
    assert_eq!(
        snapshot.application(&one).unwrap().route_ids[&name("main")],
        one_id
    );
    assert_eq!(
        store
            .resolve_route(&one_id)
            .await
            .unwrap()
            .unwrap()
            .deployment,
        name("v2")
    );
    snapshot.deactivate(&one).unwrap();
    store.commit(snapshot).await.unwrap();
    let snapshot = store.read(&one).await.unwrap();
    assert!(store.resolve_route(&one_id).await.unwrap().is_none());
    assert!(store.resolve_route(&two_id).await.unwrap().is_some());
    assert_eq!(
        snapshot.application(&one).unwrap().route_ids[&name("main")],
        one_id
    );
    assert_eq!(snapshot.application(&one).unwrap().resources.len(), 1);
    assert_eq!(
        store
            .read(&two)
            .await
            .unwrap()
            .application(&two)
            .unwrap()
            .resources
            .len(),
        1
    );

    // Fleet metadata can exceed 8 MiB; every individual object remains bounded.
    for id in ["large-a", "large-b", "large-c"] {
        let id = app(id);
        let mut snapshot = store.read(&id).await.unwrap();
        snapshot.create(id.clone()).unwrap();
        snapshot
            .register_resource(
                &id,
                Resource {
                    resource: ResourceRef {
                        kind: ResourceKind::D1,
                        id: name("large"),
                    },
                    configuration: serde_json::json!({"body":"x".repeat(6 * 1024 * 1024)}),
                },
            )
            .unwrap();
        store.commit(snapshot).await.unwrap();
    }
    let mut too_large = store.read(&app("large-a")).await.unwrap();
    too_large
        .application_mut(&app("large-a"))
        .unwrap()
        .resources[0]
        .configuration = serde_json::json!({"body":"x".repeat(MAX_METADATA_BYTES as usize)});
    assert!(store
        .commit(too_large)
        .await
        .unwrap_err()
        .to_string()
        .contains("individual record limit"));
    let error = store.list(256, None).await.err().unwrap();
    assert!(error.to_string().contains("smaller limit"));
    let mut cursor = None;
    let mut seen = BTreeSet::new();
    loop {
        let page = store.list(1, cursor).await.unwrap();
        assert!(page.applications.len() <= 1);
        for entry in page.applications {
            assert!(seen.insert(entry.application.id));
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen.len(), 5);
    assert!(store.list(0, None).await.is_err());
    assert!(store.list(257, None).await.is_err());
    assert!(store.list(1, Some("forged".into())).await.is_err());
    let page = store.list(1, None).await.unwrap();
    assert!(store.list(2, page.next_cursor).await.is_err());

    let id = app("scopes");
    let mut snapshot = store.read(&id).await.unwrap();
    snapshot.create(id.clone()).unwrap();
    for (kind, resource) in [
        (ResourceKind::Queue, "jobs"),
        (ResourceKind::Cron, "tick"),
        (ResourceKind::DurableObject, "objects"),
    ] {
        snapshot
            .register_resource(
                &id,
                Resource {
                    resource: ResourceRef {
                        kind,
                        id: name(resource),
                    },
                    configuration: empty_object(),
                },
            )
            .unwrap();
    }
    store.commit(snapshot).await.unwrap();
    let queue = ResourceAddress {
        application: id.clone(),
        resource: ResourceRef {
            kind: ResourceKind::Queue,
            id: name("jobs"),
        },
    };
    let scope = queue
        .cell_scope(celld_logic::queue::RESERVED_CLASS, "jobs")
        .unwrap();
    let owner = store.cell_owner(&scope).await.unwrap().unwrap();
    assert!(owner.application.active.is_none());
    assert_eq!(owner.locator.address, queue);
    assert!(store
        .cell_owner(&format!("__Queue:{}", "a".repeat(64)))
        .await
        .unwrap()
        .is_none());
    let object_address = ResourceAddress {
        application: id.clone(),
        resource: ResourceRef {
            kind: ResourceKind::DurableObject,
            id: name("objects"),
        },
    };
    let object_scope = object_address.cell_scope("Counter", "alice").unwrap();
    let forged_cron = CellLocator {
        index_version: METADATA_VERSION,
        scope: object_address
            .cell_scope(celld_logic::cron::RESERVED_CLASS, "alice")
            .unwrap(),
        address: object_address.clone(),
    };
    assert!(
        forged_cron.validate().is_err(),
        "DO resource admitted the reserved cron class"
    );
    let object = celld_applications::invocation_wire::ObjectTarget {
        class: "Counter".into(),
        id: object_scope.split_once(':').unwrap().1.into(),
        name: Some("alice".into()),
    };
    store.register_cell(&object_address, &object).await.unwrap();
    store.register_cell(&object_address, &object).await.unwrap();
    assert_eq!(
        store
            .cell_owner(&object_scope)
            .await
            .unwrap()
            .unwrap()
            .locator
            .address,
        object_address
    );
    let mut foreign = object_address.clone();
    foreign.application = one.clone();
    assert!(store.register_cell(&foreign, &object).await.is_err());
    let crons = store.cron_candidates(1, None).await.unwrap();
    assert_eq!(crons.candidates.len(), 1);
    assert_eq!(crons.candidates[0].address.application, id);
    let mut corrupted = owner.locator;
    corrupted.address.application = one.clone();
    store
        .bucket
        .put(
            &cell_index_key(&scope),
            Bytes::from(serde_json::to_vec(&corrupted).unwrap()),
        )
        .await
        .unwrap();
    assert!(store.cell_owner(&scope).await.is_err());
    let key = metadata_key(&one);
    let (original, _) = store.bucket.get(&key).await.unwrap().unwrap();
    let mut unsupported: serde_json::Value = serde_json::from_slice(&original).unwrap();
    unsupported["metadata_version"] = 1.into();
    store
        .bucket
        .put(&key, Bytes::from(serde_json::to_vec(&unsupported).unwrap()))
        .await
        .unwrap();
    assert!(store
        .read(&one)
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("unsupported application metadata version"));
    store.bucket.put(&key, original).await.unwrap();
    metadata_faults_and_bounded_calls().await;
    epoch_log_trims_and_restarts_over_unreadable_bodies().await;
    epoch_log_keeps_every_committers_change().await;
    catalog_sweep_repairs_lost_epoch_entries().await;
}

/// Static assets are original resolver behaviour over application-scoped
/// artifacts: routing, the binding, activation checks, and isolation.
pub(crate) async fn application_static_assets_isolation_and_routing() {
    use crate::protocol::{AssetConfig, AssetEntry, AssetIndex, AssetManifestRef};
    use futures_util::StreamExt;
    let directory = tempfile::tempdir().unwrap();
    std::env::set_var("CELLD_ASSET_CACHE_DIR", directory.path().join("cache"));
    let store =
        ApplicationStore::new(Bucket::open_dev(&directory.path().join("assets.sqlite")).unwrap());
    let (one, two) = (app("one"), app("two"));
    let entry = |body: &str| AssetEntry {
        sha256: content_digest(body.as_bytes()),
        bytes: body.len() as u64,
        content_type: Some("text/plain".into()),
    };
    // Uploads the named bodies (unless `skip`) and the index; returns the
    // handler's `worker_config.assets` reference.
    let publish = |application: ApplicationId,
                   files: Vec<(&'static str, &'static str)>,
                   upload: bool,
                   config: AssetConfig| {
        let store = &store;
        async move {
            let mut entries = BTreeMap::new();
            for (path, body) in &files {
                entries.insert((*path).to_owned(), entry(body));
                if upload {
                    store
                        .upload_artifact(
                            &application,
                            &content_digest(body.as_bytes()),
                            Bytes::from(*body),
                        )
                        .await
                        .unwrap();
                }
            }
            let index = serde_json::to_vec(&AssetIndex {
                schema_version: 1,
                entries,
                config,
            })
            .unwrap();
            let digest = content_digest(&index);
            store
                .upload_artifact(&application, &digest, Bytes::from(index))
                .await
                .unwrap();
            AssetManifestRef {
                index: "assets.json".into(),
                sha256: digest,
                file_count: files.len() as u32,
                total_bytes: files.iter().map(|(_, body)| body.len() as u64).sum(),
            }
        }
    };
    let config = |routes: &[&str]| AssetConfig {
        binding: Some("ASSETS".into()),
        run_worker_first: crate::protocol::RunWorkerFirst::Routes(
            routes.iter().map(|route| (*route).to_owned()).collect(),
        ),
        ..Default::default()
    };
    async fn read(response: crate::js::HttpResponse) -> (u16, String) {
        let mut body = response.body;
        if let Some(mut stream) = response.stream {
            while let Some(chunk) = stream.next().await {
                body.extend(chunk.unwrap());
            }
        }
        (response.status, String::from_utf8(body).unwrap())
    }
    let secret = publish(
        one.clone(),
        vec![("/a.txt", "secret-one"), ("/index.html", "<h1>one</h1>")],
        true,
        config(&["/api/*"]),
    )
    .await;
    let other = publish(
        two.clone(),
        vec![("/a.txt", "body-two")],
        true,
        config(&["/api/*"]),
    )
    .await;
    let one_assets = store
        .asset_resolver(&one, &secret, Some("ASSETS"))
        .await
        .unwrap();
    let get = |resolver: crate::assets::AssetResolver, method: &'static str, url: &'static str| async move {
        resolver.binding_response(url, method, &[]).await
    };
    // The binding: the original GET/HEAD semantics and not-found handling.
    let found = get(one_assets.clone(), "GET", "https://assets.local/a.txt").await;
    assert_eq!(read(found.unwrap()).await, (200, "secret-one".into()));
    let head = get(one_assets.clone(), "HEAD", "https://assets.local/a.txt").await;
    assert_eq!(read(head.unwrap()).await, (200, String::new()));
    let post = get(one_assets.clone(), "POST", "https://assets.local/a.txt").await;
    assert_eq!(post.unwrap().status, 405);
    let missing = get(one_assets.clone(), "GET", "https://assets.local/none").await;
    assert_eq!(read(missing.unwrap()).await.0, 404);
    // Ingress: assets first, the miss continues to the Worker, and
    // `run_worker_first` routes skip assets.
    let headers = axum::http::HeaderMap::new();
    assert!(one_assets
        .ingress_response("/a.txt", None, false, &headers)
        .await
        .unwrap()
        .is_some());
    assert!(one_assets
        .ingress_response("/none", None, false, &headers)
        .await
        .unwrap()
        .is_none());
    assert!(!one_assets.asset_only());
    assert!(one_assets.should_run_worker_first("/api/x"));
    assert!(!one_assets.should_run_worker_first("/a.txt"));
    // The alias must be the index's binding name, if it names one.
    assert!(store
        .asset_resolver(&one, &secret, Some("OTHER"))
        .await
        .is_err());
    // Activation: complete uploads pass; a missing body or index fails.
    store
        .verify_assets(&one, &secret, Some("ASSETS"))
        .await
        .unwrap();
    let stolen = publish(
        two.clone(),
        vec![("/a.txt", "body-two"), ("/stolen.txt", "secret-one")],
        false,
        config(&["/api/*"]),
    )
    .await;
    store
        .upload_artifact(&two, &content_digest(b"body-two"), Bytes::from("body-two"))
        .await
        .unwrap();
    assert!(store
        .verify_assets(&two, &stolen, Some("ASSETS"))
        .await
        .is_err());
    // Isolation: application two cannot name application one's index...
    assert!(store
        .asset_resolver(&two, &secret, Some("ASSETS"))
        .await
        .is_err());
    // ...serves its own bodies...
    let two_assets = store
        .asset_resolver(&two, &other, Some("ASSETS"))
        .await
        .unwrap();
    let own = get(two_assets, "GET", "https://assets.local/a.txt").await;
    assert_eq!(read(own.unwrap()).await, (200, "body-two".into()));
    // ...and an index of its own that lists one's digest reaches neither
    // one's stored body nor one's warmed local cache.
    let hostile = store
        .asset_resolver(&two, &stolen, Some("ASSETS"))
        .await
        .unwrap();
    let theft = get(hostile, "GET", "https://assets.local/stolen.txt").await;
    assert!(theft.is_err(), "foreign asset body was served");
}

#[test]
fn children_are_served_catalog_objects_by_key_shape_and_nothing_else() {
    let (digest, route) = ("a".repeat(64), "b".repeat(32));
    assert!(matches!(
        object_kind("applications/v2/metadata/one.json"),
        Some(ObjectKind::Metadata(id)) if id == app("one")
    ));
    for (key, bound) in [
        (
            format!("applications/v2/routes/{route}.json"),
            MAX_INDEX_BYTES,
        ),
        (
            format!("applications/v2/cells/{digest}.json"),
            MAX_INDEX_BYTES,
        ),
        (
            "applications/v2/apps/one/deployments/v1/manifest.json".to_owned(),
            MAX_METADATA_BYTES,
        ),
        (
            format!("applications/v2/apps/one/artifacts/{digest}"),
            MAX_ARTIFACT_BYTES as u64,
        ),
    ] {
        assert!(
            matches!(object_kind(&key), Some(ObjectKind::Immutable { max, .. }) if max == bound),
            "{key}"
        );
    }
    let refused = [
        // Secret values never leave the bucket path.
        format!("applications/v2/apps/one/secrets/token/{digest}"),
        format!("applications/v2/apps/one/artifacts/../secrets/token/{digest}"),
        format!("applications/v2/apps/../apps/one/artifacts/{digest}"),
        "applications/v2/metadata/../epoch".to_owned(),
        "applications/v2/metadata/a/b.json".to_owned(),
        "applications/v2/metadata/one.json.json".to_owned(),
        "applications/v2/metadata/.json".to_owned(),
        // Other prefixes, and a leading or missing one.
        "applications/v2/epoch".to_owned(),
        format!("applications/v2/cron-candidates/{digest}.json"),
        "applications/v2/apps/one/resources/kv/x/data".to_owned(),
        "fleet/peer-auth.json".to_owned(),
        "/applications/v2/metadata/one.json".to_owned(),
        String::new(),
        // Bad hex: uppercase, short, long, not hex, or something after it.
        format!(
            "applications/v2/apps/one/artifacts/{}",
            digest.to_uppercase()
        ),
        format!("applications/v2/apps/one/artifacts/{}", &digest[1..]),
        format!("applications/v2/apps/one/artifacts/{digest}0"),
        format!("applications/v2/apps/one/artifacts/g{}", &digest[1..]),
        format!("applications/v2/apps/one/artifacts/{digest}/x"),
        format!("applications/v2/cells/{}.json", &digest[1..]),
        format!("applications/v2/routes/{}.json", &route[1..]),
        format!("applications/v2/routes/{}.json", route.to_uppercase()),
        // The manifest and nothing else beside it.
        "applications/v2/apps/one/deployments/v1/other.json".to_owned(),
        "applications/v2/apps/one/deployments//manifest.json".to_owned(),
        "applications/v2/apps/one/deployments/v1/manifest.json/x".to_owned(),
    ];
    for key in refused {
        assert!(object_kind(&key).is_none(), "{key}");
    }
}
