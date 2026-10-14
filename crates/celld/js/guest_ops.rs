// Fork-owned file (Firecracker guest); not part of upstream celld.

//! Which ops may run in a Firecracker guest isolate, and how.
//!
//! A review ledger and CI tripwire, not enforcement: the security boundary is
//! the host (`resource_target`, `broker_owner`, `output().prove`). Every op in
//! `OP_NAMES` has one row, in table order, so a new upstream op fails the test
//! below until someone classifies it. `Prohibited` rows are what step 3 turns
//! into explicit install-time deny stubs.
//!
//! `Brokered` and `Inline` both reach the host. `Brokered` is a whole-op swap
//! (the native body is not shared, so an install-time override fits);
//! `Inline` is a guest branch interleaved with shared or lifecycle code.
//! "Explicit deny" in a reason means the op already refuses in a guest today.
// Only the tests read `CLASS`.
#![cfg_attr(not(test), allow(dead_code))]

use super::*;

/// Replace whole-op swaps with their guest versions, by `OP_NAMES` position:
/// names, parameter lists and the bootstrap code cache are untouched, only
/// the function values change. The test is the process or the isolate being a
/// guest at install time. A loaded worker flipped later by `own_guest_facet`
/// gets no override; none of these ops is reachable from one, since it has no
/// binding to call them with (`check_original_guest_loader` pins that).
/// Each override runs inside `attributed_op`, so spawn attribution is shared.
/// Drop an `install!` line to roll one back.
pub(super) fn override_ops<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    functions: &mut [v8::Local<'s, v8::Function>],
) {
    if !(guest_storage::enabled(scope) || guest_activity::enabled()) {
        return;
    }
    macro_rules! install {
        ($name:literal => $op:path) => {{
            fn attributed<'s>(
                scope: &mut v8::PinScope<'s, '_>,
                args: v8::FunctionCallbackArguments<'s>,
                rv: v8::ReturnValue<'s, v8::Value>,
            ) {
                attributed_op(scope, args, rv, $name, $op)
            }
            let at = OP_NAMES.iter().position(|name| *name == $name);
            functions[at.expect("overridden op is in OP_NAMES")] =
                v8::Function::new(scope, attributed).unwrap();
        }};
    }
    install!("__r2_head" => r2_ops::guest_head);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Class {
    /// No I/O, no state outside the isolate.
    Pure,
    /// Touches only isolate/process-local state; safe in a guest as is.
    LocalState,
    /// In a guest the whole op goes to the host broker.
    Brokered,
    /// Must never run in a guest; needs native channels, cell storage or raw network.
    Prohibited,
    /// The op (or its direct helper) has its own guest branch.
    Inline,
}

use Class::*;

#[rustfmt::skip]
pub(super) const CLASS: &[(&str, Class, &str)] = &[
    ("__heap_limit_excessively_exceeded", LocalState, "reads this isolate's heap-limit state"),
    ("__heap_over_admission_share", LocalState, "reads this isolate's heap share"),
    ("__ws_send", Inline, "guest_frame: socket frame goes to the host"),
    ("__ws_send_binary", Inline, "guest_frame: socket frame goes to the host"),
    ("__ws_close", Inline, "guest_frame: socket close goes to the host"),
    ("__ws_alloc", Inline, "guest ids start at GUEST_LOCAL_IDS"),
    ("__ws_guest", Inline, "reports the isolate's guest flag"),
    ("__ws_prepare_worker_handoff", Prohibited, "explicit deny (guest_refuses): no handoff of a Worker socket"),
    ("__ws_accept", Inline, "guest_registry: host owns the hibernatable socket"),
    ("__ws_accept_regular", Prohibited, "explicit deny (guest_refuses): use state.acceptWebSocket()"),
    ("__ws_list", Inline, "guest_registry: host answers the socket list"),
    ("__ws_attachment_set", Inline, "guest_registry: host stores the attachment"),
    ("__ws_auto_response_set", Inline, "guest_registry: host stores the auto-response"),
    ("__ws_auto_response_get", Inline, "guest_registry: host answers"),
    ("__ws_auto_response_ts", Inline, "guest_registry: host answers"),
    ("__ws_connect", Inline, "guest_dial: host dials; native path needs OUTBOUND_WS_TX"),
    ("__ws_bind_target", Prohibited, "explicit deny (guest_refuses): needs the native socket pipe"),
    ("__ws_next", Inline, "guest reads the host-fed dial queue"),
    ("__ws_upgrade", Inline, "guest_dial: host performs the upgrade"),
    ("__hr_create", LocalState, "HTMLRewriter handle in a process-local registry"),
    ("__hr_write", LocalState, "HTMLRewriter handle in a process-local registry"),
    ("__hr_end", LocalState, "HTMLRewriter handle in a process-local registry"),
    ("__hr_cmd", LocalState, "HTMLRewriter handle in a process-local registry"),
    ("__hr_event", LocalState, "HTMLRewriter handle in a process-local registry"),
    ("__hr_take", LocalState, "HTMLRewriter handle in a process-local registry"),
    ("__hr_free", LocalState, "HTMLRewriter handle in a process-local registry"),
    ("__tcp_connect", Prohibited, "raw network; blocked only by EgressPolicy::Deny, which a guest root sets (Allow would dial natively)"),
    ("__tcp_read", Prohibited, "raw socket I/O; no guest-held socket exists unless connect worked"),
    ("__tcp_write", Prohibited, "raw socket I/O; no guest-held socket exists unless connect worked"),
    ("__tcp_shutdown", Prohibited, "raw socket I/O; no guest-held socket exists unless connect worked"),
    ("__tcp_close", Prohibited, "raw socket registry; harmless but part of the tcp surface"),
    ("__tcp_starttls", Prohibited, "raw socket TLS upgrade; no guest-held socket exists"),
    ("__container_running", Brokered, "guest_state via the storage channel"),
    ("__container_address", Prohibited, "explicit deny (unconnected): native address never reaches a guest"),
    ("__container_start", Brokered, "run opens via storage op, start brokered as a resource call"),
    ("__container_monitor", Brokered, "resource call on the pinned container alias"),
    ("__container_destroy", Brokered, "resource call on the pinned container alias"),
    ("__container_signal", Brokered, "resource call on the pinned container alias"),
    ("__container_inactivity", Brokered, "resource call on the pinned container alias"),
    ("__container_exec", Brokered, "resource call on the pinned container alias"),
    ("__container_exec_read", Brokered, "process_call: the event's own container"),
    ("__container_exec_write", Brokered, "process_call: the event's own container"),
    ("__container_exec_close", Brokered, "process_call: the event's own container"),
    ("__container_exec_wait", Brokered, "process_call: the event's own container"),
    ("__container_exec_kill", Brokered, "process_call: the event's own container"),
    ("__container_exec_drop", Brokered, "process_call: the event's own container"),
    ("__container_port_fetch", Brokered, "guest-only: resource_http_request; no native path"),
    ("__container_connect", Brokered, "guest-only: PortConnect resource call; no native path"),
    ("__container_tcp_read", Brokered, "guest-only: process_call; no native path"),
    ("__container_tcp_write", Brokered, "guest-only: process_call; no native path"),
    ("__container_tcp_shutdown", Brokered, "guest-only: process_call; no native path"),
    ("__container_tcp_close", Brokered, "guest-only: process_call; no native path"),
    ("__container_tcp_starttls", Brokered, "guest-only: process_call; no native path"),
    ("__storage_get", Inline, "get_value: storage channel in a guest, host-checked cell"),
    ("__storage_get_many", Inline, "get_values: storage channel in a guest"),
    ("__sql_ingest", Inline, "ingest_sql: storage channel in a guest"),
    ("__sql_cursor_start", Inline, "start_sql: storage channel, string cursor id"),
    ("__sql_cursor_next", Inline, "storage channel SqlNext"),
    ("__sql_cursor_close", Inline, "storage channel SqlClose"),
    ("__sql_database_size", Inline, "database_size: storage channel"),
    ("__d1_run", Inline, "d1_reply: D1 over the storage channel; no channel means error, never native SQLite"),
    ("__storage_transaction_control", Inline, "transaction_control via the storage channel"),
    ("__log", LocalState, "tail log + tracing in the guest's own process; telemetry is host-only"),
    ("__storage_put", Inline, "put_value: storage channel in a guest"),
    ("__storage_put_many", Inline, "put_values: storage channel in a guest"),
    ("__storage_queue_put", Inline, "queue_serialized_puts: channel queue in a guest"),
    ("__storage_queue_put_many", Inline, "queue_serialized_puts: channel queue in a guest"),
    ("__storage_put_serialized", Inline, "put_value: storage channel in a guest"),
    ("__storage_queue_put_serialized", Inline, "queue_serialized_puts: channel queue in a guest"),
    ("__storage_flush_pending_puts", Inline, "flush: storage channel in a guest"),
    ("__storage_sync", Prohibited, "explicit deny (refuse_unbridged_guest_op); native path needs GATE_TX"),
    ("__storage_cancel_pending_puts", Inline, "channel cancel in a guest"),
    ("__actor_abort", Inline, "latches AbortObject on the host channel first"),
    ("__cron_plan", Pure, "celld_logic::cron schedule arithmetic"),
    ("__cron_schedule", Prohibited, "host catalog lookup; yields nothing only because application_vm::hosted() is None in celld-guest"),
    ("__cron_dispatch", Prohibited, "runs a cron event in another VM through hosted(); None in celld-guest"),
    ("__kv_blob", Prohibited, "fleet-bucket I/O with node credentials and native activation_epoch; no guest check, closed only by absent KV_BLOB_STORE and cell storage"),
    ("__process_exit", LocalState, "terminates only the calling isolate's execution"),
    ("__storage_delete", Inline, "storage channel Delete in a guest"),
    ("__storage_delete_many", Inline, "delete_keys: storage channel in a guest"),
    ("__storage_list", Inline, "storage channel List in a guest"),
    ("__storage_sync_list_start", Inline, "storage channel SyncListStart in a guest"),
    ("__storage_sync_list_next", Inline, "storage channel SyncListNext in a guest"),
    ("__storage_delete_all", Inline, "storage channel DeleteAll in a guest"),
    ("$$urlParse", Pure, "WHATWG URL parsing"),
    ("$$urlPatternParse", Pure, "URLPattern parsing"),
    ("$$urlPatternMatchInput", Pure, "URLPattern matching"),
    ("$$atob", Pure, "base64 decode"),
    ("$$btoa", Pure, "base64 encode"),
    ("$$textDecoderLabel", Pure, "encoding label lookup"),
    ("$$textDecoderNew", LocalState, "decoder kept in an isolate slot"),
    ("$$textDecoderDecode", LocalState, "decoder kept in an isolate slot"),
    ("$$textDecoderDecodeOnce", Pure, "one-shot decode"),
    ("$$textDecoderFree", LocalState, "decoder kept in an isolate slot"),
    ("__alarm_set", Inline, "storage channel SetAlarm; host arms and gates it"),
    ("__queue_alarm_set_wait", Prohibited, "explicit deny (refuse_unbridged_guest_op); native path needs the wake-entry gate"),
    ("__alarm_get", Inline, "storage channel alarm read"),
    ("__alarm_delete", Inline, "storage channel DeleteAlarm"),
    ("__loader_load", Inline, "child loads in-process; refuses globalOutbound, env routes and tails in a guest"),
    ("__loader_fetch", Inline, "child runs in-process; gate is NoCell in a guest"),
    ("__loader_rpc", Inline, "child runs in-process; gate is NoCell in a guest"),
    ("__loader_drop", Inline, "drops the child under guest_activity::track"),
    ("__facet_fetch", Inline, "guest branch opens the facet through the host grant"),
    ("__facet_rpc", Inline, "guest branch opens the facet through the host grant"),
    ("__facet_abort", Inline, "guest branch tells the host, then closes the local child"),
    ("__facet_delete", Inline, "guest branch deletes via the host; native needs FACET_TX"),
    ("__do_call", Brokered, "guest_storage::object_call; native path needs DO_CALL_TX"),
    ("__svc_call", Brokered, "guest_storage::service_call; native path needs SVC_CALL_TX"),
    ("__svc_call_cancellable", Brokered, "guest_storage::service_call; native path needs SVC_CALL_TX"),
    ("__svc_rpc", Brokered, "guest_storage::service_call; native path needs SVC_RPC_TX"),
    ("__queue_dispatch", Prohibited, "QUEUE_DISPATCH_TX only; no guest check, closed by the unset channel"),
    ("__queue_policy", Pure, "celld_logic::queue arithmetic on the request JSON"),
    ("__queue_consumer", Prohibited, "host catalog lookup; yields nothing only because hosted() is None in celld-guest"),
    ("__do_call_cancellable", Brokered, "guest_storage::object_call; native path needs DO_CALL_TX"),
    ("__do_call_cancel", LocalState, "fires a sender in the process cancel registry"),
    ("__rpc_signal_new", LocalState, "process-local RPC signal registry"),
    ("__rpc_signal_guest", Inline, "reports the isolate's guest flag"),
    ("__rpc_signal_abort", LocalState, "process-local RPC signal registry"),
    ("__rpc_signal_release", LocalState, "process-local RPC signal registry"),
    ("__rpc_signal_poll", LocalState, "process-local RPC signal registry"),
    ("__rpc_signal_wait", LocalState, "process-local RPC signal registry"),
    ("__rpc_signal_unsubscribe", LocalState, "process-local RPC signal registry"),
    ("__do_id", Pure, "HMAC of a JS-supplied namespace key and name or fresh random bytes"),
    ("__rpc_call", Brokered, "guest_storage::object_call; native path needs RPC_CALL_TX"),
    ("__sc_encode", Pure, "V8 structured clone to bytes"),
    ("__sc_decode", Pure, "V8 structured clone from bytes"),
    ("__structured_clone", Pure, "in-isolate structured clone"),
    ("__op_fetch", Brokered, "guest_storage::fetch; EgressPolicy::Deny is skipped in a guest"),
    ("__r2_head", Brokered, "install-time override (override_ops): guest_return to the host R2 broker"),
    ("__r2_get", Brokered, "guest_return to the host R2 broker"),
    ("__r2_put", Brokered, "guest_return to the host R2 broker"),
    ("__r2_put_begin", Brokered, "guest_return to the host R2 broker"),
    ("__r2_put_chunk", Brokered, "guest_return to the host R2 broker"),
    ("__r2_put_end", Brokered, "guest_return to the host R2 broker"),
    ("__r2_delete", Brokered, "guest_return to the host R2 broker"),
    ("__r2_list", Brokered, "guest_return to the host R2 broker"),
    ("__r2_mp_begin", Brokered, "guest_return to the host R2 broker"),
    ("__r2_mp_resume", Brokered, "guest_return to the host R2 broker"),
    ("__r2_mp_part", Brokered, "guest_return to the host R2 broker"),
    ("__r2_mp_complete", Brokered, "guest_return to the host R2 broker"),
    ("__r2_mp_abort", Brokered, "guest_return to the host R2 broker"),
    ("__asset_fetch", Brokered, "resource_http_request on the pinned assets alias"),
    ("__http_stream_read", LocalState, "process-local HTTP stream service"),
    ("__http_stream_cancel", LocalState, "process-local HTTP stream service"),
    ("__http_stream_tee", Inline, "tee pump runs under guest_activity::track"),
    ("__response_stream_create", LocalState, "process-local HTTP stream service"),
    ("__response_stream_write", Inline, "NoCell gate for a guest writer; host gates each chunk"),
    ("__response_stream_closed", LocalState, "process-local HTTP stream service"),
    ("__response_stream_close", LocalState, "process-local HTTP stream service"),
    ("__op_timer", LocalState, "tokio sleep plus the process timer registry"),
    ("__isolate_condemn", Inline, "no pool slot in a guest: asks the host to retire the VM"),
    ("__timer_alloc", LocalState, "process timer id counter"),
    ("__io_context_id", LocalState, "reads the current IoContext id"),
    ("__with_input_gate_context", LocalState, "binds CPED tokens for a callback"),
    ("__gate_acquire", Inline, "guest critical_start on the host channel"),
    ("__gate_wait", Prohibited, "explicit deny: guest stub input-gate bridge is not implemented"),
    ("__gate_release", Inline, "guest critical_finish on the host channel"),
    ("__timer_cancel", LocalState, "process timer registry"),
    ("__crypto_operation", Pure, "crypto on supplied bytes"),
    ("$$randomValues", Pure, "random bytes"),
    ("$$digest", Pure, "hash"),
    ("$$hmacSign", Pure, "HMAC"),
    ("$$hmacVerify", Pure, "HMAC"),
    ("$$aesEncrypt", Pure, "AES"),
    ("$$aesDecrypt", Pure, "AES"),
    ("$$pbkdf2", Pure, "key derivation"),
    ("$$hkdf", Pure, "key derivation"),
    ("$$timingSafeEqual", Pure, "constant-time compare"),
    ("__event_begin", LocalState, "event frame on the current IoContext"),
    ("__event_end", LocalState, "event frame on the current IoContext"),
    ("__guest_cleanup_track", Inline, "records guest cleanup promises on a guest IoContext"),
    ("__pending_event_begin", LocalState, "IoContext bookkeeping"),
    ("__pending_event_end", LocalState, "IoContext bookkeeping"),
    ("__vfs_mkdir", LocalState, "per-request in-memory /tmp tree"),
    ("__vfs_read_file", LocalState, "reads the in-memory script bundle, never the host disk"),
    ("__vfs_stat", LocalState, "in-memory /tmp tree and script bundle"),
    ("__wait_until", LocalState, "registers on the current IoContext"),
    ("__wait_until_active", LocalState, "reads the current IoContext"),
    ("__event_depth", LocalState, "reads the current IoContext"),
    ("__als_get", LocalState, "continuation-preserved data"),
    ("__als_set", LocalState, "continuation-preserved data"),
    ("__util_type_flags", Pure, "V8 type checks"),
    ("__util_constructor_name", Pure, "V8 constructor name"),
    ("__util_proxy_details", Pure, "V8 proxy inspection"),
    ("__util_promise_details", Pure, "V8 promise inspection"),
    ("__util_preview_entries", Pure, "V8 collection preview"),
    ("__builtin_module", LocalState, "compiles an embedded builtin into this isolate"),
    ("__zlib", Pure, "one-shot compression"),
    ("__zlib_stream_new", LocalState, "process-local zlib stream registry"),
    ("__zlib_stream_push", LocalState, "process-local zlib stream registry"),
    ("__zlib_stream_end", LocalState, "process-local zlib stream registry"),
    ("__zlib_stream_drop", LocalState, "process-local zlib stream registry"),
];

/// Test-build ops (`TEST_OP_NAMES`, only with `--cfg celld_internal_tests`).
/// None ships, so the class is informational.
#[cfg(celld_internal_tests)]
#[rustfmt::skip]
pub(super) const TEST_CLASS: &[(&str, Class, &str)] = &[
    ("__test_patch_op", LocalState, "test-only: per-isolate op patch table"),
    ("__test_gc", LocalState, "test-only: forces GC in this isolate"),
    ("__loader_count", LocalState, "test-only: loader registry count"),
    ("__test_set_heap_limit_excessively_exceeded", LocalState, "test-only: sets this isolate's heap flag"),
    ("__test_force_heap_admission_refusal", LocalState, "test-only: sets this isolate's heap flag"),
    ("__test_heap_share", LocalState, "test-only: heap read"),
    ("__test_external_memory", LocalState, "test-only: memory read"),
    ("__test_workflow_event_consumed", LocalState, "test-only: thread-local fault flag"),
    ("__test_workflow_meta_created", LocalState, "test-only: thread-local fault flag"),
    ("__test_workflow_terminal_alarm_set", LocalState, "test-only: thread-local fault flag"),
    ("__test_queue_dlq_accepted", LocalState, "test-only: thread-local counter"),
    ("__test_queue_metrics_materialized", LocalState, "test-only: thread-local flag"),
    ("__test_queue_producer_group", LocalState, "test-only: thread-local log"),
    ("__test_queue_rearm_bounded", LocalState, "test-only: thread-local flags"),
    ("__test_queue_lease_lookup_plan", LocalState, "test-only: thread-local log"),
    ("__test_kv_list_plan", LocalState, "test-only: thread-local log"),
    ("__sql_set_max_page_count_for_test", Prohibited, "test-only: mutates native cell storage"),
    ("__sql_set_write_fault_for_test", Prohibited, "test-only: native storage fault switch"),
    ("__sql_set_cache_size_for_test", Prohibited, "test-only: mutates native cell storage"),
    ("__sql_set_interrupt_fault_for_test", Prohibited, "test-only: mutates native cell storage"),
    ("__sql_register_nomem_function_for_test", Prohibited, "test-only: mutates native cell storage"),
];

#[cfg(test)]
mod tests {
    use super::*;

    /// One row per op, in `ops!` table order: step 3 indexes overrides by that
    /// position, and a missing, stale or duplicated row breaks the match.
    fn assert_ledger(table: &[(&str, Class, &str)], names: &[&str]) {
        let ledger: Vec<&str> = table.iter().map(|(name, ..)| *name).collect();
        assert_eq!(
            ledger, names,
            "classify every new op in js/guest_ops.rs, in table order"
        );
        assert!(table.iter().all(|(_, _, reason)| !reason.is_empty()));
    }

    #[test]
    fn every_op_has_exactly_one_class() {
        assert_ledger(CLASS, super::super::OP_NAMES);
        #[cfg(celld_internal_tests)]
        assert_ledger(TEST_CLASS, super::super::TEST_OP_NAMES);
    }
}
