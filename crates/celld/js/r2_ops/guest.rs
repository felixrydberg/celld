//! Firecracker-guest-only R2 bridge: the brokered ops' return path and the host broker entry. Moved out of `r2_ops.rs` so upstream merges stay small.

use super::*;

// Not equivalent to native: the broker proves the calling event's output
// (`output().prove`) before every brokered op, reads (head/get/list) included,
// while native reads wait on no egress gate. Stricter, so safe, but a guest
// read can wait on or fail that proof where a native one cannot.

// Original JS upload closures retain their binding alias. The broker still
// authorizes every opaque upload id against its host-derived bucket; the guest
// needs no permanent upload-id registry.
pub(super) fn guest_binding(
    scope: &mut v8::PinScope,
    args: &v8::FunctionCallbackArguments,
    index: i32,
) -> Result<String, String> {
    let binding = args.get(index);
    if !binding.is_string() {
        return Err("R2 upload has no binding alias".into());
    }
    Ok(binding.to_rust_string_lossy(scope))
}

/// `__r2_head` in a guest isolate, installed over the native op by
/// `guest_ops::override_ops`. The host broker answers.
pub(crate) fn guest_head(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let bucket_name = args.get(0).to_rust_string_lossy(scope);
    let key = args.get(1).to_rust_string_lossy(scope);
    let operation = celld_applications::resource_wire::R2Operation::Head { key };
    guest_return(scope, &mut rv, Ok(bucket_name), operation, Vec::new());
}

pub(super) fn guest_return(
    scope: &mut v8::PinScope,
    rv: &mut v8::ReturnValue<v8::Value>,
    binding: Result<String, String>,
    operation: celld_applications::resource_wire::R2Operation,
    body: Vec<u8>,
) {
    use celld_applications::resource_wire::{R2Operation, ResourceOperation};
    let read = matches!(operation, R2Operation::Get { .. });
    let request = binding.as_ref().map_err(Clone::clone).and_then(|binding| {
        guest_storage::resource_request(
            scope,
            binding.clone(),
            ResourceOperation::R2 { operation },
            body,
        )
    });
    let stream_service = http_stream_service();
    let id = asyncrt::enqueue(async move {
        let response = request?
            .await
            .map_err(|_| "R2 guest operation was abandoned".to_string())??;
        let mut stream: HttpChunkStream =
            futures_util::stream::once(std::future::ready(Ok(response.body)))
                .chain(
                    response
                        .stream
                        .unwrap_or_else(|| Box::pin(futures_util::stream::empty())),
                )
                .boxed();
        let result = if read {
            let mut bytes = bytes::BytesMut::new();
            let length = loop {
                if bytes.len() >= 4 {
                    break u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
                }
                bytes.extend_from_slice(&stream.next().await.ok_or("R2 guest record truncated")??);
            };
            if length > 1024 * 1024 {
                return Err("R2 guest record exceeds wire limit".into());
            }
            while bytes.len() < length + 4 {
                bytes.extend_from_slice(&stream.next().await.ok_or("R2 guest record truncated")??);
            }
            let mut envelope: serde_json::Value =
                serde_json::from_slice(&bytes[4..4 + length]).map_err(|error| error.to_string())?;
            if envelope["state"] == "hit" {
                let rest = bytes.split_off(length + 4).to_vec();
                let body = futures_util::stream::iter((!rest.is_empty()).then_some(Ok(rest)))
                    .chain(stream)
                    .boxed();
                let stream_id = stream_service
                    .register_source(HttpStreamSource::Stream(body))
                    .ok_or_else(|| format!("R2 get: {HTTP_STREAM_REGISTRATION_CLOSED}"))?;
                envelope["streamId"] = stream_id.into();
            }
            envelope.to_string()
        } else {
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.next().await {
                bytes.extend(chunk?);
            }
            String::from_utf8(bytes).map_err(|error| error.to_string())?
        };
        Ok::<_, String>(result)
    });
    rv.set(promise_for(scope, id));
}

/// Reuse the original object-store/R2 implementation after the trusted broker
/// has authenticated an alias and proved the calling event's native output.
/// Numeric upload handles belong to the authenticated application resource,
/// independently of whether another application binds the same real bucket.
pub(crate) async fn broker_operation(
    bucket_name: String,
    broker_owner: String,
    operation: celld_applications::resource_wire::R2Operation,
    body: Vec<u8>,
) -> Result<HttpResponse, String> {
    use celld_applications::resource_wire::R2Operation as R;
    let put = match &operation {
        R::PutChunk { id } | R::PutEnd { id, .. } => Some(*id),
        _ => None,
    };
    if let Some(id) = put {
        let entry = puts().lock().unwrap().get(&id).cloned();
        if let Some(entry) = entry {
            let entry = entry.lock().await;
            if entry.bucket_name != bucket_name
                || entry.broker_owner.as_deref() != Some(broker_owner.as_str())
            {
                return Err("R2 streaming write belongs to another binding".into());
            }
        }
    }
    let upload = match &operation {
        R::MultipartPart { id, .. }
        | R::MultipartComplete { id, .. }
        | R::MultipartAbort { id } => Some(*id),
        _ => None,
    };
    if let Some(id) = upload {
        let entry = uploads().lock().unwrap().get(&id).cloned();
        if let Some(entry) = entry {
            let entry = entry.lock().await;
            if entry.bucket_name != bucket_name
                || entry.broker_owner.as_deref() != Some(broker_owner.as_str())
            {
                return Err("R2 multipart upload belongs to another binding".into());
            }
        }
    }
    let mut response = HttpResponse {
        status: 200,
        body: vec![],
        stream: None,
        headers: vec![],
        websocket: None,
        write_position: None,
        observed_position: None,
    };
    let result = match operation {
        R::Head { key } => match head(&bucket_name, &key).await? {
            None => serde_json::json!({"state":"miss"}).to_string(),
            Some(meta) => serde_json::json!({"state":"hit", "object":object_json(&key,&meta,None)})
                .to_string(),
        },
        R::Get { key, options } => {
            let request: GetRequest = serde_json::from_str(&options)
                .map_err(|error| format!("invalid R2 get options: {error}"))?;
            let range = request
                .range
                .map(BlobRange::from)
                .unwrap_or(BlobRange::Whole);
            let conditions = BlobConditions::from(request.only_if.unwrap_or_default());
            let envelope = match read(&bucket_name, &key, range, &conditions).await? {
                BlobRead::Missing => serde_json::json!({"state":"miss"}),
                BlobRead::Unmet(meta) => {
                    serde_json::json!({"state":"unmet","object":object_json(&key,&meta,None)})
                }
                BlobRead::Hit(blob) => {
                    response.stream = Some(blob.body);
                    serde_json::json!({"state":"hit","object":object_json(&key,&blob.meta,Some(blob.range))})
                }
            };
            let bytes = serde_json::to_vec(&envelope).map_err(|error| error.to_string())?;
            response.body = (bytes.len() as u32).to_be_bytes().to_vec();
            response.body.extend(bytes);
            return Ok(response);
        }
        R::Put { key, options } => {
            let request = serde_json::from_str::<PutRequest>(&options)
                .map_err(|error| format!("invalid R2 put options: {error}"))?;
            put_once(bucket_name, key, body, request).await?
        }
        R::Delete { keys } => {
            delete(&bucket_name, &keys).await?;
            String::new()
        }
        R::List { options } => list(&bucket_name, &options).await?.to_string(),
        R::PutBegin { key, options } => {
            native_put_begin(bucket_name, key, options, Some(&broker_owner)).await?
        }
        R::PutChunk { id } => {
            native_put_chunk(id, Some(body), EgressGate::NoCell, Some(&broker_owner)).await?
        }
        R::PutEnd { id, abort } => {
            native_put_end(id, abort, EgressGate::NoCell, Some(&broker_owner)).await?
        }
        R::MultipartBegin { key, options } => {
            native_mp_begin(
                bucket_name,
                key,
                serde_json::from_str(&options)
                    .map_err(|error| format!("invalid R2 multipart options: {error}")),
                EgressGate::NoCell,
                Some(&broker_owner),
            )
            .await?
        }
        R::MultipartResume { key, id } => {
            native_mp_resume(bucket_name, key, id, Some(&broker_owner)).await?
        }
        R::MultipartPart { id, part } => {
            native_mp_part(
                id,
                part,
                Some(body),
                EgressGate::NoCell,
                Some(&broker_owner),
            )
            .await?
        }
        R::MultipartComplete { id, parts } => {
            native_mp_complete(id, Ok(parts), EgressGate::NoCell, Some(&broker_owner)).await?
        }
        R::MultipartAbort { id } => {
            let gate = uploads()
                .lock()
                .unwrap()
                .contains_key(&id)
                .then_some(EgressGate::NoCell);
            native_mp_abort(id, gate, Some(&broker_owner)).await?
        }
    };
    response.body = result.into_bytes();
    Ok(response)
}
