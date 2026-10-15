//! Firecracker-guest-only helpers of the storage ops: the guest storage bridge calls and their native fall-through. Moved out of `storage_ops.rs` so upstream merges stay small.

use super::*;

pub(in crate::js) fn refuse_unbridged_guest_op(scope: &mut v8::PinScope, name: &str) -> bool {
    if guest_storage::enabled(scope) {
        loader_throw(scope, &format!("guest {name} bridge is not available"));
        return true;
    }
    false
}

pub(super) enum SqlCursorId {
    Native(u64),
    Guest(Option<String>),
}

pub(super) type SqlStart = (
    SqlCursorId,
    Vec<String>,
    Option<Vec<storage::SqlValue>>,
    u64,
    bool,
);

pub(super) fn start_sql(
    scope: &mut v8::PinScope,
    cell: &str,
    query: &str,
    bindings: &[rusqlite::types::Value],
) -> Result<SqlStart, String> {
    if let Some(channel) = guest_storage::channel(scope, cell)? {
        let bindings = bindings
            .iter()
            .map(|value| match value {
                rusqlite::types::Value::Null => wire::SqlValue::Null {},
                rusqlite::types::Value::Integer(value) => wire::SqlValue::Integer { value: *value },
                rusqlite::types::Value::Real(value) => wire::SqlValue::Real {
                    bits: value.to_bits(),
                },
                rusqlite::types::Value::Text(value) => wire::SqlValue::Text {
                    value: value.clone(),
                },
                rusqlite::types::Value::Blob(value) => wire::SqlValue::Blob {
                    value: value.clone(),
                },
            })
            .collect();
        return match guest_storage::call(
            scope,
            &channel,
            wire::Operation::SqlStart {
                query: query.to_owned(),
                bindings,
            },
        )? {
            wire::Value::Cursor {
                cursor,
                columns,
                row,
                rows_written,
                reused_cached_query,
            } if cursor
                .as_ref()
                .is_none_or(|cursor| wire::valid_handle(cursor)) =>
            {
                Ok((
                    SqlCursorId::Guest(cursor),
                    columns,
                    row.map(guest_storage::sql_row),
                    rows_written,
                    reused_cached_query,
                ))
            }
            _ => Err(guest_storage::unexpected(scope, &channel, "SQL cursor")),
        };
    }
    storage::sql_cursor_start_values(cell, query, bindings).map(
        |(id, columns, row, written, reused)| {
            (SqlCursorId::Native(id), columns, row, written, reused)
        },
    )
}

pub(super) fn ingest_sql(
    scope: &mut v8::PinScope,
    cell: &str,
    input: &str,
) -> Result<(String, u64, u64), String> {
    if let Some(channel) = guest_storage::channel(scope, cell)? {
        return match guest_storage::call(
            scope,
            &channel,
            wire::Operation::SqlIngest {
                input: input.to_owned(),
            },
        )? {
            wire::Value::Ingest {
                remainder,
                rows_written,
                statement_count,
            } => Ok((remainder, rows_written, statement_count)),
            _ => Err(guest_storage::unexpected(scope, &channel, "SQL ingest")),
        };
    }
    storage::sql_ingest(cell, input)
}

pub(super) fn database_size(scope: &mut v8::PinScope, cell: &str) -> Result<u64, String> {
    if let Some(channel) = guest_storage::channel(scope, cell)? {
        return match guest_storage::call(scope, &channel, wire::Operation::SqlSize {})? {
            wire::Value::Size { bytes } => Ok(bytes),
            _ => Err(guest_storage::unexpected(scope, &channel, "database size")),
        };
    }
    storage::sql_database_size(cell)
}

pub(super) fn delete_keys(
    scope: &mut v8::PinScope,
    cell: &str,
    keys: &[String],
) -> Result<u64, String> {
    if let Some(channel) = guest_storage::channel(scope, cell)? {
        return match guest_storage::call(
            scope,
            &channel,
            wire::Operation::Delete {
                keys: keys.to_vec(),
            },
        )? {
            wire::Value::Count { count } => Ok(count),
            _ => Err(guest_storage::unexpected(scope, &channel, "delete")),
        };
    }
    storage::delete_many(cell, keys)
        .map(|count| count as u64)
        .map_err(|error| error.to_string())
}

pub(super) fn get_value(
    scope: &mut v8::PinScope,
    cell: &str,
    key: &str,
) -> Result<Option<storage::StoredValue>, String> {
    if let Some(channel) = guest_storage::channel(scope, cell)? {
        return guest_storage::get(scope, &channel, vec![key.to_owned()]).map(|entries| {
            entries
                .into_iter()
                .find(|(returned, _)| returned == key)
                .map(|(_, value)| value)
        });
    }
    storage::get_stored(cell, key).map_err(|error| error.to_string())
}

pub(super) fn get_values(
    scope: &mut v8::PinScope,
    cell: &str,
    keys: &[String],
) -> Result<Vec<(String, storage::StoredValue)>, String> {
    if let Some(channel) = guest_storage::channel(scope, cell)? {
        return guest_storage::get(scope, &channel, keys.to_vec());
    }
    storage::get_many_stored(cell, keys).map_err(|error| error.to_string())
}

pub(super) fn put_values(
    scope: &mut v8::PinScope,
    cell: &str,
    entries: &[(String, Vec<u8>)],
) -> Result<(), String> {
    if let Some(channel) = guest_storage::channel(scope, cell)? {
        return guest_storage::put(scope, &channel, entries);
    }
    storage::put_many_serialized(cell, entries).map_err(|error| error.to_string())
}

pub(super) fn put_value(
    scope: &mut v8::PinScope,
    cell: &str,
    key: &str,
    value: &[u8],
) -> Result<(), String> {
    if let Some(channel) = guest_storage::channel(scope, cell)? {
        return guest_storage::put(scope, &channel, &[(key.to_owned(), value.to_vec())]);
    }
    storage::put_serialized(cell, key, value).map_err(|error| error.to_string())
}

pub(super) fn d1_reply(
    scope: &mut v8::PinScope,
    cell: &str,
    request: String,
) -> Result<String, String> {
    if let Some(channel) = guest_storage::channel(scope, cell)? {
        // JSON escaping can grow a byte up to 6x; one D1Run must fit a frame.
        let mut operation = if request.len() > wire::MAX_MESSAGE_BYTES / 8 {
            let start = wire::Operation::D1RequestStart {
                bytes: request.len() as u64,
            };
            match guest_storage::call(scope, &channel, start)? {
                wire::Value::Unit {} => {}
                _ => {
                    return Err(guest_storage::unexpected(
                        scope,
                        &channel,
                        "D1 request start",
                    ))
                }
            }
            let mut offset = 0;
            while offset < request.len() {
                let mut end = (offset + celld_applications::invocation_wire::MAX_CHUNK_BYTES)
                    .min(request.len());
                while !request.is_char_boundary(end) {
                    end -= 1;
                }
                let chunk = wire::Operation::D1RequestChunk {
                    data: request[offset..end].to_owned(),
                };
                match guest_storage::call(scope, &channel, chunk)? {
                    wire::Value::Unit {} => {}
                    _ => {
                        return Err(guest_storage::unexpected(
                            scope,
                            &channel,
                            "D1 request chunk",
                        ))
                    }
                }
                offset = end;
            }
            wire::Operation::D1RequestEnd {}
        } else {
            wire::Operation::D1Run { request }
        };
        let mut output = String::new();
        loop {
            match guest_storage::call(scope, &channel, operation)? {
                wire::Value::D1 { reply, complete } => {
                    output.push_str(&reply);
                    if complete {
                        return Ok(output);
                    }
                    operation = wire::Operation::D1Next {};
                }
                _ => return Err(guest_storage::unexpected(scope, &channel, "D1")),
            }
        }
    }
    Ok(storage::d1_run_json(cell, &request))
}
