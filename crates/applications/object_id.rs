//! Original Celld's authenticated idFromName derivation, shared by host
//! resource addressing and the JavaScript adapter. Neither deployment
//! versions nor mutable binding names belong in its namespace key.

pub fn for_name(key: &[u8; 32], name: &str) -> [u8; 32] {
    use hmac::Mac;
    fn digest(key: &[u8; 32], input: &[u8]) -> [u8; 32] {
        let mut mac = <hmac::Hmac<sha2::Sha256> as Mac>::new_from_slice(key)
            .expect("SHA-256 accepts any HMAC key");
        mac.update(input);
        mac.finalize().into_bytes().into()
    }
    let mut id = [0; 32];
    id[..16].copy_from_slice(&digest(key, name.as_bytes())[..16]);
    let authentication = digest(key, &id[..16]);
    id[16..].copy_from_slice(&authentication[..16]);
    id
}

/// Whether `id` was minted for `key`, as a named or a unique id alike: its
/// second half authenticates the first, as original `idFromString` checks.
pub fn authentic(key: &[u8; 32], id: &[u8; 32]) -> bool {
    use hmac::Mac;
    <hmac::Hmac<sha2::Sha256> as Mac>::new_from_slice(key)
        .expect("SHA-256 accepts any HMAC key")
        .chain_update(&id[..16])
        .verify_truncated_left(&id[16..])
        .is_ok()
}
