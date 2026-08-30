use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

pub(crate) fn payload_sha256(payload: &[u8]) -> String {
    hex::encode(Sha256::digest(payload))
}

pub(crate) fn verify_signature(secret: &[u8], payload: &[u8], signature: &str) -> bool {
    let Some(hex_digest) = signature.strip_prefix("sha256=") else {
        return false;
    };
    let Ok(received) = hex::decode(hex_digest) else {
        return false;
    };
    let Ok(mut mac) = HmacSha256::new_from_slice(secret) else {
        return false;
    };
    mac.update(payload);
    mac.verify_slice(&received).is_ok()
}

pub(crate) fn origin_marker(job_id: &str) -> String {
    format!("lenso:{job_id}")
}

pub(crate) fn append_origin_marker(body: Option<&str>, marker: &str) -> String {
    format!(
        "{}\n\n<!-- lenso-github-sync:{marker} -->",
        body.unwrap_or_default().trim_end()
    )
}

pub(crate) fn find_origin_marker(body: Option<&str>) -> Option<&str> {
    let body = body?;
    let start = body.rfind("<!-- lenso-github-sync:")? + "<!-- lenso-github-sync:".len();
    let rest = &body[start..];
    let end = rest.find(" -->")?;
    let marker = &rest[..end];
    (!marker.is_empty()
        && marker.len() <= 128
        && marker
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':')))
    .then_some(marker)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verifies_githubs_published_sha256_vector_over_raw_bytes() {
        assert!(verify_signature(
            b"It's a Secret to Everybody",
            b"Hello, World!",
            "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17"
        ));
        assert!(!verify_signature(
            b"wrong",
            b"Hello, World!",
            "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17"
        ));
    }

    #[test]
    fn marker_round_trip_is_bounded() {
        let body = append_origin_marker(Some("hello"), "lenso:job-1");
        assert_eq!(find_origin_marker(Some(&body)), Some("lenso:job-1"));
    }
}
