//! Signed tokens. The session cookie and the access token use the same format.
//!
//! A token is `{claims}.{signature}`: base64url JSON claims, and the base64url HMAC-SHA256
//! of the claims part. The `kind` claim keeps one kind from being used as the other.
//!
//! This stands in for an identity provider such as Clerk, which issues and signs its own
//! tokens. Do not use it in production.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

/// What a token is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// The session cookie. Long-lived, and only the Worker reads it.
    Session,
    /// The access token for the WebSocket. Short-lived, and the client reads it.
    Access,
}

#[derive(Serialize, Deserialize)]
struct Claims {
    sub: String,
    kind: Kind,
    exp: u64,
}

fn mac(secret: &[u8]) -> Hmac<Sha256> {
    Hmac::new_from_slice(secret).expect("HMAC takes a key of any length")
}

/// Signs a token of this kind for `user`, valid until `exp` (seconds since the Unix epoch).
pub fn sign(secret: &[u8], kind: Kind, user: &str, exp: u64) -> String {
    let claims = Claims {
        sub: user.to_owned(),
        kind,
        exp,
    };
    let claims = BASE64.encode(serde_json::to_vec(&claims).expect("claims serialize"));
    let mut mac = mac(secret);
    mac.update(claims.as_bytes());
    let signature = BASE64.encode(mac.finalize().into_bytes());
    format!("{claims}.{signature}")
}

/// Returns the user of a token of this kind, if its signature is valid and it has not
/// expired at `now`.
pub fn verify(secret: &[u8], kind: Kind, token: &str, now: u64) -> Option<String> {
    let (claims, signature) = token.split_once('.')?;
    let mut mac = mac(secret);
    mac.update(claims.as_bytes());
    // A constant-time comparison.
    mac.verify_slice(&BASE64.decode(signature).ok()?).ok()?;
    let claims: Claims = serde_json::from_slice(&BASE64.decode(claims).ok()?).ok()?;
    (claims.kind == kind && now < claims.exp).then_some(claims.sub)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"a test secret of at least 32 bytes";

    #[test]
    fn a_signed_token_verifies_until_it_expires() {
        let token = sign(SECRET, Kind::Access, "Alice", 100);
        assert_eq!(
            verify(SECRET, Kind::Access, &token, 99).as_deref(),
            Some("Alice")
        );
        assert_eq!(verify(SECRET, Kind::Access, &token, 100), None);
    }

    #[test]
    fn a_token_of_the_other_kind_is_rejected() {
        let session = sign(SECRET, Kind::Session, "Alice", 100);
        assert_eq!(verify(SECRET, Kind::Access, &session, 0), None);
    }

    #[test]
    fn a_token_signed_with_another_secret_is_rejected() {
        let token = sign(b"another secret", Kind::Access, "Alice", 100);
        assert_eq!(verify(SECRET, Kind::Access, &token, 0), None);
    }

    #[test]
    fn changed_claims_are_rejected() {
        let token = sign(SECRET, Kind::Access, "Alice", 100);
        let (_, signature) = token.split_once('.').unwrap();
        let forged = BASE64.encode(r#"{"sub":"Bob","kind":"access","exp":100}"#);
        assert_eq!(
            verify(SECRET, Kind::Access, &format!("{forged}.{signature}"), 0),
            None
        );
    }

    #[test]
    fn malformed_tokens_are_rejected() {
        for token in ["", ".", "abc", "abc.def", "!!.!!"] {
            assert_eq!(verify(SECRET, Kind::Access, token, 0), None);
        }
    }
}
