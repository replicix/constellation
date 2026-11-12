//! What Constellation plugs into the shared `replicix-objcas` crate: its
//! error table for the transient/permanent classifier and its protocol
//! clock for lease stamps.
//!
//! [`install`] is idempotent and cheap; every process that builds a store,
//! runs an engine or drives the authority calls it before the first lease
//! stamp or classification (a test that moves the protocol clock must have
//! called it in its process, or the fault offset is silently ignored).

use std::sync::{Arc, Once};

use replicix_objcas::{Clock, ErrorClass};

use crate::error::StoreError;

/// Lease stamps read Constellation's protocol clock, which carries the
/// test fault offsets and steps.
struct ProtocolWallClock;

impl Clock for ProtocolWallClock {
    fn now_unix_ms(&self) -> i64 {
        constellation_platform::clock::protocol().wall().0
    }
}

/// Constellation's rows of the classifier: what a [`StoreError`] link
/// decides on its own (wrapped errors defer to their own link).
pub fn classify_store_error(e: &(dyn std::error::Error + 'static)) -> Option<ErrorClass> {
    let e = e.downcast_ref::<StoreError>()?;
    match e {
        StoreError::ObjectStore(_) | StoreError::Io(_) => None,
        StoreError::CasConflict | StoreError::Conflict(_) => Some(ErrorClass::Transient),
        // A credential source that could not answer (IMDS or SSO
        // unreachable) may answer later.
        StoreError::AwsCredentials(_) => Some(ErrorClass::Transient),
        StoreError::CorruptObject(_)
        | StoreError::UnknownCodec(_)
        | StoreError::HashMismatch { .. }
        | StoreError::AlreadyExists
        | StoreError::InboxVersion(_)
        | StoreError::NotFound
        | StoreError::Meta(_)
        | StoreError::Compression(_)
        | StoreError::Json(_)
        | StoreError::Parallel(_) => Some(ErrorClass::Permanent),
        _ => None,
    }
}

/// Register the classifier and the protocol clock, once per process.
pub fn install() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        replicix_objcas::register_error_classifier(classify_store_error);
        replicix_objcas::clock::set_clock(Arc::new(ProtocolWallClock));
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use replicix_objcas::classify;

    #[test]
    fn store_error_rows() {
        install();
        let t = |e: StoreError| classify(&e);
        assert_eq!(
            t(StoreError::AwsCredentials("IMDS timed out".into())),
            ErrorClass::Transient
        );
        assert_eq!(t(StoreError::CasConflict), ErrorClass::Transient);
        assert_eq!(
            t(StoreError::CorruptObject("bad frame".into())),
            ErrorClass::Permanent
        );
        assert_eq!(t(StoreError::NotFound), ErrorClass::Permanent);
        // Wrapped errors defer to the wrapped link.
        let inner = StoreError::ObjectStore(object_store::Error::PermissionDenied {
            path: "k".into(),
            source: "AccessDenied".into(),
        });
        assert_eq!(classify(&inner), ErrorClass::Permanent);
        let inner = StoreError::Io(std::io::Error::from(std::io::ErrorKind::TimedOut));
        assert_eq!(classify(&inner), ErrorClass::Transient);
    }

    #[test]
    fn lease_stamps_follow_the_protocol_clock() {
        install();
        let a = replicix_objcas::clock::now_unix_ms();
        let b = constellation_platform::clock::protocol().wall().0;
        assert!((a - b).abs() < 1000, "{a} vs {b}");
    }
}
