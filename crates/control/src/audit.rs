//! The audit trail of mutating control calls (plan 31 §9.5): *who did what,
//! when*, without ever recording *with what data*.
//!
//! One JSON object per line, appended, never rewritten:
//!
//! ```json
//! {"ts_unix_ms":1767225600000,"principal":"unix:uid=1000,gids=[1000],pid=4242",
//!  "role":"admin","method":"gc.run","encoding":"json",
//!  "params_digest":"blake3:9f2c…","outcome":"ok"}
//! ```
//!
//! ## What is and is not recorded
//!
//! - **Only calls whose [`MethodInfo::mutating`](crate::methods::MethodInfo)
//!   is set**, plus their *denied* attempts (a viewer trying `gc.run` is
//!   exactly what an audit log is for). Reads are never logged: a
//!   dashboard polling `node.status` would drown the signal.
//! - **The parameters' BLAKE3 digest, never the parameters.** Params carry
//!   passphrases (`fs.passwd`, `fs.unlock`) and file contents
//!   (`browse.write`); a digest still lets an operator prove that two calls
//!   had identical arguments, or that a given argument set was (not) used,
//!   without the log becoming a secret store. For methods whose params
//!   carry [`Secret`](crate::proto::Secret)s (`fs.passwd`, `fs.unlock`:
//!   [`MethodInfo::secret_params`](crate::methods::MethodInfo)) even the
//!   digest is withheld ([`WITHHELD_DIGEST`]): an unsalted hash of a
//!   human-chosen passphrase is a dictionary attack away from the
//!   passphrase itself, so it would make the log a secret store after all.
//!   The digest covers the
//!   *received* encoding (canonical JSON, or the postcard bytes), so a JSON
//!   and a postcard call with the same arguments digest differently; the
//!   record names the encoding.
//! - **The outcome, not the message.** `ok`, or `err` with the
//!   [`ErrorKind`]; error messages can echo user data.
//!
//! ## Trade-offs
//!
//! A record is written when the call *finishes* (its outcome is part of the
//! record), so a daemon killed mid-call leaves no record of it. A
//! write-ahead "started" record would close that gap at the price of two
//! lines per call; not done. A failing sink is logged through `tracing` and
//! does **not** fail the call: refusing to administer a filesystem because
//! the disk holding the audit log is full would turn a logging fault into an
//! outage. Deployments that need fail-closed auditing implement
//! [`AuditSink`] themselves and can panic or abort from `record`.

use crate::authz::Role;
use crate::proto::{Blob, Encoding, ErrorKind};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

/// How a mutating call ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Ok,
    Err(ErrorKind),
}

/// One audit line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditRecord {
    pub ts_unix_ms: u64,
    /// `Principal`'s display form.
    pub principal: String,
    /// The principal's role, `None` when it had none (a denied stranger).
    pub role: Option<Role>,
    pub method: String,
    pub encoding: Encoding,
    /// `blake3:<hex>` of the params as received, or [`WITHHELD_DIGEST`]
    /// (see the module docs).
    pub params_digest: String,
    pub outcome: AuditOutcome,
}

/// What `params_digest` holds for a method with secret params.
pub const WITHHELD_DIGEST: &str = "withheld:secret-params";

/// The params digest for `blob`.
pub fn params_digest(blob: &Blob) -> String {
    format!("blake3:{}", blake3::hash(&blob.canonical_bytes()).to_hex())
}

pub fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Where audit records go.
pub trait AuditSink: Send + Sync {
    fn record(&self, record: &AuditRecord);
}

/// Discards everything (the default for a router nobody configured).
#[derive(Debug, Clone, Copy, Default)]
pub struct NullAuditSink;

impl AuditSink for NullAuditSink {
    fn record(&self, _record: &AuditRecord) {}
}

/// Keeps records in memory, for tests and embedders.
#[derive(Debug, Default)]
pub struct MemoryAuditSink {
    records: Mutex<Vec<AuditRecord>>,
}

impl MemoryAuditSink {
    pub fn new() -> MemoryAuditSink {
        MemoryAuditSink::default()
    }

    pub fn records(&self) -> Vec<AuditRecord> {
        self.records
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

impl AuditSink for MemoryAuditSink {
    fn record(&self, record: &AuditRecord) {
        self.records
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(record.clone());
    }
}

/// Appends JSON lines to a file (0600 on unix: created so, or tightened). Each record is one
/// `write` on an `O_APPEND` descriptor under a mutex, so lines from
/// concurrent calls never interleave, including across processes.
#[derive(Debug)]
pub struct FileAuditSink {
    file: Mutex<std::fs::File>,
}

impl FileAuditSink {
    pub fn open(path: &Path) -> std::io::Result<FileAuditSink> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let mut options = std::fs::OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path)?;
        // `mode` only applies when the file is created: a log that already
        // exists with looser permissions (an older build, a hand-made file)
        // is tightened rather than trusted.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if file.metadata()?.permissions().mode() & 0o077 != 0 {
                file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            }
        }
        Ok(FileAuditSink {
            file: Mutex::new(file),
        })
    }
}

impl AuditSink for FileAuditSink {
    fn record(&self, record: &AuditRecord) {
        let mut line = match serde_json::to_vec(record) {
            Ok(line) => line,
            Err(e) => {
                tracing::error!(error = %e, "audit record not serializable");
                return;
            }
        };
        line.push(b'\n');
        let mut file = self.file.lock().unwrap_or_else(|p| p.into_inner());
        if let Err(e) = file.write_all(&line) {
            tracing::error!(error = %e, method = %record.method, "audit log write failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(method: &str) -> AuditRecord {
        AuditRecord {
            ts_unix_ms: 1,
            principal: "unix:uid=1".into(),
            role: Some(Role::Admin),
            method: method.into(),
            encoding: Encoding::Json,
            params_digest: params_digest(&Blob::Json(serde_json::json!({"a": 1}))),
            outcome: AuditOutcome::Err(ErrorKind::Denied),
        }
    }

    #[test]
    fn file_sink_appends_one_json_line_per_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sub/audit.jsonl");
        {
            let sink = FileAuditSink::open(&path).unwrap();
            sink.record(&record("gc.run"));
            sink.record(&record("pin.add"));
        }
        // Re-opening appends rather than truncates.
        FileAuditSink::open(&path)
            .unwrap()
            .record(&record("quota.set"));
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<AuditRecord> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(
            lines.iter().map(|r| r.method.as_str()).collect::<Vec<_>>(),
            ["gc.run", "pin.add", "quota.set"]
        );
        assert!(text.contains("\"outcome\":{\"err\":\"denied\"}"), "{text}");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(
                mode & 0o077,
                0,
                "audit log must not be group/world accessible"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn an_existing_world_readable_log_is_tightened() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        std::fs::write(&path, b"").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        FileAuditSink::open(&path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn digest_is_stable_and_hides_params() {
        let a = params_digest(&Blob::Json(serde_json::json!({"x": 1, "y": "secret"})));
        let b = params_digest(&Blob::Json(
            serde_json::from_str(r#"{"y":"secret","x":1}"#).unwrap(),
        ));
        assert_eq!(a, b);
        assert!(a.starts_with("blake3:") && a.len() == 7 + 64);
        assert!(!a.contains("secret"));
        assert_ne!(
            a,
            params_digest(&Blob::Json(serde_json::json!({"x": 2, "y": "secret"})))
        );
    }
}
