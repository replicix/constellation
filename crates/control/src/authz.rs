//! Roles, principals and the policy that maps one to the other (plan 31
//! §9.5).
//!
//! The rule: **the transport establishes who is calling; the policy decides
//! what they may do; every method declares the least role it needs.** The
//! old API had none of the three (anyone who could open the socket or reach
//! `127.0.0.1` got everything), so the default here is strictly stronger:
//!
//! - the daemon's owner uid is [`Role::Admin`],
//! - the in-process caller (the CLI running the daemon itself, the harness,
//!   the web adapter's own dispatch) is [`Role::Admin`] — it already holds
//!   the engine,
//! - everyone else is denied, *including for unknown method names* (a
//!   stranger learns nothing about the method table),
//! - unless a `[[grant]]` in the allowlist gives their uid or group a role.
//!
//! ## The allowlist file
//!
//! ```toml
//! # Any user in group "constellation-ops" may pin, prune caches, …
//! [[grant]]
//! group = "constellation-ops"     # a group name, or a numeric gid
//! role = "operator"
//!
//! [[grant]]
//! uid = 1001
//! role = "viewer"
//!
//! # Plans 33 / 35 use these two subject kinds:
//! [[grant]]
//! device = "ed25519:AbC…"         # a remote device key
//! role = "viewer"
//! [[grant]]
//! sid = "S-1-5-21-…"              # a Windows SID
//! role = "admin"
//! ```
//!
//! Exactly one subject key per entry. Group *names* are resolved to gids
//! when the file is loaded (`getgrnam_r`); a name that does not exist is a
//! load error rather than a silently dead entry, because a typo in an
//! authorization file should be loud. A principal's role is the highest of
//! all entries that match it.
//!
//! ## Roles are cumulative
//!
//! `Viewer < Operator < Admin`; a method needing `Operator` accepts
//! `Admin`. The minimum roles are declared per method in [`crate::methods`]
//! (reads: viewer; node-local mutations: operator; destructive or
//! cluster-wide: admin).

use crate::methods::MethodInfo;
use crate::proto::{ControlError, ErrorKind};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::Path;

/// What a principal may do, least to most.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Read-only: status, listings, browsing.
    Viewer,
    /// Node-local changes: pins, cache prune, designations, snapshots.
    Operator,
    /// Destructive or cluster-wide: leave, gc, fsck repair, quotas, mounts,
    /// filesystem registry changes.
    Admin,
}

impl Role {
    pub const ALL: [Role; 3] = [Role::Viewer, Role::Operator, Role::Admin];

    pub fn as_str(self) -> &'static str {
        match self {
            Role::Viewer => "viewer",
            Role::Operator => "operator",
            Role::Admin => "admin",
        }
    }

    /// This role and every lower one (what `Welcome::roles` lists).
    pub fn implied(self) -> Vec<Role> {
        Role::ALL.into_iter().filter(|r| *r <= self).collect()
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Who is on the other end of a connection, as the transport established it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub enum Principal {
    /// A local process on a unix socket: peer credentials from the kernel
    /// (`SO_PEERCRED` / `getpeereid`). `gids` is the primary gid plus the
    /// user's supplementary groups; `pid` is `None` where the OS does not
    /// report it.
    Unix {
        uid: u32,
        gids: Vec<u32>,
        pid: Option<u32>,
    },
    /// The caller shares the daemon's address space.
    InProcess,
    /// A remote device authenticated by key (plan 33).
    Remote { device: String },
    /// A Windows named-pipe client (plan 35).
    WindowsSid(String),
}

impl fmt::Display for Principal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Principal::Unix { uid, gids, pid } => {
                write!(f, "unix:uid={uid}")?;
                if !gids.is_empty() {
                    write!(f, ",gids={gids:?}")?;
                }
                if let Some(pid) = pid {
                    write!(f, ",pid={pid}")?;
                }
                Ok(())
            }
            Principal::InProcess => f.write_str("in-process"),
            Principal::Remote { device } => write!(f, "remote:{device}"),
            Principal::WindowsSid(sid) => write!(f, "sid:{sid}"),
        }
    }
}

/// What a grant matches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Subject {
    Uid(u32),
    /// A gid; matches a unix principal whose `gids` contain it.
    Gid(u32),
    Device(String),
    Sid(String),
}

/// One allowlist entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    pub subject: Subject,
    pub role: Role,
}

/// Group-name resolution, injectable so tests do not depend on the host's
/// `/etc/group`.
pub trait GroupResolver {
    fn gid_of(&self, name: &str) -> Option<u32>;
}

/// The host's group database (`getgrnam_r`); resolves nothing off unix.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemGroups;

impl GroupResolver for SystemGroups {
    fn gid_of(&self, name: &str) -> Option<u32> {
        #[cfg(unix)]
        {
            sys::gid_of_group(name)
        }
        #[cfg(not(unix))]
        {
            let _ = name;
            None
        }
    }
}

/// A problem loading the allowlist.
#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("reading {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("parsing the grant list: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("grant #{index}: {reason}")]
    BadGrant { index: usize, reason: String },
    #[error("grant #{index}: no such group {name:?}")]
    UnknownGroup { index: usize, name: String },
}

#[derive(Deserialize)]
#[serde(untagged)]
enum GroupRef {
    Gid(u32),
    Name(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGrant {
    uid: Option<u32>,
    group: Option<GroupRef>,
    device: Option<String>,
    sid: Option<String>,
    role: Role,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPolicy {
    #[serde(default)]
    grant: Vec<RawGrant>,
}

/// Who gets which role. See the [module docs](self).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Policy {
    owner_uid: Option<u32>,
    grants: Vec<Grant>,
}

impl Policy {
    /// `owner_uid` is Admin, [`Principal::InProcess`] is Admin, nobody else
    /// has a role.
    pub fn owner_only(owner_uid: u32) -> Policy {
        Policy {
            owner_uid: Some(owner_uid),
            grants: Vec::new(),
        }
    }

    /// Only the in-process caller has a role (no unix owner), e.g. a test
    /// server or a Windows daemon before its SID grants are loaded.
    pub fn in_process_only() -> Policy {
        Policy::default()
    }

    /// The owner is whoever runs this process.
    #[cfg(unix)]
    pub fn current_user() -> Policy {
        // SAFETY: geteuid has no preconditions and cannot fail.
        Policy::owner_only(unsafe { libc::geteuid() })
    }

    pub fn with_grant(mut self, grant: Grant) -> Policy {
        self.grants.push(grant);
        self
    }

    pub fn grants(&self) -> &[Grant] {
        &self.grants
    }

    /// Parse the allowlist, resolving group names with `groups`.
    pub fn from_toml(
        text: &str,
        owner_uid: Option<u32>,
        groups: &dyn GroupResolver,
    ) -> Result<Policy, PolicyError> {
        let raw: RawPolicy = toml::from_str(text)?;
        let mut grants = Vec::new();
        for (index, g) in raw.grant.into_iter().enumerate() {
            let index = index + 1;
            let named = [
                g.uid.is_some(),
                g.group.is_some(),
                g.device.is_some(),
                g.sid.is_some(),
            ]
            .into_iter()
            .filter(|b| *b)
            .count();
            if named != 1 {
                return Err(PolicyError::BadGrant {
                    index,
                    reason: "give exactly one of uid, group, device, sid".into(),
                });
            }
            let subject = if let Some(uid) = g.uid {
                Subject::Uid(uid)
            } else if let Some(group) = g.group {
                match group {
                    GroupRef::Gid(gid) => Subject::Gid(gid),
                    GroupRef::Name(name) => match groups.gid_of(&name) {
                        Some(gid) => Subject::Gid(gid),
                        None => return Err(PolicyError::UnknownGroup { index, name }),
                    },
                }
            } else if let Some(device) = g.device {
                Subject::Device(device)
            } else {
                Subject::Sid(g.sid.unwrap_or_default())
            };
            grants.push(Grant {
                subject,
                role: g.role,
            });
        }
        Ok(Policy { owner_uid, grants })
    }

    /// Read the allowlist at `path`. A missing file is the default policy
    /// (owner only) — the allowlist is optional; a present-but-broken file
    /// is an error.
    pub fn load(path: &Path, owner_uid: Option<u32>) -> Result<Policy, PolicyError> {
        match std::fs::read_to_string(path) {
            Ok(text) => Policy::from_toml(&text, owner_uid, &SystemGroups),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Policy {
                owner_uid,
                grants: Vec::new(),
            }),
            Err(source) => Err(PolicyError::Read {
                path: path.display().to_string(),
                source,
            }),
        }
    }

    /// The highest role `principal` holds, or `None`.
    pub fn role_of(&self, principal: &Principal) -> Option<Role> {
        let mut best: Option<Role> = None;
        let mut grant = |role: Role| best = Some(best.map_or(role, |b| b.max(role)));
        match principal {
            Principal::InProcess => grant(Role::Admin),
            Principal::Unix { uid, gids, .. } => {
                if self.owner_uid == Some(*uid) {
                    grant(Role::Admin);
                }
                for g in &self.grants {
                    let hit = match &g.subject {
                        Subject::Uid(u) => u == uid,
                        Subject::Gid(gid) => gids.contains(gid),
                        _ => false,
                    };
                    if hit {
                        grant(g.role);
                    }
                }
            }
            Principal::Remote { device } => {
                for g in &self.grants {
                    if matches!(&g.subject, Subject::Device(d) if d == device) {
                        grant(g.role);
                    }
                }
            }
            Principal::WindowsSid(sid) => {
                for g in &self.grants {
                    if matches!(&g.subject, Subject::Sid(s) if s == sid) {
                        grant(g.role);
                    }
                }
            }
        }
        best
    }

    /// May `principal` call `method`? Returns the role that allowed it.
    /// Denials are `kind: Denied`, `code: EACCES`, and say which role was
    /// needed.
    pub fn authorize(
        &self,
        principal: &Principal,
        method: &MethodInfo,
    ) -> Result<Role, ControlError> {
        match self.role_of(principal) {
            Some(role) if role >= method.min_role => Ok(role),
            Some(role) => Err(ControlError::denied(format!(
                "{} requires the {} role; {principal} has {role}",
                method.name, method.min_role
            ))
            .with_details(serde_json::json!({
                "method": method.name,
                "required": method.min_role,
                "held": role,
            }))
            .with_remediation(
                "ask the daemon's owner to grant a higher role in the control allowlist",
            )),
            None => Err(no_role(principal)),
        }
    }
}

/// The denial for a principal with no role at all. Also used for unknown
/// method names, so a stranger cannot probe the method table.
pub fn no_role(principal: &Principal) -> ControlError {
    ControlError::new(
        ErrorKind::Denied,
        format!("{principal} may not use the control API"),
    )
    .with_code(constellation_types::Code::Access)
    .with_remediation(
        "ask the daemon's owner to add a [[grant]] for your user or group to the control allowlist",
    )
}

/// Raw libc lookups shared with the unix transport.
#[cfg(unix)]
pub(crate) mod sys {
    use std::ffi::{CStr, CString};

    /// `getgrnam_r`, growing the scratch buffer as the libc asks.
    pub fn gid_of_group(name: &str) -> Option<u32> {
        let cname = CString::new(name).ok()?;
        let mut size = 1024usize;
        loop {
            let mut buf = vec![0u8; size];
            // SAFETY: an all-zero `group` is a valid out-parameter; libc
            // fills it and points into `buf`, which outlives the call.
            let mut grp: libc::group = unsafe { std::mem::zeroed() };
            let mut result: *mut libc::group = std::ptr::null_mut();
            let rc = unsafe {
                libc::getgrnam_r(
                    cname.as_ptr(),
                    &mut grp,
                    buf.as_mut_ptr().cast(),
                    buf.len(),
                    &mut result,
                )
            };
            if rc == 0 {
                return if result.is_null() {
                    None
                } else {
                    Some(grp.gr_gid)
                };
            }
            // ERANGE (buffer too small) is the only failure a bigger buffer
            // fixes, but the errno tables here exist for some hosts only, so
            // retry on any error until the buffer is generous.
            if size < (1 << 20) {
                size *= 4;
                continue;
            }
            return None;
        }
    }

    /// The user name for `uid`, if the passwd database has one.
    fn user_name(uid: u32) -> Option<CString> {
        let mut size = 1024usize;
        loop {
            let mut buf = vec![0u8; size];
            // SAFETY: as for `gid_of_group`.
            let mut pwd: libc::passwd = unsafe { std::mem::zeroed() };
            let mut result: *mut libc::passwd = std::ptr::null_mut();
            let rc = unsafe {
                libc::getpwuid_r(
                    uid,
                    &mut pwd,
                    buf.as_mut_ptr().cast(),
                    buf.len(),
                    &mut result,
                )
            };
            if rc == 0 {
                if result.is_null() {
                    return None;
                }
                // SAFETY: pw_name points at a NUL-terminated string in `buf`.
                return Some(unsafe { CStr::from_ptr(pwd.pw_name) }.to_owned());
            }
            // ERANGE (buffer too small) is the only failure a bigger buffer
            // fixes, but the errno tables here exist for some hosts only, so
            // retry on any error until the buffer is generous.
            if size < (1 << 20) {
                size *= 4;
                continue;
            }
            return None;
        }
    }

    /// `primary_gid` plus the supplementary groups of `uid` from the group
    /// database (`getgrouplist`). The peer's *process* may have dropped
    /// some of them; the database answer is the standard approximation and
    /// errs toward the user's configured identity, which is what an admin
    /// writing `group = "ops"` means.
    pub fn groups_of_user(uid: u32, primary_gid: u32) -> Vec<u32> {
        let mut out = vec![primary_gid];
        let Some(name) = user_name(uid) else {
            return out;
        };
        let mut count: libc::c_int = 32;
        for _ in 0..4 {
            #[cfg(target_os = "macos")]
            let mut groups: Vec<libc::c_int> = vec![0; count as usize];
            #[cfg(not(target_os = "macos"))]
            let mut groups: Vec<libc::gid_t> = vec![0; count as usize];
            let mut n = count;
            // SAFETY: `groups` has room for `n` entries, `name` is a valid
            // C string; on -1 libc stores the needed size in `n`.
            let rc = unsafe {
                libc::getgrouplist(name.as_ptr(), primary_gid as _, groups.as_mut_ptr(), &mut n)
            };
            if rc >= 0 {
                // Never trust `n` beyond the buffer we handed over.
                let filled = (n.max(0) as usize).min(groups.len());
                for g in &groups[..filled] {
                    // `c_int` on macOS, already `u32` on Linux.
                    #[allow(clippy::unnecessary_cast)]
                    let g = *g as u32;
                    if !out.contains(&g) {
                        out.push(g);
                    }
                }
                return out;
            }
            count = n.max(count * 2);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::methods::{MethodInfo, StreamKind};

    fn info(name: &'static str, min_role: Role) -> MethodInfo {
        MethodInfo {
            name,
            min_role,
            mutating: false,
            streaming: StreamKind::None,
            secret_params: false,
        }
    }

    fn unix(uid: u32, gids: &[u32]) -> Principal {
        Principal::Unix {
            uid,
            gids: gids.to_vec(),
            pid: Some(1),
        }
    }

    struct Fixed;
    impl GroupResolver for Fixed {
        fn gid_of(&self, name: &str) -> Option<u32> {
            match name {
                "ops" => Some(5000),
                "wheel" => Some(10),
                _ => None,
            }
        }
    }

    #[test]
    fn owner_is_admin_stranger_is_denied() {
        let policy = Policy::owner_only(1000);
        let admin = info("gc.run", Role::Admin);
        assert_eq!(
            policy.authorize(&unix(1000, &[1000]), &admin),
            Ok(Role::Admin)
        );
        assert_eq!(
            policy.authorize(&Principal::InProcess, &admin),
            Ok(Role::Admin)
        );
        let err = policy.authorize(&unix(2000, &[2000]), &info("node.ping", Role::Viewer));
        let err = err.unwrap_err();
        assert_eq!(err.kind, ErrorKind::Denied);
        assert_eq!(err.code, Some(constellation_types::Code::Access));
        assert!(err.remediation.is_some());
        // Remote devices and SIDs have nothing without a grant.
        assert!(policy
            .authorize(
                &Principal::Remote { device: "d".into() },
                &info("x", Role::Viewer)
            )
            .is_err());
    }

    #[test]
    fn toml_grants_by_uid_and_group() {
        let text = r#"
            [[grant]]
            group = "ops"
            role = "operator"
            [[grant]]
            uid = 1001
            role = "viewer"
            [[grant]]
            group = 77
            role = "admin"
            [[grant]]
            device = "dev-1"
            role = "viewer"
            [[grant]]
            sid = "S-1-5-21-1"
            role = "admin"
        "#;
        let p = Policy::from_toml(text, Some(1000), &Fixed).unwrap();
        assert_eq!(p.role_of(&unix(1000, &[])), Some(Role::Admin));
        assert_eq!(p.role_of(&unix(1001, &[])), Some(Role::Viewer));
        assert_eq!(p.role_of(&unix(3000, &[5000])), Some(Role::Operator));
        assert_eq!(p.role_of(&unix(3000, &[9, 77])), Some(Role::Admin));
        // Highest of all matching grants wins.
        assert_eq!(p.role_of(&unix(1001, &[5000])), Some(Role::Operator));
        assert_eq!(p.role_of(&unix(4000, &[4000])), None);
        assert_eq!(
            p.role_of(&Principal::Remote {
                device: "dev-1".into()
            }),
            Some(Role::Viewer)
        );
        assert_eq!(
            p.role_of(&Principal::WindowsSid("S-1-5-21-1".into())),
            Some(Role::Admin)
        );
    }

    #[test]
    fn role_below_the_minimum_is_denied_with_details() {
        let text = "[[grant]]\nuid = 7\nrole = \"operator\"\n";
        let p = Policy::from_toml(text, None, &Fixed).unwrap();
        let who = unix(7, &[7]);
        assert_eq!(
            p.authorize(&who, &info("pin.add", Role::Operator)),
            Ok(Role::Operator)
        );
        assert_eq!(
            p.authorize(&who, &info("node.ping", Role::Viewer)),
            Ok(Role::Operator)
        );
        let err = p.authorize(&who, &info("gc.run", Role::Admin)).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Denied);
        let details = err.details.unwrap().0;
        assert_eq!(details["required"], "admin");
        assert_eq!(details["held"], "operator");
    }

    #[test]
    fn bad_allowlists_are_loud() {
        for (text, needle) in [
            ("[[grant]]\nrole = \"admin\"\n", "exactly one"),
            (
                "[[grant]]\nuid = 1\ngroup = \"ops\"\nrole = \"admin\"\n",
                "exactly one",
            ),
            (
                "[[grant]]\ngroup = \"nope\"\nrole = \"admin\"\n",
                "no such group",
            ),
            ("[[grant]]\nuid = 1\nrole = \"root\"\n", "root"),
            ("[[grant]]\nuid = 1\nrole = \"admin\"\nextra = 1\n", "extra"),
        ] {
            let err = Policy::from_toml(text, None, &Fixed)
                .unwrap_err()
                .to_string();
            assert!(err.contains(needle), "{err}");
        }
    }

    #[test]
    fn missing_file_is_the_default_policy() {
        let dir = tempfile::tempdir().unwrap();
        let p = Policy::load(&dir.path().join("absent.toml"), Some(5)).unwrap();
        assert_eq!(p.role_of(&unix(5, &[])), Some(Role::Admin));
        assert!(p.grants().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn system_groups_resolve_root_and_reject_nonsense() {
        // "root" exists on every unix this runs on, with gid 0.
        assert_eq!(SystemGroups.gid_of("root"), Some(0));
        assert_eq!(SystemGroups.gid_of("no-such-group-zzz"), None);
        assert!(sys::groups_of_user(0, 0).contains(&0));
    }

    #[test]
    fn role_ordering_and_implied() {
        assert!(Role::Viewer < Role::Operator && Role::Operator < Role::Admin);
        assert_eq!(Role::Operator.implied(), vec![Role::Viewer, Role::Operator]);
    }
}
