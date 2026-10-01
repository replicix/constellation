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
//! ## Service principals (plan 33 U1)
//!
//! A `kind = "service"` grant additionally narrows a `unix` principal to one
//! listening socket, for an automated caller (plan 37's CSI node plugin) that
//! shares a uid (often `0`) with unrelated processes on the host:
//!
//! ```toml
//! [[grant]]
//! kind = "service"
//! principal = "uid:0"
//! socket = "/var/lib/constellation-csi/sockets/pv-1/control.sock"
//! role = "operator"
//! label = "csi-node-plugin"
//! ```
//!
//! It matches only when the caller's peer uid equals the one in `principal`
//! **and** the daemon's own listening socket (set with
//! [`Policy::with_bound_socket`], never anything the client claims) has the
//! same canonical path as `socket`. Both sides are canonicalised where
//! possible — the grant's `socket` at load time, the daemon's own path at
//! bind time — so a symlinked state dir does not silently fail to match; a
//! `socket` that does not yet exist at load time (the pod may create it
//! later) is kept as given and logged at `debug`, not an error. `principal`,
//! `socket` and `label` are only valid alongside `kind = "service"`; a
//! `service` entry that also sets `uid`/`group`/`device`/`sid` is a load
//! error (exactly one subject key per entry, as above).
//!
//! A matched service grant is reported in [`Resolution::service`] whether or
//! not it is also the row that set the role — the audit log attributes the
//! call to the service either way (plan 33: *every* entry from a service
//! grant carries `principal.kind = "service"`), while
//! [`Resolution::matched`] names the row the role actually came from.
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
use std::path::{Path, PathBuf};

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
    /// `kind = "service"` (plan 33 U1): a unix principal scoped to one
    /// listening socket, not every socket that uid might ever open. `socket`
    /// is the canonical path if one could be resolved at load time (see the
    /// [module docs](self)), else the path as written in the allowlist.
    Service {
        uid: u32,
        socket: PathBuf,
        label: String,
    },
}

impl fmt::Display for Subject {
    /// The subject keys as the allowlist file spells them, so a log line
    /// naming a row can be grepped for in `control-allow.toml`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Subject::Uid(uid) => write!(f, "uid = {uid}"),
            Subject::Gid(gid) => write!(f, "group = {gid}"),
            Subject::Device(device) => write!(f, "device = {device:?}"),
            Subject::Sid(sid) => write!(f, "sid = {sid:?}"),
            Subject::Service { uid, socket, label } => write!(
                f,
                "kind = \"service\", principal = \"uid:{uid}\", socket = {:?}, label = {label:?}",
                socket.display().to_string()
            ),
        }
    }
}

/// One allowlist entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    pub subject: Subject,
    pub role: Role,
}

/// Which allowlist row — or which hardcoded rule — produced
/// [`Resolution::role`]. Plan 33 wants every mutating call to log "the row
/// that granted it", so `doctor`/`status` can answer "why am I only a
/// viewer" without re-deriving the match by hand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchedBy {
    /// The hardcoded "the uid that started the daemon is admin" rule, which
    /// is deliberately not expressible in the allowlist file.
    Owner,
    /// A caller in the daemon's own address space (the embedded web
    /// adapter), which is admin by construction.
    InProcess,
    /// The `[[grant]]` at `index` (1-based, the way [`PolicyError`] counts
    /// rows), with a copy of its subject for the log line.
    Grant { index: usize, subject: Subject },
}

impl fmt::Display for MatchedBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MatchedBy::Owner => f.write_str("the daemon's own uid (hardcoded, not a grant)"),
            MatchedBy::InProcess => f.write_str("an in-process caller (not a grant)"),
            MatchedBy::Grant { index, subject } => write!(f, "grant #{index} ({subject})"),
        }
    }
}

/// Where a candidate role came from while [`Policy::resolve`] maximises:
/// cheap to carry (no clone per matching row), expanded into a
/// [`MatchedBy`] once, for the winner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatchSource {
    Owner,
    InProcess,
    /// 1-based index into [`Policy::grants`].
    Grant(usize),
}

/// What matched to produce [`Policy::resolve`]'s role, when it was a
/// `kind = "service"` grant — enough for the audit log to attribute the call
/// to the service rather than its bare uid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceMatch {
    pub uid: u32,
    pub socket: PathBuf,
    pub label: String,
}

/// [`Policy::resolve`]'s answer: the role, the row that produced it, and —
/// tracked separately from the role maximisation — the `kind = "service"`
/// grant that matched this caller, if there was one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolution {
    pub role: Role,
    /// The highest-scoring row. On a tie (two rows granting the same role)
    /// the first in file order, with the owner rule ahead of every row;
    /// which row is reported is then arbitrary but both are equally true,
    /// and the resulting *role* is order-independent either way.
    pub matched: MatchedBy,
    /// Set whenever a `kind = "service"` grant matched this caller (peer
    /// uid **and** this daemon's own bound socket), even when a broader row
    /// granted a higher role. Plan 33 requires *every* audit entry produced
    /// through a service grant to be attributable to the service, and the
    /// role some group row happens to carry says nothing about who called;
    /// attribution is the allowlist author's statement that "this uid on
    /// this socket is that service". With several matching service grants,
    /// the highest-role one (then file order) wins.
    pub service: Option<ServiceMatch>,
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

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum GroupRef {
    Gid(u32),
    Name(String),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGrant {
    uid: Option<u32>,
    group: Option<GroupRef>,
    device: Option<String>,
    sid: Option<String>,
    /// Only `"service"` is recognised; absent for the other four subject
    /// kinds, which are inferred from which field above is set.
    #[serde(skip_serializing_if = "Option::is_none")]
    kind: Option<String>,
    /// `kind = "service"` only: `"uid:<n>"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    principal: Option<String>,
    /// `kind = "service"` only: the exact socket path.
    #[serde(skip_serializing_if = "Option::is_none")]
    socket: Option<String>,
    /// `kind = "service"` only: a human-readable tag for the audit log.
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<String>,
    role: Role,
}

#[derive(Serialize, Deserialize)]
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
    /// The daemon's own listening socket, canonicalised at bind time by
    /// whoever calls [`Policy::with_bound_socket`] — never anything a client
    /// claims. `kind = "service"` grants match against this, not the peer's
    /// `socket` argument (there is no such argument; it is purely config).
    bound_socket: Option<PathBuf>,
}

impl Policy {
    /// `owner_uid` is Admin, [`Principal::InProcess`] is Admin, nobody else
    /// has a role.
    pub fn owner_only(owner_uid: u32) -> Policy {
        Policy {
            owner_uid: Some(owner_uid),
            ..Policy::default()
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

    /// Record the daemon's own listening socket path, for matching
    /// `kind = "service"` grants. The caller canonicalises it (it just bound
    /// the socket, so canonicalisation cannot fail the way a config-file path
    /// can); `Policy` trusts it as given.
    pub fn with_bound_socket(mut self, path: PathBuf) -> Policy {
        self.bound_socket = Some(path);
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
            let is_service = match g.kind.as_deref() {
                None => false,
                Some("service") => true,
                Some(other) => {
                    return Err(PolicyError::BadGrant {
                        index,
                        reason: format!(
                            "unknown grant kind {other:?} (the only grant kind is \"service\")"
                        ),
                    })
                }
            };
            let named = [
                g.uid.is_some(),
                g.group.is_some(),
                g.device.is_some(),
                g.sid.is_some(),
                is_service,
            ]
            .into_iter()
            .filter(|b| *b)
            .count();
            if named != 1 {
                return Err(PolicyError::BadGrant {
                    index,
                    reason: "give exactly one of uid, group, device, sid, kind = \"service\""
                        .into(),
                });
            }
            if !is_service && (g.principal.is_some() || g.socket.is_some() || g.label.is_some()) {
                return Err(PolicyError::BadGrant {
                    index,
                    reason: "principal, socket and label are only valid with kind = \"service\""
                        .into(),
                });
            }
            let subject = if is_service {
                let principal = g
                    .principal
                    .as_deref()
                    .ok_or_else(|| PolicyError::BadGrant {
                        index,
                        reason: "a service grant needs principal = \"uid:<n>\"".into(),
                    })?;
                let uid = principal
                    .strip_prefix("uid:")
                    .and_then(|s| s.parse::<u32>().ok())
                    .ok_or_else(|| PolicyError::BadGrant {
                        index,
                        reason: format!(
                            "service grant principal {principal:?} is not of the form \"uid:<n>\""
                        ),
                    })?;
                let socket = g.socket.as_deref().ok_or_else(|| PolicyError::BadGrant {
                    index,
                    reason: "a service grant needs socket = \"<path>\"".into(),
                })?;
                let label = g.label.clone().ok_or_else(|| PolicyError::BadGrant {
                    index,
                    reason: "a service grant needs label = \"...\"".into(),
                })?;
                Subject::Service {
                    uid,
                    socket: canonicalize_grant_socket(socket),
                    label,
                }
            } else if let Some(uid) = g.uid {
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
        Ok(Policy {
            owner_uid,
            grants,
            bound_socket: None,
        })
    }

    /// Read the allowlist at `path`. A missing file is the default policy
    /// (owner only) — the allowlist is optional; a present-but-broken file
    /// is an error.
    pub fn load(path: &Path, owner_uid: Option<u32>) -> Result<Policy, PolicyError> {
        match std::fs::read_to_string(path) {
            Ok(text) => Policy::from_toml(&text, owner_uid, &SystemGroups),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Ok(Policy::owner_only_opt(owner_uid))
            }
            Err(source) => Err(PolicyError::Read {
                path: path.display().to_string(),
                source,
            }),
        }
    }

    fn owner_only_opt(owner_uid: Option<u32>) -> Policy {
        Policy {
            owner_uid,
            ..Policy::default()
        }
    }

    /// Serialize back to the TOML allowlist format (the parse → format →
    /// parse round trip test; also useful for a future `doctor` dump). The
    /// owner is daemon state, not a file field, so it does not round-trip
    /// through this — only `grants` does.
    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        let raw = RawPolicy {
            grant: self.grants.iter().map(Grant::to_raw).collect(),
        };
        toml::to_string(&raw)
    }

    /// The highest role `principal` holds, or `None`.
    pub fn role_of(&self, principal: &Principal) -> Option<Role> {
        self.resolve(principal).map(|r| r.role)
    }

    /// [`Policy::role_of`], plus the row that produced the role and any
    /// `kind = "service"` grant that matched (see [`Resolution`]). Used for
    /// the audit log's principal field and the "why am I only a viewer"
    /// debug line. The result does not depend on the order of the rows.
    pub fn resolve(&self, principal: &Principal) -> Option<Resolution> {
        let mut best: Option<(Role, MatchSource)> = None;
        let mut service: Option<(Role, ServiceMatch)> = None;
        let mut consider = |role: Role, from: MatchSource| {
            if best.is_none_or(|(b, _)| role > b) {
                best = Some((role, from));
            }
        };
        match principal {
            Principal::InProcess => consider(Role::Admin, MatchSource::InProcess),
            Principal::Unix { uid, gids, .. } => {
                if self.owner_uid == Some(*uid) {
                    consider(Role::Admin, MatchSource::Owner);
                }
                for (i, g) in self.grants.iter().enumerate() {
                    let from = MatchSource::Grant(i + 1);
                    match &g.subject {
                        Subject::Uid(u) if u == uid => consider(g.role, from),
                        Subject::Gid(gid) if gids.contains(gid) => consider(g.role, from),
                        Subject::Service {
                            uid: su,
                            socket,
                            label,
                        } if su == uid
                            && self.bound_socket.as_deref() == Some(socket.as_path()) =>
                        {
                            // Recorded whatever the role maximisation does
                            // with it: the caller *is* this service as far
                            // as the allowlist is concerned, so the audit
                            // trail says so even if a broader row (or the
                            // owner rule) outranks this one.
                            if service.as_ref().is_none_or(|(r, _)| g.role > *r) {
                                service = Some((
                                    g.role,
                                    ServiceMatch {
                                        uid: *su,
                                        socket: socket.clone(),
                                        label: label.clone(),
                                    },
                                ));
                            }
                            consider(g.role, from);
                        }
                        _ => {}
                    }
                }
            }
            Principal::Remote { device } => {
                for (i, g) in self.grants.iter().enumerate() {
                    if matches!(&g.subject, Subject::Device(d) if d == device) {
                        consider(g.role, MatchSource::Grant(i + 1));
                    }
                }
            }
            Principal::WindowsSid(sid) => {
                for (i, g) in self.grants.iter().enumerate() {
                    if matches!(&g.subject, Subject::Sid(s) if s == sid) {
                        consider(g.role, MatchSource::Grant(i + 1));
                    }
                }
            }
        }
        best.map(|(role, from)| Resolution {
            role,
            matched: self.matched_by(from),
            service: service.map(|(_, m)| m),
        })
    }

    fn matched_by(&self, from: MatchSource) -> MatchedBy {
        match from {
            MatchSource::Owner => MatchedBy::Owner,
            MatchSource::InProcess => MatchedBy::InProcess,
            MatchSource::Grant(index) => MatchedBy::Grant {
                index,
                subject: self.grants[index - 1].subject.clone(),
            },
        }
    }

    /// May `principal` call `method`? Returns the role that allowed it.
    /// Denials are `kind: Denied`, `code: EACCES`, and say which role was
    /// needed.
    pub fn authorize(
        &self,
        principal: &Principal,
        method: &MethodInfo,
    ) -> Result<Role, ControlError> {
        self.authorize_role(principal, method, self.role_of(principal))
    }

    /// [`Policy::authorize`] for a caller whose role this policy already
    /// resolved (the dispatch path resolves it once, for the audit ticket,
    /// and must not walk the grants again for the same answer).
    pub fn authorize_role(
        &self,
        principal: &Principal,
        method: &MethodInfo,
        role: Option<Role>,
    ) -> Result<Role, ControlError> {
        match role {
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

/// Canonicalise a `kind = "service"` grant's configured socket path at load
/// time. The pod that will own this socket may not exist yet (it is created
/// after the grant is written, not before), so a path that does not resolve
/// is kept as given — logged at `debug`, not an error — rather than failing
/// the whole allowlist over a socket nobody has created yet.
fn canonicalize_grant_socket(raw: &str) -> PathBuf {
    let path = Path::new(raw);
    match std::fs::canonicalize(path) {
        Ok(canonical) => canonical,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::debug!(
                socket = raw,
                "service grant socket does not exist yet at load time; keeping the path as written"
            );
            path.to_path_buf()
        }
        // Not the benign case: the path exists but cannot be resolved
        // (an unreadable parent directory, a symlink loop, a name too
        // long). The literal path is kept, but it will not match a bound
        // path that resolved differently, so say so loudly.
        Err(e) => {
            tracing::warn!(
                socket = raw,
                error = %e,
                "cannot canonicalise a service grant's socket; the grant will only match a daemon bound at exactly this path"
            );
            path.to_path_buf()
        }
    }
}

impl Grant {
    fn to_raw(&self) -> RawGrant {
        let mut raw = RawGrant {
            uid: None,
            group: None,
            device: None,
            sid: None,
            kind: None,
            principal: None,
            socket: None,
            label: None,
            role: self.role,
        };
        match &self.subject {
            Subject::Uid(uid) => raw.uid = Some(*uid),
            Subject::Gid(gid) => raw.group = Some(GroupRef::Gid(*gid)),
            Subject::Device(device) => raw.device = Some(device.clone()),
            Subject::Sid(sid) => raw.sid = Some(sid.clone()),
            Subject::Service { uid, socket, label } => {
                raw.kind = Some("service".to_string());
                raw.principal = Some(format!("uid:{uid}"));
                raw.socket = Some(socket.display().to_string());
                raw.label = Some(label.clone());
            }
        }
        raw
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

    fn service_toml(uid: u32, socket: &std::path::Path, role: &str, label: &str) -> String {
        format!(
            "[[grant]]\nkind = \"service\"\nprincipal = \"uid:{uid}\"\nsocket = {:?}\nrole = \"{role}\"\nlabel = \"{label}\"\n",
            socket.display().to_string(),
        )
    }

    #[test]
    fn service_grant_matches_only_the_right_uid_and_socket() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("control.sock");
        std::fs::write(&sock, b"").unwrap(); // just needs to exist, to canonicalize
        let other_sock = dir.path().join("other.sock");
        std::fs::write(&other_sock, b"").unwrap();

        let text = service_toml(500, &sock, "operator", "csi-node-plugin");
        let p = Policy::from_toml(&text, None, &Fixed)
            .unwrap()
            .with_bound_socket(std::fs::canonicalize(&sock).unwrap());

        // Right uid, right socket: matches.
        let resolved = p.resolve(&unix(500, &[])).unwrap();
        assert_eq!(resolved.role, Role::Operator);
        assert!(
            matches!(&resolved.matched, MatchedBy::Grant { index: 1, subject }
                if matches!(subject, Subject::Service { label, .. } if label == "csi-node-plugin")),
            "{:?}",
            resolved.matched
        );
        let svc = resolved.service.unwrap();
        assert_eq!(svc.uid, 500);
        assert_eq!(svc.label, "csi-node-plugin");
        assert_eq!(svc.socket, std::fs::canonicalize(&sock).unwrap());

        // Right uid, wrong socket (a different daemon's bound path): no match.
        let p_other = Policy::from_toml(&text, None, &Fixed)
            .unwrap()
            .with_bound_socket(std::fs::canonicalize(&other_sock).unwrap());
        assert_eq!(p_other.role_of(&unix(500, &[])), None);

        // Wrong uid, right socket: no match.
        assert_eq!(p.role_of(&unix(501, &[])), None);
    }

    #[test]
    fn missing_socket_at_load_time_is_kept_literal_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("not-created-yet.sock");
        let text = service_toml(0, &sock, "operator", "csi-node-plugin");
        let p = Policy::from_toml(&text, None, &Fixed).unwrap();
        match &p.grants()[0].subject {
            Subject::Service { socket, .. } => assert_eq!(socket, &sock),
            other => panic!("{other:?}"),
        }
        // The pod creates the socket later; the daemon's own bound path
        // happens to equal the literal (uncanonicalized) path here too, so
        // matching still works once it does exist.
        let p = p.with_bound_socket(sock.clone());
        assert_eq!(p.role_of(&unix(0, &[])), Some(Role::Operator));
    }

    #[test]
    fn highest_role_wins_between_a_group_grant_and_a_service_grant() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("control.sock");
        std::fs::write(&sock, b"").unwrap();
        let canonical = std::fs::canonicalize(&sock).unwrap();

        // Service grant outranks the group grant: the winning role is
        // attributed to the service.
        let text = format!(
            "[[grant]]\ngroup = \"ops\"\nrole = \"viewer\"\n{}",
            service_toml(500, &sock, "admin", "csi-node-plugin")
        );
        let p = Policy::from_toml(&text, None, &Fixed)
            .unwrap()
            .with_bound_socket(canonical.clone());
        let resolved = p.resolve(&unix(500, &[5000])).unwrap();
        assert_eq!(resolved.role, Role::Admin);
        assert!(resolved.service.is_some());

        // Group grant outranks the service grant: the role comes from the
        // group row (and the debug line says so), but the call is still
        // attributed to the service — plan 33 wants *every* entry a service
        // grant matched to carry the service principal, and the role a
        // human's group row happens to hold says nothing about who called.
        let text = format!(
            "[[grant]]\ngroup = \"ops\"\nrole = \"admin\"\n{}",
            service_toml(500, &sock, "viewer", "csi-node-plugin")
        );
        let p = Policy::from_toml(&text, None, &Fixed)
            .unwrap()
            .with_bound_socket(canonical);
        let resolved = p.resolve(&unix(500, &[5000])).unwrap();
        assert_eq!(resolved.role, Role::Admin);
        assert_eq!(
            resolved.matched,
            MatchedBy::Grant {
                index: 1,
                subject: Subject::Gid(5000)
            }
        );
        assert_eq!(
            resolved.service.as_ref().map(|s| s.label.as_str()),
            Some("csi-node-plugin")
        );
    }

    #[test]
    fn service_attribution_survives_a_tie_the_owner_rule_and_file_order() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("control.sock");
        std::fs::write(&sock, b"").unwrap();
        let canonical = std::fs::canonicalize(&sock).unwrap();
        let svc = service_toml(7, &sock, "operator", "csi-node-plugin");
        let uid_row = "[[grant]]\nuid = 7\nrole = \"operator\"\n";

        // A same-role uid row, in either order: the role is the same and
        // the service is attributed either way (the brief: file order is
        // irrelevant to the result).
        for text in [format!("{uid_row}{svc}"), format!("{svc}{uid_row}")] {
            let p = Policy::from_toml(&text, None, &Fixed)
                .unwrap()
                .with_bound_socket(canonical.clone());
            let resolved = p.resolve(&unix(7, &[])).unwrap();
            assert_eq!(resolved.role, Role::Operator);
            assert_eq!(
                resolved.service.as_ref().map(|s| s.label.as_str()),
                Some("csi-node-plugin"),
                "{text}"
            );
        }

        // The owner rule (admin, hardcoded) outranks the service grant and
        // is what the debug line names, but a mutation that came in through
        // the service grant is still attributable to it: plan 37's own
        // example is `principal = "uid:0"`.
        let p = Policy::from_toml(&svc, Some(7), &Fixed)
            .unwrap()
            .with_bound_socket(canonical);
        let resolved = p.resolve(&unix(7, &[])).unwrap();
        assert_eq!(resolved.role, Role::Admin);
        assert_eq!(resolved.matched, MatchedBy::Owner);
        assert_eq!(
            resolved.service.as_ref().map(|s| s.label.as_str()),
            Some("csi-node-plugin")
        );
    }

    #[test]
    fn the_highest_role_service_grant_wins_the_attribution() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("control.sock");
        std::fs::write(&sock, b"").unwrap();
        let canonical = std::fs::canonicalize(&sock).unwrap();
        let text = format!(
            "{}{}",
            service_toml(7, &sock, "viewer", "low"),
            service_toml(7, &sock, "admin", "high")
        );
        let p = Policy::from_toml(&text, None, &Fixed)
            .unwrap()
            .with_bound_socket(canonical);
        let resolved = p.resolve(&unix(7, &[])).unwrap();
        assert_eq!(resolved.role, Role::Admin);
        assert_eq!(resolved.service.unwrap().label, "high");
    }

    #[test]
    fn the_matched_row_is_named_for_every_kind_of_match() {
        let p = Policy::from_toml(
            "[[grant]]\ngroup = \"ops\"\nrole = \"viewer\"\n[[grant]]\ndevice = \"dev-1\"\nrole = \"operator\"\n",
            Some(1000),
            &Fixed,
        )
        .unwrap();
        assert_eq!(
            p.resolve(&unix(1000, &[])).unwrap().matched.to_string(),
            "the daemon's own uid (hardcoded, not a grant)"
        );
        assert_eq!(
            p.resolve(&unix(1, &[5000])).unwrap().matched.to_string(),
            "grant #1 (group = 5000)"
        );
        assert_eq!(
            p.resolve(&Principal::Remote {
                device: "dev-1".into()
            })
            .unwrap()
            .matched
            .to_string(),
            "grant #2 (device = \"dev-1\")"
        );
        assert_eq!(
            p.resolve(&Principal::InProcess)
                .unwrap()
                .matched
                .to_string(),
            "an in-process caller (not a grant)"
        );
        assert_eq!(p.resolve(&unix(1, &[])), None);
    }

    #[test]
    fn a_symlinked_socket_path_still_matches_the_daemons_bound_path() {
        // The asymmetry the two canonicalisations exist for: the allowlist
        // names the socket through a symlinked parent (a hostPath dir), the
        // daemon binds — and canonicalises — the real one.
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let sock = real.join("control.sock");
        std::fs::write(&sock, b"").unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let text = service_toml(7, &link.join("control.sock"), "operator", "csi-node-plugin");
        let p = Policy::from_toml(&text, None, &Fixed)
            .unwrap()
            .with_bound_socket(std::fs::canonicalize(&sock).unwrap());
        assert_eq!(p.role_of(&unix(7, &[])), Some(Role::Operator));
    }

    #[test]
    fn service_grant_rejects_a_second_subject_key_and_stray_fields() {
        for (text, needle) in [
            (
                "[[grant]]\nkind = \"service\"\nuid = 1\nprincipal = \"uid:1\"\nsocket = \"/s\"\nrole = \"operator\"\nlabel = \"x\"\n",
                "exactly one",
            ),
            (
                "[[grant]]\nkind = \"service\"\nsocket = \"/s\"\nrole = \"operator\"\nlabel = \"x\"\n",
                "principal",
            ),
            (
                "[[grant]]\nkind = \"service\"\nprincipal = \"uid:1\"\nrole = \"operator\"\nlabel = \"x\"\n",
                "socket",
            ),
            (
                "[[grant]]\nkind = \"service\"\nprincipal = \"uid:1\"\nsocket = \"/s\"\nrole = \"operator\"\n",
                "label",
            ),
            (
                "[[grant]]\nkind = \"service\"\nprincipal = \"not-a-uid\"\nsocket = \"/s\"\nrole = \"operator\"\nlabel = \"x\"\n",
                "uid:<n>",
            ),
            (
                "[[grant]]\nkind = \"vendor\"\nuid = 1\nrole = \"operator\"\n",
                "unknown grant kind",
            ),
            (
                "[[grant]]\nuid = 1\nsocket = \"/s\"\nrole = \"operator\"\n",
                "only valid with kind",
            ),
        ] {
            let err = Policy::from_toml(text, None, &Fixed)
                .unwrap_err()
                .to_string();
            assert!(err.contains(needle), "{needle:?} not in {err}");
        }
    }

    #[test]
    fn allowlist_parse_format_parse_round_trip_including_a_service_row() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("control.sock");
        std::fs::write(&sock, b"").unwrap();
        let text = format!(
            "[[grant]]\ngroup = \"ops\"\nrole = \"operator\"\n[[grant]]\nuid = 1001\nrole = \"viewer\"\n[[grant]]\ndevice = \"dev-1\"\nrole = \"viewer\"\n[[grant]]\nsid = \"S-1-5-21-1\"\nrole = \"admin\"\n{}",
            service_toml(500, &sock, "operator", "csi-node-plugin")
        );
        let p1 = Policy::from_toml(&text, Some(1000), &Fixed).unwrap();
        let formatted = p1.to_toml().unwrap();
        let p2 = Policy::from_toml(&formatted, Some(1000), &Fixed).unwrap();
        assert_eq!(p1.grants(), p2.grants(), "formatted:\n{formatted}");
    }

    /// One allowlist key, with a valid value first and then the near-misses
    /// the fuzzer should also reach.
    const KEYS: &[(&str, &[&str])] = &[
        (
            "kind",
            &["\"service\"", "\"vendor\"", "1", "\"\"", "\"Service\""],
        ),
        (
            "principal",
            &[
                "\"uid:0\"",
                "\"uid:4294967296\"",
                "\"uid:\"",
                "\"uid:-1\"",
                "\"not-a-uid\"",
                "\"\"",
                "[]",
                "0",
            ],
        ),
        (
            "socket",
            &[
                "\"/tmp/constellation-fuzz/control.sock\"",
                "\"\"",
                "\"/\"",
                "\"relative/control.sock\"",
                "\"/proc/self/root/x\"",
                "7",
            ],
        ),
        ("label", &["\"csi-node-plugin\"", "\"\"", "0", "[]"]),
        ("uid", &["1000", "0", "-1", "4294967296", "\"1000\""]),
        ("group", &["\"ops\"", "5000", "\"nonesuch\"", "-1", "[]"]),
        ("device", &["\"dev-1\"", "\"\"", "1"]),
        ("sid", &["\"S-1-5-21-1\"", "\"\"", "3"]),
        (
            "role",
            &[
                "\"operator\"",
                "\"viewer\"",
                "\"admin\"",
                "\"nonesuch\"",
                "\"\"",
                "1",
            ],
        ),
    ];

    /// The key sets of a well-formed row of each kind, plus `&[]` for "pick
    /// keys at random".
    const SHAPES: &[&[&str]] = &[
        &["kind", "principal", "socket", "label", "role"],
        &["uid", "role"],
        &["group", "role"],
        &["device", "role"],
        &["sid", "role"],
        &[],
    ];

    #[test]
    fn fuzz_allowlist_parse_never_panics() {
        // Deterministic pseudo-random allowlists grown from the *structure*
        // of a real one — whole rows of each kind, with a key dropped, a
        // value swapped for a near-miss, or raw byte soup spliced in — so
        // that the grant-level validation this chunk adds (the subject-key
        // count, `kind`, the `"uid:<n>"` parser, `canonicalize_grant_socket`)
        // is actually reached, not only the TOML tokeniser. Parsing must
        // always terminate and never panic, and so must resolving against
        // whatever policy came out.
        let mut state: u64 = 0x9e3779b97f4a7c15;
        let mut next = move || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            (state >> 33) as usize
        };
        let soup = b"[]./=\"'kind-grolabcnpstyud:0123456789 \n\t,";
        let mut with_grants = 0usize;
        let mut with_service = 0usize;
        for _ in 0..20_000 {
            let mut text = String::new();
            for _ in 0..1 + next() % 3 {
                text.push_str("[[grant]]\n");
                let shape = SHAPES[next() % SHAPES.len()];
                for &(key, values) in KEYS {
                    let wanted = if shape.is_empty() {
                        next() % 3 == 0
                    } else {
                        shape.contains(&key) && next() % 16 != 0
                    };
                    if !wanted {
                        continue;
                    }
                    // Mostly the valid value, sometimes a near-miss.
                    let value = if next() % 4 == 0 {
                        values[next() % values.len()]
                    } else {
                        values[0]
                    };
                    text.push_str(key);
                    text.push_str(" = ");
                    text.push_str(value);
                    text.push('\n');
                }
                if next() % 8 == 0 {
                    for _ in 0..next() % 24 {
                        text.push(soup[next() % soup.len()] as char);
                    }
                    text.push('\n');
                }
            }
            if let Ok(p) = Policy::from_toml(&text, Some(1000), &Fixed) {
                if !p.grants().is_empty() {
                    with_grants += 1;
                }
                if p.grants()
                    .iter()
                    .any(|g| matches!(g.subject, Subject::Service { .. }))
                {
                    with_service += 1;
                }
                // Resolving against a fuzzed policy must not panic either,
                // bound socket or not.
                let bound = p.clone().with_bound_socket("/tmp/x".into());
                for p in [&p, &bound] {
                    let _ = p.resolve(&unix(0, &[0, 5000]));
                    let _ = p.resolve(&Principal::Remote {
                        device: "dev-1".into(),
                    });
                    let _ = p.to_toml();
                }
            }
        }
        // A generator that only ever produced unparseable TOML would meet
        // "never panics" trivially without touching any of this chunk's
        // code; keep it honest.
        assert!(
            with_grants > 2_000 && with_service > 500,
            "the fuzzer barely reaches grant validation: {with_grants} policies with grants, {with_service} with a service grant"
        );
    }

    /// The seeded fuzzer must actually reach the code this chunk adds — a
    /// generator that only ever produces unparseable TOML would "never
    /// panic" trivially. Asserted on the same fragments, so it fails if a
    /// later edit makes them stop composing into valid rows.
    #[test]
    fn the_fuzz_fragments_do_compose_into_parseable_service_grants() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("control.sock");
        std::fs::write(&sock, b"").unwrap();
        let service = service_toml(0, &sock, "operator", "csi-node-plugin");
        let p = Policy::from_toml(
            &format!("{service}[[grant]]\nuid = 0\nrole = \"admin\"\n"),
            Some(1000),
            &Fixed,
        )
        .unwrap();
        assert_eq!(p.grants().len(), 2);
        assert!(matches!(p.grants()[0].subject, Subject::Service { .. }));
        // And the error paths the fuzzer mixes in are errors, not panics.
        for bad in [
            "[[grant]]\nkind = \"service\"\nprincipal = \"uid:4294967296\"\nsocket = \"/s\"\nlabel = \"x\"\nrole = \"admin\"\n",
            "[[grant]]\nkind = \"vendor\"\nuid = 0\nrole = \"admin\"\n",
            "[[grant]]\nuid = -1\nrole = \"admin\"\n",
            "[[grant]]\nrole = \"nonesuch\"\n",
        ] {
            assert!(Policy::from_toml(bad, Some(1000), &Fixed).is_err(), "{bad}");
        }
    }
}
