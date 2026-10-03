//! `StorageClass` parameters (plan 37 §6): parsed and validated once per
//! `CreateVolume`, before anything reaches an engine pod, so a typo in a
//! class is an `INVALID_ARGUMENT` naming the key rather than a filesystem
//! created with defaults the operator did not ask for.
//!
//! Every class defines exactly one pool (optionally sharded) or one
//! dedicated-layout configuration (§2.3). The CSI sidecars inject their own
//! `csi.storage.k8s.io/*` keys (secret references, and the PVC/PV names
//! under `--extra-create-metadata`); those are never class configuration
//! and are skipped here. Any *other* unknown key is refused.

use constellation_control::proto::types::FsCreateParams;
use std::collections::HashMap;

/// Upper bound on `shards`. Plan 37 "K0 results" Track B found no PV-count
/// ceiling to shard against and that sharding buys only throughput — about
/// 1.2-1.4k successful `CreateVolume` sequences/s per pool filesystem —
/// while every shard costs one idle metadata tailer per node with a mounted
/// PV of it (§2.3). 64 shards is ~80k provisions/s, beyond any cluster's
/// provisioning rate; a larger value is a typo, not a plan.
pub const MAX_SHARDS: u32 = 64;

/// The bucket prefix a class gets without an explicit `prefix`. §6 wants
/// `constellation-csi/<storageclass-name>`, but `CreateVolumeRequest` does
/// not carry the class name (external-provisioner's
/// `--extra-create-metadata` adds the PVC and PV names only), so the
/// default is one shared prefix: two classes sharing a bucket must set
/// distinct prefixes, or they share (or, with different filesystem
/// parameters, conflict on) one pool.
pub const DEFAULT_PREFIX: &str = "constellation-csi";

/// Sidecar-injected keys (`--extra-create-metadata`) the controller reads
/// for the volume record's `pvc`/`namespace` xattrs.
pub const PVC_NAME_KEY: &str = "csi.storage.k8s.io/pvc/name";
pub const PVC_NAMESPACE_KEY: &str = "csi.storage.k8s.io/pvc/namespace";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    Pool,
    Dedicated,
}

/// Where a class's engine pods get their S3 credentials (plan 37 §9,
/// StorageClass parameter `credentialSource`). Whatever the kind, nothing
/// reaches an engine pod through its spec, environment, image or a
/// hostPath file: bytes travel only as `fs.unlock` over its control socket
/// and live in its `EphemeralSecretStore`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CredentialMode {
    /// `static-ephemeral` (the default): the `csi.storage.k8s.io/*-secret-*`
    /// Secret the sidecars and kubelet resolve per request. The engine pod
    /// waits for its first `fs.unlock` (`serve --await-unlock`); a changed
    /// Secret reaches a running pod with the next request that carries it
    /// (a `CreateVolume`, a `NodeStageVolume`).
    StaticEphemeral,
    /// `refreshing`: as `static-ephemeral`, plus the plugins watch the
    /// Secret named by `credentialSecretName`/`credentialSecretNamespace`
    /// and push every change to the running engine pods at once
    /// (`Refreshing(callback)`: rotation without a remount).
    Refreshing,
    /// `aws-default-chain`: no Secret at all; the engine pod's own
    /// ServiceAccount (IRSA, EKS Pod Identity) and the AWS SDK's chain. An
    /// E2E pool still gets its passphrase through `fs.unlock`.
    AwsDefaultChain,
}

impl CredentialMode {
    pub fn as_str(self) -> &'static str {
        match self {
            CredentialMode::StaticEphemeral => "static-ephemeral",
            CredentialMode::Refreshing => "refreshing",
            CredentialMode::AwsDefaultChain => "aws-default-chain",
        }
    }
}

/// A Secret to watch (`credentialSource: refreshing`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SecretRef {
    pub namespace: String,
    pub name: String,
}

impl std::fmt::Display for SecretRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}

/// The class parameters naming the Secret a `refreshing` class watches.
pub const CREDENTIAL_SECRET_NAME: &str = "credentialSecretName";
pub const CREDENTIAL_SECRET_NAMESPACE: &str = "credentialSecretNamespace";

/// One `StorageClass`'s parameters, validated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassParams {
    pub bucket: String,
    /// Normalized: no leading/trailing `/`.
    pub prefix: String,
    pub layout: Layout,
    /// `1` = unsharded; always `1` for `Layout::Dedicated`.
    pub shards: u32,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub chunk_size: Option<u32>,
    pub compression: Option<String>,
    pub e2e: bool,
    pub write_mode: Option<String>,
    pub credentials: CredentialMode,
    /// The Secret a `refreshing` class watches (`None` for the others).
    pub credential_secret: Option<SecretRef>,
}

impl ClassParams {
    pub fn parse(params: &HashMap<String, String>) -> Result<ClassParams, String> {
        let mut unknown: Vec<&str> = params
            .keys()
            .map(String::as_str)
            .filter(|k| !k.starts_with("csi.storage.k8s.io/"))
            .filter(|k| {
                !matches!(
                    *k,
                    "bucket"
                        | "prefix"
                        | "endpoint"
                        | "region"
                        | "layout"
                        | "shards"
                        | "chunkSize"
                        | "compression"
                        | "e2e"
                        | "writeMode"
                        | "engineProfile"
                        | "credentialSource"
                        | "credentialSecretName"
                        | "credentialSecretNamespace"
                )
            })
            .collect();
        if !unknown.is_empty() {
            unknown.sort_unstable();
            return Err(format!(
                "unknown StorageClass parameter(s): {}",
                unknown.join(", ")
            ));
        }
        let get = |k: &str| params.get(k).map(|v| v.trim()).filter(|v| !v.is_empty());

        let bucket = get("bucket")
            .ok_or("StorageClass parameter `bucket` is required")?
            .to_string();
        if bucket.contains('/') {
            return Err(format!("bucket {bucket:?} must not contain '/'"));
        }
        let prefix = match params.get("prefix") {
            None => DEFAULT_PREFIX.to_string(),
            Some(p) => p.trim().trim_matches('/').to_string(),
        };
        if prefix.split('/').any(|c| c == "." || c == "..") || prefix.contains("//") {
            return Err(format!(
                "prefix {prefix:?} has an empty, '.' or '..' component"
            ));
        }
        let layout = match get("layout") {
            None | Some("pool") => Layout::Pool,
            Some("dedicated") => Layout::Dedicated,
            Some(other) => {
                return Err(format!(
                    "layout {other:?} is not one of \"pool\", \"dedicated\""
                ))
            }
        };
        let shards = match get("shards") {
            None => 1,
            Some(s) => s
                .parse::<u32>()
                .ok()
                .filter(|n| (1..=MAX_SHARDS).contains(n))
                .ok_or_else(|| format!("shards {s:?} must be an integer in 1..={MAX_SHARDS}"))?,
        };
        if layout == Layout::Dedicated && shards != 1 {
            return Err("shards applies to layout \"pool\" only".into());
        }
        let chunk_size = get("chunkSize").map(parse_chunk_size).transpose()?;
        let e2e = match get("e2e") {
            None | Some("false") => false,
            Some("true") => true,
            Some(other) => return Err(format!("e2e {other:?} must be \"true\" or \"false\"")),
        };
        // §6: "server" is the only supported profile through K6.
        if let Some(profile) = get("engineProfile") {
            if profile != "server" {
                return Err(format!(
                    "engineProfile {profile:?} is not supported (only \"server\")"
                ));
            }
        }
        let credentials = match get("credentialSource") {
            None | Some("static-ephemeral") => CredentialMode::StaticEphemeral,
            Some("refreshing") => CredentialMode::Refreshing,
            Some("aws-default-chain") => CredentialMode::AwsDefaultChain,
            Some(other) => {
                return Err(format!(
                    "credentialSource {other:?} is not one of \"static-ephemeral\", \
                     \"refreshing\", \"aws-default-chain\""
                ))
            }
        };
        let credential_secret = match (
            get(CREDENTIAL_SECRET_NAME),
            get(CREDENTIAL_SECRET_NAMESPACE),
            credentials,
        ) {
            (Some(name), Some(namespace), CredentialMode::Refreshing) => {
                for (key, v) in [
                    (CREDENTIAL_SECRET_NAME, name),
                    (CREDENTIAL_SECRET_NAMESPACE, namespace),
                ] {
                    if !is_dns_subdomain(v) {
                        return Err(format!("{key} {v:?} is not a Kubernetes object name"));
                    }
                }
                Some(SecretRef {
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                })
            }
            (_, _, CredentialMode::Refreshing) => {
                return Err(format!(
                    "credentialSource \"refreshing\" needs {CREDENTIAL_SECRET_NAME} and \
                     {CREDENTIAL_SECRET_NAMESPACE}: the Secret to watch"
                ))
            }
            (None, None, _) => None,
            _ => {
                return Err(format!(
                    "{CREDENTIAL_SECRET_NAME}/{CREDENTIAL_SECRET_NAMESPACE} apply to \
                     credentialSource \"refreshing\" only"
                ))
            }
        };
        Ok(ClassParams {
            bucket,
            prefix,
            layout,
            shards,
            endpoint: get("endpoint").map(str::to_string),
            region: get("region").map(str::to_string),
            chunk_size,
            compression: get("compression").map(str::to_string),
            e2e,
            write_mode: get("writeMode").map(str::to_string),
            credentials,
            credential_secret,
        })
    }

    /// Whether this class's engine pods start with `--await-unlock`: they
    /// need something only `fs.unlock` brings — S3 keys, or an E2E
    /// passphrase even on the AWS chain.
    pub fn awaits_unlock(&self) -> bool {
        self.credentials != CredentialMode::AwsDefaultChain || self.e2e
    }

    /// The shard `name` lives on: FNV-1a over the name, mod `shards`.
    /// Deterministic across processes, versions and platforms (unlike
    /// `std`'s `DefaultHasher`) — a controller replica that restarts or
    /// fails over must route a retried `CreateVolume` to the same shard.
    pub fn shard_for(&self, name: &str) -> u32 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for b in name.bytes() {
            hash ^= u64::from(b);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        (hash % u64::from(self.shards)) as u32
    }

    /// The bucket prefix of shard `shard`'s pool filesystem: the class
    /// prefix itself when unsharded, `<prefix>/shard-<k>` otherwise (§2.3).
    pub fn pool_prefix(&self, shard: u32) -> String {
        if self.shards == 1 {
            self.prefix.clone()
        } else {
            join(&self.prefix, &format!("shard-{shard}"))
        }
    }

    /// The bucket prefix of dedicated volume `name`'s own filesystem.
    pub fn dedicated_prefix(&self, name: &str) -> String {
        join(&self.prefix, name)
    }

    /// `fs.create` for the filesystem at `prefix`, carrying the class's
    /// filesystem-creation defaults. `fs.create` is idempotent on
    /// `(bucket, prefix)` with matching parameters.
    pub fn fs_create(&self, prefix: String) -> FsCreateParams {
        FsCreateParams {
            name: None,
            bucket: self.bucket.clone(),
            prefix,
            endpoint: self.endpoint.clone(),
            region: self.region.clone(),
            chunk_size: self.chunk_size,
            compression: self.compression.clone(),
            e2e: self.e2e,
            write_mode: self.write_mode.clone(),
        }
    }
}

/// The `volume_context` a provisioned volume carries (plan 37 K3): its
/// class's own parameters, so `NodeStageVolume` brings up the engine pod
/// of the right pool filesystem from the request alone (the id names the
/// filesystem by uuid, not where it lives). The sidecars' own
/// `csi.storage.k8s.io/*` keys are dropped: they name secrets and the PVC,
/// and a volume's context must not carry either. A statically provisioned
/// PV spells the same keys in its `volumeAttributes` (and `shard`, for a
/// sharded pool).
pub fn volume_context(parameters: &HashMap<String, String>) -> HashMap<String, String> {
    parameters
        .iter()
        .filter(|(k, _)| !k.starts_with("csi.storage.k8s.io/"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// A statically provisioned volume's shard (`volumeAttributes.shard`);
/// the one context key that is not a class parameter.
pub const SHARD_KEY: &str = "shard";

/// A Kubernetes object name (RFC 1123 subdomain).
fn is_dns_subdomain(s: &str) -> bool {
    s.len() <= 253
        && s.split('.').all(|l| {
            !l.is_empty()
                && l.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                && !l.starts_with('-')
                && !l.ends_with('-')
        })
}

fn join(prefix: &str, leaf: &str) -> String {
    if prefix.is_empty() {
        leaf.to_string()
    } else {
        format!("{prefix}/{leaf}")
    }
}

/// `4MiB`, `512KiB`, `1048576`: a positive byte count that fits `u32`.
fn parse_chunk_size(s: &str) -> Result<u32, String> {
    let bad = || format!("chunkSize {s:?} must be a byte count like \"4MiB\"");
    let digits = s.bytes().take_while(u8::is_ascii_digit).count();
    let (num, unit) = s.split_at(digits);
    let num: u64 = num.parse().map_err(|_| bad())?;
    let mult: u64 = match unit.trim() {
        "" | "B" => 1,
        "K" | "KiB" => 1 << 10,
        "M" | "MiB" => 1 << 20,
        "G" | "GiB" => 1 << 30,
        _ => return Err(bad()),
    };
    num.checked_mul(mult)
        .filter(|n| *n > 0)
        .and_then(|n| u32::try_from(n).ok())
        .ok_or_else(bad)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(kv: &[(&str, &str)]) -> HashMap<String, String> {
        kv.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn minimal_pool_class() {
        let p = ClassParams::parse(&params(&[("bucket", "b")])).unwrap();
        assert_eq!(p.layout, Layout::Pool);
        assert_eq!(p.shards, 1);
        assert_eq!(p.prefix, DEFAULT_PREFIX);
        assert_eq!(p.pool_prefix(0), DEFAULT_PREFIX);
        assert!(!p.e2e);
    }

    #[test]
    fn full_class_and_sidecar_keys() {
        let p = ClassParams::parse(&params(&[
            ("bucket", "b"),
            ("prefix", "/team/x/"),
            ("layout", "pool"),
            ("shards", "4"),
            ("chunkSize", "4MiB"),
            ("e2e", "true"),
            ("writeMode", "strict"),
            ("engineProfile", "server"),
            ("credentialSource", "aws-default-chain"),
            ("csi.storage.k8s.io/provisioner-secret-name", "s"),
            (PVC_NAME_KEY, "data"),
        ]))
        .unwrap();
        assert_eq!(p.prefix, "team/x");
        assert_eq!(p.shards, 4);
        assert_eq!(p.chunk_size, Some(4 << 20));
        assert!(p.e2e);
        assert_eq!(p.pool_prefix(3), "team/x/shard-3");
        let fs = p.fs_create(p.pool_prefix(3));
        assert_eq!(
            (fs.bucket.as_str(), fs.prefix.as_str()),
            ("b", "team/x/shard-3")
        );
        assert_eq!(fs.write_mode.as_deref(), Some("strict"));
    }

    #[test]
    fn bad_classes_name_the_problem() {
        for (kv, needle) in [
            (vec![], "bucket"),
            (vec![("bucket", "  ")], "bucket"),
            (vec![("bucket", "a/b")], "bucket"),
            (vec![("bucket", "b"), ("layout", "zfs")], "layout"),
            (vec![("bucket", "b"), ("shards", "0")], "shards"),
            (vec![("bucket", "b"), ("shards", "65")], "shards"),
            (vec![("bucket", "b"), ("shards", "-1")], "shards"),
            (vec![("bucket", "b"), ("shards", "two")], "shards"),
            (
                vec![("bucket", "b"), ("layout", "dedicated"), ("shards", "2")],
                "shards",
            ),
            (vec![("bucket", "b"), ("chunkSize", "4XB")], "chunkSize"),
            (vec![("bucket", "b"), ("chunkSize", "8GiB")], "chunkSize"),
            (vec![("bucket", "b"), ("e2e", "yes")], "e2e"),
            (
                vec![("bucket", "b"), ("engineProfile", "desktop")],
                "engineProfile",
            ),
            (
                vec![("bucket", "b"), ("credentialSource", "x")],
                "credentialSource",
            ),
            (
                vec![("bucket", "b"), ("credentialSource", "secret")],
                "credentialSource",
            ),
            (
                vec![("bucket", "b"), ("credentialSource", "refreshing")],
                "credentialSecretName",
            ),
            (
                vec![
                    ("bucket", "b"),
                    ("credentialSource", "refreshing"),
                    ("credentialSecretName", "s"),
                ],
                "credentialSecretNamespace",
            ),
            (
                vec![
                    ("bucket", "b"),
                    ("credentialSource", "refreshing"),
                    ("credentialSecretName", "Not_A_Name"),
                    ("credentialSecretNamespace", "ns"),
                ],
                "credentialSecretName",
            ),
            (
                vec![
                    ("bucket", "b"),
                    ("credentialSecretName", "s"),
                    ("credentialSecretNamespace", "ns"),
                ],
                "refreshing",
            ),
            (vec![("bucket", "b"), ("prefix", "a/../b")], "prefix"),
            (vec![("bucket", "b"), ("filesystem", "old")], "filesystem"),
        ] {
            let e = ClassParams::parse(&params(&kv)).unwrap_err();
            assert!(e.contains(needle), "{kv:?}: {e}");
        }
    }

    /// Plan 37 §9: the three credential sources, and which of them make
    /// an engine pod wait for `fs.unlock`.
    #[test]
    fn credential_sources_are_selected_by_the_class() {
        let parse = |kv: &[(&str, &str)]| {
            let mut all = vec![("bucket", "b")];
            all.extend_from_slice(kv);
            ClassParams::parse(&params(&all)).unwrap()
        };
        let default = parse(&[]);
        assert_eq!(default.credentials, CredentialMode::StaticEphemeral);
        assert_eq!(default.credential_secret, None);
        assert!(default.awaits_unlock());
        assert_eq!(
            parse(&[("credentialSource", "static-ephemeral")]).credentials,
            CredentialMode::StaticEphemeral
        );
        let refreshing = parse(&[
            ("credentialSource", "refreshing"),
            ("credentialSecretName", "s3-creds"),
            ("credentialSecretNamespace", "tenant-a"),
        ]);
        assert_eq!(refreshing.credentials, CredentialMode::Refreshing);
        assert_eq!(
            refreshing.credential_secret,
            Some(SecretRef {
                namespace: "tenant-a".into(),
                name: "s3-creds".into()
            })
        );
        assert!(refreshing.awaits_unlock());
        let chain = parse(&[("credentialSource", "aws-default-chain")]);
        assert_eq!(chain.credentials, CredentialMode::AwsDefaultChain);
        assert!(!chain.awaits_unlock(), "IRSA: nothing to wait for");
        let chain_e2e = parse(&[("credentialSource", "aws-default-chain"), ("e2e", "true")]);
        assert!(
            chain_e2e.awaits_unlock(),
            "the passphrase still comes by fs.unlock"
        );
        // The watched Secret's name flows to the node with the volume
        // context (not a sidecar key), so the plugin knows what to watch.
        let ctx = volume_context(&params(&[
            ("bucket", "b"),
            ("credentialSource", "refreshing"),
            ("credentialSecretName", "s3-creds"),
            ("credentialSecretNamespace", "tenant-a"),
            ("csi.storage.k8s.io/node-stage-secret-name", "s3-creds"),
        ]));
        assert_eq!(
            ctx.get("credentialSecretName").map(String::as_str),
            Some("s3-creds")
        );
        assert!(!ctx.keys().any(|k| k.starts_with("csi.storage.k8s.io/")));
    }

    #[test]
    fn sharding_is_deterministic_and_spreads() {
        let p = ClassParams::parse(&params(&[("bucket", "b"), ("shards", "4")])).unwrap();
        // Pinned values (FNV-1a 64, computed independently): a change here
        // re-routes existing classes' retried CreateVolumes, so it must be
        // a deliberate, versioned change.
        let pinned: Vec<u32> = ["pvc-0", "pvc-1", "pvc-2", "pvc-3"]
            .iter()
            .map(|n| p.shard_for(n))
            .collect();
        assert_eq!(pinned, [1, 2, 3, 0]);
        let mut seen = [0u32; 4];
        for i in 0..400 {
            seen[p.shard_for(&format!("pvc-{i}")) as usize] += 1;
        }
        assert!(seen.iter().all(|n| *n > 50), "{seen:?}");
        let one = ClassParams::parse(&params(&[("bucket", "b")])).unwrap();
        assert_eq!(one.shard_for("anything"), 0);
    }

    #[test]
    fn dedicated_prefix_is_per_volume() {
        let p = ClassParams::parse(&params(&[
            ("bucket", "b"),
            ("prefix", "iso"),
            ("layout", "dedicated"),
        ]))
        .unwrap();
        assert_eq!(p.dedicated_prefix("pvc-1"), "iso/pvc-1");
    }
}
