//! The simulated bucket: `object_store::memory::InMemory` behind seeded
//! latency, scripted error codes shaped like `object_store`'s S3 client's
//! (plan 30 M4's `faulty.rs` rules, shared by idea rather than by file so
//! this crate does not depend on an unmerged milestone), per-node
//! reachability and per-request fault probabilities.
//!
//! Every node gets its own [`NodeStore`] handle over the one shared
//! [`Bucket`], so a partition between one node and S3 is a per-handle
//! switch while the bucket's contents stay shared and real: the real
//! `LogStore`/`LeaseStore`/`CommitChain` run on top, CAS semantics
//! included.

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpKind {
    Put,
    Get,
    List,
}

/// What a firing rule does (M4's `Fault`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Answer with this HTTP status; the operation does not take effect.
    Status(u16),
    /// The operation times out without taking effect.
    Timeout,
    /// The operation takes effect, then the caller sees this status.
    AppliedThen(u16),
    /// The operation takes effect, then the caller sees a timeout.
    AppliedThenTimeout,
}

/// A scripted rule: `op` calls on paths containing `pattern`, from `node`
/// (or any), fire `fault` on the n-th matching call (`Nth`) or with
/// probability `p` (`Random`).
#[derive(Debug, Clone)]
pub struct Rule {
    pub node: Option<u64>,
    pub op: OpKind,
    pub pattern: String,
    pub when: When,
    pub fault: Fault,
    seen: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum When {
    Nth(usize),
    /// The n-th matching call and every later one.
    From(usize),
    Random(f64),
}

impl Rule {
    pub fn new(node: Option<u64>, op: OpKind, pattern: &str, when: When, fault: Fault) -> Self {
        Self {
            node,
            op,
            pattern: pattern.to_string(),
            when,
            fault,
            seen: 0,
        }
    }
}

#[derive(Debug)]
struct HttpStatus(u16);

impl std::fmt::Display for HttpStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Error performing request (injected) - Server returned non-2xx status code: {}",
            self.0
        )
    }
}

impl std::error::Error for HttpStatus {}

#[derive(Debug)]
struct Timeout;

impl std::fmt::Display for Timeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "error sending request (injected): operation timed out")
    }
}

impl std::error::Error for Timeout {}

#[derive(Debug)]
struct Unreachable;

impl std::fmt::Display for Unreachable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "error sending request (simulated): S3 unreachable from this node"
        )
    }
}

impl std::error::Error for Unreachable {}

/// The error `object_store` 0.14's S3 client returns for `status` on a
/// PUT in `mode` (M4's table).
fn s3_error(path: &str, status: u16, mode: &PutMode) -> object_store::Error {
    let path = path.to_string();
    let http = || Box::new(HttpStatus(status)) as Box<dyn std::error::Error + Send + Sync>;
    match (status, mode) {
        (412, PutMode::Create) => object_store::Error::AlreadyExists {
            path: path.clone(),
            source: Box::new(object_store::Error::Precondition {
                path,
                source: http(),
            }),
        },
        (412, _) => object_store::Error::Precondition {
            path,
            source: http(),
        },
        (409, _) => object_store::Error::AlreadyExists {
            path,
            source: http(),
        },
        (404, PutMode::Update(_)) => object_store::Error::Precondition {
            path,
            source: http(),
        },
        (404, _) => object_store::Error::NotFound {
            path,
            source: http(),
        },
        _ => object_store::Error::Generic {
            store: "S3",
            source: http(),
        },
    }
}

fn timeout_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "S3",
        source: Box::new(Timeout),
    }
}

fn unreachable_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "S3",
        source: Box::new(Unreachable),
    }
}

/// Request counts, per kind (the plan's "S3 requests per op").
#[derive(Debug, Default, Clone, Copy)]
pub struct Counts {
    pub puts: u64,
    pub gets: u64,
    pub lists: u64,
}

/// Plan 43 C7b: called with `(writer node, the lease body replaced, the body
/// written)` when a CAS of the lease object lands.
pub type LeaseHook = Arc<
    dyn Fn(u64, Option<constellation_store_s3::Lease>, constellation_store_s3::Lease) + Send + Sync,
>;

pub struct Bucket {
    inner: Arc<InMemory>,
    /// Plan 43 C7b: the lease-claim oracle's tap on the lease object.
    lease_hook: Mutex<Option<LeaseHook>>,
    /// Plan 43 C0: what the log's segments carry, noted as they land (the
    /// I1 oracle judges an answer synchronously and cannot await the bucket).
    pub archive: super::oracle::ArchiveIndex,
    rng: Mutex<StdRng>,
    /// Per-request latency range, ms.
    latency: (u64, u64),
    rules: Mutex<Vec<Rule>>,
    cut: Mutex<HashSet<u64>>,
    counts: Mutex<Counts>,
}

impl Bucket {
    pub fn new(seed: u64, latency: (u64, u64)) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(InMemory::new()),
            lease_hook: Mutex::new(None),
            archive: super::oracle::ArchiveIndex::default(),
            rng: Mutex::new(StdRng::seed_from_u64(seed ^ 0x5335_5335)),
            latency,
            rules: Mutex::new(Vec::new()),
            cut: Mutex::new(HashSet::new()),
            counts: Mutex::new(Counts::default()),
        })
    }

    /// The backing store, for the checkers (never faulted).
    pub fn raw(&self) -> Arc<dyn ObjectStore> {
        self.inner.clone()
    }

    /// The lease object as it is in the bucket this instant, read
    /// synchronously (plan 43 C0's I1 oracle: the configuration a
    /// reconfigurer would read). The backing store never waits.
    pub fn peek_lease(&self) -> Option<constellation_store_s3::Lease> {
        let leases = constellation_store_s3::LeaseStore::new(
            self.raw(),
            "p0",
            constellation_store_s3::LeaseMode::Cas,
        );
        futures::executor::block_on(leases.get())
            .ok()
            .flatten()
            .map(|(l, _)| l)
    }

    /// Install the tap on the lease object (the oracles' plumbing).
    pub fn set_lease_hook(&self, hook: LeaseHook) {
        *self.lease_hook.lock().unwrap() = Some(hook);
    }

    /// The lease body in the bucket right before a put to `location`, when
    /// it is the lease object and a tap is installed (read synchronously:
    /// the in-memory store does not wait, so nothing lands in between).
    fn lease_before(&self, location: &Path) -> Option<Option<constellation_store_s3::Lease>> {
        if location.as_ref() != "leases/p0.json" || self.lease_hook.lock().unwrap().is_none() {
            return None;
        }
        Some(self.peek_lease())
    }

    /// A put that took effect: a log segment goes to the archive index.
    fn put_landed(
        &self,
        node: u64,
        prev: Option<Option<constellation_store_s3::Lease>>,
        location: &Path,
        payload: &PutPayload,
    ) {
        if let Some(prev) = prev {
            let hook = self.lease_hook.lock().unwrap().clone();
            let body: bytes::Bytes = payload.clone().into();
            if let (Some(hook), Ok(new)) = (hook, serde_json::from_slice(&body)) {
                hook(node, prev, new);
            }
        }
        let name = location.as_ref();
        if let Some(seq) = name
            .strip_prefix("log/p0/")
            .and_then(|n| n.strip_suffix(".zst"))
            .and_then(|n| u64::from_str_radix(n, 16).ok())
        {
            let body: bytes::Bytes = payload.clone().into();
            self.archive.note(seq, &body);
        }
    }

    pub fn handle(self: &Arc<Self>, node: u64) -> Arc<dyn ObjectStore> {
        Arc::new(NodeStore {
            bucket: self.clone(),
            node,
        })
    }

    pub fn script(&self, rule: Rule) {
        self.rules.lock().unwrap().push(rule);
    }

    /// Drop every scripted rule on `op` calls on paths containing
    /// `pattern` from `node` (`FaultKind::S3RuleFor`).
    pub fn unscript(&self, node: Option<u64>, op: OpKind, pattern: &str) {
        self.rules
            .lock()
            .unwrap()
            .retain(|r| !(r.node == node && r.op == op && r.pattern == pattern));
    }

    /// Cut or heal one node's path to the bucket.
    pub fn set_cut(&self, node: u64, cut: bool) {
        let mut set = self.cut.lock().unwrap();
        if cut {
            set.insert(node);
        } else {
            set.remove(&node);
        }
    }

    /// Whether `node`'s path to the bucket is cut.
    pub fn is_cut(&self, node: u64) -> bool {
        self.cut.lock().unwrap().contains(&node)
    }

    pub fn counts(&self) -> Counts {
        *self.counts.lock().unwrap()
    }

    fn latency(&self) -> Duration {
        let (lo, hi) = self.latency;
        let ms = if hi > lo {
            self.rng.lock().unwrap().random_range(lo..=hi)
        } else {
            lo
        };
        Duration::from_millis(ms)
    }

    fn fault_for(&self, node: u64, op: OpKind, path: &Path) -> Option<Fault> {
        {
            let mut counts = self.counts.lock().unwrap();
            match op {
                OpKind::Put => counts.puts += 1,
                OpKind::Get => counts.gets += 1,
                OpKind::List => counts.lists += 1,
            }
        }
        let path = path.as_ref();
        let mut rules = self.rules.lock().unwrap();
        let mut fired = None;
        for rule in rules.iter_mut() {
            if rule.op != op
                || !path.contains(&rule.pattern)
                || rule.node.is_some_and(|n| n != node)
            {
                continue;
            }
            rule.seen += 1;
            if fired.is_some() {
                continue;
            }
            let fires = match rule.when {
                When::Nth(k) => rule.seen == k,
                When::From(k) => rule.seen >= k,
                When::Random(p) => self.rng.lock().unwrap().random_bool(p),
            };
            if fires {
                fired = Some(rule.fault);
            }
        }
        fired
    }
}

/// One node's view of the bucket.
pub struct NodeStore {
    bucket: Arc<Bucket>,
    node: u64,
}

impl std::fmt::Debug for NodeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NodeStore({})", self.node)
    }
}

impl std::fmt::Display for NodeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NodeStore({})", self.node)
    }
}

impl NodeStore {
    async fn admit(&self, op: OpKind, path: &Path) -> object_store::Result<Option<Fault>> {
        tokio::time::sleep(self.bucket.latency()).await;
        if self.bucket.cut.lock().unwrap().contains(&self.node) {
            return Err(unreachable_error());
        }
        Ok(self.bucket.fault_for(self.node, op, path))
    }
}

#[async_trait]
impl ObjectStore for NodeStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let mode = opts.mode.clone();
        match self.admit(OpKind::Put, location).await? {
            None => {
                let landed = payload.clone();
                let before = self.bucket.lease_before(location);
                let r = self.bucket.inner.put_opts(location, payload, opts).await?;
                self.bucket.put_landed(self.node, before, location, &landed);
                Ok(r)
            }
            Some(Fault::Status(status)) => Err(s3_error(location.as_ref(), status, &mode)),
            Some(Fault::Timeout) => Err(timeout_error()),
            Some(Fault::AppliedThen(status)) => {
                let landed = payload.clone();
                let before = self.bucket.lease_before(location);
                self.bucket.inner.put_opts(location, payload, opts).await?;
                self.bucket.put_landed(self.node, before, location, &landed);
                Err(s3_error(location.as_ref(), status, &mode))
            }
            Some(Fault::AppliedThenTimeout) => {
                let landed = payload.clone();
                let before = self.bucket.lease_before(location);
                self.bucket.inner.put_opts(location, payload, opts).await?;
                self.bucket.put_landed(self.node, before, location, &landed);
                Err(timeout_error())
            }
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.bucket.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        match self.admit(OpKind::Get, location).await? {
            None | Some(Fault::AppliedThen(_)) | Some(Fault::AppliedThenTimeout) => {
                self.bucket.inner.get_opts(location, options).await
            }
            Some(Fault::Status(404)) => Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: Box::new(HttpStatus(404)),
            }),
            Some(Fault::Status(status)) => {
                Err(s3_error(location.as_ref(), status, &PutMode::Overwrite))
            }
            Some(Fault::Timeout) => Err(timeout_error()),
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.bucket.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let path = prefix.cloned().unwrap_or_else(|| Path::from(""));
        let bucket = self.bucket.clone();
        let node = self.node;
        let inner = self.bucket.inner.clone();
        Box::pin(async_stream_list(bucket, node, path, inner))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let path = prefix.cloned().unwrap_or_else(|| Path::from(""));
        match self.admit(OpKind::List, &path).await? {
            None | Some(Fault::AppliedThen(_)) | Some(Fault::AppliedThenTimeout) => {
                self.bucket.inner.list_with_delimiter(prefix).await
            }
            Some(Fault::Status(status)) => {
                Err(s3_error(path.as_ref(), status, &PutMode::Overwrite))
            }
            Some(Fault::Timeout) => Err(timeout_error()),
        }
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.bucket.inner.copy_opts(from, to, options).await
    }
}

/// A LIST as one delayed, faultable request whose page is the backing
/// store's listing.
fn async_stream_list(
    bucket: Arc<Bucket>,
    node: u64,
    prefix: Path,
    inner: Arc<InMemory>,
) -> impl futures::Stream<Item = object_store::Result<ObjectMeta>> + Send + 'static {
    use futures::StreamExt;
    futures::stream::once(async move {
        tokio::time::sleep(bucket.latency()).await;
        if bucket.cut.lock().unwrap().contains(&node) {
            return vec![Err(unreachable_error())];
        }
        match bucket.fault_for(node, OpKind::List, &prefix) {
            Some(Fault::Status(status)) => {
                return vec![Err(s3_error(prefix.as_ref(), status, &PutMode::Overwrite))]
            }
            Some(Fault::Timeout) => return vec![Err(timeout_error())],
            _ => {}
        }
        inner.list(Some(&prefix)).collect::<Vec<_>>().await
    })
    .flat_map(futures::stream::iter)
}
