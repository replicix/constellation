//! A fault-injecting [`ObjectStore`] for this crate's tests (plan 30 §M4
//! item 1): an [`InMemory`] store plus scripted HTTP answers — 412, 409,
//! 404, 500 and timeouts — chosen per operation kind, path pattern and
//! call count.
//!
//! The injected errors have exactly the shape `object_store` 0.14's S3
//! client produces for the same status (see `crate::cas`'s module doc),
//! so a test exercises the real classifier, not a convenient stand-in:
//!
//! | status | `PutMode::Create` | `PutMode::Update` | other ops |
//! |---|---|---|---|
//! | 412 | `AlreadyExists` wrapping `Precondition` | `Precondition` | `Precondition` |
//! | 409 | `AlreadyExists` wrapping the HTTP error | `AlreadyExists` wrapping the HTTP error | `AlreadyExists` |
//! | 404 | `NotFound` | `Precondition` wrapping the HTTP 404 | `NotFound` |
//! | 304 | `AlreadyExists` wrapping `NotModified` | `NotModified` | `NotModified` |
//! | other | `Generic` wrapping the HTTP error | same | same |
//!
//! [`Fault::AppliedThen`] models the ambiguous case the CAS rules exist
//! for: the write reaches the backing store, and the caller is answered
//! with an error anyway (a 5xx whose `object_store` retry then met the
//! object, or a reply lost to a timeout).

// A test helper: not every knob is used by every build of the tests.
#![allow(dead_code)]

use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Which operations a rule applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpKind {
    Put,
    Get,
    /// A `HEAD` (`get_opts` with `head: true`), counted apart from `GET`.
    Head,
}

/// Which calls (counted per rule, 1-based, over calls whose path contains
/// the rule's pattern) a rule fires on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Calls {
    /// Only the n-th matching call.
    Nth(usize),
    /// The first n matching calls.
    First(usize),
    /// Every matching call.
    Every,
}

impl Calls {
    fn fires(&self, n: usize) -> bool {
        match *self {
            Calls::Nth(k) => n == k,
            Calls::First(k) => n <= k,
            Calls::Every => true,
        }
    }
}

/// What a firing rule does.
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
    /// GET only: the first `n` bytes of the object, as a complete
    /// response — the torn read an emulator serving a file mid-overwrite
    /// produces (real S3 GETs are atomic). No effect on a PUT.
    Truncated(usize),
}

struct Rule {
    op: OpKind,
    pattern: String,
    calls: Calls,
    fault: Fault,
    seen: usize,
}

/// The raw HTTP error, worded like `object_store`'s private `RetryError`.
#[derive(Debug)]
struct HttpStatus(u16);

impl std::fmt::Display for HttpStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reason = match self.0 {
            304 => "Not Modified",
            404 => "Not Found",
            409 => "Conflict",
            412 => "Precondition Failed",
            500 => "Internal Server Error",
            503 => "Service Unavailable",
            _ => "Error",
        };
        write!(
            f,
            "Error performing PUT (injected) - Server returned non-2xx status code: {} {reason}: \
             <Error><Code>injected</Code></Error>",
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

/// The error `object_store`'s S3 client returns for `status` on a PUT in
/// `mode` (see the module doc's table).
pub fn s3_error(path: &str, status: u16, mode: &PutMode) -> object_store::Error {
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
        (304, PutMode::Create) => object_store::Error::AlreadyExists {
            path: path.clone(),
            source: Box::new(object_store::Error::NotModified {
                path,
                source: http(),
            }),
        },
        (412, _) => object_store::Error::Precondition {
            path,
            source: http(),
        },
        (304, _) => object_store::Error::NotModified {
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

/// See the module doc.
pub struct FaultyStore {
    inner: Arc<InMemory>,
    rules: Mutex<Vec<Rule>>,
    calls: Mutex<HashMap<(OpKind, String), usize>>,
}

impl std::fmt::Debug for FaultyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultyStore")
    }
}

impl std::fmt::Display for FaultyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultyStore")
    }
}

impl FaultyStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(InMemory::new()),
            rules: Mutex::new(Vec::new()),
            calls: Mutex::new(HashMap::new()),
        })
    }

    /// The backing store, untouched by any rule (for arranging state).
    pub fn inner(&self) -> Arc<InMemory> {
        self.inner.clone()
    }

    /// Add a rule: `op` calls whose path contains `pattern` get `fault`
    /// on the calls `calls` selects. Rules are checked in the order added;
    /// the first that fires wins.
    pub fn script(&self, op: OpKind, pattern: &str, calls: Calls, fault: Fault) {
        self.rules.lock().unwrap().push(Rule {
            op,
            pattern: pattern.to_string(),
            calls,
            fault,
            seen: 0,
        });
    }

    /// Drop every rule.
    pub fn clear(&self) {
        self.rules.lock().unwrap().clear();
    }

    /// How many `op` calls on paths containing `pattern` were made.
    pub fn calls(&self, op: OpKind, pattern: &str) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|((kind, path), _)| *kind == op && path.contains(pattern))
            .map(|(_, n)| *n)
            .sum()
    }

    fn fault_for(&self, op: OpKind, path: &Path) -> Option<Fault> {
        let path = path.as_ref().to_string();
        *self
            .calls
            .lock()
            .unwrap()
            .entry((op, path.clone()))
            .or_default() += 1;
        let mut rules = self.rules.lock().unwrap();
        let mut fired = None;
        for rule in rules.iter_mut() {
            if rule.op != op || !path.contains(&rule.pattern) {
                continue;
            }
            rule.seen += 1;
            if fired.is_none() && rule.calls.fires(rule.seen) {
                fired = Some(rule.fault);
            }
        }
        fired
    }
}

#[async_trait::async_trait]
impl ObjectStore for FaultyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let mode = opts.mode.clone();
        match self.fault_for(OpKind::Put, location) {
            None | Some(Fault::Truncated(_)) => self.inner.put_opts(location, payload, opts).await,
            Some(Fault::Status(status)) => Err(s3_error(location.as_ref(), status, &mode)),
            Some(Fault::Timeout) => Err(timeout_error()),
            Some(Fault::AppliedThen(status)) => {
                self.inner.put_opts(location, payload, opts).await?;
                Err(s3_error(location.as_ref(), status, &mode))
            }
            Some(Fault::AppliedThenTimeout) => {
                self.inner.put_opts(location, payload, opts).await?;
                Err(timeout_error())
            }
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let kind = if options.head {
            OpKind::Head
        } else {
            OpKind::Get
        };
        match self.fault_for(kind, location) {
            None | Some(Fault::AppliedThen(_)) | Some(Fault::AppliedThenTimeout) => {
                self.inner.get_opts(location, options).await
            }
            Some(Fault::Status(404)) => Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: Box::new(HttpStatus(404)),
            }),
            Some(Fault::Status(status)) => {
                Err(s3_error(location.as_ref(), status, &PutMode::Overwrite))
            }
            Some(Fault::Timeout) => Err(timeout_error()),
            Some(Fault::Truncated(n)) => {
                let full = self.inner.get_opts(location, options).await?;
                let (mut meta, attributes) = (full.meta.clone(), full.attributes.clone());
                let mut bytes = full.bytes().await?;
                bytes.truncate(n);
                meta.size = bytes.len() as u64;
                Ok(GetResult {
                    range: 0..meta.size,
                    payload: object_store::GetResultPayload::Stream(Box::pin(
                        futures::stream::once(async move { Ok(bytes) }),
                    )),
                    meta,
                    attributes,
                    extensions: Default::default(),
                })
            }
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
