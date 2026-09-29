//! [`ViewSpec`] and [`ViewQos`]: what [`crate::Engine::open_view`] opens
//! (plan 31 §4, §9.10, §6.12).

use std::collections::BTreeMap;

/// The labels a metric may carry as its `view` dimension (plan 31 §9.10's
/// bounded-cardinality policy): only these keys, never the full map.
/// Every label stays visible in `view.list` and in tracing; only these
/// are promoted to a metric label, so an operator labelling views with
/// high-cardinality values cannot create unbounded Prometheus series.
pub const METRIC_LABELS: &[&str] = &["pv"];

/// One view of the engine's filesystem.
///
/// The frontend's own mount options (a FUSE mountpoint, `allow_other`,
/// the source name, worker threads) are the frontend's, not the view's:
/// they travel beside the spec, to the frontend (`constellation-
/// frontend-fuse`'s `MountOptions`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ViewSpec {
    /// The subtree root (`/`, `/data`) or a snapshot selector
    /// (`/data@nightly`), as `mount`'s TARGET spells it.
    pub root: String,
    /// Serve a snapshot selector writable, through a clone of it.
    pub rw_snapshot: bool,
    /// Where `rw_snapshot`'s clone goes (absolute, or beside the
    /// snapshot's path).
    pub clone_name: Option<String>,
    /// `rw_snapshot` through a temporary clone, removed when the view
    /// closes.
    pub ephemeral: bool,
    /// Free-form labels (`{pv, pvc, namespace}` for a CSI volume): in
    /// the view's listing and tracing; metrics see only
    /// [`METRIC_LABELS`].
    pub labels: BTreeMap<String, String>,
    pub qos: ViewQos,
    /// §6.12: `link()` across a link-domain boundary answers `EXDEV`
    /// (the rule is in [`crate::view`]'s module doc, "Link domains").
    pub confine_links: bool,
}

/// Per-view admission limits (plan 31 §9.10): `None` is unlimited, as
/// every view was before them. Over a limit an op waits; past its
/// deadline (the op's own, else `CONSTELLATION_VIEW_ADMISSION_WAIT_MS`,
/// default 30 s) it completes with `Code::Again`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ViewQos {
    /// Ops of this view in flight at once. `flush`, `release`, the lock
    /// ops and `sync_view` are never held back: they end work others
    /// wait for.
    pub max_inflight_ops: Option<u32>,
    /// Bytes this view's open write sessions may stage (a share of the
    /// node's staging budget). A write that would exceed it waits for
    /// the view's own flushes.
    pub max_staging_bytes: Option<u64>,
}

impl ViewSpec {
    /// A view of `root` (`/` for the whole filesystem) with no options.
    pub fn new(root: impl Into<String>) -> Self {
        Self {
            root: root.into(),
            ..Self::default()
        }
    }

    /// A snapshot selector served as the frozen snapshot itself: nothing
    /// in the view can be written.
    pub fn is_frozen(&self) -> bool {
        self.root.contains('@') && !self.rw_snapshot
    }

    /// The labels a metric may carry (see [`METRIC_LABELS`]).
    pub fn metric_labels(&self) -> BTreeMap<&str, &str> {
        self.labels
            .iter()
            .filter(|(k, _)| METRIC_LABELS.contains(&k.as_str()))
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_allowlisted_labels_reach_metrics() {
        let mut spec = ViewSpec::new("/volumes/pv-1");
        spec.labels.insert("pv".into(), "pv-1".into());
        spec.labels.insert("pvc".into(), "data-7f3a".into());
        spec.labels.insert("namespace".into(), "team-a".into());
        assert_eq!(
            spec.metric_labels().into_iter().collect::<Vec<_>>(),
            [("pv", "pv-1")]
        );
        assert_eq!(spec.labels.len(), 3, "the full map stays on the view");
        assert!(!spec.is_frozen());
        assert!(ViewSpec::new("/a@snap").is_frozen());
        let rw = ViewSpec {
            rw_snapshot: true,
            ..ViewSpec::new("/a@snap")
        };
        assert!(!rw.is_frozen());
        assert_eq!(ViewSpec::default().qos, ViewQos::default());
    }
}
