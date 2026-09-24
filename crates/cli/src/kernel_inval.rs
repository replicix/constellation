//! Kernel cache invalidation for another node's writes (plan 30 §M7).
//!
//! The FUSE mounts answer lookups and attributes with a 1 s TTL
//! (`fusefs::TTL`), so the kernel keeps serving what it cached — a
//! negative dentry, a file's old size, its old pages — for up to a second
//! after the replica underneath has applied another node's change. The
//! `visibility-after-burst` scenario measured exactly that: a marker file
//! whose create and data happened to ship in different segments was
//! listed, read back as empty, and then stayed empty for a whole TTL even
//! though its data arrived ~100 ms later — so cross-node visibility had a
//! ~1.1 s tail that no amount of faster log delivery could remove.
//!
//! Every foreign segment the replica applies is therefore turned into
//! `FUSE_NOTIFY_INVAL_*` notifications for what its records touched: the
//! dentries it created or removed, the parents whose listing and times
//! changed, and the inodes whose attributes or content changed. They are
//! sent from one dedicated thread, fed through an unbounded channel that
//! nothing ever waits on: a notification can block in the kernel until a
//! FUSE request on the same directory finishes, and that request may
//! itself be waiting for the sync task, so neither the sync task nor a
//! FUSE worker may ever wait for this thread.
//!
//! Only a replica's *foreign* applies are reported (a local write went
//! through this kernel already). Views mounted on a subtree translate
//! their root; snapshot views are frozen and never registered.
//! `CONSTELLATION_KERNEL_INVALIDATE=0` turns it off (the TTL bound then
//! applies as before).

use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::Ino;
use constellation_meta::LogRecord;
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::sync::mpsc;

/// One thing to invalidate, in replica inode numbers.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Inval {
    /// A name in a directory (created, removed, renamed).
    Entry { parent: Ino, name: String },
    /// An inode's attributes, and with `data` its cached pages too.
    Inode { ino: Ino, data: bool },
}

enum Msg {
    Register {
        id: u64,
        notifier: fuser::Notifier,
        view_root: Ino,
    },
    Unregister(u64),
    Batch(Vec<Inval>),
}

/// The node's invalidation thread (one for every mounted view).
pub struct KernelInvalidator {
    tx: mpsc::Sender<Msg>,
}

/// `CONSTELLATION_KERNEL_INVALIDATE` (default on).
pub fn enabled() -> bool {
    !matches!(
        std::env::var("CONSTELLATION_KERNEL_INVALIDATE")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "off" | "false"
    )
}

impl KernelInvalidator {
    pub fn start() -> Self {
        let (tx, rx) = mpsc::channel::<Msg>();
        std::thread::Builder::new()
            .name("kernel-inval".into())
            .spawn(move || run(rx))
            .expect("spawning the kernel invalidation thread");
        Self { tx }
    }

    /// A view mounted with `view_root` as its root directory.
    pub fn register(&self, id: u64, notifier: fuser::Notifier, view_root: Ino) {
        let _ = self.tx.send(Msg::Register {
            id,
            notifier,
            view_root,
        });
    }

    pub fn unregister(&self, id: u64) {
        let _ = self.tx.send(Msg::Unregister(id));
    }

    /// The hook for `Meta::set_foreign_apply_hook`.
    pub fn hook(&self) -> constellation_meta::ForeignApplyHook {
        let tx = self.tx.clone();
        Box::new(move |records: &[LogRecord]| {
            let batch = invalidations(records);
            if !batch.is_empty() {
                let _ = tx.send(Msg::Batch(batch));
            }
        })
    }
}

/// What `records` make stale in a kernel's caches.
fn invalidations(records: &[LogRecord]) -> Vec<Inval> {
    let mut out = BTreeSet::new();
    let entry = |out: &mut BTreeSet<Inval>, parent: Ino, name: &str| {
        out.insert(Inval::Entry {
            parent,
            name: name.to_string(),
        });
        out.insert(Inval::Inode {
            ino: parent,
            data: true,
        });
    };
    for rec in records {
        match rec {
            LogRecord::Mkdir { parent, name, .. }
            | LogRecord::Create { parent, name, .. }
            | LogRecord::Symlink { parent, name, .. }
            | LogRecord::Mknod { parent, name, .. }
            | LogRecord::Unlink { parent, name, .. }
            | LogRecord::Rmdir { parent, name, .. } => entry(&mut out, *parent, name),
            LogRecord::Link {
                ino, parent, name, ..
            } => {
                entry(&mut out, *parent, name);
                out.insert(Inval::Inode {
                    ino: *ino,
                    data: false,
                });
            }
            LogRecord::Rename {
                parent,
                name,
                new_parent,
                new_name,
                ..
            } => {
                entry(&mut out, *parent, name);
                entry(&mut out, *new_parent, new_name);
            }
            LogRecord::Setattr { ino, size, .. } => {
                out.insert(Inval::Inode {
                    ino: *ino,
                    data: size.is_some(),
                });
            }
            LogRecord::WriteManifest { ino, .. } => {
                out.insert(Inval::Inode {
                    ino: *ino,
                    data: true,
                });
            }
            LogRecord::SetXattr { ino, .. } | LogRecord::RemoveXattr { ino, .. } => {
                out.insert(Inval::Inode {
                    ino: *ino,
                    data: false,
                });
            }
            // Atime is best effort and order free; the rest carry no
            // namespace change a kernel could have cached.
            _ => {}
        }
    }
    out.into_iter().collect()
}

/// A replica inode as `view_root`'s view numbers it (`FuseFs::real_ino`'s
/// inverse), or `None` when the view cannot see it.
fn in_view(ino: Ino, view_root: Ino) -> Option<Ino> {
    if ino == view_root {
        Some(ROOT_INO)
    } else if ino == ROOT_INO {
        None
    } else {
        Some(ino)
    }
}

fn run(rx: mpsc::Receiver<Msg>) {
    let mut views: Vec<(u64, fuser::Notifier, Ino)> = Vec::new();
    while let Ok(msg) = rx.recv() {
        match msg {
            Msg::Register {
                id,
                notifier,
                view_root,
            } => views.push((id, notifier, view_root)),
            Msg::Unregister(id) => views.retain(|(v, _, _)| *v != id),
            Msg::Batch(batch) => {
                for (_, notifier, view_root) in &views {
                    for inval in &batch {
                        // ENOENT (nothing cached) is the common answer;
                        // every error only means there was nothing to drop.
                        let _ = match inval {
                            Inval::Entry { parent, name } => match in_view(*parent, *view_root) {
                                Some(p) => {
                                    notifier.inval_entry(fuser::INodeNo(p), OsStr::new(name))
                                }
                                None => Ok(()),
                            },
                            Inval::Inode { ino, data } => match in_view(*ino, *view_root) {
                                Some(i) => notifier.inval_inode(
                                    fuser::INodeNo(i),
                                    if *data { 0 } else { -1 },
                                    0,
                                ),
                                None => Ok(()),
                            },
                        };
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_create_and_its_data_invalidate_the_entry_the_parent_and_the_file() {
        let got = invalidations(&[
            LogRecord::Create {
                parent: 5,
                name: "m1".into(),
                ino: 9,
                mode: 0o644,
                uid: 0,
                gid: 0,
                time_ns: 1,
            },
            LogRecord::WriteManifest {
                ino: 9,
                base_manifest: None,
                manifest: Vec::new(),
                size: 8,
                time_ns: 2,
            },
        ]);
        assert_eq!(
            got,
            vec![
                Inval::Entry {
                    parent: 5,
                    name: "m1".into()
                },
                Inval::Inode { ino: 5, data: true },
                Inval::Inode { ino: 9, data: true },
            ]
        );
    }

    #[test]
    fn a_subtree_view_renumbers_its_root_and_hides_the_real_root() {
        assert_eq!(in_view(42, 42), Some(ROOT_INO));
        assert_eq!(in_view(ROOT_INO, 42), None);
        assert_eq!(in_view(7, 42), Some(7));
        assert_eq!(in_view(ROOT_INO, ROOT_INO), Some(ROOT_INO));
    }
}
