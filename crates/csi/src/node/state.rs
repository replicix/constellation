//! The node plugin's own record of what it staged ([`StateStore`]): one
//! small JSON file per staged volume under `<hostRoot>/volumes/`, the
//! hostPath every node-plugin generation on the node sees.
//!
//! **Why it exists.** Everything else survives a node-plugin restart on its
//! own: the staging and publish mounts live in the host's mount namespace
//! (`Bidirectional` propagation), and the engine pods hold the FUSE
//! connections. What a restarted plugin would not know is which volume a
//! staging path belongs to, which engine pod serves it, and what the
//! volume's record looked like when it was staged (the `VolumeCondition`
//! baseline) — so `NodeUnstageVolume` could not reach the right engine,
//! `NodeGetVolumeStats` could not find the view, and a dead staging mount
//! could not be restaged from a `NodePublishVolume` that carries no
//! node-stage secret. That is all a record holds; never a secret.
//!
//! Records are written whole (temporary file, `fsync`, rename), keyed by a
//! hash of the `volume_id` (ids hold `/` and may outgrow a file name). A
//! record that does not parse is logged and skipped: the volume it was for
//! is then treated as not staged here, and kubelet's next
//! `NodeStageVolume` adopts the view the engine still serves
//! (`crate::node`).

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// The record format; bump on any change (no migration of old records:
/// an unreadable record is skipped, see the module docs).
const FORMAT: u32 = 2;

/// One staged volume (module docs).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeRecord {
    pub format: u32,
    pub volume_id: String,
    pub staging_path: PathBuf,
    /// The engine pod's unit (`<hostRoot>/sockets/<unit>/`) and name.
    pub unit: String,
    pub pod: String,
    pub fs_uuid: String,
    pub subtree: String,
    /// The volume directory's `user.constellation.csi.*` xattrs when it
    /// was staged (lossy UTF-8), the baseline `NodeGetVolumeHealth`
    /// compares against (plan 37 §11 "VolumeCondition").
    pub xattrs: BTreeMap<String, String>,
    /// The volume context it was staged with (the pool's location; the
    /// sidecars put no secret there), for a restage from a request that
    /// lacks it.
    pub context: BTreeMap<String, String>,
    /// Where it is bind-mounted now.
    pub published: BTreeSet<PathBuf>,
}

impl VolumeRecord {
    pub fn new(volume_id: &str, staging_path: &Path) -> VolumeRecord {
        VolumeRecord {
            format: FORMAT,
            volume_id: volume_id.to_string(),
            staging_path: staging_path.to_path_buf(),
            unit: String::new(),
            pod: String::new(),
            fs_uuid: String::new(),
            subtree: String::new(),
            xattrs: BTreeMap::new(),
            context: BTreeMap::new(),
            published: BTreeSet::new(),
        }
    }
}

/// The records, in memory and (unless [`Self::in_memory`]) on disk.
pub struct StateStore {
    dir: Option<PathBuf>,
    volumes: Mutex<BTreeMap<String, VolumeRecord>>,
}

fn file_name(volume_id: &str) -> String {
    format!(
        "{}.json",
        &blake3::hash(volume_id.as_bytes()).to_hex()[..32]
    )
}

impl StateStore {
    /// Load every record under `dir` (created if missing).
    pub fn open(dir: &Path) -> io::Result<StateStore> {
        std::fs::create_dir_all(dir)?;
        let mut volumes = BTreeMap::new();
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let parsed = std::fs::read(&path)
                .map_err(|e| e.to_string())
                .and_then(|b| {
                    serde_json::from_slice::<VolumeRecord>(&b).map_err(|e| e.to_string())
                });
            match parsed {
                Ok(record) if record.format == FORMAT => {
                    volumes.insert(record.volume_id.clone(), record);
                }
                Ok(record) => tracing::warn!(path = %path.display(), format = record.format,
                    "skipping a staged-volume record of another format"),
                Err(e) => tracing::warn!(path = %path.display(), error = %e,
                    "skipping an unreadable staged-volume record"),
            }
        }
        tracing::info!(dir = %dir.display(), volumes = volumes.len(), "loaded staged-volume records");
        Ok(StateStore {
            dir: Some(dir.to_path_buf()),
            volumes: Mutex::new(volumes),
        })
    }

    /// Records that live only as long as this process (tests).
    pub fn in_memory() -> StateStore {
        StateStore {
            dir: None,
            volumes: Mutex::default(),
        }
    }

    pub fn get(&self, volume_id: &str) -> Option<VolumeRecord> {
        self.volumes.lock().unwrap().get(volume_id).cloned()
    }

    pub fn all(&self) -> Vec<VolumeRecord> {
        self.volumes.lock().unwrap().values().cloned().collect()
    }

    /// Write `record` (durably, before it is visible to [`Self::get`]).
    pub fn put(&self, record: VolumeRecord) -> io::Result<()> {
        let mut volumes = self.volumes.lock().unwrap();
        if let Some(dir) = &self.dir {
            let path = dir.join(file_name(&record.volume_id));
            let tmp = path.with_extension("json.tmp");
            let body = serde_json::to_vec_pretty(&record).map_err(io::Error::other)?;
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(&body)?;
            file.sync_all()?;
            std::fs::rename(&tmp, &path)?;
        }
        volumes.insert(record.volume_id.clone(), record);
        Ok(())
    }

    pub fn remove(&self, volume_id: &str) -> io::Result<()> {
        let mut volumes = self.volumes.lock().unwrap();
        if let Some(dir) = &self.dir {
            match std::fs::remove_file(dir.join(file_name(volume_id))) {
                Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
                _ => {}
            }
        }
        volumes.remove(volume_id);
        Ok(())
    }

    /// How many staged volumes engine pod `pod` serves.
    pub fn views_of(&self, pod: &str) -> usize {
        self.volumes
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.pod == pod)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_survive_a_restart_and_garbage_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let store = StateStore::open(dir.path()).unwrap();
        let mut a = VolumeRecord::new("v1/pool/0/u/volumes/pvc-a", Path::new("/s/a"));
        a.pod = "constellation-engine-x-n1".into();
        a.published.insert("/t/1".into());
        a.xattrs
            .insert("user.constellation.csi.pv".into(), "pvc-a".into());
        store.put(a.clone()).unwrap();
        let mut b = VolumeRecord::new("u/datasets/x", Path::new("/s/b"));
        b.pod = a.pod.clone();
        store.put(b.clone()).unwrap();
        std::fs::write(dir.path().join("junk.json"), b"{not json").unwrap();
        std::fs::write(dir.path().join("README"), b"ignored").unwrap();

        let reopened = StateStore::open(dir.path()).unwrap();
        assert_eq!(reopened.get(&a.volume_id), Some(a.clone()));
        assert_eq!(reopened.get(&b.volume_id), Some(b.clone()));
        assert_eq!(reopened.views_of(&a.pod), 2);
        reopened.remove(&b.volume_id).unwrap();
        reopened.remove(&b.volume_id).unwrap();
        let again = StateStore::open(dir.path()).unwrap();
        assert_eq!(again.all(), vec![a]);
    }
}
