//! Debugging aid (read-only): walk a filesystem's commit chain and print,
//! for each probed key, every commit at which its value changed (present
//! or absent, and the dentry's ino / the inode's nlink), with the commit's
//! author, epoch and `applied` position.
//!
//! `commit_probe BUCKET PREFIX FROM TO STEP KEY...`, `KEY` = `d:<parent>:<name>`
//! or `i:<ino>`. Credentials: the standard AWS chain (IMDS on EC2).
//! Only GET and LIST requests are made.

use constellation_mtree::{keys, record, Tree};
use constellation_store_s3::{CommitChain, NodeCache, PackStore, SHARD0};
use object_store::prefix::PrefixStore;
use object_store::ObjectStore;
use std::sync::Arc;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (bucket, prefix) = (args[1].clone(), args[2].clone());
    let from: u64 = args[3].parse().unwrap();
    let to: u64 = args[4].parse().unwrap();
    let step: usize = args[5].parse().unwrap();
    let probes: Vec<(String, Vec<u8>)> = args[6..]
        .iter()
        .map(|k| {
            let parts: Vec<&str> = k.splitn(3, ':').collect();
            let key = match parts[0] {
                "d" => keys::dentry(parts[1].parse().unwrap(), parts[2].as_bytes()),
                "i" => keys::inode(parts[1].parse().unwrap()),
                other => panic!("key kind {other}"),
            };
            (k.clone(), key)
        })
        .collect();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let store: Arc<dyn ObjectStore> = rt.block_on(async {
        let s3 = constellation_store_s3::amazon_s3_builder(&bucket)
            .await
            .unwrap()
            .build()
            .unwrap();
        Arc::new(PrefixStore::new(s3, prefix.as_str())) as Arc<dyn ObjectStore>
    });
    let dir = std::env::temp_dir().join(format!("commit-probe-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let cache = Arc::new(NodeCache::new(
        PackStore::new(store.clone()),
        Arc::new(constellation_fs_core::cache::DiskCache::open(&dir, 1 << 32).unwrap()),
        constellation_mtree::Hasher::Plain,
        rt.handle().clone(),
    ));
    let n = rt.block_on(cache.refresh_catalog()).unwrap();
    eprintln!("attached {n} pack indices");
    let tree = Tree::with_config(cache.clone(), record::config()).unwrap();
    let chain = CommitChain::new(store.clone());
    let mut last: Vec<Option<String>> = vec![None; probes.len()];
    for seq in (from..=to).step_by(step) {
        let Some(c) = rt.block_on(chain.get(seq)).unwrap() else {
            println!("commit {seq}: missing");
            continue;
        };
        let root = c.root(SHARD0).unwrap();
        for (i, (name, key)) in probes.iter().enumerate() {
            let v = tree.get(&root, key).unwrap();
            let desc = match v {
                None => "absent".to_string(),
                Some(bytes) if name.starts_with("d:") => {
                    match record::DentryRecord::decode(&bytes) {
                        Ok(d) => format!("ino {}", d.ino),
                        Err(e) => format!("undecodable {e}"),
                    }
                }
                Some(bytes) => match record::InodeRecord::decode(&bytes) {
                    Ok(r) => format!("nlink {} size {}", r.attrs.nlink, r.attrs.size),
                    Err(e) => format!("undecodable {e}"),
                },
            };
            if last[i].as_deref() != Some(desc.as_str()) {
                println!(
                    "commit {seq} author {} epoch {} applied {} ms {}: {name}: {desc}",
                    c.author, c.epoch, c.applied, c.unix_ms
                );
                last[i] = Some(desc);
            }
        }
    }
}
