//! Bucket key layout (DESIGN.md §2).

use constellation_fs_core::ChunkHash;
use object_store::path::Path;

pub fn meta_json() -> Path {
    Path::from("meta.json")
}

/// `chunks/<aa>/<bb>/<hex>`: two hex-pair shard levels. AWS partitions on
/// the full key (levels are for LIST parallelism and directory-backed
/// backends); see DESIGN.md §2.
pub fn chunk_key(hash: &ChunkHash) -> Path {
    let hex = hash.to_hex();
    Path::from(format!("chunks/{}/{}/{}", &hex[0..2], &hex[2..4], hex))
}

pub fn log_segment(partition: &str, seq: u64) -> Path {
    Path::from(format!("log/{partition}/{seq:016x}.zst"))
}

pub fn log_prefix(partition: &str) -> Path {
    Path::from(format!("log/{partition}"))
}

pub fn checkpoint(partition: &str, txid: u64) -> Path {
    Path::from(format!("checkpoints/{partition}/{txid:016x}.zst"))
}

pub fn lease(partition: &str) -> Path {
    Path::from(format!("leases/{partition}.json"))
}

pub fn registry(node_id: &str) -> Path {
    Path::from(format!("registry/{node_id}.json"))
}

pub fn hold(node_id: &str) -> Path {
    Path::from(format!("holds/{node_id}.json"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_key_sharding() {
        let h = ChunkHash::of(b"x");
        let hex = h.to_hex();
        let key = chunk_key(&h).to_string();
        assert_eq!(key, format!("chunks/{}/{}/{}", &hex[0..2], &hex[2..4], hex));
    }

    #[test]
    fn log_keys_sort_by_seq() {
        assert!(log_segment("p0", 1).to_string() < log_segment("p0", 2).to_string());
        assert!(log_segment("p0", 255).to_string() < log_segment("p0", 256).to_string());
    }
}
