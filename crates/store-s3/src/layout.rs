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

/// `packs/<hex>`: a sealed concatenation of zstd'd metadata nodes
/// (plan 28 §P8). Flat, not sharded like `chunks/`: a pack is 1–16 MiB
/// where a chunk is ~1 MiB of *one* object, so a census-scale bucket
/// holds thousands of packs rather than millions of chunks and the
/// LIST-parallelism the shard levels buy is not worth the extra path
/// components.
pub fn pack(hash_hex: &str) -> Path {
    Path::from(format!("packs/{hash_hex}"))
}

/// `packs/<hex>.idx`: the `(node hash, offset, len)` table for
/// [`pack`]. A sibling object rather than a footer inside the pack so
/// that a reader can fetch it without knowing the pack's size, and a
/// sibling rather than an inline field of the commit because a pack
/// outlives the commit that wrote it — see `packs.rs`.
pub fn pack_index(hash_hex: &str) -> Path {
    Path::from(format!("packs/{hash_hex}.idx"))
}

pub fn packs_prefix() -> Path {
    Path::from("packs")
}

/// `blobs/<hex>`: a metadata value that exceeded
/// `mtree::record::VALUE_SPILL` — a long symlink target, a big xattr, a
/// spilled chunk list (plan 28 §P6).
///
/// Its own prefix rather than a corner of `chunks/`, because the two
/// namespaces have different lifetimes and different sweepers. A chunk
/// is reachable from a manifest and is swept by the chunk-store rules;
/// a metadata blob is reachable only from an `mtree` node and must be
/// swept by §P10's mark from the commit roots. Filing one under
/// `chunks/` would hand it to a GC that cannot see what references it.
///
/// Flat like `packs/` and for the same reason: blobs are the rare case
/// by construction (§P6's budgets exist to keep them rare), so the
/// LIST-parallelism that `chunks/`'s shard levels buy is not worth the
/// extra path components.
pub fn blob(hash_hex: &str) -> Path {
    Path::from(format!("blobs/{hash_hex}"))
}

pub fn blobs_prefix() -> Path {
    Path::from("blobs")
}

/// `commits/<seq:016x>`: the one linearization point (§P2), CAS-created
/// and immutable. Zero-padded hex for the same reason log segments are:
/// lexicographic order is numeric order, so LIST-with-offset is a
/// numeric seek.
pub fn commit(seq: u64) -> Path {
    Path::from(format!("commits/{seq:016x}"))
}

pub fn commits_prefix() -> Path {
    Path::from("commits")
}

pub fn lease(partition: &str) -> Path {
    Path::from(format!("leases/{partition}.json"))
}

/// `designations/<hash-of-path>.json` (DESIGN.md §5.2). Hashed rather
/// than the literal path so an arbitrarily deep/long path never produces
/// an unwieldy or invalid object key.
pub fn designation(path_hash: &str) -> Path {
    Path::from(format!("designations/{path_hash}.json"))
}

pub fn designations_prefix() -> Path {
    Path::from("designations")
}

pub fn snapshot(id: &str) -> Path {
    Path::from(format!("snaps/{id}.json"))
}

pub fn snapshots_prefix() -> Path {
    Path::from("snaps")
}

pub fn registry(node_id: &str) -> Path {
    Path::from(format!("registry/{node_id}.json"))
}

pub fn hold(node_id: &str) -> Path {
    Path::from(format!("holds/{node_id}.json"))
}

pub fn gc_condemned() -> Path {
    Path::from("gc/condemned.json")
}

pub fn gc_journal(ts: i64, nonce: &str) -> Path {
    Path::from(format!("gc/journal/{ts:016x}-{nonce}.json"))
}

pub fn gc_journal_prefix() -> Path {
    Path::from("gc/journal")
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

    #[test]
    fn commit_keys_sort_by_seq() {
        assert_eq!(commit(1).to_string(), "commits/0000000000000001");
        assert!(commit(255).to_string() < commit(256).to_string());
        assert!(commit(u64::MAX - 1).to_string() < commit(u64::MAX).to_string());
    }

    #[test]
    fn a_pack_index_is_a_sibling_of_its_pack() {
        assert_eq!(pack("abc").to_string(), "packs/abc");
        assert_eq!(pack_index("abc").to_string(), "packs/abc.idx");
    }
}
