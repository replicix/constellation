//! Packed metadata-node blobs: `packs/<hex>` plus a sibling
//! `packs/<hex>.idx` (plan 28 §P8).
//!
//! A metadata node is ~8 KiB (§14.1 measured 9.35 KiB at level 0) and a
//! census-scale filesystem has hundreds of thousands of them, so
//! addressing each one as its own S3 object is the shape the plan 26
//! Appendix measured and rejected: 64 packed 1 MiB blobs beat 4096 loose
//! ones by 4.7× on the far AWS path and beat everything same-region.
//! Nodes therefore go into packs, and a single-node read is a *ranged*
//! GET into a pack — one request either way, but the pack is what makes
//! the *next* read of an adjacent node free.
//!
//! ## Pack in key order, because that is where the locality comes from
//!
//! [`build_packs`] sorts by `(level, first key)` before filling, and
//! that sort is the whole point rather than tidiness. §P6 chooses the
//! key encoding so that a directory's dentries are one contiguous key
//! range; sorting by key at pack time turns that into *pack* locality,
//! and §14.2 measured the payoff — a cold `ls -la` of a 2,330-entry
//! directory touches **one distinct pack**. Fill packs in any other
//! order (hash order, arrival order) and the same `ls -la` becomes one
//! ranged GET per leaf. `one_directory_is_one_pack` in `node_cache.rs`
//! is the test that holds this property down.
//!
//! Level participates in the sort ahead of the key so interior nodes
//! cluster with interior nodes: they are read on every descent, stay
//! resident (§14.1's 19.75 MiB for the whole census-scale interior),
//! and a bootstrap that wants only the interior — ADR-5's partial
//! replica, S6's job — can then fetch a handful of whole packs instead
//! of ranges scattered through every leaf pack.
//!
//! ## Why the index is a sibling object and not a field of the commit
//!
//! §P8 offers both. The sibling wins for one structural reason: **a
//! pack outlives the commit that wrote it.** A commit names only the
//! packs *it* created; the overwhelming majority of the nodes a reader
//! resolves live in packs written by ancestors, most of them long
//! outside any retained commit window (§P10b bounds commit retention
//! precisely because commits are cheap to delete). An inline index
//! would therefore mean "to read a node, first find the commit that
//! introduced its pack", which is an unbounded walk back through
//! history — and impossible once retention has deleted that commit,
//! even though the pack is still live and still reachable. Three
//! secondary consequences all point the same way:
//!
//! - the commit object stays O(new packs) instead of O(nodes written).
//!   A 10k-op commit rewrites 9.12 MiB of nodes (§14.5) — roughly 1,100
//!   nodes, so ~44 KiB of index entries in an object that is otherwise
//!   a few hundred bytes;
//! - the index's lifetime is exactly the pack's, so S7's sweep and
//!   compaction delete a pair and have no cross-object bookkeeping to
//!   get wrong;
//! - a cold reader can fetch a few KiB of index without touching the
//!   1–16 MiB body, which is what a partial replica needs.
//!
//! The cost is honest and small: writing a pack is two PUTs, and
//! reading a whole pack cold is two GETs. Both are amortized over ~128
//! nodes.
//!
//! The index is *untrusted*. It says where to find bytes; it never says
//! what they are. Every node that comes out of a pack is hashed and
//! compared against the hash the caller asked for, and then parsed with
//! [`constellation_mtree::NodeRef::parse`] — see `node_cache.rs`. A
//! corrupt or lying index produces a failed read, never a wrong answer.
//!
//! ## §P13 hooks
//!
//! Both objects carry a `flags` byte and two reserved bytes in their
//! header. `FLAG_SEALED_NODES` is defined and *refused* on read, so
//! turning on AEAD sealing later is a flag flip plus a seal/open pair
//! around the per-node frames, not a format version bump. Keyed
//! addressing (the other half of §P13) needs nothing here at all: node
//! identity is whatever `mtree::Hasher` computes, and this module only
//! ever compares it.

use crate::error::StoreError;
use crate::layout;
use constellation_mtree::{MtreeError, NodeHash, NodeRef};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use std::sync::Arc;

const ZSTD_LEVEL: i32 = 3;

pub const PACK_MAGIC: [u8; 4] = *b"CPK1";
pub const PACK_INDEX_MAGIC: [u8; 4] = *b"CPI1";
pub const PACK_FORMAT_VERSION: u8 = 1;

/// Per-node AEAD sealing (§P13). Reserved, never set by this code, and
/// refused on read so that a future writer cannot be silently
/// misread by a reader that predates it.
pub const FLAG_SEALED_NODES: u8 = 0x01;

/// magic(4) + version(1) + flags(1) + reserved(2) + node count(4).
const HEADER_LEN: usize = 12;

/// hash(32) + level(1) + key len(2) + offset(4) + clen(4) + len(4).
const ENTRY_FIXED_LEN: usize = 47;

/// Default target for a sealed pack body, from §P8's 1–16 MiB range and
/// the plan 26 Appendix's measurement that 1 MiB pieces already capture
/// most of the packing win. 4 MiB sits above the point where per-request
/// overhead matters and below the point where a compaction rewrite
/// (§14.5: 117% of the commit write rate) starts moving pointless bytes.
pub const DEFAULT_PACK_TARGET_BYTES: usize = 4 << 20;
pub const MIN_PACK_TARGET_BYTES: usize = 1 << 20;
pub const MAX_PACK_TARGET_BYTES: usize = 16 << 20;

/// Env `CONSTELLATION_PACK_TARGET_BYTES`: sealed size a pack is filled
/// to before the next one is started, clamped to §P8's 1–16 MiB.
/// Unparseable or out of range falls back to the default.
pub fn pack_target_bytes() -> usize {
    std::env::var("CONSTELLATION_PACK_TARGET_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| (MIN_PACK_TARGET_BYTES..=MAX_PACK_TARGET_BYTES).contains(v))
        .unwrap_or(DEFAULT_PACK_TARGET_BYTES)
}

/// Content address of a pack object: blake3 of the sealed body.
///
/// Deliberately not a [`NodeHash`] and not a `ChunkHash`: a pack is not
/// a node and not a chunk, it is the container both may travel in, and
/// the three namespaces are swept by different rules (§P10). Mixing
/// them up should not typecheck.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PackHash(pub [u8; 32]);

impl PackHash {
    pub fn of(body: &[u8]) -> PackHash {
        PackHash(*blake3::hash(body).as_bytes())
    }

    pub fn to_hex(self) -> String {
        let mut s = String::with_capacity(64);
        for b in self.0 {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }

    pub fn from_hex(hex: &str) -> Option<PackHash> {
        if hex.len() != 64 {
            return None;
        }
        let mut out = [0u8; 32];
        for (i, pair) in hex.as_bytes().chunks(2).enumerate() {
            let hi = (pair[0] as char).to_digit(16)?;
            let lo = (pair[1] as char).to_digit(16)?;
            out[i] = ((hi << 4) | lo) as u8;
        }
        Some(PackHash(out))
    }
}

impl std::fmt::Display for PackHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl std::fmt::Debug for PackHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PackHash({})", &self.to_hex()[..12])
    }
}

/// A node on its way into a pack.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackNode {
    pub hash: NodeHash,
    pub level: u8,
    /// The node's first key — the sort key that produces §14.2's
    /// one-pack-per-directory property. An empty node (only the empty
    /// tree's root) has none, and sorts first.
    pub first_key: Vec<u8>,
    /// Encoded, uncompressed node bytes, exactly as `mtree` produced
    /// them: `hash` is their hash.
    pub bytes: Vec<u8>,
}

impl PackNode {
    /// Read `level` and `first_key` off the encoded node itself, so a
    /// caller cannot describe a node as something it is not.
    pub fn from_bytes(hash: NodeHash, bytes: Vec<u8>) -> Result<PackNode, MtreeError> {
        let (level, first_key) = {
            let node = NodeRef::new(&bytes)?;
            let first_key = match node.count() {
                0 => Vec::new(),
                _ => node.key(0)?.to_vec(),
            };
            (node.level(), first_key)
        };
        Ok(PackNode {
            hash,
            level,
            first_key,
            bytes,
        })
    }
}

/// Where one node lives inside a pack.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackEntry {
    pub hash: NodeHash,
    pub level: u8,
    pub first_key: Vec<u8>,
    /// Absolute offset of the compressed frame within the pack object,
    /// so it can be handed to a ranged GET with no arithmetic.
    pub offset: u32,
    /// Compressed length; `offset..offset + compressed_len` is the
    /// range to fetch.
    pub compressed_len: u32,
    /// Uncompressed length, so a reader can size its buffer and refuse
    /// an implausible zstd frame before decoding it.
    pub len: u32,
}

/// The `(node hash, offset, len)` table of one pack.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PackIndex {
    pub entries: Vec<PackEntry>,
}

impl PackIndex {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + self.entries.len() * 48);
        out.extend_from_slice(&PACK_INDEX_MAGIC);
        out.push(PACK_FORMAT_VERSION);
        out.push(0); // flags
        out.extend_from_slice(&[0u8; 2]); // reserved
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for entry in &self.entries {
            out.extend_from_slice(&entry.hash.0);
            out.push(entry.level);
            out.extend_from_slice(&(entry.first_key.len() as u16).to_le_bytes());
            out.extend_from_slice(&entry.offset.to_le_bytes());
            out.extend_from_slice(&entry.compressed_len.to_le_bytes());
            out.extend_from_slice(&entry.len.to_le_bytes());
            out.extend_from_slice(&entry.first_key);
        }
        out
    }

    pub fn decode(buf: &[u8]) -> Result<PackIndex, StoreError> {
        let count = decode_header(buf, &PACK_INDEX_MAGIC, "pack index")?;
        let mut at = HEADER_LEN;
        let mut entries = Vec::with_capacity(count.min(1 << 16));
        for _ in 0..count {
            let fixed = buf
                .get(at..at + ENTRY_FIXED_LEN)
                .ok_or_else(|| StoreError::CorruptObject("pack index entry is truncated".into()))?;
            let hash = NodeHash(fixed[0..32].try_into().expect("32 bytes"));
            let level = fixed[32];
            let key_len = u16::from_le_bytes([fixed[33], fixed[34]]) as usize;
            let offset = u32::from_le_bytes(fixed[35..39].try_into().expect("4 bytes"));
            let compressed_len = u32::from_le_bytes(fixed[39..43].try_into().expect("4 bytes"));
            let len = u32::from_le_bytes(fixed[43..47].try_into().expect("4 bytes"));
            at += ENTRY_FIXED_LEN;
            let first_key = buf
                .get(at..at + key_len)
                .ok_or_else(|| StoreError::CorruptObject("pack index key is truncated".into()))?
                .to_vec();
            at += key_len;
            entries.push(PackEntry {
                hash,
                level,
                first_key,
                offset,
                compressed_len,
                len,
            });
        }
        Ok(PackIndex { entries })
    }
}

fn decode_header(buf: &[u8], magic: &[u8; 4], what: &str) -> Result<usize, StoreError> {
    if buf.len() < HEADER_LEN {
        return Err(StoreError::CorruptObject(format!(
            "{what} is shorter than a header"
        )));
    }
    if &buf[0..4] != magic {
        return Err(StoreError::CorruptObject(format!("{what} has bad magic")));
    }
    if buf[4] != PACK_FORMAT_VERSION {
        return Err(StoreError::CorruptObject(format!(
            "{what} is format version {}, this build reads {PACK_FORMAT_VERSION}",
            buf[4]
        )));
    }
    if buf[5] & FLAG_SEALED_NODES != 0 {
        return Err(StoreError::CorruptObject(format!(
            "{what} holds AEAD-sealed nodes, which this build cannot open"
        )));
    }
    Ok(u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]) as usize)
}

/// A pack assembled in memory and not yet durable.
#[derive(Clone, Debug)]
pub struct BuiltPack {
    pub hash: PackHash,
    pub body: Vec<u8>,
    pub index: PackIndex,
}

impl BuiltPack {
    /// Nodes in this pack, in pack order.
    pub fn nodes(&self) -> impl Iterator<Item = &PackEntry> {
        self.index.entries.iter()
    }
}

/// Assemble `nodes` into packs of roughly `target_bytes`, in key order.
///
/// `target_bytes` is taken as given rather than clamped to §P8's
/// 1–16 MiB: [`pack_target_bytes`] is where the operator-facing knob is
/// validated, and a caller that computes a size from its own shape (or
/// a test that wants several packs out of a small tree) is not the
/// thing that range exists to protect against.
///
/// Duplicate hashes are dropped: nodes are immutable and
/// content-addressed, so packing one twice would only waste bytes.
/// A node whose compressed frame alone exceeds the target still gets a
/// pack of its own rather than being refused — `MAX_ENTRIES` bounds a
/// node at ~120 KiB (§14.1), so this is a theoretical branch that must
/// nevertheless not be a panic.
pub fn build_packs(
    mut nodes: Vec<PackNode>,
    target_bytes: usize,
) -> Result<Vec<BuiltPack>, StoreError> {
    nodes.sort_by(|a, b| {
        a.level
            .cmp(&b.level)
            .then_with(|| a.first_key.cmp(&b.first_key))
            .then_with(|| a.hash.0.cmp(&b.hash.0))
    });
    nodes.dedup_by(|a, b| a.hash == b.hash);

    let target = target_bytes.max(1);
    let mut packs = Vec::new();
    let mut body: Vec<u8> = Vec::new();
    let mut index = PackIndex::default();

    for node in nodes {
        if body.is_empty() {
            body.extend_from_slice(&PACK_MAGIC);
            body.push(PACK_FORMAT_VERSION);
            body.push(0); // flags: §P13 sealing off
            body.extend_from_slice(&[0u8; 2]); // reserved
            body.extend_from_slice(&[0u8; 4]); // node count, patched at seal
        }
        let frame = zstd::encode_all(&node.bytes[..], ZSTD_LEVEL)
            .map_err(|e| StoreError::Compression(e.to_string()))?;
        let offset = u32::try_from(body.len())
            .map_err(|_| StoreError::CorruptObject("pack grew past 4 GiB".into()))?;
        let compressed_len = u32::try_from(frame.len())
            .map_err(|_| StoreError::CorruptObject("node frame grew past 4 GiB".into()))?;
        body.extend_from_slice(&frame);
        index.entries.push(PackEntry {
            hash: node.hash,
            level: node.level,
            first_key: node.first_key,
            offset,
            compressed_len,
            len: node.bytes.len() as u32,
        });
        if body.len() >= target {
            packs.push(seal(std::mem::take(&mut body), std::mem::take(&mut index)));
        }
    }
    if !body.is_empty() {
        packs.push(seal(body, index));
    }
    Ok(packs)
}

/// [`build_packs`], with the compression and the sealing spread across
/// `threads` (§S7a; `0` means one per core).
///
/// This exists for one measured reason. §14.9 found the reachability
/// mark parallelizing ~4× while compaction topped out at ~1.7×,
/// "because the pack writer is serial" — and §14.5 found the compactor
/// having to rewrite 117% of the bytes the commit path itself writes.
/// A serial writer therefore sets a floor on steady-state GC cost that
/// no amount of parallel *reading* can lift.
///
/// The parallelism is split in three, and the middle step is the reason
/// this is not simply `par_iter` over `build_packs`:
///
/// 1. every node's zstd frame is computed in parallel. This is
///    essentially all of the CPU: the rest is `memcpy` and blake3.
/// 2. the pack boundaries are then decided **serially**, by exactly
///    [`build_packs`]' rule — seal once the body reaches `target_bytes`.
///    That pass is O(nodes) of integer arithmetic over already-known
///    frame lengths, with no I/O and no hashing, so it is not a
///    meaningful serial fraction; and keeping it serial is what makes
///    the output *identical* to [`build_packs`], which
///    `concurrent_and_serial_builds_agree_byte_for_byte` asserts. A
///    scheme that partitioned the node list up front to avoid this pass
///    would cut packs at different places, and pack composition is
///    exactly what §14.2's one-directory-one-pack property is about.
/// 3. the independent output packs are assembled, count-patched and
///    hashed in parallel.
///
/// Callers that rewrite pack after pack should not pay for a fresh pool
/// each time; `compact.rs` holds one for its lifetime and uses the
/// crate-internal entry point below.
pub fn build_packs_concurrent(
    nodes: Vec<PackNode>,
    target_bytes: usize,
    threads: usize,
) -> Result<Vec<BuiltPack>, StoreError> {
    let pool = crate::parallel::thread_pool(threads)?;
    build_packs_in(&pool, nodes, target_bytes)
}

pub(crate) fn build_packs_in(
    pool: &rayon::ThreadPool,
    mut nodes: Vec<PackNode>,
    target_bytes: usize,
) -> Result<Vec<BuiltPack>, StoreError> {
    use rayon::prelude::*;

    // At width 1 the two-phase shape is a pure loss and measurably so:
    // holding every frame in its own allocation costs one extra
    // `Vec` per node and one extra copy of every byte, which
    // [`build_packs`] avoids by compressing straight into the body it
    // is filling. Measured on a 4-core host, 259 MiB of nodes: 1.75 s
    // serial against 2.66 s for the concurrent path pinned to one
    // thread. The cost buys parallelism, so pay it only when there is
    // parallelism to buy.
    if pool.current_num_threads() < 2 {
        return build_packs(nodes, target_bytes);
    }

    nodes.sort_by(|a, b| {
        a.level
            .cmp(&b.level)
            .then_with(|| a.first_key.cmp(&b.first_key))
            .then_with(|| a.hash.0.cmp(&b.hash.0))
    });
    nodes.dedup_by(|a, b| a.hash == b.hash);
    if nodes.is_empty() {
        return Ok(Vec::new());
    }
    let target = target_bytes.max(1);

    let frames: Vec<Vec<u8>> = pool.install(|| {
        nodes
            .par_iter()
            .map(|node| {
                zstd::encode_all(&node.bytes[..], ZSTD_LEVEL)
                    .map_err(|e| StoreError::Compression(e.to_string()))
            })
            .collect::<Result<Vec<_>, StoreError>>()
    })?;

    let mut cuts: Vec<std::ops::Range<usize>> = Vec::new();
    let mut start = 0usize;
    let mut body_len = HEADER_LEN;
    for (i, frame) in frames.iter().enumerate() {
        body_len += frame.len();
        if body_len >= target {
            cuts.push(start..i + 1);
            start = i + 1;
            body_len = HEADER_LEN;
        }
    }
    if start < nodes.len() {
        cuts.push(start..nodes.len());
    }

    pool.install(|| {
        cuts.par_iter()
            .map(|range| assemble(&nodes[range.clone()], &frames[range.clone()]))
            .collect::<Result<Vec<_>, StoreError>>()
    })
}

/// One output pack from an already-compressed run of nodes. Byte-for-byte
/// what [`build_packs`]' inner loop would have produced for the same run.
fn assemble(nodes: &[PackNode], frames: &[Vec<u8>]) -> Result<BuiltPack, StoreError> {
    let mut body: Vec<u8> =
        Vec::with_capacity(HEADER_LEN + frames.iter().map(|frame| frame.len()).sum::<usize>());
    body.extend_from_slice(&PACK_MAGIC);
    body.push(PACK_FORMAT_VERSION);
    body.push(0); // flags: §P13 sealing off
    body.extend_from_slice(&[0u8; 2]); // reserved
    body.extend_from_slice(&[0u8; 4]); // node count, patched at seal
    let mut index = PackIndex::default();
    for (node, frame) in nodes.iter().zip(frames) {
        let offset = u32::try_from(body.len())
            .map_err(|_| StoreError::CorruptObject("pack grew past 4 GiB".into()))?;
        let compressed_len = u32::try_from(frame.len())
            .map_err(|_| StoreError::CorruptObject("node frame grew past 4 GiB".into()))?;
        body.extend_from_slice(frame);
        index.entries.push(PackEntry {
            hash: node.hash,
            level: node.level,
            first_key: node.first_key.clone(),
            offset,
            compressed_len,
            len: node.bytes.len() as u32,
        });
    }
    Ok(seal(body, index))
}

fn seal(mut body: Vec<u8>, index: PackIndex) -> BuiltPack {
    body[8..12].copy_from_slice(&(index.entries.len() as u32).to_le_bytes());
    BuiltPack {
        hash: PackHash::of(&body),
        index,
        body,
    }
}

/// Read and write `packs/*` against one bucket prefix.
///
/// Cheap to clone: everything behind it is an `Arc`.
#[derive(Clone)]
pub struct PackStore {
    store: Arc<dyn ObjectStore>,
    target_bytes: usize,
}

impl PackStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> PackStore {
        PackStore {
            store,
            target_bytes: pack_target_bytes(),
        }
    }

    /// Override the fill target. For tests that need several packs out
    /// of a small tree, and for a caller that knows its own shape
    /// better than the env var does.
    pub fn with_target_bytes(mut self, target_bytes: usize) -> PackStore {
        self.target_bytes = target_bytes;
        self
    }

    pub fn target_bytes(&self) -> usize {
        self.target_bytes
    }

    pub fn inner(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    /// Make a pack durable: body first, then index.
    ///
    /// The order matters and the reverse would be a bug. An index
    /// naming a body that does not exist is a pack that reads as
    /// corrupt; a body with no index yet is merely unreachable, which
    /// is the same state a crash between the two leaves and which S7's
    /// reachability sweep reclaims either way.
    ///
    /// Both writes are `Create`: packs are content-addressed, so a
    /// second writer producing the same pack is producing the same
    /// bytes and an `AlreadyExists` is success, not a conflict. That is
    /// what makes a retry after a partial failure converge instead of
    /// duplicating.
    pub async fn put_pack(&self, pack: &BuiltPack) -> Result<(), StoreError> {
        let hex = pack.hash.to_hex();
        self.put_created(&layout::pack(&hex), pack.body.clone())
            .await?;
        self.put_created(&layout::pack_index(&hex), pack.index.encode())
            .await
    }

    async fn put_created(
        &self,
        key: &object_store::path::Path,
        body: Vec<u8>,
    ) -> Result<(), StoreError> {
        match self
            .store
            .put_opts(
                key,
                PutPayload::from(body),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_index(&self, hash: &PackHash) -> Result<PackIndex, StoreError> {
        let key = layout::pack_index(&hash.to_hex());
        let body = self.store.get(&key).await?.bytes().await?;
        PackIndex::decode(&body)
    }

    /// One node, by ranged GET.
    ///
    /// Returns the *uncompressed* node bytes and nothing else: verifying
    /// the hash and parsing the structure belong to the caller, which is
    /// the only party that knows which hash it asked for. `node_cache.rs`
    /// is that caller and does both.
    pub async fn get_node_bytes(
        &self,
        pack: &PackHash,
        offset: u32,
        compressed_len: u32,
    ) -> Result<Vec<u8>, StoreError> {
        let key = layout::pack(&pack.to_hex());
        let range = offset as u64..(offset as u64 + compressed_len as u64);
        let frame = self.store.get_range(&key, range).await?;
        zstd::decode_all(&frame[..]).map_err(|e| StoreError::Compression(e.to_string()))
    }

    /// Whole pack body, for compaction and `fsck` (S7, S6).
    pub async fn get_body(&self, hash: &PackHash) -> Result<Vec<u8>, StoreError> {
        let key = layout::pack(&hash.to_hex());
        Ok(self.store.get(&key).await?.bytes().await?.to_vec())
    }

    /// Whether both objects of a pack are present. The reachability
    /// check the crash-ordering invariant is stated in terms of.
    pub async fn contains(&self, hash: &PackHash) -> Result<bool, StoreError> {
        let hex = hash.to_hex();
        for key in [layout::pack(&hex), layout::pack_index(&hex)] {
            match self.store.head(&key).await {
                Ok(_) => {}
                Err(object_store::Error::NotFound { .. }) => return Ok(false),
                Err(e) => return Err(e.into()),
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_mtree::{node, Entry};
    use object_store::memory::InMemory;

    fn pack_node(level: u8, keys: &[&[u8]]) -> PackNode {
        let entries: Vec<Entry> = keys
            .iter()
            .map(|k| match level {
                0 => Entry::leaf(k.to_vec(), b"v"),
                _ => Entry::child(k.to_vec(), NodeHash([1u8; 32]), Default::default()),
            })
            .collect();
        let bytes = node::encode(level, &entries);
        let hash = NodeHash(*blake3::hash(&bytes).as_bytes());
        PackNode::from_bytes(hash, bytes).unwrap()
    }

    #[test]
    fn index_round_trips_including_keys_and_extents() {
        let index = PackIndex {
            entries: vec![
                PackEntry {
                    hash: NodeHash([1u8; 32]),
                    level: 0,
                    first_key: b"aardvark".to_vec(),
                    offset: 12,
                    compressed_len: 900,
                    len: 8192,
                },
                PackEntry {
                    hash: NodeHash([2u8; 32]),
                    level: 7,
                    first_key: Vec::new(),
                    offset: 912,
                    compressed_len: 40,
                    len: 10,
                },
            ],
        };
        assert_eq!(PackIndex::decode(&index.encode()).unwrap(), index);
    }

    #[test]
    fn a_truncated_or_alien_index_is_an_error_not_a_panic() {
        let index = PackIndex {
            entries: vec![PackEntry {
                hash: NodeHash([3u8; 32]),
                level: 0,
                first_key: b"k".to_vec(),
                offset: 12,
                compressed_len: 5,
                len: 5,
            }],
        };
        let encoded = index.encode();
        for cut in 0..encoded.len() {
            assert!(PackIndex::decode(&encoded[..cut]).is_err(), "cut at {cut}");
        }
        let mut wrong_magic = encoded.clone();
        wrong_magic[0] = b'X';
        assert!(PackIndex::decode(&wrong_magic).is_err());
        let mut future_version = encoded.clone();
        future_version[4] = PACK_FORMAT_VERSION + 1;
        assert!(PackIndex::decode(&future_version).is_err());
    }

    /// §P13's hook: a future writer that seals node frames sets the
    /// flag, and a reader that predates sealing must refuse the object
    /// rather than hand back ciphertext that fails a hash check with a
    /// confusing message.
    #[test]
    fn a_sealed_pack_is_refused_by_a_build_that_cannot_open_it() {
        let mut encoded = PackIndex::default().encode();
        encoded[5] = FLAG_SEALED_NODES;
        let err = PackIndex::decode(&encoded).unwrap_err().to_string();
        assert!(err.contains("sealed"), "{err}");
    }

    #[test]
    fn packs_are_filled_in_key_order_within_a_level() {
        let nodes = vec![
            pack_node(0, &[b"d", b"e"]),
            pack_node(1, &[b"a"]),
            pack_node(0, &[b"a", b"b"]),
            pack_node(0, &[b"f"]),
        ];
        let packs = build_packs(nodes, MIN_PACK_TARGET_BYTES).unwrap();
        assert_eq!(packs.len(), 1, "four tiny nodes fit one pack");
        let order: Vec<(u8, Vec<u8>)> = packs[0]
            .nodes()
            .map(|e| (e.level, e.first_key.clone()))
            .collect();
        assert_eq!(
            order,
            vec![
                (0, b"a".to_vec()),
                (0, b"d".to_vec()),
                (0, b"f".to_vec()),
                (1, b"a".to_vec()),
            ]
        );
    }

    #[test]
    fn a_repeated_node_is_packed_once() {
        let node = pack_node(0, &[b"a"]);
        let packs = build_packs(
            vec![node.clone(), node.clone(), node],
            MIN_PACK_TARGET_BYTES,
        )
        .unwrap();
        assert_eq!(packs[0].index.entries.len(), 1);
    }

    /// The operator-facing knob is where §P8's 1–16 MiB range is
    /// enforced: an out-of-range env var must not be able to turn
    /// packing off by asking for 4 KiB packs.
    #[test]
    fn the_env_knob_refuses_a_target_outside_the_plan_range() {
        // Cannot set env vars safely from a test that runs in parallel
        // with others, so exercise the predicate the reader applies.
        let accepted = |v: usize| (MIN_PACK_TARGET_BYTES..=MAX_PACK_TARGET_BYTES).contains(&v);
        assert!(!accepted(4096));
        assert!(!accepted(1 << 30));
        assert!(accepted(DEFAULT_PACK_TARGET_BYTES));
        assert!(accepted(MIN_PACK_TARGET_BYTES));
        assert!(accepted(MAX_PACK_TARGET_BYTES));
    }

    #[test]
    fn a_full_pack_is_sealed_and_the_next_one_started() {
        let nodes: Vec<PackNode> = (0u32..400)
            .map(|i| {
                let key = i.to_be_bytes();
                pack_node(0, &[&key])
            })
            .collect();
        let one = build_packs(nodes.clone(), 1 << 20).unwrap();
        assert_eq!(one.len(), 1);
        let many = build_packs(nodes, 1024).unwrap();
        assert!(many.len() > 4, "{}", many.len());
        // Every node lands in exactly one pack, and key order survives
        // the split: each pack's range is above the previous one's.
        let mut last: Option<Vec<u8>> = None;
        let mut total = 0usize;
        for pack in &many {
            total += pack.index.entries.len();
            let first = pack.index.entries[0].first_key.clone();
            assert!(last.as_ref().is_none_or(|prev| *prev < first));
            last = Some(pack.index.entries.last().unwrap().first_key.clone());
        }
        assert_eq!(total, 400);
    }

    #[tokio::test]
    async fn a_node_round_trips_through_a_ranged_get() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let packs = PackStore::new(store);
        let node = pack_node(0, &[b"alpha", b"beta"]);
        let built = build_packs(vec![node.clone()], MIN_PACK_TARGET_BYTES).unwrap();
        packs.put_pack(&built[0]).await.unwrap();
        assert!(packs.contains(&built[0].hash).await.unwrap());

        let index = packs.get_index(&built[0].hash).await.unwrap();
        assert_eq!(index, built[0].index);
        let entry = &index.entries[0];
        let bytes = packs
            .get_node_bytes(&built[0].hash, entry.offset, entry.compressed_len)
            .await
            .unwrap();
        assert_eq!(bytes, node.bytes);
        assert_eq!(NodeHash(*blake3::hash(&bytes).as_bytes()), node.hash);
    }

    #[tokio::test]
    async fn writing_the_same_pack_twice_converges() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let packs = PackStore::new(store);
        let built = build_packs(vec![pack_node(0, &[b"a"])], MIN_PACK_TARGET_BYTES).unwrap();
        packs.put_pack(&built[0]).await.unwrap();
        packs.put_pack(&built[0]).await.unwrap();
        assert!(packs.contains(&built[0].hash).await.unwrap());
    }

    #[tokio::test]
    async fn an_absent_pack_is_not_contained() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let packs = PackStore::new(store);
        assert!(!packs.contains(&PackHash([9u8; 32])).await.unwrap());
    }

    /// The concurrent writer is an optimization, not a second format.
    /// If it ever cut packs differently it would change pack
    /// composition — which is what §14.2's one-directory-one-pack
    /// property is a statement about — so "faster" has to mean
    /// "identical bytes".
    #[test]
    fn concurrent_and_serial_builds_agree_byte_for_byte() {
        for count in [0u32, 1, 7, 400] {
            let nodes: Vec<PackNode> = (0..count)
                .map(|i| {
                    let key = i.to_be_bytes();
                    pack_node(if i % 5 == 0 { 1 } else { 0 }, &[&key])
                })
                .collect();
            for target in [1usize, 1024, 8192, MIN_PACK_TARGET_BYTES] {
                for threads in [1usize, 4] {
                    let serial = build_packs(nodes.clone(), target).unwrap();
                    let concurrent =
                        build_packs_concurrent(nodes.clone(), target, threads).unwrap();
                    assert_eq!(
                        serial.len(),
                        concurrent.len(),
                        "{count} nodes, target {target}, {threads} threads"
                    );
                    for (a, b) in serial.iter().zip(&concurrent) {
                        assert_eq!(a.hash, b.hash);
                        assert_eq!(a.body, b.body);
                        assert_eq!(a.index, b.index);
                    }
                }
            }
        }
    }

    #[test]
    fn pack_hex_round_trips() {
        let hash = PackHash::of(b"body");
        assert_eq!(PackHash::from_hex(&hash.to_hex()), Some(hash));
        assert_eq!(PackHash::from_hex("nope"), None);
    }
}
