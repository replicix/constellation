//! Merkle-tree node parsing: bytes off a pack, a peer, or the disk cache.
//! `parse` is the untrusted-boundary validator; every accessor on an
//! accepted node must be total.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = constellation_mtree::keys::Key::parse(data);
    if let Ok(node) = constellation_mtree::node::NodeRef::parse(data) {
        for i in 0..node.count() {
            let _ = node.key(i);
            let _ = node.leaf_value(i);
            let _ = node.child(i);
        }
    }
});
