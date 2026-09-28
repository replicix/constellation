//! The control-plane objects read back from the bucket as JSON — meta.json,
//! leases, designations, snapshot and condemned records, node registry
//! entries — plus the heartbeat promise and delegation-table codecs. Each
//! is whatever the bucket holds; decoding one must never panic, and the
//! values a decode admits must be safe in the arithmetic that consumes them.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(meta) = serde_json::from_slice::<constellation_store_s3::store::FsMeta>(data) {
        let _ = meta.gossip_seed();
        let _ = meta.compression.parse::<constellation_store_s3::CompressionSetting>();
        // What `load_fs` admits must survive every `/ chunk_size` downstream.
        if constellation_fs_core::validate_chunk_size(meta.chunk_size).is_ok() {
            let layout = constellation_fs_core::ChunkLayout::new(meta.chunk_size);
            let _ = layout.chunk_count(u64::MAX);
            let _ = layout.chunk_count(data.len() as u64);
        }
    }
    if let Ok(lease) = serde_json::from_slice::<constellation_store_s3::lease::Lease>(data) {
        let _ = lease.is_claimable(i64::MIN);
        let _ = lease.is_claimable(i64::MAX);
        let _ = lease.expires_in_ms(i64::MIN);
        if lease.epoch <= constellation_store_s3::lease::MAX_EPOCH {
            let _ = lease.fenced_for_retirement(1, 0);
        }
    }
    if let Ok(d) = serde_json::from_slice::<constellation_store_s3::Designation>(data) {
        let _ = d.overlaps("/");
        let _ = d.overlaps(&d.path);
        let _ = constellation_store_s3::designation::path_hash(&d.path);
    }
    let _ = serde_json::from_slice::<constellation_store_s3::snapshot::SnapshotRecord>(data);
    let _ = serde_json::from_slice::<constellation_store_s3::gc::CondemnedList>(data);
    let _ = serde_json::from_slice::<constellation_store_s3::nodes::NodeInfo>(data);
    let _ = constellation_store_s3::heartbeat::Promise::decode(data);
    let _ = constellation_meta::delegation::DelegationTable::decode(data);
});
