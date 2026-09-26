//! Debugging aid: decode a directory of downloaded log segments
//! (`log/p0/<16 hex>.zst`, as `aws s3 sync` leaves them) into JSON lines,
//! one per segment, with manifest and xattr bytes elided.
//!
//! `cargo run -p constellation-authority --example dump_log -- DIR > out.jsonl`

use constellation_store_s3::log::LogStore;
use object_store::memory::InMemory;
use serde_json::{json, Value};
use std::sync::Arc;

fn strip(v: &mut Value) {
    match v {
        Value::Object(m) => {
            for k in ["manifest", "base_manifest", "value", "nodes"] {
                if let Some(x) = m.get_mut(k) {
                    if x.is_array() {
                        *x = json!(format!("<{} bytes>", x.as_array().unwrap().len()));
                    }
                }
            }
            for (_, x) in m.iter_mut() {
                strip(x);
            }
        }
        Value::Array(a) => a.iter_mut().for_each(strip),
        _ => {}
    }
}

fn main() {
    let dir = std::env::args().nth(1).expect("DIR");
    let logs = LogStore::new(Arc::new(InMemory::new()));
    let mut names: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.ends_with(".zst"))
        .collect();
    names.sort();
    for n in names {
        let seq = u64::from_str_radix(n.trim_end_matches(".zst"), 16).unwrap();
        let body = std::fs::read(format!("{dir}/{n}")).unwrap();
        let out = match logs
            .open_segment(seq, &body)
            .map_err(|e| e.to_string())
            .and_then(|p| constellation_authority::segment::decode(&p).map_err(|e| e.to_string()))
        {
            Ok(s) => {
                let mut recs = serde_json::to_value(&s.records).unwrap();
                strip(&mut recs);
                json!({"seq": seq, "node": s.node, "epoch": s.epoch, "through": s.through,
                       "rows": s.rows, "origins": s.origins, "records": recs})
            }
            Err(e) => json!({"seq": seq, "error": e}),
        };
        println!("{out}");
    }
}
