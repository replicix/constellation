//! Conflict-copy naming for stranded ops (DESIGN.md §6 relaxed mode, §9).
//!
//! Plan 30 §M3b replaced reintegration's classify-against-a-side-replica
//! pass with rollback plus replay by rid (`store::spec`, `cli::recovery`):
//! a deposed holder's unshipped transactions are rolled back from their
//! before-images and re-executed, exactly once, through whoever holds the
//! lease now. A replay the current state refuses is a genuine overlap and
//! is still materialized under `.constellation-conflict/` — what remains
//! here is that directory's name and the naming rule for a copy. Conflicts
//! are never silent.

pub const CONFLICT_DIR: &str = ".constellation-conflict";

pub fn conflict_dentry_name(name: &str, node_id: u64, ts_unix: i64) -> String {
    format!("{name}@{node_id}-{ts_unix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflict_names_carry_node_and_time() {
        assert_eq!(
            conflict_dentry_name("foo", 7, 1_700_000_000),
            "foo@7-1700000000"
        );
        assert_eq!(CONFLICT_DIR, ".constellation-conflict");
    }
}
