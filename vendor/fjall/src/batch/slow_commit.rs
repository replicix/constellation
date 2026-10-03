// CONSTELLATION PATCH: a commit that takes long says where its time
// went (see CONSTELLATION-PATCH.md, change 5).
//
// A write transaction of a `SingleWriterTxDatabase` holds the database's
// single-writer lock until its commit returns, so a slow commit stalls
// every writer. Constellation saw commits of a few rows take up to 11 s
// under I/O pressure, nearly all of it in the memtable inserts: an insert
// takes the LSM tree's version-history lock (read), which a flush or a
// compaction holds (write) while it persists the new version — several
// fsyncs. This names the phase.

use std::time::{Duration, Instant};

/// A commit at least this long is reported.
const SLOW: Duration = Duration::from_millis(500);

#[derive(Clone, Copy)]
pub(super) enum Phase {
    /// Waiting for the journal writer's lock.
    JournalLock,
    /// Appending the batch to the journal.
    JournalWrite,
    /// The batch's own `durability` persist.
    Persist,
    /// The keyspace table's lock.
    KeyspacesLock,
    /// The memtable inserts (each takes its tree's version-history lock).
    Memtables,
    /// Memtable rotation and the write stall/halt back-pressure.
    Backpressure,
}

const PHASES: usize = 6;
const NAMES: [&str; PHASES] = [
    "journal_lock",
    "journal_write",
    "persist",
    "keyspaces_lock",
    "memtables",
    "backpressure",
];

pub(super) struct SlowCommit {
    started: Instant,
    last: Instant,
    took: [Duration; PHASES],
}

impl SlowCommit {
    pub(super) fn start() -> Self {
        let now = Instant::now();
        Self {
            started: now,
            last: now,
            took: [Duration::ZERO; PHASES],
        }
    }

    /// The phase that just ended.
    pub(super) fn mark(&mut self, phase: Phase) {
        let now = Instant::now();
        self.took[phase as usize] += now - self.last;
        self.last = now;
    }

    pub(super) fn report(&self, items: usize) {
        let total = self.last - self.started;
        if total < SLOW {
            return;
        }
        let phases: Vec<String> = NAMES
            .iter()
            .zip(self.took.iter())
            .filter(|(_, d)| !d.is_zero())
            .map(|(name, d)| format!("{name}={}ms", d.as_millis()))
            .collect();
        log::warn!(
            "slow commit: {} ms for {items} items ({})",
            total.as_millis(),
            phases.join(" "),
        );
    }
}
