//! Jepsen-style history of invoke/complete events.

use crate::op::{Complete, Op};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Invoke,
    Ok,
    Fail,
    Info,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub index: u64,
    pub kind: EventKind,
    pub worker_id: usize,
    pub op_id: u64,
    /// Wall time in unix nanos (best-effort).
    pub time_ns: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub op: Option<Op>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub complete: Option<Complete>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<String>,
}

#[derive(Debug, Default)]
pub struct History {
    events: Vec<Event>,
    next_index: u64,
}

impl History {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn events(&self) -> &[Event] {
        &self.events
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    fn now_ns() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }

    pub fn record_invoke(&mut self, worker_id: usize, op_id: u64, op: Op) {
        let index = self.next_index;
        self.next_index += 1;
        self.events.push(Event {
            index,
            kind: EventKind::Invoke,
            worker_id,
            op_id,
            time_ns: Self::now_ns(),
            op: Some(op),
            complete: None,
            info: None,
        });
    }

    pub fn record_complete(&mut self, worker_id: usize, op_id: u64, complete: Complete) {
        let index = self.next_index;
        self.next_index += 1;
        let kind = match complete.outcome {
            crate::op::Outcome::Ok => EventKind::Ok,
            crate::op::Outcome::Fail => EventKind::Fail,
        };
        self.events.push(Event {
            index,
            kind,
            worker_id,
            op_id,
            time_ns: Self::now_ns(),
            op: None,
            complete: Some(complete),
            info: None,
        });
    }

    pub fn record_info(&mut self, info: impl Into<String>) {
        let index = self.next_index;
        self.next_index += 1;
        self.events.push(Event {
            index,
            kind: EventKind::Info,
            worker_id: usize::MAX,
            op_id: 0,
            time_ns: Self::now_ns(),
            op: None,
            complete: None,
            info: Some(info.into()),
        });
    }

    pub fn write_jsonl(&self, path: &Path) -> anyhow::Result<()> {
        let f = File::create(path)?;
        let mut w = BufWriter::new(f);
        for ev in &self.events {
            serde_json::to_writer(&mut w, ev)?;
            w.write_all(b"\n")?;
        }
        w.flush()?;
        Ok(())
    }

    pub fn load_jsonl(path: &Path) -> anyhow::Result<Self> {
        let f = File::open(path)?;
        let reader = BufReader::new(f);
        let mut hist = Self::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let ev: Event = serde_json::from_str(&line)?;
            hist.next_index = hist.next_index.max(ev.index + 1);
            hist.events.push(ev);
        }
        Ok(hist)
    }
}
