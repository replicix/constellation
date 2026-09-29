//! Bounded daemon-log capture for the control plane.
//!
//! The formatter may split one event across several `Write::write` calls, so
//! the ring keeps a partial byte line until newline. Capture is best-effort:
//! logging must never fail the daemon because an operator panel is slow.

use std::collections::VecDeque;
use std::io::Write;
use std::sync::{Arc, Mutex};

const CAPACITY: usize = 2_000;

#[derive(Clone, Default)]
pub struct LogBuffer(Arc<Mutex<State>>);

#[derive(Default)]
struct State {
    lines: VecDeque<String>,
    partial: Vec<u8>,
    /// Lines ever completed (the position `since` counts from).
    total: u64,
}

impl LogBuffer {
    pub fn writer(&self) -> LogWriter {
        LogWriter(self.clone())
    }

    pub fn tail(&self, count: usize) -> Vec<String> {
        let state = self.0.lock().unwrap();
        state
            .lines
            .iter()
            .rev()
            .take(count)
            .rev()
            .cloned()
            .collect()
    }

    /// The last `count` lines and the position after them, for a follower
    /// (`node.logs.tail` with `follow`) to pass to [`LogBuffer::since`].
    pub fn tail_at(&self, count: usize) -> (u64, Vec<String>) {
        let state = self.0.lock().unwrap();
        let lines = state
            .lines
            .iter()
            .rev()
            .take(count)
            .rev()
            .cloned()
            .collect();
        (state.total, lines)
    }

    /// The lines completed after position `seen` (as many as the ring still
    /// holds), and the new position.
    pub fn since(&self, seen: u64) -> (u64, Vec<String>) {
        let state = self.0.lock().unwrap();
        let new = state
            .total
            .saturating_sub(seen)
            .min(state.lines.len() as u64) as usize;
        let lines = state
            .lines
            .iter()
            .skip(state.lines.len() - new)
            .cloned()
            .collect();
        (state.total, lines)
    }
}

pub struct LogWriter(LogBuffer);

impl Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let mut state = self.0 .0.lock().unwrap();
        for byte in bytes {
            if *byte == b'\n' {
                let line = String::from_utf8_lossy(&state.partial).into_owned();
                state.partial.clear();
                state.lines.push_back(line);
                state.total += 1;
                if state.lines.len() > CAPACITY {
                    state.lines.pop_front();
                }
            } else {
                state.partial.push(*byte);
            }
        }
        drop(state);
        let _ = std::io::stderr().write_all(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stderr().flush()
    }
}
