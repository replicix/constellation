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
