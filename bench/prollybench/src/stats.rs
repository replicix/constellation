//! Latency samples, percentiles, and the markdown the report is made of.

use std::io::Write;

pub struct Samples(pub Vec<u32>);

impl Samples {
    pub fn with_capacity(n: usize) -> Samples {
        Samples(Vec::with_capacity(n))
    }

    pub fn push_ns(&mut self, ns: u128) {
        self.0.push(ns.min(u32::MAX as u128) as u32);
    }

    pub fn pct(&mut self, p: f64) -> u32 {
        if self.0.is_empty() {
            return 0;
        }
        self.0.sort_unstable();
        let i = ((self.0.len() - 1) as f64 * p).round() as usize;
        self.0[i]
    }
}

pub fn ns(v: u32) -> String {
    if v >= 1_000_000 {
        format!("{:.2} ms", v as f64 / 1e6)
    } else if v >= 1_000 {
        format!("{:.1} µs", v as f64 / 1e3)
    } else {
        format!("{v} ns")
    }
}

pub fn rate(n: u64, secs: f64) -> String {
    let r = n as f64 / secs;
    if r >= 1e6 {
        format!("{:.2} M/s", r / 1e6)
    } else if r >= 1e3 {
        format!("{:.0} k/s", r / 1e3)
    } else {
        format!("{r:.0}/s")
    }
}

pub fn bytes(v: u64) -> String {
    const U: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut f = v as f64;
    let mut i = 0;
    while f >= 1024.0 && i < 4 {
        f /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{v} B")
    } else {
        format!("{:.2} {}", f, U[i])
    }
}

/// Collects the markdown that ends up pasted into the plan.
#[derive(Default)]
pub struct Report {
    pub out: String,
}

impl Report {
    pub fn line(&mut self, s: impl AsRef<str>) {
        println!("{}", s.as_ref());
        self.out.push_str(s.as_ref());
        self.out.push('\n');
    }

    pub fn blank(&mut self) {
        self.line("");
    }

    pub fn head(&mut self, s: &str) {
        self.blank();
        self.line(s);
        self.blank();
    }

    pub fn save(&self, path: &std::path::Path) {
        let mut f = std::fs::File::create(path).expect("results file");
        f.write_all(self.out.as_bytes()).expect("write results");
        println!("\nwrote {}", path.display());
    }
}
