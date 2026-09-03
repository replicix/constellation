//! Turns raw [`crate::run::RunResult`]s into something a human (or a
//! spreadsheet) can compare.

use std::io::Write;
use std::path::Path;

use crate::run::RunResult;

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

pub struct Summary {
    pub controller_name: &'static str,
    pub duration_secs: f64,
    pub total_mib: f64,
    pub mean_throughput_mib_s: f64,
    pub peak_throughput_mib_s: f64,
    pub total_successes: u64,
    pub total_errors: u64,
    pub error_rate: f64,
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub mean_concurrency: f64,
    pub max_concurrency: usize,
    pub final_concurrency: usize,
}

pub fn summarize(result: &RunResult) -> Summary {
    let mut latencies = result.latencies_ms.clone();
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // Peak throughput over a trailing ~1 second window. Adjacent samples
    // can be only milliseconds apart when the final in-flight upload
    // drains; treating one completion over that tiny interval as a rate
    // produces physically impossible "peaks".
    let mut peak_mib_s = 0.0f64;
    let mut mean_concurrency_acc = 0.0f64;
    let mut peak_start = 0usize;
    for (end_idx, end) in result.samples.iter().enumerate() {
        while peak_start + 1 < end_idx
            && end.t.saturating_sub(result.samples[peak_start + 1].t)
                >= std::time::Duration::from_secs(1)
        {
            peak_start += 1;
        }
        let start = result.samples[peak_start];
        let dt = end.t.saturating_sub(start.t).as_secs_f64();
        if dt >= 0.5 {
            let dbytes = end.cumulative_bytes.saturating_sub(start.cumulative_bytes) as f64;
            peak_mib_s = peak_mib_s.max(dbytes / dt / (1024.0 * 1024.0));
        }
    }
    for sample in result
        .samples
        .iter()
        .take(result.samples.len().saturating_sub(1))
    {
        mean_concurrency_acc += sample.concurrency_target as f64;
    }
    let mean_concurrency = if result.samples.len() > 1 {
        mean_concurrency_acc / (result.samples.len() - 1) as f64
    } else {
        0.0
    };
    let max_concurrency = result
        .samples
        .iter()
        .map(|s| s.concurrency_target)
        .max()
        .unwrap_or(0);
    let final_concurrency = result
        .samples
        .last()
        .map(|s| s.concurrency_target)
        .unwrap_or(0);

    let duration_secs = result.duration.as_secs_f64().max(0.001);
    let total_mib = result.total_bytes as f64 / (1024.0 * 1024.0);
    let total_attempts = result.total_successes + result.total_errors;

    Summary {
        controller_name: result.controller_name,
        duration_secs,
        total_mib,
        mean_throughput_mib_s: total_mib / duration_secs,
        peak_throughput_mib_s: peak_mib_s,
        total_successes: result.total_successes,
        total_errors: result.total_errors,
        error_rate: if total_attempts > 0 {
            result.total_errors as f64 / total_attempts as f64
        } else {
            0.0
        },
        p50_latency_ms: percentile(&latencies, 0.50),
        p95_latency_ms: percentile(&latencies, 0.95),
        mean_concurrency,
        max_concurrency,
        final_concurrency,
    }
}

/// Seconds from the end of a fault window until achieved throughput
/// first climbs back to within `tolerance` of the pre-fault mean
/// throughput. The baseline is measured over `baseline_window` ending
/// at `fault_start` (i.e. strictly before the fault, not overlapping
/// it); post-fault throughput is measured over the same window length,
/// trailing each sample point, to smooth out sample-to-sample noise.
/// Returns `None` if it never recovers within the run, or if there
/// isn't enough data to establish a baseline.
pub fn recovery_time_secs(
    result: &RunResult,
    fault_start: std::time::Duration,
    fault_end: std::time::Duration,
    baseline_window: std::time::Duration,
    tolerance: f64,
) -> Option<f64> {
    let rate_over = |from: std::time::Duration, to: std::time::Duration| -> Option<f64> {
        let (t0, b0) = at(result, from)?;
        let (t1, b1) = at(result, to)?;
        let dt = (t1.as_secs_f64() - t0.as_secs_f64()).max(0.001);
        Some(b1.saturating_sub(b0) as f64 / dt)
    };

    let baseline_start = fault_start.checked_sub(baseline_window)?;
    let pre_fault_rate = rate_over(baseline_start, fault_start)?;
    if pre_fault_rate <= 0.0 {
        return None;
    }
    let target_rate = pre_fault_rate * tolerance;

    for sample in result
        .samples
        .iter()
        .filter(|s| s.t >= fault_end + baseline_window)
    {
        let Some(rate) = rate_over(sample.t - baseline_window, sample.t) else {
            continue;
        };
        if rate >= target_rate {
            return Some(sample.t.as_secs_f64() - fault_end.as_secs_f64());
        }
    }
    None
}

fn at(result: &RunResult, t: std::time::Duration) -> Option<(std::time::Duration, u64)> {
    result
        .samples
        .iter()
        .rfind(|s| s.t <= t)
        .map(|s| (s.t, s.cumulative_bytes))
}

pub fn print_summary_table(summaries: &[Summary]) {
    println!(
        "{:<8} {:>7} {:>9} {:>9} {:>9} {:>9} {:>7} {:>7} {:>8} {:>8} {:>8} {:>7} {:>7}",
        "ctrl",
        "secs",
        "mean MiB/s",
        "peak MiB/s",
        "total MiB",
        "oks",
        "errs",
        "err%",
        "p50 ms",
        "p95 ms",
        "mean N",
        "max N",
        "final N"
    );
    for s in summaries {
        println!(
            "{:<8} {:>7.1} {:>9.2} {:>9.2} {:>9.1} {:>9} {:>7} {:>6.1}% {:>8.1} {:>8.1} {:>8.1} {:>7} {:>7}",
            s.controller_name,
            s.duration_secs,
            s.mean_throughput_mib_s,
            s.peak_throughput_mib_s,
            s.total_mib,
            s.total_successes,
            s.total_errors,
            s.error_rate * 100.0,
            s.p50_latency_ms,
            s.p95_latency_ms,
            s.mean_concurrency,
            s.max_concurrency,
            s.final_concurrency,
        );
    }
}

/// Write one CSV with every controller's full time series, for
/// plotting (`t_secs,controller,concurrency_target,in_flight,mib_per_s,cumulative_errors`).
pub fn write_csv(path: &Path, results: &[RunResult]) -> anyhow::Result<()> {
    let mut f = std::fs::File::create(path)?;
    writeln!(
        f,
        "t_secs,controller,concurrency_target,in_flight,mib_per_s,cumulative_bytes,cumulative_successes,cumulative_errors"
    )?;
    for result in results {
        let mut prev: Option<(std::time::Duration, u64)> = None;
        for s in &result.samples {
            let mib_per_s = match prev {
                Some((pt, pb)) => {
                    let dt = (s.t.as_secs_f64() - pt.as_secs_f64()).max(0.001);
                    (s.cumulative_bytes.saturating_sub(pb)) as f64 / dt / (1024.0 * 1024.0)
                }
                None => 0.0,
            };
            writeln!(
                f,
                "{:.3},{},{},{},{:.3},{},{},{}",
                s.t.as_secs_f64(),
                result.controller_name,
                s.concurrency_target,
                s.in_flight,
                mib_per_s,
                s.cumulative_bytes,
                s.cumulative_successes,
                s.cumulative_errors,
            )?;
            prev = Some((s.t, s.cumulative_bytes));
        }
    }
    Ok(())
}
