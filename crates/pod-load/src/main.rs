//! `constellation-pod-load run --dir <dir> --ctl <dir> [--seed N]
//! [--write-kib-s N] [--creates-s N] [--reads-s N] [--slow-ms N]`: the
//! trio of [`constellation_pod_load`] in `--dir` until `<ctl>/stop`
//! appears, then `<ctl>/summary.json`. Progress (calls so far) is in
//! `<ctl>/progress`; a setup failure is `<ctl>/failed` and exit status 1.

use constellation_pod_load::{run, Opts};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn usage() -> ! {
    eprintln!(
        "usage: constellation-pod-load run --dir DIR --ctl DIR [--seed N] [--write-kib-s N] \
         [--creates-s N] [--reads-s N] [--slow-ms N]"
    );
    std::process::exit(2)
}

/// Write `data` to `path` through a rename, so a reader never sees it
/// half-written.
fn publish(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, path)
}

fn main() {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() != Some("run") {
        usage();
    }
    let (mut dir, mut ctl) = (None::<PathBuf>, None::<PathBuf>);
    let mut opts = Opts::default();
    while let Some(flag) = args.next() {
        let value = args.next().unwrap_or_else(|| usage());
        let num = || value.parse::<u64>().unwrap_or_else(|_| usage());
        match flag.as_str() {
            "--dir" => dir = Some(value.clone().into()),
            "--ctl" => ctl = Some(value.clone().into()),
            "--seed" => opts.seed = num(),
            "--write-kib-s" => opts.write_kib_s = num(),
            "--creates-s" => opts.creates_s = num(),
            "--reads-s" => opts.reads_s = num(),
            "--slow-ms" => opts.slow_ms = num(),
            _ => usage(),
        }
    }
    let (Some(dir), Some(ctl)) = (dir, ctl) else {
        usage()
    };
    if let Err(e) = std::fs::create_dir_all(&ctl) {
        eprintln!("creating {}: {e}", ctl.display());
        std::process::exit(1);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicU64::new(0));
    let watcher = {
        let (stop, calls, ctl) = (stop.clone(), calls.clone(), ctl.clone());
        std::thread::spawn(move || {
            let mut ticks = 0u64;
            while !stop.load(Ordering::SeqCst) {
                if ctl.join("stop").exists() {
                    stop.store(true, Ordering::SeqCst);
                    break;
                }
                if ticks.is_multiple_of(10) {
                    let n = calls.load(Ordering::Relaxed);
                    let _ = publish(&ctl.join("progress"), format!("{n}\n").as_bytes());
                }
                ticks += 1;
                std::thread::sleep(Duration::from_millis(100));
            }
        })
    };
    match run(&dir, opts, stop.clone(), calls) {
        Ok(summary) => {
            let _ = watcher.join();
            let json = serde_json::to_vec(&summary).expect("a summary serializes");
            if let Err(e) = publish(&ctl.join("summary.json"), &json) {
                eprintln!("writing the summary: {e}");
                std::process::exit(1);
            }
            eprintln!(
                "stopped: {} calls, {} error(s), {} records, {} files, {} reads",
                summary.calls,
                summary.error_count(),
                summary.appended,
                summary.created,
                summary.reads
            );
        }
        Err(e) => {
            stop.store(true, Ordering::SeqCst);
            let _ = publish(&ctl.join("failed"), format!("{e}\n").as_bytes());
            eprintln!("setting up the load in {}: {e}", dir.display());
            std::process::exit(1);
        }
    }
}
