//! A daemon's threads, as the kernel sees them at one moment
//! (`/proc/<pid>/task/*/{comm,wchan,stat,syscall}`), kept when a control
//! call gets no answer.
//!
//! "The daemon did not answer Hello" says only that nothing answered. Whether
//! it was a process frozen with its host, a runtime whose workers were all
//! blocked, or threads waiting on a disk is in the threads' states: how many
//! were runnable, how many sat in `futex` (idle, or waiting on a lock a
//! blocked thread holds), how many were inside a `write`/`fsync` (a stalled
//! device). [`classify`] sums them up in a line the error carries; [`save`]
//! keeps the raw files for the next look.

use std::path::{Path, PathBuf};

/// One thread of the process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Thread {
    pub tid: u32,
    pub comm: String,
    /// `R`, `S`, `D`, `T`, `Z`... (`stat` field 3).
    pub state: char,
    /// What the thread waits in inside the kernel (`wchan`; `0` or empty:
    /// nothing).
    pub wchan: String,
    /// The system call it is in (`syscall`'s first field), if it is in one.
    pub syscall: Option<u64>,
    /// Utime + stime in clock ticks (`stat` fields 14 and 15).
    pub cpu_ticks: u64,
}

/// What the thread is doing, in a word.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Activity {
    /// Runnable or running (`R`).
    Runnable,
    /// In an I/O write path (`write`, `pwrite`, `fsync`, `sync_file_range`,
    /// ...).
    Write,
    /// In a read path.
    Read,
    /// In `futex`.
    Futex,
    /// Waiting for events (`epoll_wait`, `poll`, ...).
    Idle,
    /// In `nanosleep`: a thread that called `thread::sleep`. A runtime
    /// worker here is blocked, not parked (it parks in `futex`).
    Sleep,
    /// Uninterruptible sleep (`D`) not in one of the calls above.
    DiskWait,
    /// Anything else.
    Other,
}

impl Activity {
    pub fn label(self) -> &'static str {
        match self {
            Activity::Runnable => "runnable",
            Activity::Write => "write",
            Activity::Read => "read",
            Activity::Futex => "futex",
            Activity::Idle => "idle",
            Activity::Sleep => "sleep",
            Activity::DiskWait => "disk-wait",
            Activity::Other => "other",
        }
    }
}

/// The system call numbers [`classify_thread`] recognises (x86-64 and
/// aarch64 differ; an unknown architecture classifies on `wchan` and the
/// state alone).
fn syscall_kind(nr: u64) -> Option<Activity> {
    // x86-64: read pread64 readv preadv preadv2 | write pwrite64 writev
    // msync fsync fdatasync ftruncate fallocate sync_file_range pwritev
    // syncfs pwritev2 | futex | poll select wait4 epoll_wait pselect6
    // epoll_pwait (nanosleep 35 and clock_nanosleep 230 are `Sleep`).
    #[cfg(target_arch = "x86_64")]
    let (reads, writes, futex, idle): (&[u64], &[u64], &[u64], &[u64]) = (
        &[0, 17, 19, 295, 327],
        &[1, 18, 20, 26, 74, 75, 77, 285, 277, 296, 306, 328],
        &[202],
        &[7, 23, 61, 232, 270, 281],
    );
    // aarch64: read readv pread64 preadv preadv2 | write writev pwrite64
    // pwritev ftruncate fallocate msync fsync fdatasync sync_file_range
    // syncfs pwritev2 | futex | epoll_pwait pselect6 ppoll wait4
    // (nanosleep 101 and clock_nanosleep 115 are `Sleep`).
    #[cfg(target_arch = "aarch64")]
    let (reads, writes, futex, idle): (&[u64], &[u64], &[u64], &[u64]) = (
        &[63, 65, 67, 69, 286],
        &[64, 66, 68, 70, 46, 47, 227, 82, 83, 84, 267, 287],
        &[98],
        &[22, 72, 73, 260],
    );
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let (reads, writes, futex, idle): (&[u64], &[u64], &[u64], &[u64]) = (&[], &[], &[], &[]);
    #[cfg(target_arch = "x86_64")]
    let sleeps: &[u64] = &[35, 230];
    #[cfg(target_arch = "aarch64")]
    let sleeps: &[u64] = &[101, 115];
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let sleeps: &[u64] = &[];
    if sleeps.contains(&nr) {
        Some(Activity::Sleep)
    } else if futex.contains(&nr) {
        Some(Activity::Futex)
    } else if writes.contains(&nr) {
        Some(Activity::Write)
    } else if reads.contains(&nr) {
        Some(Activity::Read)
    } else if idle.contains(&nr) {
        Some(Activity::Idle)
    } else {
        None
    }
}

pub fn classify_thread(t: &Thread) -> Activity {
    if t.state == 'R' {
        return Activity::Runnable;
    }
    if let Some(kind) = t.syscall.and_then(syscall_kind) {
        return kind;
    }
    let w = t.wchan.as_str();
    if w.contains("futex") {
        Activity::Futex
    } else if w.contains("nanosleep") {
        Activity::Sleep
    } else if w.contains("epoll") || w.contains("ep_poll") || w.contains("poll_schedule") {
        Activity::Idle
    } else if t.state == 'D' || w.contains("io_schedule") || w.contains("jbd2") {
        Activity::DiskWait
    } else {
        Activity::Other
    }
}

/// The threads of `pid` now (empty when it is gone). A thread that exits
/// while being read is skipped.
pub fn threads(pid: u32) -> Vec<Thread> {
    threads_in(Path::new(&format!("/proc/{pid}/task")))
}

fn threads_in(task_dir: &Path) -> Vec<Thread> {
    let Ok(entries) = std::fs::read_dir(task_dir) else {
        return Vec::new();
    };
    let mut out: Vec<Thread> = entries
        .flatten()
        .filter_map(|e| {
            let tid: u32 = e.file_name().to_str()?.parse().ok()?;
            let dir = e.path();
            let stat = std::fs::read_to_string(dir.join("stat")).ok()?;
            let (state, cpu_ticks) = parse_stat(&stat)?;
            let read = |name: &str| {
                std::fs::read_to_string(dir.join(name))
                    .map(|s| s.trim().to_string())
                    .unwrap_or_default()
            };
            Some(Thread {
                tid,
                comm: read("comm"),
                state,
                wchan: read("wchan"),
                syscall: parse_syscall(&read("syscall")),
                cpu_ticks,
            })
        })
        .collect();
    out.sort_by_key(|t| t.tid);
    out
}

/// `(state, utime + stime)` of a `stat` line. The command is in
/// parentheses and may itself hold spaces and parentheses: the fields
/// start after the *last* `)`.
pub fn parse_stat(line: &str) -> Option<(char, u64)> {
    let rest = line.get(line.rfind(')')? + 1..)?;
    let mut fields = rest.split_whitespace();
    let state = fields.next()?.chars().next()?;
    // After the state: ppid pgrp session tty_nr tpgid flags minflt cminflt
    // majflt cmajflt utime stime.
    let mut rest = fields.skip(10);
    let utime: u64 = rest.next()?.parse().ok()?;
    let stime: u64 = rest.next()?.parse().ok()?;
    Some((state, utime + stime))
}

/// The call number of a `syscall` file's text (`running`, or `-1 ...`
/// when the thread is not in one).
fn parse_syscall(text: &str) -> Option<u64> {
    text.split_whitespace()
        .next()?
        .parse::<i64>()
        .ok()?
        .try_into()
        .ok()
}

/// A line summing the threads up: `42 threads: 3 runnable, 2 write, 30
/// futex; longest wchans ...`.
pub fn classify(threads: &[Thread]) -> String {
    if threads.is_empty() {
        return "no threads (the process is gone)".to_string();
    }
    let mut counts: std::collections::BTreeMap<Activity, usize> = Default::default();
    for t in threads {
        *counts.entry(classify_thread(t)).or_default() += 1;
    }
    let parts: Vec<String> = counts
        .iter()
        .map(|(a, n)| format!("{n} {}", a.label()))
        .collect();
    let mut line = format!("{} threads: {}", threads.len(), parts.join(", "));
    // The busy ones by name: a runtime's workers all runnable or all in
    // one call is the shape of a blocked runtime.
    let mut busy: std::collections::BTreeMap<(String, Activity), usize> = Default::default();
    for t in threads {
        let a = classify_thread(t);
        if matches!(
            a,
            Activity::Runnable | Activity::Write | Activity::DiskWait | Activity::Sleep
        ) {
            *busy.entry((t.comm.clone(), a)).or_default() += 1;
        }
    }
    if !busy.is_empty() {
        let named: Vec<String> = busy
            .iter()
            .map(|((comm, a), n)| format!("{n} {comm} {}", a.label()))
            .collect();
        line.push_str(&format!(" [{}]", named.join(", ")));
    }
    line
}

/// The raw files of every thread of `pid` (`comm`, `wchan`, `stat`,
/// `syscall`), under a header with the classification, as text.
pub fn render(pid: u32, why: &str) -> String {
    use std::fmt::Write;
    let ts = threads(pid);
    let mut out = String::new();
    let _ = writeln!(out, "# {why}");
    let _ = writeln!(out, "# pid {pid}: {}", classify(&ts));
    let _ = writeln!(out, "# tid comm state activity wchan syscall cpu_ticks");
    for t in &ts {
        let _ = writeln!(
            out,
            "{} {} {} {} {} {} {}",
            t.tid,
            t.comm,
            t.state,
            classify_thread(t).label(),
            if t.wchan.is_empty() { "-" } else { &t.wchan },
            t.syscall.map_or("-".to_string(), |n| n.to_string()),
            t.cpu_ticks
        );
    }
    let _ = writeln!(out, "\n# raw");
    for t in &ts {
        let dir = format!("/proc/{pid}/task/{}", t.tid);
        for name in ["comm", "wchan", "stat", "syscall"] {
            let text = std::fs::read_to_string(format!("{dir}/{name}")).unwrap_or_default();
            let _ = writeln!(out, "{} {name}: {}", t.tid, text.trim_end());
        }
    }
    out
}

/// Write [`render`] to `dir/<label>.tasks` (the directory is created).
pub fn save(pid: u32, dir: &Path, label: &str, why: &str) -> std::io::Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(format!("{label}.tasks"));
    std::fs::write(&path, render(pid, why))?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stat_line_with_parentheses_in_the_command_still_parses() {
        let line = "1234 (tokio (run) w) S 1 1 1 0 -1 4194560 100 0 5 0 17 25 0 0 20 0 9 0 99 1 2";
        assert_eq!(parse_stat(line), Some(('S', 42)));
        assert_eq!(parse_stat("garbage"), None);
    }

    #[test]
    fn threads_are_summed_by_what_they_do() {
        let t = |comm: &str, state: char, wchan: &str, syscall: Option<u64>| Thread {
            tid: 1,
            comm: comm.into(),
            state,
            wchan: wchan.into(),
            syscall,
            cpu_ticks: 0,
        };
        assert_eq!(classify_thread(&t("w", 'R', "0", None)), Activity::Runnable);
        assert_eq!(
            classify_thread(&t("w", 'S', "futex_wait_queue", None)),
            Activity::Futex
        );
        assert_eq!(
            classify_thread(&t("w", 'D', "jbd2_log_wait_commit", None)),
            Activity::DiskWait
        );
        assert_eq!(
            classify_thread(&t("w", 'S', "do_epoll_wait", None)),
            Activity::Idle
        );
        assert_eq!(
            classify_thread(&t("w", 'S', "ep_poll", None)),
            Activity::Idle
        );
        assert_eq!(
            classify_thread(&t("w", 'S', "hrtimer_nanosleep", None)),
            Activity::Sleep
        );
        #[cfg(target_arch = "x86_64")]
        {
            // write(2) / fsync(2) win over the wchan.
            assert_eq!(
                classify_thread(&t("w", 'D', "jbd2_log_wait_commit", Some(1))),
                Activity::Write
            );
            assert_eq!(
                classify_thread(&t("w", 'S', "futex_wait_queue", Some(202))),
                Activity::Futex
            );
        }
        let line = classify(&[
            t("tokio-rt-worker", 'R', "0", None),
            t("tokio-rt-worker", 'R', "0", None),
            t("cst-control", 'S', "do_epoll_wait", None),
        ]);
        assert!(line.starts_with("3 threads: 2 runnable, 1 idle"), "{line}");
        assert!(line.contains("2 tokio-rt-worker runnable"), "{line}");
    }

    #[test]
    fn this_process_can_be_snapshotted() {
        let me = std::process::id();
        let ts = threads(me);
        assert!(!ts.is_empty());
        let text = render(me, "a test");
        assert!(text.contains("# raw"), "{text}");
        let dir = tempfile::tempdir().unwrap();
        let path = save(me, dir.path(), "self", "a test").unwrap();
        assert!(std::fs::read_to_string(path).unwrap().contains("# pid"));
    }
}
