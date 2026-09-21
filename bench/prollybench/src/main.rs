//! Plan 28 Step 0: the three measurements that decide whether an
//! S3-native prolly-tree metadata plane is worth building.
//!
//! Standalone by design (like `bench/dbbench`): it depends on
//! `constellation-meta` only to size today's shipped log records against
//! plan 28's commits, and it touches no product code.

mod b01;
mod b02;
mod b03;
mod b05;
mod b06;
mod corpus;
mod keys;
mod node;
mod stats;
mod store;
mod tree;

use std::path::PathBuf;
use std::time::Instant;

use clap::{Parser, Subcommand};

use crate::stats::{bytes, rate, Report};
use crate::store::Store;
use crate::tree::Tree;

#[derive(Parser)]
#[command(about = "plan 28 Step 0 measurements")]
struct Cli {
    /// Namespace entries (the census is 11.9M; each entry is 3 keys).
    #[arg(long, default_value_t = 11_900_000)]
    entries: usize,
    /// Random lookups per residency tier.
    #[arg(long, default_value_t = 1_000_000)]
    samples: usize,
    /// Wall clock for the §P10b steady-state run.
    #[arg(long, default_value_t = 20.0)]
    minutes: f64,
    /// Operations per commit in the steady-state run.
    #[arg(long, default_value_t = 10_000)]
    commit_ops: usize,
    /// Retained commits (and the create→unlink lag).
    #[arg(long, default_value_t = 64)]
    retention: usize,
    #[arg(long, default_value_t = 200)]
    gc_every: usize,
    /// Share of commits that are scattered random-ino chmod.
    #[arg(long, default_value_t = 5)]
    scattered_pct: u64,
    /// Leaf cache for the steady-state run, MiB.
    #[arg(long, default_value_t = 256)]
    leaf_cache: usize,
    /// Hard ceiling on entries per node (0 = pure key-driven chunking).
    #[arg(long, default_value_t = 0)]
    max_entries: usize,
    /// Drop §P6's denormalized attr copy from the `0x02` dentry value, so
    /// `ls -la` becomes a range scan plus a point read per child. Shorthand
    /// for `--enc nocopy`.
    #[arg(long)]
    no_attr_copy: bool,
    /// `0x02` value shape: `copy` (§P6 as written), `nocopy`, or
    /// `dentry-auth` (no `0x01` record for `nlink == 1`; `getattr` hops
    /// through `0x04`). Every subcommand honours it.
    #[arg(long, default_value = "copy")]
    enc: String,
    #[arg(long, default_value = "data/packs")]
    pack_dir: PathBuf,
    #[arg(long, default_value = "RESULTS.md")]
    out: PathBuf,
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// 0.1 + 0.2 + 0.3 (default).
    All,
    /// 0.1 only, including the residency tiers.
    Reads,
    /// 0.2 only.
    Commits,
    /// The steady-state run only.
    Sustained,
    /// 0.3 only.
    Diff,
    /// 0.5 thread scaling (reads + commit shards + mark/compact).
    Threads,
    /// S1 — settle the dentry attr copy: all three `0x02` value shapes,
    /// measured against each other (ignores `--enc`).
    AttrCopy {
        /// Directories the `ls -la` rows scan.
        #[arg(long, default_value_t = 64)]
        dirs: usize,
        #[arg(long, default_value_t = 10)]
        generations: usize,
        #[arg(long, default_value_t = 20_000)]
        age_ops: usize,
    },
    /// The 10M-entry-directory shapes, in their own process (they need
    /// their own corpus).
    BigDir {
        #[arg(long, default_value_t = 10_000_000)]
        entries: usize,
    },
}

fn main() {
    let cli = Cli::parse();
    node::MAX_ENTRIES.store(cli.max_entries, std::sync::atomic::Ordering::Relaxed);
    let enc = if cli.no_attr_copy {
        keys::Enc::NoCopy
    } else {
        match cli.enc.as_str() {
            "copy" => keys::Enc::Copy,
            "nocopy" | "no-copy" => keys::Enc::NoCopy,
            "dentry-auth" | "dentryauth" => keys::Enc::DentryAuth,
            other => panic!("unknown --enc {other:?} (copy | nocopy | dentry-auth)"),
        }
    };
    keys::set_enc(enc);
    let mut rep = Report::default();
    rep.line("# prollybench — plan 28 Step 0 results");
    rep.blank();
    rep.line(format!(
        "Dentry value shape: {} (`--enc {}`).",
        enc.label(),
        enc.flag()
    ));
    rep.line(format!(
        "Host: {} cores, generated {}. Single-threaded unless stated. Node entry clamp: {}.",
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
        chrono_ish(),
        if cli.max_entries == 0 {
            "none".to_string()
        } else {
            format!("{} entries", cli.max_entries)
        }
    ));

    match cli.cmd.as_ref().unwrap_or(&Cmd::All) {
        Cmd::BigDir { entries } => big_dir(&mut rep, *entries),
        cmd => {
            let want_reads = matches!(cmd, Cmd::All | Cmd::Reads);
            let want_commits = matches!(cmd, Cmd::All | Cmd::Commits);
            let want_diff = matches!(cmd, Cmd::All | Cmd::Diff);
            let want_threads = matches!(cmd, Cmd::All | Cmd::Threads);
            let want_sustained = matches!(cmd, Cmd::All | Cmd::Sustained);

            let t0 = Instant::now();
            let corpus = corpus::generate(cli.entries, 0xc0de);
            rep.blank();
            rep.line(format!(
                "Corpus: {} entries, {} logical bytes, {} keys, generated in {:.0} s.",
                corpus.len(),
                bytes(corpus.total_bytes),
                corpus.key_count(),
                t0.elapsed().as_secs_f64()
            ));

            if let Cmd::AttrCopy {
                dirs,
                generations,
                age_ops,
            } = cmd
            {
                b06::settle_attr_copy(
                    &mut rep,
                    &corpus,
                    &b06::Cfg {
                        pack_dir: cli.pack_dir.join("attrcopy"),
                        dirs: *dirs,
                        age_generations: *generations,
                        age_ops: *age_ops,
                    },
                );
                rep.save(&cli.out);
            }

            if want_reads || want_commits || want_diff || want_threads {
                let store = Store::logical_packs(1 << 20);
                let t = Tree::new(&store);
                let t0 = Instant::now();
                let root = t.build_sorted(corpus.all_entries());
                let build = t0.elapsed().as_secs_f64();
                rep.line(format!(
                    "Bulk build: {} keys in {:.0} s ({}), {} resident.",
                    corpus.key_count(),
                    build,
                    rate(corpus.key_count(), build),
                    bytes(store.resident_bytes())
                ));
                b01::structure(&mut rep, &store, &root, &corpus);

                if want_reads || want_threads {
                    let t0 = Instant::now();
                    let packed = export_packs(&store, &root, &cli.pack_dir);
                    rep.blank();
                    rep.line(format!(
                        "Exported {} packs ({} on disk) in {:.0} s.",
                        packed.pack_count(),
                        bytes(packed.pack_bytes_total()),
                        t0.elapsed().as_secs_f64()
                    ));
                    if want_reads {
                        b01::throughput(&mut rep, &store, &packed, &root, &corpus, cli.samples);
                        b01::fuse_shapes(&mut rep, &store, &root, &corpus);
                    }
                    if want_threads {
                        b05::thread_scaling(
                            &mut rep,
                            &store,
                            &packed,
                            &root,
                            &corpus,
                            cli.samples.min(500_000),
                        );
                    }
                    drop(packed);
                    let _ = std::fs::remove_dir_all(&cli.pack_dir);
                }
                if want_commits {
                    b02::commit_cost(&mut rep, &store, &root, &corpus);
                }
                if want_diff {
                    b03::diff_and_merge(&mut rep, &store, &root, &corpus);
                    b03::determinism(&mut rep, 100_000, 50);
                }
                rep.save(&cli.out);
            }

            if want_sustained {
                b02::sustained(
                    &mut rep,
                    &corpus,
                    &b02::Sustained {
                        minutes: cli.minutes,
                        commit_ops: cli.commit_ops,
                        retention: cli.retention,
                        gc_every: cli.gc_every,
                        scattered_pct: cli.scattered_pct,
                        pack_dir: cli.pack_dir.join("sustained"),
                        leaf_cache_bytes: cli.leaf_cache << 20,
                    },
                );
            }
        }
    }
    rep.save(&cli.out);
}

/// Write every node of the tree into packs in key order, which is how a
/// commit would have written them (§P6: write locality == read locality).
pub fn export_packs(src: &Store, root: &node::Hash, dir: &PathBuf) -> Store {
    let _ = std::fs::remove_dir_all(dir);
    let dst = Store::packed(dir.clone(), store::DEFAULT_PACK_BYTES, true, 64 << 20);
    let mut stack = vec![*root];
    let mut interior: Vec<node::Hash> = Vec::new();
    // Leaves first, in key order, so adjacent keys share a pack.
    let mut leaves: Vec<node::Hash> = Vec::new();
    while let Some(h) = stack.pop() {
        let buf = src.get(&h);
        let n = node::NodeRef::new(&buf);
        if n.level == 0 {
            leaves.push(h);
        } else {
            interior.push(h);
            for i in (0..n.count).rev() {
                stack.push(n.child(i).0);
            }
        }
    }
    for h in leaves {
        let buf = src.get(&h);
        dst.put(0, buf.as_ref().clone());
    }
    for h in interior {
        let buf = src.get(&h);
        let level = node::NodeRef::new(&buf).level;
        dst.put(level, buf.as_ref().clone());
    }
    dst.flush();
    dst
}

/// A single directory with `entries` children: the Appendix B shapes.
fn big_dir(rep: &mut Report, entries: usize) {
    let store = Store::logical_packs(1 << 20);
    let t = Tree::new(&store);
    let parent = 1u64;
    let t0 = Instant::now();
    let mut all: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(entries * 2);
    let a = keys::Attrs {
        kind: keys::KIND_FILE,
        mode: 0o100644,
        uid: 1000,
        gid: 1000,
        nlink: 1,
        size: 4096,
        mtime_ns: 1_760_000_000_000_000_000,
        ctime_ns: 1_760_000_000_000_000_000,
        atime_ns: 1_760_000_000_000_000_000,
    };
    for i in 0..entries {
        let ino = i as u64 + 2;
        all.push((
            keys::inode_key(ino),
            keys::inode_val(&a, &[corpus::fake_hash(ino, 0)], &[]),
        ));
    }
    let names: Vec<String> = (0..entries).map(|i| format!("f{i:09}")).collect();
    for (i, name) in names.iter().enumerate() {
        all.push((
            keys::dentry_key(parent, name.as_bytes()),
            keys::dentry_val(i as u64 + 2, &a),
        ));
    }
    let root = t.build_sorted(all.into_iter());
    rep.head(&format!(
        "### A single {entries}-entry directory (Appendix B shapes)"
    ));
    rep.line(format!(
        "Built {} keys in {:.0} s; {} resident.",
        entries * 2,
        t0.elapsed().as_secs_f64(),
        bytes(store.resident_bytes())
    ));

    // Paged readdir over the whole directory.
    let prefix = keys::dentry_prefix(parent);
    let t0 = Instant::now();
    let mut cursor = prefix.clone();
    let mut pages = 0u64;
    let mut seen = 0usize;
    loop {
        let page = t.collect_from(&root, &cursor, &prefix, 101);
        let page: Vec<Vec<u8>> = if pages == 0 {
            page
        } else {
            page.into_iter().skip(1).collect()
        };
        if page.is_empty() {
            break;
        }
        seen += page.len();
        cursor = page.last().unwrap().clone();
        pages += 1;
    }
    let secs = t0.elapsed().as_secs_f64();
    rep.line(format!(
        "- Paged `readdir`: {} dirents in {} pages of 100, {:.0} s total, {:.1} µs/page, {} dirents/s. The cursor is the key, so resumption is exact.",
        seen,
        pages,
        secs,
        secs * 1e6 / pages as f64,
        (seen as f64 / secs) as u64
    ));

    // Rename the directory itself: independent of what it holds.
    let dname = b"bigdir";
    let mut muts: Vec<tree::Mut> = vec![
        (
            keys::dentry_key(1, dname),
            Some(keys::dentry_val(parent, &a)),
        ),
        (keys::rdentry_key(parent, 1, dname), Some(Vec::new())),
    ];
    muts.sort();
    let with_dir = t.apply(&root, &muts);
    store.counters.reset();
    let t0 = Instant::now();
    let mut ren: Vec<tree::Mut> = vec![
        (keys::dentry_key(1, dname), None),
        (
            keys::dentry_key(1, b"bigdir.renamed"),
            Some(keys::dentry_val(parent, &a)),
        ),
        (keys::rdentry_key(parent, 1, dname), None),
        (
            keys::rdentry_key(parent, 1, b"bigdir.renamed"),
            Some(Vec::new()),
        ),
    ];
    ren.sort();
    let _ = t.apply(&with_dir, &ren);
    let o = std::sync::atomic::Ordering::Relaxed;
    rep.line(format!(
        "- `rename` of the directory holding {} entries: {} keys, {} nodes, {} → {:.1} µs, independent of the subtree.",
        entries,
        ren.len(),
        store.counters.new_nodes.load(o),
        bytes(store.counters.new_bytes.load(o)),
        t0.elapsed().as_secs_f64() * 1e6
    ));
}

fn chronos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Civil date from a unix timestamp (Hinnant's algorithm) — the report
/// header wants a date and the crate has no time dependency.
fn chrono_ish() -> String {
    let z = chronos() as i64 / 86_400 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y}-{m:02}-{d:02}")
}
