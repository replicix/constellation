use std::time::Instant;

const NDIRS: u64 = 1_320_000;
const NFILES: u64 = 10_080_000;
const LOOKUPS: u64 = 1_000_000;
const SCANS: u64 = 20_000;
const BATCH: u64 = 100_000;

#[inline]
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

// entry i (0..NFILES): parent dir, name (~15 B), ino
fn entry(i: u64) -> (u64, String, u64) {
    let h = mix(i);
    (h % NDIRS, format!("f{:015x}", h), NDIRS + i)
}

fn inode_rec(ino: u64) -> [u8; 100] {
    let mut r = [0u8; 100];
    r[..8].copy_from_slice(&ino.to_be_bytes());
    r[8..16].copy_from_slice(&mix(ino).to_be_bytes());
    r
}

fn dkey(parent: u64, name: &str) -> Vec<u8> {
    let mut k = parent.to_be_bytes().to_vec();
    k.extend_from_slice(name.as_bytes());
    k
}

struct Res { load_s: f64, lookup_s: f64, scan_s: f64, iread_s: f64 }

fn report(name: &str, r: Res) {
    println!("RESULT {name} load={:.1}s ({:.0}k ins/s) lookup={:.2}s ({:.0}k op/s) scan={:.2}s ({:.0} dir/s) iread={:.2}s ({:.0}k op/s)",
        r.load_s, (NFILES as f64/r.load_s)/1e3,
        r.lookup_s, (LOOKUPS as f64/r.lookup_s)/1e3,
        r.scan_s, SCANS as f64/r.scan_s,
        r.iread_s, (LOOKUPS as f64/r.iread_s)/1e3);
}

fn bench_sqlite(dir: &str) -> Res {
    let conn = rusqlite::Connection::open(format!("{dir}/meta.db")).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-524288;
        CREATE TABLE dentry(parent INTEGER, name TEXT, ino INTEGER, PRIMARY KEY(parent,name)) WITHOUT ROWID;
        CREATE TABLE inode(ino INTEGER PRIMARY KEY, rec BLOB) WITHOUT ROWID;").unwrap();
    let t = Instant::now();
    let mut i = 0u64;
    while i < NFILES {
        let tx = conn.unchecked_transaction().unwrap();
        {
            let mut d = tx.prepare_cached("INSERT INTO dentry VALUES(?,?,?)").unwrap();
            let mut n = tx.prepare_cached("INSERT INTO inode VALUES(?,?)").unwrap();
            for j in i..(i + BATCH).min(NFILES) {
                let (p, name, ino) = entry(j);
                d.execute(rusqlite::params![p as i64, name, ino as i64]).unwrap();
                n.execute(rusqlite::params![ino as i64, &inode_rec(ino)[..]]).unwrap();
            }
        }
        tx.commit().unwrap();
        i += BATCH;
    }
    let load_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let mut q = conn.prepare("SELECT ino FROM dentry WHERE parent=? AND name=?").unwrap();
    let mut acc = 0i64;
    for j in 0..LOOKUPS {
        let (p, name, _) = entry(mix(j) % NFILES);
        acc += q.query_row(rusqlite::params![p as i64, name], |r| r.get::<_, i64>(0)).unwrap();
    }
    let lookup_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let mut s = conn.prepare("SELECT count(*) FROM dentry WHERE parent=?").unwrap();
    for j in 0..SCANS {
        acc += s.query_row([(mix(j ^ 7) % NDIRS) as i64], |r| r.get::<_, i64>(0)).unwrap();
    }
    let scan_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let mut g = conn.prepare("SELECT length(rec) FROM inode WHERE ino=?").unwrap();
    for j in 0..LOOKUPS {
        let ino = NDIRS + mix(j ^ 13) % NFILES;
        acc += g.query_row([ino as i64], |r| r.get::<_, i64>(0)).unwrap();
    }
    let iread_s = t.elapsed().as_secs_f64();
    assert!(acc != 0);
    Res { load_s, lookup_s, scan_s, iread_s }
}

fn bench_redb(dir: &str) -> Res {
    use redb::{Database, TableDefinition};
    const D: TableDefinition<&[u8], u64> = TableDefinition::new("dentry");
    const I: TableDefinition<u64, &[u8]> = TableDefinition::new("inode");
    let db = Database::create(format!("{dir}/meta.redb")).unwrap();
    let t = Instant::now();
    let mut i = 0u64;
    while i < NFILES {
        let tx = db.begin_write().unwrap();
        {
            let mut d = tx.open_table(D).unwrap();
            let mut n = tx.open_table(I).unwrap();
            for j in i..(i + BATCH).min(NFILES) {
                let (p, name, ino) = entry(j);
                d.insert(dkey(p, &name).as_slice(), ino).unwrap();
                n.insert(ino, &inode_rec(ino)[..]).unwrap();
            }
        }
        tx.commit().unwrap();
        i += BATCH;
    }
    let load_s = t.elapsed().as_secs_f64();
    let rtx = db.begin_read().unwrap();
    let d = rtx.open_table(D).unwrap();
    let n = rtx.open_table(I).unwrap();
    let t = Instant::now();
    let mut acc = 0u64;
    for j in 0..LOOKUPS {
        let (p, name, _) = entry(mix(j) % NFILES);
        acc += d.get(dkey(p, &name).as_slice()).unwrap().unwrap().value();
    }
    let lookup_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    for j in 0..SCANS {
        let p = mix(j ^ 7) % NDIRS;
        let lo = p.to_be_bytes().to_vec();
        let hi = (p + 1).to_be_bytes().to_vec();
        acc += d.range(lo.as_slice()..hi.as_slice()).unwrap().count() as u64;
    }
    let scan_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    for j in 0..LOOKUPS {
        let ino = NDIRS + mix(j ^ 13) % NFILES;
        acc += n.get(ino).unwrap().unwrap().value().len() as u64;
    }
    let iread_s = t.elapsed().as_secs_f64();
    assert!(acc != 0);
    Res { load_s, lookup_s, scan_s, iread_s }
}

fn bench_fjall(dir: &str) -> Res {
    let ks = fjall::Config::new(format!("{dir}/meta.fjall")).open().unwrap();
    let d = ks.open_partition("dentry", Default::default()).unwrap();
    let n = ks.open_partition("inode", Default::default()).unwrap();
    let t = Instant::now();
    for j in 0..NFILES {
        let (p, name, ino) = entry(j);
        d.insert(dkey(p, &name), &ino.to_be_bytes()).unwrap();
        n.insert(ino.to_be_bytes(), &inode_rec(ino)[..]).unwrap();
        if j % BATCH == 0 { ks.persist(fjall::PersistMode::Buffer).unwrap(); }
    }
    ks.persist(fjall::PersistMode::SyncAll).unwrap();
    let load_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let mut acc = 0u64;
    for j in 0..LOOKUPS {
        let (p, name, _) = entry(mix(j) % NFILES);
        acc += d.get(dkey(p, &name)).unwrap().map(|v| v.len() as u64).unwrap();
    }
    let lookup_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    for j in 0..SCANS {
        let p = mix(j ^ 7) % NDIRS;
        acc += d.prefix(p.to_be_bytes()).count() as u64;
    }
    let scan_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    for j in 0..LOOKUPS {
        let ino = NDIRS + mix(j ^ 13) % NFILES;
        acc += n.get(ino.to_be_bytes()).unwrap().map(|v| v.len() as u64).unwrap();
    }
    let iread_s = t.elapsed().as_secs_f64();
    assert!(acc != 0);
    Res { load_s, lookup_s, scan_s, iread_s }
}

fn bench_lmdb(dir: &str) -> Res {
    use heed::types::Bytes;
    let path = format!("{dir}/meta.lmdb");
    std::fs::create_dir_all(&path).unwrap();
    let env = unsafe {
        heed::EnvOpenOptions::new().map_size(32 << 30).max_dbs(2)
            .flags(heed::EnvFlags::NO_SYNC).open(&path).unwrap()
    };
    let mut wtx = env.write_txn().unwrap();
    let d: heed::Database<Bytes, Bytes> = env.create_database(&mut wtx, Some("dentry")).unwrap();
    let n: heed::Database<Bytes, Bytes> = env.create_database(&mut wtx, Some("inode")).unwrap();
    wtx.commit().unwrap();
    let t = Instant::now();
    let mut i = 0u64;
    while i < NFILES {
        let mut tx = env.write_txn().unwrap();
        for j in i..(i + BATCH).min(NFILES) {
            let (p, name, ino) = entry(j);
            d.put(&mut tx, &dkey(p, &name), &ino.to_be_bytes()).unwrap();
            n.put(&mut tx, &ino.to_be_bytes(), &inode_rec(ino)[..]).unwrap();
        }
        tx.commit().unwrap();
        i += BATCH;
    }
    env.force_sync().unwrap();
    let load_s = t.elapsed().as_secs_f64();
    let rtx = env.read_txn().unwrap();
    let t = Instant::now();
    let mut acc = 0u64;
    for j in 0..LOOKUPS {
        let (p, name, _) = entry(mix(j) % NFILES);
        acc += d.get(&rtx, &dkey(p, &name)).unwrap().unwrap().len() as u64;
    }
    let lookup_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    for j in 0..SCANS {
        let p = mix(j ^ 7) % NDIRS;
        acc += d.prefix_iter(&rtx, &p.to_be_bytes()).unwrap().count() as u64;
    }
    let scan_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    for j in 0..LOOKUPS {
        let ino = NDIRS + mix(j ^ 13) % NFILES;
        acc += n.get(&rtx, &ino.to_be_bytes()).unwrap().unwrap().len() as u64;
    }
    let iread_s = t.elapsed().as_secs_f64();
    assert!(acc != 0);
    Res { load_s, lookup_s, scan_s, iread_s }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let engine = args.get(1).expect("usage: dbbench <sqlite|redb|fjall|lmdb> <dir>");
    let dir = args.get(2).expect("dir").clone();
    std::fs::create_dir_all(&dir).unwrap();
    let r = match engine.as_str() {
        "sqlite" => bench_sqlite(&dir),
        "redb" => bench_redb(&dir),
        "fjall" => bench_fjall(&dir),
        "lmdb" => bench_lmdb(&dir),
        _ => panic!("unknown engine"),
    };
    report(engine, r);
}
