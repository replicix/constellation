//! The seeded workload of `crate::workload`, for a tree the harness can
//! only reach through `kubectl exec`: a block of operations becomes one
//! shell script (file contents inline, base64), run by the pod's `sh` with
//! coreutils; the same operations are applied to the [`Model`] as the
//! script is built. One `exec` per block, so a block of a few dozen
//! operations costs one API round trip, not one per operation.
//!
//! The op mix and its weights follow `Workload::one_op`. Contents are
//! smaller (most files up to 64 KiB, one in ten up to 2 MiB, so still
//! multi-chunk at the scenarios' 1 MiB chunk size) because they travel in
//! the script.

use crate::model::{sha256_of, Model, Node, Observed};
use anyhow::{bail, Context, Result};
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

pub struct RemoteWorkload {
    rng: StdRng,
    counter: u64,
    ops: u64,
    prefix: String,
}

/// `'...'` quoting for `sh`.
fn q(p: &Path) -> String {
    format!("'{}'", p.to_string_lossy().replace('\'', r"'\''"))
}

pub fn base64(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4 + data.len() / 57 + 1);
    for (i, c) in data.chunks(3).enumerate() {
        // `base64 -d` takes any line length; break lines so no single
        // line of the script is megabytes long.
        if i > 0 && i % 19 == 0 {
            out.push('\n');
        }
        let n = (c[0] as u32) << 16
            | (*c.get(1).unwrap_or(&0) as u32) << 8
            | *c.get(2).unwrap_or(&0) as u32;
        out.push(T[(n >> 18) as usize & 63] as char);
        out.push(T[(n >> 12) as usize & 63] as char);
        out.push(if c.len() > 1 {
            T[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if c.len() > 2 {
            T[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// `base64 -d` of `data` as a here-document, piped into `sink`.
fn heredoc(script: &mut String, sink: &str, data: &[u8]) {
    let _ = writeln!(script, "base64 -d <<'B64' {sink}\n{}\nB64", base64(data));
}

impl RemoteWorkload {
    pub fn new(seed: u64, prefix: &str) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            counter: 0,
            ops: 0,
            prefix: prefix.into(),
        }
    }

    fn fresh_name(&mut self, kind: &str) -> String {
        self.counter += 1;
        format!("{}-{kind}-{}", self.prefix, self.counter)
    }

    fn data(&mut self, max_len: usize) -> Vec<u8> {
        let len = self.rng.random_range(0..=max_len);
        let mut v = vec![0u8; len];
        self.rng.fill(&mut v[..]);
        v
    }

    fn pick(&mut self, v: &[PathBuf]) -> Option<PathBuf> {
        v.choose(&mut self.rng).cloned()
    }

    /// A script running `n` random operations under `root` (an absolute
    /// directory in the pod), each mirrored into `model` as it is
    /// generated. Every file is closed when the script ends.
    pub fn block(&mut self, root: &str, model: &mut Model, n: usize) -> String {
        let mut s = String::new();
        let _ = writeln!(
            s,
            "set -eu\nop=start\ntrap 'rc=$?; [ $rc = 0 ] || echo \"failed at op $op (exit $rc)\" >&2' EXIT\ncd {}",
            q(Path::new(root))
        );
        for _ in 0..n {
            self.one_op(&mut s, model);
        }
        s
    }

    fn one_op(&mut self, s: &mut String, model: &mut Model) {
        let dice = self.rng.random_range(0..100);
        self.ops += 1;
        let _ = writeln!(s, "op={}", self.ops);
        match dice {
            0..=24 => self.op_create(s, model),
            25..=39 => self.op_overwrite(s, model),
            40..=49 => self.op_append(s, model),
            50..=57 => self.op_truncate(s, model),
            58..=69 => self.op_mkdir(s, model),
            70..=79 => self.op_rename(s, model),
            80..=89 => self.op_unlink(s, model),
            90..=93 => self.op_rmdir(s, model),
            94..=96 => self.op_symlink(s, model),
            _ => self.op_read_check(s, model),
        }
    }

    fn file_len(model: &Model, rel: &Path) -> usize {
        match model.nodes.get(rel) {
            Some(Node::File { data }) => data.len(),
            _ => 0,
        }
    }

    fn op_create(&mut self, s: &mut String, model: &mut Model) {
        let Some(dir) = self.pick(&model.dirs()) else {
            return;
        };
        let rel = dir.join(self.fresh_name("f"));
        let max = if self.rng.random_range(0..10) == 0 {
            2 << 20
        } else {
            64 << 10
        };
        let data = self.data(max);
        heredoc(s, &format!("> {}", q(&rel)), &data);
        model.write_file(&rel, data);
    }

    fn op_overwrite(&mut self, s: &mut String, model: &mut Model) {
        let Some(rel) = self.pick(&model.files()) else {
            return;
        };
        let len = Self::file_len(model, &rel);
        let offset = if len == 0 {
            0
        } else {
            self.rng.random_range(0..len)
        };
        let patch = self.data(32 << 10);
        heredoc(
            s,
            &format!(
                "| dd of={} bs=4096 seek={offset} oflag=seek_bytes iflag=fullblock conv=notrunc status=none",
                q(&rel)
            ),
            &patch,
        );
        model.overwrite(&rel, offset, &patch);
    }

    fn op_append(&mut self, s: &mut String, model: &mut Model) {
        let Some(rel) = self.pick(&model.files()) else {
            return;
        };
        let extra = self.data(32 << 10);
        heredoc(s, &format!(">> {}", q(&rel)), &extra);
        model.append(&rel, &extra);
    }

    fn op_truncate(&mut self, s: &mut String, model: &mut Model) {
        let Some(rel) = self.pick(&model.files()) else {
            return;
        };
        let len = Self::file_len(model, &rel);
        let new_len = self.rng.random_range(0..=len.max(1));
        let _ = writeln!(s, "truncate -s {new_len} {}", q(&rel));
        model.truncate(&rel, new_len);
    }

    fn op_mkdir(&mut self, s: &mut String, model: &mut Model) {
        let Some(dir) = self.pick(&model.dirs()) else {
            return;
        };
        let rel = dir.join(self.fresh_name("d"));
        let _ = writeln!(s, "mkdir {}", q(&rel));
        model.mkdir(&rel);
    }

    fn op_rename(&mut self, s: &mut String, model: &mut Model) {
        let Some(from) = self.pick(&model.files()) else {
            return;
        };
        let Some(dir) = self.pick(&model.dirs()) else {
            return;
        };
        let to = dir.join(self.fresh_name("r"));
        let _ = writeln!(s, "mv -T {} {}", q(&from), q(&to));
        model.rename(&from, &to);
    }

    fn op_unlink(&mut self, s: &mut String, model: &mut Model) {
        let Some(rel) = self.pick(&model.files()) else {
            return;
        };
        let _ = writeln!(s, "rm {}", q(&rel));
        model.remove(&rel);
    }

    fn op_rmdir(&mut self, s: &mut String, model: &mut Model) {
        let dirs: Vec<PathBuf> = model
            .dirs()
            .into_iter()
            .filter(|d| !d.as_os_str().is_empty() && model.children(d) == 0)
            .collect();
        let Some(rel) = self.pick(&dirs) else {
            return;
        };
        let _ = writeln!(s, "rmdir {}", q(&rel));
        model.remove(&rel);
    }

    fn op_symlink(&mut self, s: &mut String, model: &mut Model) {
        let Some(dir) = self.pick(&model.dirs()) else {
            return;
        };
        let rel = dir.join(self.fresh_name("l"));
        let target = self
            .pick(&model.files())
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| "dangling".into());
        let _ = writeln!(s, "ln -s {} {}", q(Path::new(&target)), q(&rel));
        model.symlink(&rel, &target);
    }

    /// Read a file back inside the block and compare with the model there
    /// and then (the writer's own view must be exact at once).
    fn op_read_check(&mut self, s: &mut String, model: &mut Model) {
        let Some(rel) = self.pick(&model.files()) else {
            return;
        };
        let Some(Node::File { data }) = model.nodes.get(&rel) else {
            return;
        };
        let want: String = sha256_of(data).iter().map(|b| format!("{b:02x}")).collect();
        let _ = writeln!(
            s,
            "[ \"$(sha256sum < {p} | cut -c1-64)\" = {want} ] || {{ echo \"MODEL DIVERGENCE on read of \"{p} >&2; exit 1; }}",
            p = q(&rel)
        );
    }
}

/// The pod tool (`pod_tool.pl`): xattrs, `fcntl` locks, `SEEK_HOLE` /
/// `SEEK_DATA`, an `O_APPEND` writer and its checker, as `perl` (the
/// driver image has perl-base, not attr or python). [`pod_tool_script`]
/// installs it at [`POD_TOOL`]; scripts then run `perl /tmp/pt.pl OP ...`.
pub const POD_TOOL: &str = "/tmp/pt.pl";

/// A script fragment writing the pod tool to [`POD_TOOL`].
pub fn pod_tool_script() -> String {
    format!(
        "cat > {POD_TOOL} <<'POD_TOOL_EOF'\n{}POD_TOOL_EOF\n",
        include_str!("pod_tool.pl")
    )
}

/// A script printing the tree under `dir` one entry per line, sorted:
/// `d<TAB>path`, `l<TAB>path<TAB>target`, `f<TAB>path<TAB>size<TAB>sha256`
/// (paths relative, `./`-prefixed). It fails on any error reading the tree.
pub fn listing_script(dir: &str) -> String {
    format!(
        r#"set -eu
cd {dir}
t=/tmp/k8s-ls.$$
trap 'rm -f $t' EXIT
find . -mindepth 1 > $t
LC_ALL=C sort -o $t $t
while IFS= read -r p; do
  if [ -L "$p" ]; then printf 'l\t%s\t%s\n' "$p" "$(readlink "$p")"
  elif [ -d "$p" ]; then printf 'd\t%s\n' "$p"
  elif [ -f "$p" ]; then
    size=$(stat -c %s "$p")
    sum=$(sha256sum < "$p")
    printf 'f\t%s\t%s\t%s\n' "$p" "$size" "${{sum%% *}}"
  else printf 'o\t%s\n' "$p"
  fi
done < $t
"#,
        dir = q(Path::new(dir))
    )
}

fn unhex(s: &str) -> Result<[u8; 32]> {
    if s.len() != 64 {
        bail!("bad sha256 {s:?}");
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).context("bad sha256")?;
    }
    Ok(out)
}

/// Parse [`listing_script`]'s output.
pub fn parse_listing(out: &str) -> Result<BTreeMap<PathBuf, Observed>> {
    let mut m = BTreeMap::new();
    for line in out.lines().filter(|l| !l.is_empty()) {
        let f: Vec<&str> = line.split('\t').collect();
        let path = |p: &str| PathBuf::from(p.strip_prefix("./").unwrap_or(p));
        let (p, o) = match f.as_slice() {
            ["d", p] => (path(p), Observed::Dir),
            ["l", p, t] => (
                path(p),
                Observed::Symlink {
                    target: t.to_string(),
                },
            ),
            ["f", p, size, sum] => (
                path(p),
                Observed::File {
                    size: size.parse().context("size")?,
                    sha256: unhex(sum)?,
                },
            ),
            _ => bail!("unexpected listing line {line:?}"),
        };
        m.insert(p, o);
    }
    Ok(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_the_reference_alphabet() {
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(b"foobar"), "Zm9vYmFy");
        assert_eq!(base64(&[0xff, 0xfe, 0xfd]), "//79");
    }

    #[test]
    fn listings_parse_into_observed_entries() {
        let sum = "ab".repeat(32);
        let out = format!("d\t./a\nf\t./a/x\t3\t{sum}\nl\t./y\ta/x\n");
        let m = parse_listing(&out).unwrap();
        assert_eq!(m[Path::new("a")], Observed::Dir);
        assert_eq!(
            m[Path::new("a/x")],
            Observed::File {
                size: 3,
                sha256: [0xab; 32]
            }
        );
        assert_eq!(
            m[Path::new("y")],
            Observed::Symlink {
                target: "a/x".into()
            }
        );
    }

    #[test]
    fn a_block_mirrors_into_the_model_deterministically() {
        let (mut m1, mut m2) = (Model::default(), Model::default());
        let s1 = RemoteWorkload::new(7, "p").block("/data/v", &mut m1, 50);
        let s2 = RemoteWorkload::new(7, "p").block("/data/v", &mut m2, 50);
        assert_eq!(s1, s2);
        assert_eq!(m1.nodes, m2.nodes);
        assert!(!m1.nodes.is_empty());
    }

    /// The scripts are what the pods run: here `sh` and coreutils run them
    /// on a local directory, which `Model::verify` (the local oracle) and
    /// the listing then check.
    #[cfg(target_os = "linux")]
    #[test]
    fn blocks_and_listings_run_by_sh_agree_with_the_model() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let sh = |script: &str| {
            let mut c = std::process::Command::new("sh")
                .arg("-s")
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            std::io::Write::write_all(&mut c.stdin.take().unwrap(), script.as_bytes()).unwrap();
            let out = c.wait_with_output().unwrap();
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8(out.stdout).unwrap()
        };
        let mut model = Model::default();
        let mut w = RemoteWorkload::new(3, "t");
        for _ in 0..4 {
            sh(&w.block(root, &mut model, 40));
            model.verify(dir.path()).unwrap();
        }
        let seen = parse_listing(&sh(&listing_script(root))).unwrap();
        model.verify_observed(&seen).unwrap();
        assert!(model.files().len() > 3);
    }
}
