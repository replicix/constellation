//! `xattr`: extended attributes and the virtual `user.constellation.*`
//! ones, listed per `FrontendCaps::virtual_xattrs_listed`.

use super::{must, refused, refused_any, Env, TestResult};
use crate::caps::XattrSupport;
use crate::types::SetXattrFlags;
use constellation_types::Code;

const NONE: SetXattrFlags = SetXattrFlags::empty();
const RSIZE: &str = "user.constellation.rsize";
const RCOUNT: &str = "user.constellation.rcount";

fn without_virtual(names: Vec<String>) -> Vec<String> {
    names
        .into_iter()
        .filter(|n| !n.starts_with("user.constellation."))
        .collect()
}

pub(super) fn set_get_list_remove(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let f = must("put f", c.put(c.root(), "f", b"x")).attr.ino;
    must("set color", c.setxattr(f, "user.color", b"blue", NONE));
    must("set empty", c.setxattr(f, "user.empty", b"", NONE));
    assert_eq!(must("get color", c.getxattr(f, "user.color")), b"blue");
    assert_eq!(must("get empty", c.getxattr(f, "user.empty")), b"");
    assert_eq!(
        without_virtual(must("list", c.listxattr(f))),
        ["user.color", "user.empty"],
        "sorted, and only what was set (plus the virtual ones)"
    );
    must("overwrite", c.setxattr(f, "user.color", b"red", NONE));
    assert_eq!(must("get", c.getxattr(f, "user.color")), b"red");
    must("remove color", c.removexattr(f, "user.color"));
    refused("get removed", c.getxattr(f, "user.color"), Code::NoData);
    assert_eq!(
        without_virtual(must("list", c.listxattr(f))),
        ["user.empty"]
    );
    // On a directory too; binary values round-trip.
    let d = must("mkdir", c.mkdir(c.root(), "d")).attr.ino;
    let blob: Vec<u8> = (0..=255u8).collect();
    must("set blob", c.setxattr(d, "user.blob", &blob, NONE));
    assert_eq!(must("get blob", c.getxattr(d, "user.blob")), blob);
    Ok(())
}

pub(super) fn create_and_replace_flags(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let f = must("put f", c.put(c.root(), "f", b"x")).attr.ino;
    refused(
        "REPLACE of a missing name",
        c.setxattr(f, "user.a", b"1", SetXattrFlags::REPLACE),
        Code::NoData,
    );
    must(
        "CREATE of a missing name",
        c.setxattr(f, "user.a", b"1", SetXattrFlags::CREATE),
    );
    refused(
        "CREATE of an existing name",
        c.setxattr(f, "user.a", b"2", SetXattrFlags::CREATE),
        Code::Exists,
    );
    assert_eq!(
        must("get", c.getxattr(f, "user.a")),
        b"1",
        "a refused set changes nothing"
    );
    must(
        "REPLACE of an existing name",
        c.setxattr(f, "user.a", b"3", SetXattrFlags::REPLACE),
    );
    assert_eq!(must("get", c.getxattr(f, "user.a")), b"3");
    refused(
        "CREATE and REPLACE together",
        c.setxattr(
            f,
            "user.a",
            b"4",
            SetXattrFlags::CREATE | SetXattrFlags::REPLACE,
        ),
        Code::Invalid,
    );
    refused(
        "a flag the contract does not name",
        c.setxattr(f, "user.a", b"4", SetXattrFlags::UNSUPPORTED),
        Code::Invalid,
    );
    Ok(())
}

pub(super) fn missing_names(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let f = must("put f", c.put(c.root(), "f", b"x")).attr.ino;
    refused(
        "get of a missing name",
        c.getxattr(f, "user.nope"),
        Code::NoData,
    );
    refused(
        "remove of a missing name",
        c.removexattr(f, "user.nope"),
        Code::NoData,
    );
    assert!(without_virtual(must("list", c.listxattr(f))).is_empty());
    refused_any(
        "get on an inode that does not exist",
        c.getxattr(1 << 50, "user.a"),
        &[Code::NotFound, Code::Stale],
    );
    Ok(())
}

pub(super) fn namespaces_and_name_limits(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    if fx.caps.xattrs != XattrSupport::Native {
        super::skip!(
            "the Linux xattr namespaces apply to native xattrs only (this frontend maps names)"
        );
    }
    let user = fx.client();
    let root = fx.root_client();
    let f = must("put f", user.put(user.root(), "f", b"x")).attr.ino;
    refused(
        "security.*",
        user.setxattr(f, "security.x", b"1", NONE),
        Code::NotSupported,
    );
    refused(
        "system.*",
        root.setxattr(f, "system.posix_acl_access", b"1", NONE),
        Code::NotSupported,
    );
    refused(
        "trusted.* as a user",
        user.setxattr(f, "trusted.x", b"1", NONE),
        Code::Perm,
    );
    must(
        "trusted.* as root",
        root.setxattr(f, "trusted.x", b"1", NONE),
    );
    assert_eq!(must("get as root", root.getxattr(f, "trusted.x")), b"1");
    refused("empty name", user.setxattr(f, "", b"1", NONE), Code::Range);
    refused(
        "a name of 256 bytes",
        user.setxattr(f, &format!("user.{}", "n".repeat(251)), b"1", NONE),
        Code::Range,
    );
    must(
        "a name of 255 bytes",
        user.setxattr(f, &format!("user.{}", "n".repeat(250)), b"1", NONE),
    );
    refused(
        "a name that is not UTF-8",
        user.setxattr_bytes(f, b"user.\xff", b"1", NONE),
        Code::Invalid,
    );
    Ok(())
}

pub(super) fn value_size_limit(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let f = must("put f", c.put(c.root(), "f", b"x")).attr.ino;
    let max = vec![0xabu8; 64 * 1024];
    must("a value of 64 KiB", c.setxattr(f, "user.max", &max, NONE));
    assert_eq!(must("get", c.getxattr(f, "user.max")), max);
    refused(
        "a value of 64 KiB + 1",
        c.setxattr(f, "user.big", &vec![0u8; 64 * 1024 + 1], NONE),
        Code::TooBig,
    );
    refused(
        "the refused value was not stored",
        c.getxattr(f, "user.big"),
        Code::NoData,
    );
    Ok(())
}

pub(super) fn virtual_xattrs_follow_the_listing_capability(env: &Env<'_>) -> TestResult {
    for listed in [true, false] {
        let mut caps = env.caps().clone();
        caps.virtual_xattrs_listed = listed;
        let fx = env.fresh_with(&caps);
        let c = fx.client();
        let root = c.root();
        let d = must("mkdir d", c.mkdir(root, "d")).attr.ino;
        must("put d/a", c.put(d, "a", b"12345"));
        must("put d/b", c.put(d, "b", b"1234567"));
        must("set", c.setxattr(d, "user.mine", b"1", NONE));
        // Readable by name whatever the listing says.
        assert_eq!(
            must("rsize", c.getxattr(d, RSIZE)),
            b"12",
            "recursive size in bytes"
        );
        assert_eq!(
            must("rcount", c.getxattr(d, RCOUNT)),
            b"2",
            "recursive file count"
        );
        let names = must("list", c.listxattr(d));
        let mut want = vec!["user.mine".to_string()];
        if listed {
            want.push(RCOUNT.into());
            want.push(RSIZE.into());
        }
        want.sort();
        assert_eq!(names, want, "listed = {listed}");
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted, "the listing is sorted");
    }
    Ok(())
}

pub(super) fn virtual_xattrs_are_read_only(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let d = must("mkdir d", c.mkdir(c.root(), "d")).attr.ino;
    refused("set rsize", c.setxattr(d, RSIZE, b"1", NONE), Code::Perm);
    refused(
        "set rcount with CREATE",
        c.setxattr(d, RCOUNT, b"1", SetXattrFlags::CREATE),
        Code::Perm,
    );
    refused("remove rsize", c.removexattr(d, RSIZE), Code::Perm);
    refused("remove rcount", c.removexattr(d, RCOUNT), Code::Perm);
    assert_eq!(must("rsize", c.getxattr(d, RSIZE)), b"0", "still computed");
    Ok(())
}
