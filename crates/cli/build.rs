use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/refs");
    println!("cargo:rerun-if-env-changed=CONSTELLATION_GIT_DESCRIBE");

    let version = std::env::var("CONSTELLATION_GIT_DESCRIBE")
        .ok()
        .filter(|version| !version.is_empty())
        .or_else(|| {
            Command::new("git")
                .args(["describe", "--tags", "--always", "--dirty"])
                .current_dir("../..")
                .output()
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| String::from_utf8(output.stdout).ok())
                .map(|version| version.trim().to_owned())
                .filter(|version| !version.is_empty())
        })
        .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());

    println!("cargo:rustc-env=CONSTELLATION_VERSION={version}");
}
