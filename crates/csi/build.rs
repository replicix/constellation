//! Generates the CSI v1 gRPC/protobuf bindings from `proto/csi.proto`
//! (vendored; the exact spec tag is in `proto/VERSION`).
//!
//! `protoc` comes from `protoc-bin-vendored` (a precompiled binary, no C/C++
//! toolchain needed) rather than relying on a system install, so `cargo
//! build` works unmodified on a clean box — pointed at via `PROTOC`, which
//! `prost-build` reads itself.

fn main() {
    println!("cargo:rerun-if-changed=proto/csi.proto");
    println!("cargo:rerun-if-changed=proto/VERSION");

    let protoc = protoc_bin_vendored::protoc_bin_path()
        .expect("protoc-bin-vendored has no binary for this host platform");
    // SAFETY: build scripts run single-threaded before any code that could
    // race on the process environment.
    unsafe {
        std::env::set_var("PROTOC", &protoc);
    }

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/csi.proto"], &["proto"])
        .expect("compiling proto/csi.proto");
}
