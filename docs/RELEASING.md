# Releasing Constellation

Release artifacts are reproducible outputs of a reviewed commit. The
coordinator, not an implementation phase, creates the release tag.

1. Confirm the nightly full matrix is green on the release commit.
2. Update the workspace version if the release changes it.
3. Create and push an annotated `vMAJOR.MINOR.PATCH` tag.
4. Run the nightly workflow manually for that tag/commit.
5. Download the Linux musl and macOS archives from the workflow artifacts.
6. Verify each archive contains `constellation`, `LICENSE`, and `README.md`;
   run `constellation --version` and confirm it reports the tag.
7. On Linux, run `ldd constellation` and require `not a dynamic executable`
   (or equivalent static-binary output).
8. Create the GitHub release and attach both archives and checksums.

Linux artifacts are built with `make dist-linux`. This uses the
`x86_64-unknown-linux-musl` target and bundled SQLite, so no libc or SQLite
shared object is required on the destination. `make dist-macos` is intentionally
native-only: macFUSE/FUSE must not be cross-compiled from Linux.
