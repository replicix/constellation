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

## Kubernetes CSI image and chart

The CSI driver ships as one image (`constellation-csi`, with the
`constellation` engine binary its engine pods run) and one Helm chart
(`deploy/helm/constellation-csi`). Both are built locally; nothing in the
repository pushes them.

```bash
make csi-image-dist CSI_IMAGE=constellation-csi:X.Y.Z   # image + target/dist/csi/constellation-csi_X.Y.Z.tar.gz
make csi-chart                                           # chart check + target/dist/csi/constellation-csi-<chart version>.tgz
```

Before publishing, the nightly `kind-e2e` and `upgrade-under-load` jobs
must be green on the release commit. Set the chart's `version` and
`appVersion` (`Chart.yaml`) and `image.tag` (`values.yaml`) to the
release in the release commit itself. Then publish by hand, `REGISTRY`
being the release registry (for example `ghcr.io/<org>`):

```bash
docker tag constellation-csi:X.Y.Z "$REGISTRY/constellation-csi:X.Y.Z"
docker push "$REGISTRY/constellation-csi:X.Y.Z"
helm registry login "${REGISTRY%%/*}"
helm push target/dist/csi/constellation-csi-X.Y.Z.tgz "oci://$REGISTRY/charts"
```

Attach the image tarball and the chart `.tgz` to the GitHub release with
their checksums (`sha256sum target/dist/csi/*`). Users install with
`helm install constellation-csi oci://$REGISTRY/charts/constellation-csi
--version X.Y.Z --set image.repository=$REGISTRY/constellation-csi`
([Kubernetes CSI how-to](../kubernetes-csi.md)): the chart's
`image.repository` default is the unqualified `constellation-csi`, which
only a node that already has the image (kind, `docker load`) resolves, so
an install from the registry sets it; `image.tag` is the chart's
`appVersion` as released.
