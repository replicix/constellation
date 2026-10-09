# External storage e2e: what is skipped, and why

`tests/csi/e2e.sh` runs every spec `e2e.test -ginkgo.focus='External.Storage'
-storage.testdriver=tests/csi/testdriver.yaml` selects for the driver: 89
specs with Kubernetes v1.37.0 (`kindest/node:v1.37.0`, the matching
`e2e.test`). There is no skip regex. Every skip below is the suite's own
decision, made from the capabilities `testdriver.yaml` declares or from the
cluster. The message is the suite's, quoted from the JUnit report
(`tests/csi/e2e-tally.py`). The upstream source is
`test/e2e/storage/testsuites/` at v1.37.0.

Last run (37-k7a, 2026-10-05, kind v0.33.0, 6 ginkgo processes): **64
passed, 0 failed, 25 skipped**, 310 s of specs.

## The driver lacks the capability (9)

| Specs | Suite's reason | Why the driver has no such capability |
|---|---|---|
| `[Dynamic PV (default fs)] provisioning should provision storage with mount options` | `Driver "constellation.csi.replicix.com" does not define supported mount option` | `NodeStageVolume`/`NodePublishVolume` take no mount flags: the staging mount is the driver's own `fuse_mount_fd` (settled decision 5), so `testdriver.yaml` declares no `SupportedMountOption`. |
| `[Dynamic PV (default fs)] volume-modify …` ×5 (vac-protection finalizer; create with VAC; modify with/without a VAC; recover from an invalid VAC) | `Driver "constellation.csi.replicix.com" has no configured VolumeAttributesClass` | `ControllerModifyVolume` is `UNIMPLEMENTED` and `MODIFY_VOLUME` is not advertised: plan 37 has no mutable volume attributes. |
| `[Dynamic PV (filesystem volmode)] multiVolume should concurrently access the volume and restored snapshot from pods on the same node` | `Driver "constellation.csi.replicix.com" does not support snapshots` | Not the driver: upstream (`multivolume.go`) skips this when `pattern.SnapshotType == ""`, which holds for every *Dynamic PV* pattern. The capability check before it (`snapshotDataSource: true`) passed. The same flow (a volume and its restored snapshot, both mounted) passes in the snapshottable and provisioning specs below and in `harness k8s-scenario csi-snapshot-clone-mount`. |
| `[Dynamic PV (block volmode)] pvc-deletion-performance …` and `[Dynamic PV (filesystem volmode)] volume-lifecycle-performance …` (both `[Serial] [Slow]`) | `Driver constellation.csi.replicix.com doesn't specify performance test options` | These need a `PerformanceTestOptions` block with provisioning-latency SLOs. Plan 37 sets none, and K0 Track B found the pool's metadata ceiling depends on the host, not on a fixed number (§"K0 results"). The block-mode one would skip anyway (`block: false`). |

## Needs an SSH-reachable cloud node (7)

| Specs | Suite's reason |
|---|---|
| `[Dynamic PV (default fs)] subPath should unmount if pod is force deleted while kubelet is down` and `… gracefully deleted while kubelet is down` (`[Disruptive] [Slow]`) | `No SSH Key for provider skeleton` |
| `[Dynamic PV (filesystem volmode)] disruptive …` ×5 (pod deleted / force deleted while kubelet is down, with and without a new SELinux context; pv written before kubelet restart) | `Only supported for providers [gce aws local azure] (not skeleton)` |

These stop and start kubelet over SSH, on a provider's VMs. A kind cluster
is provider `skeleton` with no SSH access to its node containers, so no
driver capability could enable them here. The kubelet-independence they
probe (mounts outlive the plugin) is covered by `harness k8s-scenario
csi-plugin-restart-survives`. The engine pod holds the FUSE session, not
kubelet or the plugin.

## The test pattern does not apply (9)

Upstream skips these for every driver. They are cross products of a test
and a pattern that do not fit together, so no capability enables them:

| Specs | Suite's reason |
|---|---|
| `[Dynamic Snapshot (delete policy)]`, `[Dynamic Snapshot (retain policy)]`, `[Pre-provisioned Snapshot (delete policy)]`, `[Pre-provisioned Snapshot (retain policy)]` `snapshottable … check deletion (ephemeral)` | `volume type "DynamicPV" is not ephemeral` |
| `[Ephemeral Snapshot (delete policy)]`, `[Ephemeral Snapshot (retain policy)]` `snapshottable … check deletion (persistent)` | `volume type "GenericEphemeralVolume" is ephemeral` |
| `[Dynamic PV (filesystem volmode)] multiVolume should access to two volumes with different volume mode and retain data across pod recreation on the same node` and `… on different node` | `Filesystem volume case should be covered by block volume case` (the block-mode variants are not generated: `block: false`) |
| `[Generic Ephemeral-volume (default fs) (immediate-binding)] ephemeral should support multiple inline ephemeral volumes` | `Multiple generic ephemeral volumes with immediate binding may cause pod startup failures when the volumes get created in separate topology segments.` (its late-binding twin runs and passes) |

The counterpart of each of the first two rows runs and passes:
`check deletion (persistent)` in the Dynamic and Pre-provisioned patterns,
and `check deletion (ephemeral)` in the Ephemeral ones.

## Not generated at all

Patterns the declared capabilities rule out never become specs, so they are
not in the 89: block volumes (`block: false`), topology (`topology:
false`), volume limits (`volumeLimits: false`), node expansion
(`nodeExpansion: false`; controller expansion runs, online and offline),
SELinux mount (`seLinuxMount: false`), group snapshots, snapshot metadata,
CSI inline (non-generic) ephemeral volumes, and the in-tree/pre-provisioned
PV patterns an external driver has no `PreprovisionedVolumeTestDriver` for.
