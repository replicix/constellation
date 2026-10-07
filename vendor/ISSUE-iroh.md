# `RemoteStateActor::apply_selected_path` logs `could not close last open path` at ERROR for a benign race

## Version

- `iroh` **1.1.0** / **1.2.0** from crates.io (`src/socket/remote_map/remote_state.rs`, `apply_selected_path`, ~line 718); `noq-proto` 1.2/1.3.

## Problem

`apply_selected_path` closes redundant IP paths when its own `conn_state.paths` lists more than one IP path. That map can still hold a path noq has already abandoned (the `PathAbandoned` event is not yet processed), so the count is stale. `path.close()` then runs on what noq sees as the only non-abandoned path and returns `ClosePathError::LastOpenPath` (`noq-proto src/connection/mod.rs:638-641`). iroh logs that with `error!` and goes on. Nothing is closed, nothing leaks, the connection keeps working on its remaining path; the arm is a no-op by design (noq protects the last path). It should be `debug!`/`trace!`, like the `ClosedPath` arm next to it, which is silent.

## Reproducer

Not deterministic: it needs a direct-path change while a previous close is in flight (two IP paths on a client connection, the selected one switches, the other is abandoned by the peer at the same moment). Seen in the Constellation `overload-cascade-2` and `stress-ng-fs` harness scenarios, which fail on any ERROR line. Search a daemon log for `could not close last open path`.

## Workaround

Constellation's CLI installs a tracing layer (`IrohNoise`, `crates/cli/src/main.rs`) whose `event_enabled` drops only events with target `iroh::socket::remote_map::remote_state` and message exactly `could not close last open path`; the module's other events (`Opening path failed`, `multipath not negotiated`, ...) keep their levels. `RUST_LOG` naming `remote_state` re-enables the line.
