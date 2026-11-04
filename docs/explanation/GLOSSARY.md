# Roles and terms, in plain words

This page explains the people in the play — which node does what — and the
handful of words the design, the ADRs and the plans lean on, without the
detail. Each entry links to where the precise rule lives. Read this first;
then [Design](DESIGN.md) §4, §5 and §9.

## The setting

Constellation is one file system shared by several machines (**nodes**).
File contents go to S3 as chunks; the *names and attributes* (create,
rename, unlink, chmod, ...) are **metadata**. Every metadata change goes
into one shared, ordered history in S3, **the log**. Every node keeps a
full copy of the metadata (its **replica**), built by reading the log.
Keeping that one history in one order, even when nodes crash or the
network splits, is what most of the machinery is for.

## Roles

- **Root (lease holder).** The one node in charge of the log at a time.
  It holds the **root lease**, a time-limited "I am in charge" record in
  S3 (`leases/p0.json`), renewed well before it runs out. The root orders
  changes and writes them to the log in batches (**segments**). Only one
  node can hold the lease; a node that lost it can no longer write to the
  log (the lease's **epoch** number fences it out).
  [Design §4, "One log, one root lease"](DESIGN.md#one-log-one-root-lease).

- **Backup.** A node that keeps a copy of the root's recent changes that
  are not yet in the log. The root counts a change as safe (**committed**)
  only once its backups hold it, so if the root dies, a backup can take
  over as the new root without losing anything the old root had already
  confirmed. Backups are chosen among nearby nodes.
  [Durability and failover](../reference/features/durability-and-failover.md).

- **Delegate.** A node busy in one directory, to which the root hands the
  authority over that directory (and what is under it), so the delegate
  answers operations there itself, without a round trip to the root. One
  hand-over is a **generation**, numbered by the tenure that grants it
  (`epoch:n`: the lease epoch and that tenure's counter, so no two tenures
  mint the same number,
  [ADR-43](DECISIONS.md#adr-43-a-generation-number-carries-the-epoch-of-the-tenure-that-minted-it)).
  The delegate:
  - **streams** its changes back to the root, which appends them to the
    log in order;
  - **renews** the generation every few seconds; a generation not renewed
    runs out (**lapses**);
  - can be **recalled**: the root takes the directory back, after the
    delegate's changes are all in the log.
  A delegate may have a backup of its own. Delegations never overlap or
  nest. [Design §4, "Delegated sub-sequencers"](DESIGN.md#delegated-sub-sequencers),
  [Delegations](../reference/features/delegations.md).

- **Sequencer.** Whoever orders the changes of a given file or directory:
  the root, or the delegate of its directory.

- **Requester (client).** The node where an application does a file
  operation. It sends the operation to the sequencer (a **forward**), or
  executes it itself if it is the sequencer.
  [Forwarded mutations](../reference/features/forwarded-mutations.md).

- **Group.** The nodes that must hold a change before it counts as
  committed: the root and its backups, or a delegate and its backup (and,
  for a delegate with no backup, the root's copy of the delegate's rows,
  [ADR-41](DECISIONS.md#adr-41-a-delegate-counts-the-roots-copy-only-through-rows-the-roots-tenure-has-committed-interim-rule-for-q16)).
  A **lone** sequencer is one whose group is itself, because no peer is
  near enough.

## Words for what happens

- **Journal.** A sequencer's local, ordered list of changes not yet in the
  log. **Shipping** writes them to the log.

- **Request id (rid).** Every operation carries one id for its whole life,
  across retries and redirects, so it is executed at most once; the log
  records each id as `Completed` or `Refused`.

- **Committed** (a change). Held by every member of its group, or already
  in the log. Only a committed change may be reported to the application
  as done.

- **Seal.** A member's persisted "I accept nothing more from the old
  sequencer", used to fence a dead or cut-off root or delegate before
  someone else takes over.

- **Takeover.** A new node becoming root: it wins the lease in S3, reads
  the log to its end, and writes a marker that fences the old root.

- **Takeover gate.** The period after a takeover in which the new root
  does not yet offer full service. It has two parts:
  1. the **journal work**: settle what the old root left behind, i.e.
     re-ship its committed changes held by the backups and decide the
     operations that were in flight; once this is done the root is
     **shippable**;
  2. the **read quarantine**: the old root may have handed
     out short-lived **read delegations** (permissions for a node to
     answer reads of an inode from its own copy) that no surviving record
     lists, so the new root waits until any such permission has surely
     expired before it changes what they cover.
  [Design §4](DESIGN.md#one-log-one-root-lease),
  [Close-to-open modes, read delegations](../reference/features/cto-modes.md#read-delegations).

- **Deadline and settle.** An operation that has no clear answer after its
  attempts (1.5 lease TTLs) is not simply failed. The client first
  **settles** it, within the rest of its deadline: it asks the sequencer in force, "did this id take
  effect?", and gets the late success, or a definitive refusal
  (`Abandoned`), after which the operation can never take effect. The
  application then hears success, or `EIO`.
  [Design §4, "Request ids and exactly-once"](DESIGN.md#request-ids-and-exactly-once).

- **Unsettled EIO.** The `EIO` an application hears when no sequencer
  could answer the settle in time. Such an operation may still take
  effect later. The design allows this only for an *isolated* requester,
  one that could reach no sequencer at all during its settle window.

## The rules the tests check (plan 43, plan 46)

- **I1: no definitive answer without a committed change.** "Done" means
  the change survives any single failure.
- **I2: one identity per operation.** An operation executes once, under
  its id, whatever the retries.
- **I4: an error never lands later.** If the application was told `EIO`,
  the operation must not take effect afterwards (except the isolated case
  above). Like a bank: "your transfer failed" must not be followed by the
  money moving an hour later.

The simulator (`crates/authority/tests/sim.rs`) runs thousands of seeded
failure schedules per configuration and checks these rules on every run.
