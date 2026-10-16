# Explanation

Understanding-oriented discussion: why Constellation exists, how the
architecture fits together, and the decisions that shaped it.

Included here are:

- [Goals](GOALS.md) — goals, non-goals, motivating scenarios, prior art
- [Design](DESIGN.md) — architecture: data/metadata planes, leases,
  pin/offline/epochs, caching, compression, security, failure matrix
- [Decisions (ADRs)](DECISIONS.md) — what was chosen, what was rejected, and why
- [Arbiter study](ARBITER-STUDY.md) — should S3 stop being the arbiter? The
  week of 2026-10-16's bugs classified, a survey of comparable systems,
  options and a recommended commit rule (study, not yet a plan)

Related reference: [P2P relays](../reference/features/p2p-relays.md).
