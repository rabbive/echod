# ADR 0004: Deterministic seeded workload for fair 3-way protocol comparison

**Status:** Accepted

## Context

Early comparisons between Raft and ECHO were not apples-to-apples: Raft ran with
essentially no injected client traffic while ECHO's leaves actively generated sensor
readings, so Raft was being measured "doing nothing" against ECHO "doing real work."
Any message-count or latency comparison built on that setup would be meaningless —
and it would get worse, not better, with a third protocol (echoD) added on top,
since three protocols with three different ad hoc traffic patterns can't be compared
at all.

## Decision

`simulation/workload.py` generates one **deterministic, seeded** event schedule
(`generate_workload`) — a list of `(time, source_index, value)` readings following a
per-source random walk (mostly small sub-threshold drifts, ~30% large jumps that
breach `DELTA_THRESHOLD`). The same schedule (same `--seed`) is replayed through
`deliver_event` for Raft, ECHO, and echoD in the same run of `simulation.main`. For
tiered protocols the event is injected into the corresponding leaf; for flat Raft it's
proposed directly to the current leader as a client command, so each protocol sees the
same logical events via its own normal path.

## Consequences

- Any difference in messages, latency, or energy between the three protocols is attributable to the protocol design, not to different injected traffic — this is what makes the numbers in `docs/ECHOD_VS_RAFT_ECHO.md` and the README defensible.
- Reproducibility: identical `--seed` (default 42) always produces the identical schedule, so results are re-runnable and diffable across code changes.
- The ~30% breach rate is a deliberately chosen constant to exercise both the filtering path (most readings) and the consensus path (breaching readings) — if this ratio is changed, all historical comparison numbers need to be re-measured, not just re-quoted.
- `--no-workload` exists specifically to isolate idle-cluster behavior (e.g., pure heartbeat/ping overhead with zero sensor events) from workload-driven behavior — don't conflate the two when reasoning about "why does echoD send fewer messages," since idle savings (optimization 3) and workload savings (optimizations 1, 2) are separate mechanisms measured differently.
- This harness only covers Phase 1 (simulation). Phase 3's real-hardware validation (see `PRD.md` §5.3) needs an equivalent reproducible workload procedure on physical devices — a seeded RNG schedule doesn't transfer directly to real sensor readings, and that gap is one of the open questions flagged in the PRD.
