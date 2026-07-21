# ADR 0002: Deterministic battery-ordered election timeouts instead of energy-weighted vote scoring

**Status:** Accepted (echoD supersedes ECHO's approach)

## Context

ECHO's original approach to energy-aware leadership was **energy-weighted scoring**:
elections used standard randomized timeouts (like Raft), and ties in vote count were
broken by comparing candidate battery levels (`election_score()` in
`EchoCoordinator.handle_request_vote_response`). In practice this scoring path was
rarely exercised — random timeouts usually produce a single candidate well before a
tie can occur, so the energy-awareness was close to dead code. Worse, random timeouts
across 5+ coordinators regularly produced split votes: one measured run burned 84
RequestVote RPCs to elect a single leader.

## Decision

echoD replaces randomized timeouts with a **deterministic function of battery level**:

```
timeout = ECHOD_ELECTION_TIMEOUT_MIN + (1 - battery) * spread + crc32(node_id) % ELECTION_TIE_BREAK_MS
```

(`EchoDCoordinator._new_election_deadline` in `simulation/protocols/echod.py`). The
highest-battery coordinator always has the shortest timeout, so it always nominates
itself first and is elected on the first ballot before any other node becomes a
candidate. The `crc32(node_id)` term is a small, stable (not `hash()`, which isn't
stable across process runs) per-node offset that breaks exact battery ties
deterministically.

## Consequences

- Split votes are eliminated by construction — first candidate always wins, energy-optimal leader for free, no wasted RequestVote rounds. Measured: 4 RequestVote RPCs for a clean single-ballot election vs. ECHO's 14 (and worse before the wakeup-precision fix, see ADR context in `docs/ECHOD_VS_RAFT_ECHO.md` §4).
- Election timeouts widen to 300–600ms (`ECHOD_ELECTION_TIMEOUT_MIN/MAX`) vs. Raft/ECHO's 150–300ms, because the range needs enough spread to order nodes reliably and to stay clear of the adaptive ping backoff ceiling (`ECHOD_PING_MAX_INTERVAL`). This is the documented tradeoff: worst-case failure detection is ~150ms slower in exchange for far less idle traffic. Always state this alongside the win — see `CLAUDE.md`'s "Known tradeoff" note.
- This requires the node's wakeup/idle loop to fire at precise per-node deadlines rather than a coarse fixed polling quantum — a real bug (fixed) where a ~50ms quantum collapsed the deliberately-staggered 30ms tie-break offsets back into lockstep split votes. Any future change to `Cluster`/`Node` idle scheduling must preserve per-node deadline precision, not just "close enough."
- This mechanism only ranks coordinators against each other by battery; it does not, by itself, prevent a low-battery node from being a *candidate* at all — that's still handled by the separate `T_LOW` observer-mode gate (ADR-independent, inherited from ECHO).
