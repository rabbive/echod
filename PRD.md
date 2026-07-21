# PRD — echod: Energy-Aware Consensus for IoT Edge Networks

**Status:** Living document — Phase 1 & 2 goals complete, Phase 3 (real-hardware three-way comparison) planned.
**Owner:** rabbive/echod

---

## 1. Background & Problem Statement

Consensus protocols (Raft, Paxos, etc.) keep a cluster of nodes agreed on shared state. They are battle-tested in datacenters, but their core assumptions break down at the IoT edge:

| Raft's assumption | IoT edge reality |
|---|---|
| All nodes are equal, wall-powered servers | Mixed hardware, often battery-constrained |
| Network is stable | Partitions are routine; sub-clusters must keep serving locally |
| Idle heartbeats are cheap | Radio transmission dominates a battery node's energy budget |

**echod** exists to study this gap empirically: build the protocols, run them under identical conditions, measure the difference, and — where the baseline is still wasteful — design something better.

---

## 2. Goals

The project has three sequential goals:

### Goal 1 — Establish the baseline: Raft vs ECHO ✅ Done
Build a fair, identical-conditions simulation harness and determine whether ECHO (tiered, energy-aware, event-driven) actually outperforms plain Raft for IoT-style workloads.

**Outcome:** Confirmed — ECHO wins by a wide margin on message efficiency, availability under partition, and energy fairness (low-battery nodes step aside instead of disrupting elections).

### Goal 2 — Build echoD: an optimized hybrid that beats *both* parents ✅ Done
Measuring ECHO against Raft under the same seeded workload exposed **ECHO's own waste** — most notably, a liveness ping broadcast to every node (including leaves) that accounted for 75–80% of ECHO's total traffic, plus unnecessary per-event consensus rounds and un-ordered elections that caused split votes. **echoD** is the hybrid built from those measurements: it keeps ECHO's tiered architecture and partition tolerance, and adds six targeted optimizations (see §5).

**Outcome:** Confirmed — echoD beats both Raft and ECHO on total messages (~5–7× fewer than ECHO over 30s), consensus latency, availability, and energy balance (rotates leadership before any node dies, instead of draining one node to near-death).

### Goal 3 — Validate on real hardware: Raft vs ECHO vs echoD ⏳ Planned
Everything above is simulated (Phase 1) or run over a real MQTT broker with mocked sensors (Phase 2, ECHO only). The remaining goal is to run **all three protocols** on **real ESP32 hardware** and produce a final, hardware-validated three-way comparison — closing the loop from "measured in simulation" to "measured in the field."

---

## 3. Non-Goals

- Replacing Raft/etcd/Consul in datacenter deployments — echoD targets energy-constrained edge participants specifically, not general-purpose consensus.
- Formal TLA+/proof-level safety verification (tests cover the practical scenarios; a formal proof is out of scope for this phase).
- Filing the patent / writing the academic paper itself — this project produces the evidence and design doc (`docs/ECHOD_VS_RAFT_ECHO.md`) that such a paper would cite, but authoring it is a separate downstream activity.
- Building a production-grade deployment story (auth, TLS, ops tooling) beyond what Phase 2's demo already has.

---

## 4. Target Use Cases / Personas

echoD targets deployments where **consensus participants themselves** are battery- or energy-constrained and partitions are routine — the niche datacenter protocols ignore and cloud-gateway architectures route around:

| Domain | Scenario | echoD feature that matters |
|---|---|---|
| Precision agriculture | Solar-powered field gateways replicate soil/irrigation state; backhaul drops daily | Edge filtering saves radio energy; battery-ordered elections pick the best-charged gateway; provisional mode survives uplink loss |
| Wildfire / environmental monitoring | Forest sensor mesh on multi-year batteries; only real changes matter | Delta filtering (near-zero radio cost for unchanged readings); adaptive liveness during quiet seasons |
| Disaster response | First-responder mesh nodes on battery packs; teams separate and reunite | Provisional consensus keeps each team operational; batched reconciliation merges state on rejoin; handoff moves leadership off dying nodes |
| Industrial IoT / pipelines | Thousands of ESP32-class sensors under a few gateway coordinators | Tiered consensus (O(k) not O(n)); edge deadband filtering; per-coordinator leaf keepalives |
| Smart microgrids | Islanded operation during grid faults, re-sync after | Partition-triggered provisional mode + epoch-tagged reconciliation |
| Defense / field operations | Networks disconnected by design for hours, merging on contact | All of the above — disconnection is the normal case, not the failure case |

---

## 5. Functional Requirements

### 5.1 Phase 1 — Simulation harness (✅ complete)

- FR1.1 — Implement Raft, ECHO, and echoD against the same `Cluster`/`MessageBus` abstraction so message counting is apples-to-apples.
- FR1.2 — A single seeded workload generator (`simulation/workload.py`) replays an **identical** event schedule across all three protocols per run.
- FR1.3 — Energy model: role-weighted idle drain (leader/candidate/follower/observer/leaf) + per-message TX/RX cost, so messaging efficiency shows up directly in energy metrics.
- FR1.4 — Metrics collector tracks: total messages (by type), consensus latency, leader changes, availability %, per-node energy over time.
- FR1.5 — CLI (`simulation/main.py`) runs all three protocols back-to-back and exports comparative CSVs + optional charts.
- FR1.6 — echoD implements all six optimizations (edge filtering, batching, adaptive coordinators-only liveness, battery-ordered elections, directed handoff, batched reconciliation) as `EchoDCoordinator`/`EchoDLeaf`, subclassing ECHO rather than duplicating it.
- FR1.7 — Test suite covers election, energy transitions, partition/reconciliation, and echoD-specific behavior for all three protocols.

**Acceptance criteria:** `python3 -m simulation.main` runs all three protocols under the same workload and produces a CSV with per-protocol metrics; `pytest simulation/tests/` passes on Python 3.10 and 3.12.

### 5.2 Phase 2 — Hardware-free MQTT demo (⚠️ ECHO only)

- FR2.1 (done) — ECHO coordinators run over real MQTT (paho-mqtt), with mock leaves and a live Flask/Socket.IO dashboard.
- FR2.2 (not done) — Port echoD's six optimizations to the MQTT/dashboard stack (`rpi/coordinator/`), so Phase 2 can demo all three protocols, not just ECHO.
- FR2.3 (not done) — Run a real Raft baseline over the same MQTT harness for a like-for-like Phase 2 comparison.

**Acceptance criteria (for the outstanding items):** `bash scripts/demo.sh` can launch an echoD cluster (not just ECHO) and the dashboard distinguishes protocol mode; a Raft variant of the same demo exists for comparison.

### 5.3 Phase 3 — Real hardware validation (⏳ planned — the current focus)

This is the remaining goal: **run Raft, ECHO, and echoD on real ESP32 hardware and produce a final hardware-measured three-way comparison**, mirroring the simulation's Raft-vs-ECHO-vs-echoD structure but with real radios, real batteries, and real network conditions.

- FR3.1 — Coordinator firmware/host for all three protocols on real devices (today, coordinators only exist as Python processes in Phase 1/2; ESP32 firmware only has a leaf role). Decide whether coordinators run as Raspberry Pi processes (reusing/extending `rpi/coordinator/`) or as ESP32 firmware, and implement accordingly.
- FR3.2 — Extend ESP32 leaf firmware (`esp32/main/echo_leaf.c`) to support Raft-style flat client commands (for the Raft arm) and echoD's edge-side delta filtering (currently only ECHO's coordinator-side filtering exists in firmware).
- FR3.3 — Real battery/energy measurement path (replacing the simulation's normalized energy model and Phase 2's mock/real ADS1115 battery monitor) so energy numbers are physically measured, not modeled.
- FR3.4 — A repeatable field/bench test procedure: same physical topology, same workload pattern (as close as possible to `simulation/workload.py`'s seeded schedule), run once per protocol.
- FR3.5 — Metrics capture on real hardware: message counts (via network capture or firmware-side counters), latency, availability during induced partitions (e.g. physically disabling a radio link), and battery drain over the run.
- FR3.6 — A final report/section (likely extending `docs/ECHOD_VS_RAFT_ECHO.md` or a new `docs/PHASE3_HARDWARE_RESULTS.md`) presenting the real-hardware numbers side-by-side with the Phase 1 simulation numbers, and calling out any divergence (e.g., real radio contention, clock drift, or ADC noise that the simulation didn't model).

**Acceptance criteria:** all three protocols run on real ESP32 (+ coordinator host) hardware for a fixed test duration; message counts, latency, availability, and energy drain are captured for each; results are written up and compared against the Phase 1 simulation predictions.

**Open questions to resolve before implementation (flag to the user, don't assume):**
1. Do coordinators run on ESP32 or on Raspberry Pi-class hosts for Phase 3? (Affects whether FR3.1 extends `esp32/main/` or `rpi/coordinator/`.)
2. What real battery hardware is available (coin cell / LiPo + fuel gauge) and does `rpi/coordinator/battery.py`'s ADS1115 path already cover it?
3. How is a "partition" induced physically (powering off a node, disabling WiFi, physical distance/Faraday enclosure)?
4. What's the target cluster size for the hardware test (does it need to match the simulation's 5 coordinators / 10 leaves, or is a smaller proof-of-concept acceptable first)?

---

## 6. Success Metrics

Carried over from the simulation (Phase 1, already measured) as the targets Phase 3 must reproduce on real hardware:

| Metric | Raft | ECHO | echoD (target to reproduce on hardware) |
|---|---|---|---|
| Total messages (30s run) | 6876 | 9448 | **1430** (~5–7× fewer than ECHO) |
| Availability | 98.98% | 97.95% | **99.32%** |
| Max node energy drain | 0.905 (nearly dead) | 0.898 | 0.85, rest balanced ~0.31 |
| Avg consensus latency (5s run) | 0.53 ms | 0.40 ms | **0.13 ms** |

Phase 3 succeeds if the real-hardware run shows the **same directional result** (echoD < ECHO < Raft on messages; echoD ≥ ECHO ≥ Raft on availability and energy balance), even if absolute numbers differ from simulation due to real radio/network effects.

---

## 7. Known Tradeoffs & Risks

- **Slower worst-case failure detection:** echoD's election timeouts (300–600ms) are wider than Raft/ECHO's (150–300ms) so the adaptive ping backoff doesn't cause spurious elections. This must be disclosed alongside every efficiency win, in both docs and any hardware report.
- **Prior art / novelty:** `docs/ECHOD_VS_RAFT_ECHO.md` documents that most individual echoD ingredients (batching, tiered voters, leader transfer, deadband filtering, energy-aware rotation) already exist separately in industry (etcd, Kafka KRaft, HashiCorp Raft, LEACH, Bayou). The novel contribution is the **integration** into one Raft-family protocol plus the controlled three-way measurement — not any single mechanism. A proper prior-art search (IEEE Xplore, Google Patents) is flagged as required before any patent filing or paper submission — not yet done.
- **Simulation-to-hardware gap:** the energy model, message costs, and network behavior in Phase 1 are idealized. Phase 3 must be treated as validation, not a formality — results may diverge (e.g., real WiFi contention could change the "coordinators-only ping" savings ratio).
- **Phase 2/3 currently ECHO-only:** there is real engineering work (FR2.2, FR2.3, FR3.1–FR3.2) before a three-way hardware comparison is even possible — this isn't a small last step.

---

## 8. Milestones / Status Summary

| Milestone | Status |
|---|---|
| Raft baseline in simulation | ✅ Done |
| ECHO in simulation, proven better than Raft | ✅ Done |
| Fair 3-way simulation harness (seeded workload, energy model, metrics) | ✅ Done |
| echoD hybrid designed and implemented (6 optimizations) | ✅ Done |
| echoD proven better than both parents (simulation) | ✅ Done |
| Design/prior-art writeup (`docs/ECHOD_VS_RAFT_ECHO.md`) | ✅ Done |
| ECHO ported to Phase 2 (MQTT + dashboard) | ✅ Done |
| echoD + Raft ported to Phase 2 | ⏳ Not started |
| ESP32 leaf firmware (ECHO) | ✅ Done |
| ESP32 coordinator firmware / host decision | ⏳ Not started |
| echoD + Raft on ESP32 | ⏳ Not started |
| Real-hardware 3-way comparison + report | ⏳ Not started — **current goal** |

---

## 9. References

- [`README.md`](README.md) — protocol comparison, measured simulation results, real-world use cases
- [`docs/ECHOD_VS_RAFT_ECHO.md`](docs/ECHOD_VS_RAFT_ECHO.md) — full design comparison, six optimizations, measured numbers, prior-art analysis
- [`docs/ECHO_ARCHITECTURE.plan.md`](docs/ECHO_ARCHITECTURE.plan.md) — simulation + MQTT architecture
- [`docs/ECHO_DEMO_AND_RAFT.md`](docs/ECHO_DEMO_AND_RAFT.md) — dashboard and demo controls
- [`CLAUDE.md`](CLAUDE.md) — engineering conventions, commands, and gotchas for this repo
