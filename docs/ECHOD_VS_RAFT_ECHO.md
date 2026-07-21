# echoD — Design Comparison & Industry Positioning

> How the echoD hybrid differs from Raft and ECHO, the optimizations it adds,
> how it behaves in comparison, and an honest analysis of prior art and novelty.
>
> *Last updated: July 2026 | Based on `simulation/protocols/echod.py` and
> three-way simulation results (5 coordinators + 10 leaves, seeded workload)*

---

## Table of Contents

1. [The One-Line Version](#1-the-one-line-version)
2. [Same Event, Three Protocols](#2-same-event-three-protocols)
3. [The Six Optimizations](#3-the-six-optimizations)
4. [Supporting Fixes That Made the Comparison Honest](#4-supporting-fixes-that-made-the-comparison-honest)
5. [Measured Results](#5-measured-results)
6. [The Honest Tradeoff](#6-the-honest-tradeoff)
7. [Is echoD Implemented in Industry?](#7-is-echod-implemented-in-industry)
8. [Where Each echoD Idea Already Exists](#8-where-each-echod-idea-already-exists)
9. [Why Industry Hasn't Assembled echoD](#9-why-industry-hasnt-assembled-echod)
10. [What Is Actually Novel in echoD](#10-what-is-actually-novel-in-echod)
11. [Prior-Art Caveat](#11-prior-art-caveat)

---

## 1. The One-Line Version

| Protocol | Characterization |
|---|---|
| **Raft** | Flat, always-on, reactive — every node does everything, all the time, whether or not there is work. |
| **ECHO** | Tiered and event-driven, but still chatty in the wrong places (broadcast pings, per-event rounds, dead-code energy logic). |
| **echoD** | Keeps ECHO's architecture but forces *every message to justify itself* — nothing is sent that a filter, a batch, or a smarter timer could eliminate. |

---

## 2. Same Event, Three Protocols

Scenario: **10 leaves sense a temperature spike simultaneously.**

### Raft
The spike arrives as 10 separate client commands → **10 separate consensus rounds**, each fanning out AppendEntries to all 4 peers (~40 messages for the burst). Meanwhile the leader's full heartbeats to every peer continue every 50 ms forever, even between bursts. Sub-threshold readings are replicated too — Raft has no concept of filtering.

### ECHO
All 10 leaves transmit (the delta check happens at the *coordinator*, so the radio cost is already paid) → each breaching reading triggers its **own consensus round** → all the while the leader broadcasts liveness pings to **all 14 nodes** every 50 ms. In a 5-second window that is ~1,200 ping messages alone — measured: pings were **75 % of ECHO's total traffic**.

### echoD
Each leaf checks the delta **locally**; sub-threshold readings are never transmitted → breaching leaves report → the leader buffers them and replicates the whole burst as **one log entry, one round** → pings go only to the 4 coordinators, and slow down exponentially while the cluster is idle.

---

## 3. The Six Optimizations

| # | Optimization | Raft / ECHO behavior | echoD behavior |
|---|---|---|---|
| 1 | **Edge-side delta filtering** | Raft: no filtering. ECHO: filters at the coordinator *after* transmission. | Filter lives on the leaf — suppressed readings cost **zero messages**. |
| 2 | **Batched consensus** | Both: one round per event. | Events within a 50 ms window → **1 log entry, 1 round** (instant flush when the batch fills — no latency penalty under load). |
| 3 | **Adaptive coordinators-only liveness** | Raft: full heartbeats to all peers every 50 ms. ECHO: ping *broadcast to all 15 nodes* every 50 ms. | Leader pings **coordinators only**, backing off 50 → 250 ms while idle, snapping back on activity; each coordinator keepalives *its own* leaves at 1 s. |
| 4 | **Battery-ordered election timeouts** | Both: random timeouts → split votes, wasted ballots (ECHO burned 84 RequestVotes in one run; its energy-weighted scoring was dead code). | `timeout = 300 + (1−battery)×300 + crc32(node_id) % 30` ms — the highest-battery node always nominates first and **wins on the first ballot**; energy-optimal leader for free. |
| 5 | **Leader handoff** | Both: leader drains until election timeout → availability gap + full randomized re-election. | Below 20 % battery (`T_HANDOFF`), the leader sends one directed `LeadershipHandoff` to its highest-battery peer (tracked via `responder_battery` in AppendEntries responses) — **no gap, 1 message**. |
| 6 | **Batched reconciliation** | ECHO: provisional entries replay one round each after a partition heals. | Entire provisional log replays as **one batch entry**. |

**Design rule:** in echoD, *consensus traffic never touches leaves at all* — RequestVote, AppendEntries, and pings are coordinator-to-coordinator only. (This also fixed a real ECHO bug: leaves registered to non-leader coordinators used to flap between ACTIVE and SEARCHING because only the leader's broadcast ping kept them alive.)

---

## 4. Supporting Fixes That Made the Comparison Honest

Not echoD features, but without them the numbers would be meaningless:

- **Identical seeded workload** replayed across all three protocols (Raft previously ran with zero client traffic — it was doing *nothing* while ECHO did work).
- **Energy model**: role-weighted idle drain + per-message TX/RX costs (previously every node drained identically — the energy metric was meaningless).
- **Followers forward sensor reports to the leader** (previously dropped silently).
- **Wakeup-precision fix**: nodes polled their inbox on a fixed 50 ms quantum, collapsing echoD's 30 ms-apart ordered timeouts into lockstep split votes. Nodes now wake exactly at their deadline — this fix alone cut ECHO's own election churn from 84 RequestVotes to 14.

---

## 5. Measured Results

### 5-second run (5 coordinators + 10 leaves, seeded workload)

| Metric | Raft | ECHO | echoD | echoD vs best |
|---|---|---|---|---|
| Total messages | 1112 | 1588 | **255** | **4.4× fewer** |
| Avg consensus latency | 0.53 ms | 0.40 ms | **0.13 ms** | **3× lower** |
| Leader energy drain | 0.209 | 0.286 | **0.158** | **24–45 % less** |
| RequestVote RPCs | 4 | 14 | **4** | clean single ballot |

### 30-second run — the paper story

| Metric | Raft | ECHO | echoD |
|---|---|---|---|
| Total messages | 6876 | 9448 | **1430** (~5–7× fewer) |
| Availability | 98.98 % | 97.95 % | **99.32 %** |
| Max node energy drain | **0.905 (nearly dead)** | 0.898 | 0.85, rest balanced ~0.31 |

Raft's long-term leader drained to near-death while echoD's handoff rotated leadership, keeping energy drain balanced across nodes.

---

## 6. The Honest Tradeoff

echoD's election timeouts are 300–600 ms vs Raft's 150–300 ms (required so adaptive pings can back off without triggering spurious elections). Worst-case failure detection is ~150 ms slower in exchange for ~5× less idle traffic — the right trade for battery-powered IoT, but it must be stated in the paper.

---

## 7. Is echoD Implemented in Industry?

**No.** echoD as a complete protocol is new — it was built in this project. However, **almost every individual ingredient already exists somewhere in industry**. What nobody has done is assemble them into one Raft-family protocol for IoT edge. The novelty is the *combination and integration*, not the parts.

---

## 8. Where Each echoD Idea Already Exists

| echoD optimization | Prior art in industry | Where it is used |
|---|---|---|
| Edge delta filtering | "Report-by-exception" / deadband filtering — decades old | **OPC UA** (DataChangeFilter deadbands) in factory SCADA; vehicular telemetry; industrial MQTT sensor networks |
| Batched consensus | Log batching is standard practice | **etcd**, **Kafka**, **MongoDB** (batched oplog), LogCabin — nearly every production consensus system |
| Tiered nodes (voters vs observers) | Controller/worker split | **Kafka KRaft** (small controller quorum, brokers don't vote), **Ceph** (Paxos monitors vs OSDs), **Hyperledger Fabric** (orderers vs peers) |
| Directed leader handoff | `LeadershipTransfer` / `TransferLeader` APIs | **HashiCorp Raft** (Consul, Nomad, Vault) and **etcd's raft library** both have exactly this — TimeoutNow-style. Must be cited as prior art. |
| Adaptive / leased heartbeats | Leader leases | **etcd leases**, HashiCorp Raft lease-based reads |
| Energy-aware leader election | **LEACH** (2000), HEED, PEGASIS — classic WSN protocols rotating cluster heads by battery | Academic + some industrial wireless sensor networks. **But these are NOT consensus protocols** — they give up strong consistency entirely. |
| Provisional writes during partition | Tentative writes + reconciliation | **Bayou** (1995, Xerox PARC), Dynamo-style eventual consistency, CRDTs — even **Git** is conceptually "provisional commits + manual reconcile" |

---

## 9. Why Industry Hasn't Assembled echoD

Four structural reasons:

1. **Industry avoids consensus at the edge — it sidesteps it, not fails at it.** The dominant architecture is gateway + cloud: edge devices are treated as dumb, unreliable data sources; consensus happens in the cloud (Kafka, etcd, DynamoDB). AWS Greengrass, Azure IoT Edge, KubeEdge — none run consensus *among edge nodes*. echoD solves a problem the industry has been routing around, not attacking.

2. **Raft's assumptions hold in the datacenter.** Raft was designed for homogeneous, wall-powered servers on stable networks. There, energy-weighted elections and delta filtering are pointless — so nobody added them to etcd/Consul. The pain only appears when consensus participants are battery-powered and the network partitions routinely, i.e., IoT edge.

3. **The two research communities never merged.** The WSN community solved energy (LEACH & friends) but abandoned strong consistency. The systems community solved consensus (Raft/Paxos) but ignored energy. echoD lives in the intersection — academically open territory rather than a crowded field.

4. **Battery-powered consensus participants are rare in practice.** Most deployed edge servers (Jetsons, industrial gateways) are wall-powered; only leaves are battery-powered, and in tiered designs leaves don't vote. The scenario where *coordinators themselves* are energy-constrained (field-deployed Pis, disaster-response networks, agricultural sensor gateways) is real but niche — too small for a cloud vendor to productize, exactly the right size for a research paper.

---

## 10. What Is Actually Novel in echoD

Frame the contribution this way:

1. **The integration**: a single Raft-family protocol combining energy-gated tiered membership, edge-filtered event-driven triggers, and provisional partition operation — with safety arguments that hold across all of them at once.

2. **Battery-ordered election timeouts** (optimization 4): deterministic energy-ranked self-nomination with hash tie-breaking. LEACH does probabilistic energy rotation; HashiCorp/etcd do manual transfer. *Deterministic battery-ordered election timing inside a consensus protocol* is not found in any known production system — the cleanest patent claim, stronger than the original "energy-weighted scoring" idea.

3. **Provisional consensus with epoch-tagged reconciliation** on top of Raft specifically — Bayou did tentative writes, but nobody has bolted provisional-then-replay onto Raft's log structure this way.

4. **The empirical comparison itself**: a controlled three-way evaluation (Raft vs ECHO vs echoD) under identical seeded workloads is a publishable contribution even where individual mechanisms have precedent.

---

## 11. Prior-Art Caveat

Knowledge of "who has shipped what" is solid for established systems but not exhaustive for recent edge-computing startups. Before writing the related-work section or filing the patent, run a proper prior-art search (IEEE Xplore, Google Patents) for:

- "energy-aware Raft"
- "battery-weighted leader election"
- "hierarchical consensus IoT edge"

If a claims examiner finds an energy-aware Raft paper first and it isn't cited, that is a much worse look than citing a dozen directly.

**Bottom line:** echoD isn't implemented in industry because industry deliberately keeps consensus away from the edge — which is precisely the gap this project attacks. The contribution isn't "nobody thought of batching"; it is "nobody built and measured the whole thing as one protocol." That is a legitimate, publishable claim — as long as the paper cites the prior art above rather than claiming each mechanism from scratch.
