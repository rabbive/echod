# Equipment Request — echoD Consensus Protocol Hardware Evaluation

**Project:** echoD — Energy-aware Clustered Hierarchical Consensus for IoT Edge Networks
**Purpose:** Hardware validation of the echoD hybrid consensus protocol against Raft and ECHO baselines
**Prepared:** July 2026

---

## 1. Objective

We have built and simulated three consensus protocols — **Raft** (baseline), **ECHO**, and **echoD** (our hybrid) — and measured echoD to use **5–7× fewer network messages** with **24–45 % lower leader energy drain** in simulation. The next phase requires validating these results on physical IoT hardware.

**One cluster tests all three protocols.** Raft, ECHO, and echoD are software-only differences — the same hardware runs each protocol in turn, so no duplicate equipment is needed.

The evaluation has four experiments:

| # | Experiment | Hardware dependency |
|---|---|---|
| 1 | Message-count & latency comparison | Coordinators + router only |
| 2 | Energy consumption (headline result) | **Real batteries + voltage/current sensing on coordinators** |
| 3 | Leader-drain & directed-handoff behaviour | **Real batteries on coordinators** |
| 4 | Partition tolerance & reconciliation | Router with firewall (or free `iptables` alternative) |

Only experiments 2–3 require battery hardware — this enables the cost-saving hybrid option (C) below.

---

## 2. Coordinator board options (choose one)

Consensus coordinators are ordinary Linux processes (Python 3 + MQTT). Any board that runs Linux and WiFi works.

| Board | Price (each) | RAM / CPU | Notes |
|---|---|---|---|
| **A · Raspberry Pi 4 (2 GB)** | ~$45–55 | 2 GB / quad 1.5 GHz | Best supported; ADS1115 battery driver already written for it |
| **B · Raspberry Pi Zero 2 W** | ~$15–20 | 512 MB / quad 1 GHz | Cheapest Pi; runs the coordinator + MQTT comfortably; broker must run on the laptop |
| **C · Raspberry Pi 3B/3B+ (used)** | ~$20–30 used | 1 GB / quad 1.2 GHz | Labs often have spares; fully capable |
| **D · Orange Pi Zero 2W** | ~$20–25 | 1–1.5 GB / quad 1.5 GHz | Cheapest new board with WiFi; Armbian Linux. Battery driver needs a minor I2C change (supported via Adafruit Blinka, to be verified) |

> **Free alternative — recycled machines:** any old lab laptop/desktop (or VMs) can serve as coordinators for experiments 1 and 4 (messages, latency, partitions). Only energy experiments need battery-powered boards. This is the basis of Option C below.

---

## 3. Proposed configurations (probable ways, by budget)

### Option A — Recommended (5× Raspberry Pi 4)
Matches simulation defaults exactly (5 coordinators + 10 leaves) → hardware results directly comparable to published simulation numbers.

| Item | Qty | Unit price | Subtotal |
|---|---|---|---|
| Raspberry Pi 4 (2 GB) | 5 | $50 | $250 |
| microSD 32 GB (high endurance) | 5 | $8 | $40 |
| LiPo cell + ADS1115 ADC + holder (battery rig) | 5 | $30 | $150 |
| ESP32 DevKit (WROOM-32) | 10 | $8 | $80 |
| DHT22 temperature/humidity sensor | 10 | $5 | $50 |
| 18650 cells + holders / small power banks (leaf power) | 10 | $8 | $80 |
| INA219 current sensor (leaf energy measurement) | 3 | $7 | $21 |
| OpenWrt-capable router (GL.iNet travel router) | 1 | $50 | $50 |
| Breadboards, jumper wires, resistors, misc | — | — | $25 |
| **Total** | | | **~$750** |

### Option B — Budget Pi (5× Raspberry Pi Zero 2 W)
Same topology, ~35 % cheaper coordinators. 512 MB RAM is sufficient for one coordinator process per board (MQTT broker runs on the laptop).

| Item | Qty | Subtotal |
|---|---|---|
| Raspberry Pi Zero 2 W | 5 | $90 |
| Everything else as Option A (battery rigs, leaves, router, misc) | — | $496 |
| **Total** | | **~$590** |

### Option C — Hybrid / minimal spend ⭐ (if Pis are hard to get)
**2 battery-equipped Pis + 3 recycled lab machines (or VMs) as coordinators.** All four experiments still possible: consensus/message/partition tests use all 5 nodes; energy and handoff tests focus on the 2 battery Pis (a leader + a successor — exactly what the handoff demo needs).

| Item | Qty | Subtotal |
|---|---|---|
| Raspberry Pi 4 (2 GB) — battery-equipped coordinators | 2 | $100 |
| Recycled lab laptop/desktop/VM coordinators | 3 | $0 |
| microSD 32 GB | 2 | $16 |
| LiPo + ADS1115 battery rig | 2 | $60 |
| ESP32 DevKit + DHT22 + leaf power | 5 | $105 |
| OpenWrt router | 1 | $50 |
| Breadboards, wires, misc | — | $25 |
| **Total** | | **~$360** |

### Option D — Non-Pi SBC (5× Orange Pi Zero 2W)
Cheapest all-new-SBC fleet (~$500–550 total, same list as Option B). Caveat: the ADS1115 battery driver targets Raspberry Pi GPIO via Adafruit Blinka; Blinka supports Orange Pi but must be verified, or the driver can be switched to raw `smbus2` I2C (small code change, ~20 lines).

### Free software fallback (already working)
The Phase 2 hardware-free demo (`scripts/demo.sh`) already runs the full cluster over real MQTT on one laptop with mock batteries. Procurement is only needed for the **energy** and **physical-realism** claims.

---

## 4. Notes for the committee

- **Odd coordinator counts only** (3, 5, or 7) — consensus quorum requires it.
- **The battery rig is the critical item**, not the board: the paper's headline claim is energy reduction, which requires real battery telemetry (ADS1115 voltage sensing, driver already written) and ideally INA219 current sensing on sample leaves.
- **Partition injection needs no purchase** — `iptables` rules on the nodes suffice; the router makes it cleaner and keeps traffic off the university LAN.
- **Everything is reusable** — Pis/ESP32s/sensors return to the lab inventory for future embedded-systems courses and projects.
- Optional stretch item (not requested now): SX1276 LoRa modules ×4 (~$40) for wide-area leaf transport.

---

## 5. Recommendation

1. **First choice:** Option A (5× Pi 4) — direct comparability with simulation.
2. **If Pi 4 stock/budget is limited:** Option B (Pi Zero 2 W) or D (Orange Pi Zero 2W).
3. **If only 2 Pis are available:** Option C — full experimental coverage at ~$360 using recycled machines for non-energy nodes.

*Software, protocol implementations, battery drivers, and the comparison harness are complete and tested (52 automated tests passing). The project is blocked only on hardware.*
