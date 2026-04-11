---
name: ECHO architecture (simulation + MQTT)
overview: Visual architecture, topic layout, and data flows for the asyncio simulation (Raft vs ECHO) and the Phase 2 MQTT hardware-free demo.
isProject: false
---

# Echod — Simulation + MQTT architecture (visual plan)

This file is a **reference plan** with **Mermaid** diagrams you can render on GitHub or in any Markdown viewer that supports Mermaid.

| Doc | Purpose |
|-----|---------|
| This file | **Architecture & flows** (diagrams first) |
| [ECHO_DEMO_AND_RAFT.md](ECHO_DEMO_AND_RAFT.md) | Narrative: dashboard controls, ECHO vs Raft, thresholds |

---

## 1. Repository layers (mental model)

```mermaid
flowchart TB
  subgraph sim [Simulation layer — pure Python asyncio]
    SMain["python3 -m simulation.main"]
    SOut["CSV + optional PNG charts → results/"]
  end
  subgraph mqtt [Phase 2 — MQTT + dashboard]
    Broker[Mosquitto broker :1883]
    Dash["Flask dashboard + Socket.IO"]
    Coords["echo_node coordinators"]
    Leaves["mock_leaf processes"]
  end
  SMain --> SOut
  Dash -->|HTTP| Browser[Browser UI]
  Coords <-->|MQTT| Broker
  Leaves <-->|MQTT| Broker
  Dash <-->|MQTT| Broker
```

- **Simulation** compares **Raft** vs **ECHO** under the same `Cluster` / `MessageBus` — no real network.
- **MQTT demo** runs **ECHO coordinators** over **MQTT** with a **live dashboard** (observability + demo controls).

---

## 2. Simulation architecture

### 2.1 Component diagram

`simulation/main.py` builds two clusters sequentially — **Raft baseline**, then **ECHO** — and writes comparative metrics.

```mermaid
flowchart TB
  subgraph entry [Entry]
    CLI["simulation.main CLI"]
  end
  subgraph builders [Protocol builders]
    BR["build_raft_cluster()"]
    BE["build_echo_cluster()"]
  end
  subgraph runtime [Shared runtime]
    C["Cluster"]
    MB["MessageBus in-process router"]
    N["Nodes: RaftNode / EchoCoordinator / EchoLeaf"]
  end
  subgraph side [Side effects loop]
    T["tick: battery drain, partition inject/heal, availability"]
  end
  subgraph metrics [Metrics]
    MC["MetricsCollector"]
    R["export_csv / export_energy_csv"]
    CH["generate_charts optional"]
  end

  CLI --> BR
  CLI --> BE
  BR --> C
  BE --> C
  C --> MB
  MB --> N
  CLI --> T
  T --> N
  C --> MC
  MC --> R
  MC --> CH
```

Key implementation anchors:

- **Orchestration:** [`simulation/main.py`](../simulation/main.py) (runner)
- **Fair comparison:** same [`Cluster`](../simulation/core/cluster.py) + [`MessageBus`](../simulation/core/cluster.py) for both protocols
- **Protocols:** [`simulation/protocols/raft.py`](../simulation/protocols/raft.py), [`simulation/protocols/echo.py`](../simulation/protocols/echo.py)

### 2.2 Message bus vs MQTT (conceptual)

| Aspect | Simulation `MessageBus` | Phase 2 MQTT |
|--------|-------------------------|----------------|
| Transport | In-process `async` delivery | TCP to broker; pub/sub topics |
| Partitions | `Cluster.inject_partition` / `heal_partition` | Not modeled the same way in `rpi/` demo |
| Fair metrics | Single collector hook on delivered messages | Dashboard + per-node logs |

```mermaid
flowchart LR
  subgraph simBus [Simulation]
    A[Node A] -->|Message| MB[MessageBus.route]
    MB --> B[Node B]
  end
  subgraph mqttBus [MQTT demo]
    P[Publisher] -->|PUBLISH topic| BRK[Broker]
    BRK -->|DELIVER| S[Subscriber]
  end
```

---

## 3. Simulation run flow (CLI → CSV)

```mermaid
flowchart TD
  Start([Start]) --> Parse[Parse CLI args]
  Parse --> RunRaft[Run Raft scenario]
  RunRaft --> RunEcho[Run ECHO scenario]
  RunEcho --> Export[Export CSV / energy CSV]
  Export --> Charts{--charts?}
  Charts -->|yes| PNG[Write PNG charts]
  Charts -->|no| Print[Print stdout summaries]
  PNG --> Print
  Print --> End([Done])
```

---

## 4. Raft vs ECHO in the simulator (behavioral)

```mermaid
flowchart TB
  subgraph raft [Raft baseline]
    RLeader[Leader]
    RHeart[Periodic AppendEntries heartbeats]
    RLeader --> RHeart
  end
  subgraph echo [ECHO]
    ELeader[Leader / Local leader under partition]
    ETrig[Event-driven replication triggers]
    EProv[Provisional mode + reconcile on heal]
    ELeader --> ETrig
    ELeader --> EProv
  end
```

See [`simulation/protocols/raft.py`](../simulation/protocols/raft.py) vs [`simulation/protocols/echo.py`](../simulation/protocols/echo.py) for the exact differences (energy gating, triggers, partition epoch).

---

## 5. MQTT architecture (Phase 2)

### 5.1 Runtime topology

```mermaid
flowchart TB
  subgraph broker [MQTT broker]
    M[Mosquitto :1883]
  end
  subgraph coordProcs [Coordinator processes]
    C0[coord-0 + MQTTTransport]
    C1[coord-1 + MQTTTransport]
    CN[coord-N + MQTTTransport]
  end
  subgraph leafProc [Leaf process]
    L[mock_leaf many leaf IDs]
  end
  subgraph dashProc [Dashboard process]
    D[Flask app + Socket.IO MQTT client]
  end
  Browser[Web browser]

  C0 <--> M
  C1 <--> M
  CN <--> M
  L <--> M
  D <--> M
  Browser <-->|HTTP + WebSocket| D
```

Each **coordinator** runs:

- **MQTT client** (background thread) + **asyncio** loop for protocol logic — see [`rpi/coordinator/echo_node.py`](../rpi/coordinator/echo_node.py) and [`rpi/coordinator/transport.py`](../rpi/coordinator/transport.py).

### 5.2 Topic layout (namespace)

Prefix: `echo/<cluster_id>/…` (default cluster from [`rpi/config.py`](../rpi/config.py)).

```mermaid
flowchart LR
  subgraph topics [Topic families]
    RPC["echo/cluster/rpc/coord-X"]
    BC["echo/cluster/broadcast"]
    ST["echo/cluster/status/coord-X retained"]
    DL["echo/cluster/demo/leaves"]
  end
```

| Topic pattern | Direction | Purpose |
|---------------|-----------|---------|
| `rpc/<node_id>` | Any → node | Directed RPC (`request_vote`, `append_entries`, `sensor_data`, …) |
| `broadcast` | All subscribers | `liveness_ping`, `demo_control`, … |
| `status/<node_id>` | Coordinator → all | Retained JSON snapshot for dashboard |
| `demo/leaves` | Dashboard → leaves | `{ "paused": true/false }` for mock leaf send loop |

### 5.3 MQTTTransport internal wiring

```mermaid
flowchart TD
  subgraph mt [MQTTTransport per coordinator]
    MC[MQTT client paho]
    Sub[Subscribe rpc + broadcast]
    Pub[Publish send / broadcast / status]
    H[Handler callback to EchoCoordinator]
  end
  Broker[Broker]
  MC --> Sub
  MC --> Pub
  Broker <--> MC
  MC -->|on_message JSON| H
```

---

## 6. Live dashboard: data path

```mermaid
sequenceDiagram
  participant Coord as EchoCoordinator
  participant MQTT as MQTT broker
  participant Dash as Dashboard Flask
  participant SIO as Socket.IO
  participant UI as Browser

  Coord->>MQTT: PUBLISH retained status echo/.../status/coord-X
  MQTT->>Dash: DELIVER status JSON
  Dash->>SIO: emit node_update
  SIO->>UI: live node cards + header stats

  Coord->>MQTT: PUBLISH broadcast envelope
  MQTT->>Dash: DELIVER broadcast
  Dash->>SIO: emit broadcast_msg
  SIO->>UI: message table + traffic chart
```

HTTP **demo** controls (`POST /api/demo/...`) cause the dashboard to **PUBLISH** MQTT messages; coordinators then update state and **publish new status**, closing the loop.

---

## 7. Demo control flow (MQTT)

```mermaid
flowchart LR
  UI[Browser button] -->|POST /api/demo/*| API[Flask route]
  API -->|PUBLISH demo_control| BC[echo/.../broadcast]
  BC -->|all coordinators| EH[EchoCoordinator._handle_demo_control]
  EH -->|mock battery| Battery[BatteryMonitor]
  EH -->|publish_status| ST[echo/.../status/coord-X]
  ST -->|Socket.IO| UI2[Dashboard updates]
```

Leaves pause:

```mermaid
flowchart LR
  UI[Pause leaves] -->|POST| API[Flask]
  API -->|PUBLISH paused| DL[echo/.../demo/leaves]
  DL --> ML[mock_leaf on_message]
  ML --> Loop[Skip send_reading while paused]
```

---

## 8. File map (quick navigation)

| Area | Path |
|------|------|
| Simulation CLI | [`simulation/main.py`](../simulation/main.py) |
| Cluster + bus | [`simulation/core/cluster.py`](../simulation/core/cluster.py) |
| Raft | [`simulation/protocols/raft.py`](../simulation/protocols/raft.py) |
| ECHO | [`simulation/protocols/echo.py`](../simulation/protocols/echo.py) |
| MQTT transport | [`rpi/coordinator/transport.py`](../rpi/coordinator/transport.py) |
| Coordinator | [`rpi/coordinator/echo_node.py`](../rpi/coordinator/echo_node.py) |
| Dashboard | [`rpi/dashboard/app.py`](../rpi/dashboard/app.py), [`rpi/dashboard/templates/index.html`](../rpi/dashboard/templates/index.html) |
| Mock leaves | [`rpi/mock_leaf.py`](../rpi/mock_leaf.py) |
| One-command demo | [`scripts/demo.sh`](../scripts/demo.sh) |

---

## 9. Rendering notes

- **GitHub:** Mermaid in Markdown is supported in many files; if a diagram fails to render, paste the same ` ```mermaid ` block into a GitHub issue or use the [Mermaid Live Editor](https://mermaid.live).
- **Cursor / VS Code:** use a Mermaid preview extension for local viewing.
