#!/usr/bin/env python3
"""Generate the Excalidraw diagrams embedded in the README.

Writes six ``.excalidraw`` scene files to ``docs/diagrams/`` and, unless
``--no-svg`` is given, renders matching SVGs through the kroki service
(https://kroki.io), which embeds Excalidraw's real hand-drawn fonts.

Usage::

    python3 scripts/generate_diagrams.py            # scenes + SVGs
    python3 scripts/generate_diagrams.py --no-svg   # scenes only (offline)

The ``.excalidraw`` files can be opened and edited at https://excalidraw.com.
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
import urllib.request

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "docs", "diagrams")
KROKI_URL = "https://kroki.io/excalidraw/svg"

# Excalidraw colour palette
INK = "#1e1e1e"
BLUE, BLUE_BG = "#1971c2", "#a5d8ff"
GREEN, GREEN_BG = "#2f9e44", "#b2f2bb"
RED, RED_BG = "#e03131", "#ffc9c9"
ORANGE, ORANGE_BG = "#f08c00", "#ffec99"
PURPLE, PURPLE_BG = "#9c36b5", "#eedbff"
GRAY, GRAY_BG = "#868e96", "#e9ecef"
PANEL_BG = "#f1f3f5"

_ids = itertools.count(1)


def nid() -> str:
    return f"el{next(_ids)}"


# ---------------------------------------------------------------------------
# Element helpers
# ---------------------------------------------------------------------------

def _base(etype: str, x: float, y: float, w: float, h: float) -> dict:
    n = next(_ids) + 10_000
    return {
        "id": nid(), "type": etype, "x": x, "y": y, "width": w, "height": h,
        "angle": 0, "strokeColor": INK, "backgroundColor": "transparent",
        "fillStyle": "solid", "strokeWidth": 2, "strokeStyle": "solid",
        "roughness": 1, "opacity": 100, "groupIds": [], "frameId": None,
        "roundness": {"type": 3}, "seed": n, "version": 1, "versionNonce": n,
        "isDeleted": False, "boundElements": None, "updated": 1,
        "link": None, "locked": False,
    }


def box(x, y, w, h, bg="transparent", stroke=INK, style="solid", sw=2, rough=1):
    e = _base("rectangle", x, y, w, h)
    e.update({"backgroundColor": bg, "strokeColor": stroke,
              "strokeStyle": style, "strokeWidth": sw, "roughness": rough})
    return e


def circle(cx, cy, d, bg="transparent", stroke=INK, sw=2, style="solid"):
    e = _base("ellipse", cx - d / 2, cy - d / 2, d, d)
    e.update({"backgroundColor": bg, "strokeColor": stroke,
              "strokeWidth": sw, "strokeStyle": style})
    return e


def text(cx, y, s, size=16, color=INK, align="center", family=1, center=True):
    """Standalone text. ``cx`` is the horizontal centre when center=True."""
    lines = s.split("\n")
    longest = max(len(ln) for ln in lines)
    w = longest * size * 0.58
    h = len(lines) * size * 1.25
    x = cx - w / 2 if center else cx
    e = _base("text", x, y, w, h)
    e.update({
        "strokeColor": color, "roundness": None,
        "text": s, "fontSize": size, "fontFamily": family,
        "textAlign": align, "verticalAlign": "top", "containerId": None,
        "originalText": s, "lineHeight": 1.25,
    })
    return e


def arrow(x1, y1, x2, y2, color=INK, sw=2, style="solid", head="arrow"):
    e = _base("arrow", x1, y1, abs(x2 - x1), abs(y2 - y1))
    e.update({
        "strokeColor": color, "strokeWidth": sw, "strokeStyle": style,
        "points": [[0, 0], [x2 - x1, y2 - y1]],
        "lastCommittedPoint": None, "startBinding": None, "endBinding": None,
        "startArrowhead": None, "endArrowhead": head, "roundness": {"type": 2},
    })
    return e


def line(x1, y1, x2, y2, color=INK, sw=1, style="solid"):
    return arrow(x1, y1, x2, y2, color=color, sw=sw, style=style, head=None)


def labeled_node(cx, cy, w, h, label, bg, stroke=INK, size=15, sub=None):
    """Box with centred label (and optional smaller subtitle)."""
    els = [box(cx - w / 2, cy - h / 2, w, h, bg=bg, stroke=stroke)]
    if sub:
        els.append(text(cx, cy - h / 2 + 8, label, size=size))
        els.append(text(cx, cy - h / 2 + 8 + size * 1.4, sub, size=size - 3,
                        color=GRAY))
    else:
        els.append(text(cx, cy - (size * 1.25) / 2, label, size=size))
    return els


# ---------------------------------------------------------------------------
# Diagram 1 — architecture comparison
# ---------------------------------------------------------------------------

def architecture() -> list[dict]:
    els = [text(750, 16, "Architecture: flat Raft vs tiered ECHO / echoD", 26)]
    panels = [("RAFT — flat, all equal", 40), ("ECHO — tiered, but chatty", 520),
              ("echoD — tiered + disciplined", 1000)]
    for title, px in panels:
        els.append(box(px, 70, 460, 640, bg=PANEL_BG, stroke=GRAY, sw=1, rough=0))
        els.append(text(px + 230, 88, title, 20))

    # --- Raft panel: leader + 4 peers, heartbeats to everyone ---
    px = 40
    els += labeled_node(px + 230, 190, 110, 56, "leader", BLUE_BG, BLUE)
    peer_x = [px + 70, px + 160, px + 300, px + 390]
    for i, x in enumerate(peer_x):
        els += labeled_node(x, 330, 74, 46, f"peer", GRAY_BG, GRAY, size=13)
        els.append(arrow(px + 230, 218, x, 306, color=RED, sw=1))
    els.append(text(px + 230, 262, "heartbeat ×4 every 50 ms", 14, color=RED))
    els.append(text(px + 230, 284, "even when idle — forever", 14, color=RED))
    els.append(text(px + 230, 420,
                    "every node votes & replicates\n"
                    "consensus cost O(n)\n"
                    "halts without quorum\n"
                    "no energy awareness", 15, align="left", center=True))

    # --- ECHO panel: coordinators + leaves, ping broadcast to ALL ---
    px = 520
    coords = [px + 60, px + 150, px + 240, px + 330, px + 420]
    for i, x in enumerate(coords):
        if i == 2:
            els += labeled_node(x, 170, 78, 46, "coord", GREEN_BG, GREEN, size=13)
        else:
            els += labeled_node(x, 170, 78, 46, "coord", BLUE_BG, BLUE, size=13)
    leaves = [(px + 70 + 55 * (i % 5), 300 + 52 * (i // 5)) for i in range(10)]
    for i, (lx, ly) in enumerate(leaves):
        els.append(circle(lx, ly, 30, bg=ORANGE_BG, stroke=ORANGE, sw=1))
        if i % 3 == 0:
            els.append(line(lx, ly - 15, coords[(i * 2) % 5], 196, color=GRAY, sw=1))
    els.append(text(px + 240, 215, "ping broadcast to ALL 15 nodes", 14, color=RED))
    els.append(text(px + 240, 237, "≈ 75 % of ECHO's total traffic", 14, color=RED))
    els.append(text(px + 420, 96 + 26, "leader", 12, color=GREEN))
    els.append(text(px + 230, 420,
                    "coordinators vote; leaves observe\n"
                    "delta filter AFTER transmission\n"
                    "one consensus round per event\n"
                    "random elections → split votes", 15, center=True))

    # --- echoD panel: same topology, disciplined traffic ---
    px = 1000
    for i, x in enumerate(coords := [px + 60, px + 150, px + 240, px + 330, px + 420]):
        if i == 2:
            els += labeled_node(x, 170, 78, 46, "coord", GREEN_BG, GREEN, size=13)
        else:
            els += labeled_node(x, 170, 78, 46, "coord", BLUE_BG, BLUE, size=13)
    for i in range(10):
        lx, ly = px + 70 + 55 * (i % 5), 300 + 52 * (i // 5)
        els.append(circle(lx, ly, 30, bg=ORANGE_BG, stroke=ORANGE, sw=1))
        if i % 4 == 0:
            els.append(line(lx, ly - 15, coords[(i + 1) % 5], 196, color=GRAY, sw=1))
    els.append(text(px + 240, 208, "leader → coords: adaptive ping", 13, color=GREEN))
    els.append(text(px + 240, 228, "50 → 250 ms backoff while idle", 13, color=GREEN))
    els.append(text(px + 240, 248, "coord → own leaves: 1 s keepalive", 13, color=GRAY))
    els.append(text(px + 240, 372, "δ-filter ON the leaf", 13, color=ORANGE))
    els.append(text(px + 230, 420,
                    "consensus traffic never touches leaves\n"
                    "bursts batched into ONE round\n"
                    "battery-ordered elections, no split votes\n"
                    "handoff instead of re-election", 15, center=True))
    return els


# ---------------------------------------------------------------------------
# Diagram 2 — one sensor event, message flow
# ---------------------------------------------------------------------------

def message_flow() -> list[dict]:
    els = [text(750, 16, "One sensor event: what hits the network", 26)]
    cols = [("RAFT", 40, RED), ("ECHO", 520, ORANGE), ("echoD", 1000, GREEN)]
    for title, px, accent in cols:
        els.append(box(px, 70, 460, 640, bg=PANEL_BG, stroke=GRAY, sw=1, rough=0))
        els.append(text(px + 230, 88, title, 20, color=accent))
        # lifelines
        els += labeled_node(px + 90, 150, 110, 44,
                            "client" if title == "RAFT" else "leaf",
                            GRAY_BG, GRAY, size=14)
        els += labeled_node(px + 350, 150, 150, 44, "leader + peers",
                            BLUE_BG, BLUE, size=14)
        els.append(line(px + 90, 172, px + 90, 560, color=GRAY, sw=1, style="dashed"))
        els.append(line(px + 350, 172, px + 350, 560, color=GRAY, sw=1, style="dashed"))

    # Raft sequence
    px = 40
    els.append(arrow(px + 90, 230, px + 350, 230, color=INK))
    els.append(text(px + 220, 206, "1. command", 13))
    els.append(arrow(px + 350, 290, px + 90, 330, color=INK))
    els.append(text(px + 220, 274, "2. AppendEntries ×4  →", 13))
    els.append(text(px + 220, 316, "3. ←  responses ×4", 13))
    els.append(text(px + 230, 400, "+ heartbeats to all peers", 14, color=RED))
    els.append(text(px + 230, 422, "every 50 ms, even when idle", 14, color=RED))
    els += labeled_node(px + 230, 620, 300, 56, "≈ 10 msgs / event", RED_BG, RED,
                        sub="plus a permanent heartbeat tax")

    # ECHO sequence
    px = 520
    els.append(arrow(px + 90, 220, px + 350, 220, color=INK))
    els.append(text(px + 220, 196, "1. report (sent unfiltered)", 13))
    els.append(text(px + 220, 252, "delta checked at coordinator —", 13, color=ORANGE))
    els.append(text(px + 220, 272, "radio cost already spent", 13, color=ORANGE))
    els.append(arrow(px + 350, 330, px + 90, 370, color=INK))
    els.append(text(px + 220, 314, "2. AppendEntries ×4  →", 13))
    els.append(text(px + 220, 356, "3. ←  responses ×4", 13))
    els.append(text(px + 230, 424, "+ ping broadcast to all 15 nodes", 14, color=ORANGE))
    els += labeled_node(px + 230, 620, 300, 56, "≈ 10 msgs / event", ORANGE_BG,
                        ORANGE, sub="plus 14-msg ping every 50 ms")

    # echoD sequence
    px = 1000
    els.append(text(px + 90, 210, "δ-check on leaf", 13, color=GREEN))
    els.append(text(px + 90, 230, "sub-threshold = dropped", 12, color=GREEN))
    els.append(text(px + 90, 246, "(0 messages)", 12, color=GREEN))
    els.append(arrow(px + 90, 300, px + 350, 300, color=INK))
    els.append(text(px + 220, 276, "1. breach report", 13))
    els.append(arrow(px + 350, 360, px + 90, 400, color=INK))
    els.append(text(px + 220, 344, "2. batched AppendEntries ×4  →", 13))
    els.append(text(px + 220, 386, "3. ←  responses ×4", 13))
    els.append(text(px + 230, 448, "burst of k events = still ONE round", 14, color=GREEN))
    els += labeled_node(px + 230, 620, 300, 56, "0 msgs (filtered)", GREEN_BG,
                        GREEN, sub="or one batched round per burst")
    return els


# ---------------------------------------------------------------------------
# Diagram 3 — battery-ordered election
# ---------------------------------------------------------------------------

def election() -> list[dict]:
    els = [text(750, 16, "Battery-ordered election timeouts", 26)]
    els += labeled_node(1060, 92, 620, 52,
                        "timeout = 300 + (1 − battery) × 300 + crc32(node_id) % 30",
                        PURPLE_BG, PURPLE, size=16)

    rows = [("coord-1", 0.90, 330, GREEN), ("coord-2", 0.60, 420, BLUE),
            ("coord-3", 0.40, 480, GRAY)]
    x0, x1, y_axis = 320, 1180, 470        # timeline 0 → 600 ms
    scale = (x1 - x0) / 600.0

    for i, (name, batt, t_ms, color) in enumerate(rows):
        y = 170 + i * 100
        els += labeled_node(180, y, 200, 52, name, GREEN_BG if i == 0 else GRAY_BG,
                            color if i == 0 else GRAY, sub=f"battery {int(batt*100)} %")
        els.append(line(x0, y, x1, y, color=GRAY, sw=1))
        tx = x0 + t_ms * scale
        els.append(circle(tx, y, 16, bg=RED_BG, stroke=RED))
        els.append(text(tx, y - 44, f"{t_ms} ms", 13, color=RED))

    # coord-1 fires first and wins
    tx1 = x0 + 330 * scale
    els.append(arrow(tx1, 178, tx1, 262, color=GREEN, sw=2))
    els.append(arrow(tx1, 178, tx1, 362, color=GREEN, sw=2, style="dashed"))
    els.append(text(tx1 + 125, 250, "RequestVote to all peers", 13, color=GREEN))
    els.append(text(760, 400, "coord-1 nominates first, every peer grants — "
                    "wins on the FIRST ballot", 16, color=GREEN))

    els.append(line(x0, y_axis, x1, y_axis, color=INK, sw=2))
    for t in (0, 150, 300, 450, 600):
        els.append(text(x0 + t * scale, y_axis + 8, f"{t} ms", 12, color=GRAY))

    els += labeled_node(750, 570, 900, 56,
                        "Raft / ECHO: random 150–300 ms timeouts",
                        RED_BG, RED,
                        sub="simultaneous candidacies → split votes → re-election rounds")
    return els


# ---------------------------------------------------------------------------
# Diagram 4 — directed leader handoff
# ---------------------------------------------------------------------------

def handoff() -> list[dict]:
    els = [text(750, 16, "Leader handoff: succession without an election storm", 26)]

    lanes = [("old leader", 260), ("follower A — 90 %", 700),
             ("follower B — 70 %", 1140)]
    for name, x in lanes:
        els += labeled_node(x, 110, 220, 52, name, GRAY_BG, GRAY, size=15)
        els.append(line(x, 136, x, 560, color=GRAY, sw=1, style="dashed"))

    # battery drain on old leader
    els.append(text(150, 170, "battery\n19 %", 14, color=RED))
    els.append(text(260, 170, "< T_HANDOFF (20 %)", 13, color=RED))

    # step 1: handoff message
    els.append(arrow(260, 250, 700, 250, color=PURPLE, sw=2))
    els.append(text(480, 222, "1. LeadershipHandoff", 14, color=PURPLE))
    els.append(text(480, 240, "(1 directed message)", 12, color=PURPLE))

    # step 2: A starts election immediately
    els.append(text(700, 292, "2. A starts election immediately", 13, color=GREEN))
    els.append(arrow(700, 340, 260, 380, color=GREEN, sw=1))
    els.append(arrow(700, 340, 1140, 380, color=GREEN, sw=1))
    els.append(text(480, 350, "3. RequestVote", 12, color=GREEN))
    els.append(text(920, 350, "3. RequestVote", 12, color=GREEN))

    # step 3: A wins
    els += labeled_node(700, 470, 260, 56, "A = LEADER", GREEN_BG, GREEN)
    els.append(text(260, 470, "steps down to FOLLOWER", 12, color=GRAY))

    els += labeled_node(380, 620, 560, 56, "echoD: no availability gap", GREEN_BG,
                        GREEN, sub="leadership moves in one message round-trip")
    els += labeled_node(1060, 620, 560, 56, "Raft / ECHO: 150–600 ms gap", RED_BG,
                        RED, sub="timeout wait + full randomized re-election")
    return els


# ---------------------------------------------------------------------------
# Diagram 5 — batching window
# ---------------------------------------------------------------------------

def batching() -> list[dict]:
    els = [text(750, 16, "Batched event-driven consensus", 26)]

    x0, x1 = 250, 1150                    # timeline 0 → 60 ms region
    scale = 14.0                          # px per ms
    events = [("e1", 5), ("e2", 12), ("e3", 28), ("e4", 41), ("e5", 47)]

    # --- ECHO lane ---
    els.append(text(110, 110, "ECHO", 20, color=ORANGE))
    els.append(line(x0, 170, x1, 170, color=GRAY, sw=1))
    for name, t in events:
        ex = x0 + t * scale
        els.append(circle(ex, 170, 12, bg=ORANGE_BG, stroke=ORANGE))
        els.append(text(ex, 146, name, 12, color=ORANGE))
        els += labeled_node(ex, 232, 80, 36, "1 round", RED_BG, RED, size=12)
        els.append(arrow(ex, 176, ex, 212, color=RED, sw=1))
    els.append(text(1320, 140, "5 events", 16, color=RED))
    els.append(text(1320, 166, "= 5 rounds", 16, color=RED))
    els.append(text(1320, 192, "≈ 50 msgs", 16, color=RED))

    # --- echoD lane ---
    els.append(text(110, 360, "echoD", 20, color=GREEN))
    els.append(line(x0, 420, x1, 420, color=GRAY, sw=1))
    wx0, wx1 = x0 + 5 * scale, x0 + 55 * scale
    els.append(box(wx0, 396, wx1 - wx0, 48, bg=GREEN_BG, stroke=GREEN, sw=1,
                   style="dashed"))
    els.append(text((wx0 + wx1) / 2, 448, "50 ms batch window", 12, color=GREEN))
    for name, t in events:
        ex = x0 + t * scale
        els.append(circle(ex, 420, 12, bg=GREEN_BG, stroke=GREEN))
    els += labeled_node(1240, 420, 240, 48, "LogEntry {batch: e1…e5}",
                        GREEN_BG, GREEN, size=14)
    els.append(arrow(wx1, 420, 1115, 420, color=GREEN, sw=2))
    els.append(text(1320, 460, "= 1 round", 16, color=GREEN))
    els.append(text(1320, 486, "≈ 10 msgs", 16, color=GREEN))
    els.append(text(750, 540, "full batch (≥ MAX_BATCH_SIZE) flushes immediately — "
                    "no added latency under load", 14, color=GRAY))
    return els


# ---------------------------------------------------------------------------
# Diagram 6 — partition and reconciliation
# ---------------------------------------------------------------------------

def partition() -> list[dict]:
    els = [text(750, 16, "Partition-tolerant provisional consensus", 26)]
    phases = [("1 · normal", 40), ("2 · partition", 520), ("3 · heal + reconcile",
                                                             1000)]
    for title, px in phases:
        els.append(box(px, 70, 460, 560, bg=PANEL_BG, stroke=GRAY, sw=1, rough=0))
        els.append(text(px + 230, 88, title, 20))
    els.append(arrow(500, 350, 520, 350, color=GRAY, sw=2))
    els.append(arrow(980, 350, 1000, 350, color=GRAY, sw=2))

    # phase 1: healthy cluster
    px = 40
    pts = [(px + 130, 200), (px + 330, 200), (px + 230, 290), (px + 150, 390),
           (px + 310, 390)]
    for i, (x, y) in enumerate(pts):
        els.append(circle(x, y, 44, bg=GREEN_BG if i == 2 else BLUE_BG,
                          stroke=GREEN if i == 2 else BLUE))
        for j in range(i):
            els.append(line(x, y, pts[j][0], pts[j][1], color=GRAY, sw=1))
    els.append(text(px + 230, 330, "leader", 11, color=GREEN))
    els.append(text(px + 230, 460, "one leader, one log\ncommit index = 42", 15))

    # phase 2: split brain with provisional leaders
    px = 520
    els.append(line(px + 230, 120, px + 230, 460, color=RED, sw=2, style="dashed"))
    left = [(px + 90, 200), (px + 170, 300), (px + 90, 400)]
    right = [(px + 370, 250), (px + 300, 390)]
    for x, y in left:
        els.append(circle(x, y, 44, bg=PURPLE_BG, stroke=PURPLE))
    for x, y in right:
        els.append(circle(x, y, 44, bg=ORANGE_BG, stroke=ORANGE))
    for group in (left, right):
        for i in range(1, len(group)):
            els.append(line(group[i][0], group[i][1], group[0][0], group[0][1],
                            color=GRAY, sw=1))
    els.append(text(px + 120, 130, "LOCAL_LEADER", 12, color=PURPLE))
    els.append(text(px + 340, 160, "LOCAL_LEADER", 12, color=ORANGE))
    els.append(text(px + 120, 470, "provisional entries\n(epoch = 1) — still serving",
                    13, color=PURPLE))
    els.append(text(px + 345, 470, "provisional entries\n(epoch = 1)", 13,
                    color=ORANGE))

    # phase 3: reconcile
    px = 1000
    els += labeled_node(px + 230, 190, 340, 60,
                        "winner: highest commit index", GREEN_BG, GREEN, size=15)
    els += labeled_node(px + 230, 320, 340, 60,
                        "loser: truncate provisional log", RED_BG, RED, size=15)
    els.append(arrow(px + 230, 230, px + 230, 288, color=GRAY, sw=1))
    els += labeled_node(px + 230, 450, 340, 60,
                        "replay as ONE batch entry", PURPLE_BG, PURPLE, size=15)
    els.append(arrow(px + 230, 352, px + 230, 418, color=GRAY, sw=1))
    els.append(text(px + 230, 520, "Raft: minority side simply halts — "
                    "no quorum, 0 % availability", 13, color=RED))
    return els


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

DIAGRAMS = {
    "architecture": architecture,
    "message-flow": message_flow,
    "election": election,
    "handoff": handoff,
    "batching": batching,
    "partition": partition,
}


def write_scene(name: str, elements: list[dict], out_dir: str) -> str:
    scene = {
        "type": "excalidraw", "version": 2,
        "source": "https://github.com/rabbive/echod",
        "elements": elements,
        "appState": {"gridSize": None, "viewBackgroundColor": "#ffffff"},
    }
    path = os.path.join(out_dir, f"{name}.excalidraw")
    with open(path, "w") as f:
        json.dump(scene, f)
    return path


def render_svg(scene_path: str, svg_path: str) -> bool:
    """Render a scene to SVG via kroki (embeds Excalidraw's fonts)."""
    try:
        with open(scene_path, "rb") as f:
            data = f.read()
        req = urllib.request.Request(
            KROKI_URL, data=data,
            headers={
                "Content-Type": "text/plain",
                # kroki rejects the default python-urllib UA with 403
                "User-Agent": "curl/8.0",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            svg = resp.read()
        if not svg.startswith(b"<svg") and b"<svg" not in svg[:200]:
            return False
        with open(svg_path, "wb") as f:
            f.write(svg)
        return True
    except Exception as exc:  # network or service failure — keep scenes
        print(f"  ! SVG render failed for {scene_path}: {exc}")
        return False


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--no-svg", action="store_true",
                    help="only write .excalidraw scenes, skip kroki rendering")
    args = ap.parse_args()

    out_dir = os.path.abspath(OUT_DIR)
    os.makedirs(out_dir, exist_ok=True)

    for name, builder in DIAGRAMS.items():
        scene_path = write_scene(name, builder(), out_dir)
        print(f"  wrote {scene_path} ({len(builder())} elements)")
        if not args.no_svg:
            svg_path = os.path.join(out_dir, f"{name}.svg")
            ok = render_svg(scene_path, svg_path)
            print(f"  {'wrote ' + svg_path if ok else 'SKIPPED svg for ' + name}")

    print("done.")


if __name__ == "__main__":
    main()
