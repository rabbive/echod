# Project Status

Single quick-glance source of truth for "what's done vs. what's next." The
authoritative, more detailed version of this table lives in
[`PRD.md` §8](PRD.md#8-milestones--status-summary) — update both together.

**Last updated:** 2026-07-21

## Current focus

**Goal 3: validate Raft vs ECHO vs echoD on real ESP32 hardware.** Goals 1 (ECHO beats
Raft) and 2 (echoD beats both) are done and measured in simulation. See `PRD.md` §5.3
for the open questions that need answering before this work starts (coordinator
hosting decision, real battery hardware, how to induce a physical partition, target
cluster size).

## At a glance

| Area | Status |
|---|---|
| Phase 1 — simulation (Raft, ECHO, echoD, seeded workload, metrics) | ✅ Complete |
| Phase 2 — MQTT demo | ⚠️ ECHO only — echoD/Raft not ported |
| Phase 3 — ESP32 hardware | ⏳ Leaf firmware (ECHO) exists; no coordinator firmware, no echoD/Raft, no 3-way comparison run yet |
| Docs (CLAUDE.md, PRD.md, ADRs, CONTRIBUTING, TESTING) | ✅ Up to date as of this commit |
| CI | ✅ Green on Python 3.10 / 3.12 |

## Not currently planned

- Formal proof of safety (TLA+ or similar) — see `PRD.md` §3 non-goals.
- Production hardening (auth/TLS/ops tooling) beyond the Phase 2 demo.
- The academic paper / patent filing itself (this repo produces the evidence, not the paper).

## Where to look next

- Full milestone table and success metrics: `PRD.md`
- Why key design decisions were made: `docs/decisions/`
- What each test validates: `docs/TESTING.md`
- Full echoD design + measured numbers: `docs/ECHOD_VS_RAFT_ECHO.md`
