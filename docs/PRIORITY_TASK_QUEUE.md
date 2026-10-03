# Futures Bot Priority Task Queue

_Last updated: 2026-05-27 05:08:02 +02:00 (Europe/Rome)_

## P0 — Candidate safety evidence pack (highest)
**Goal:** Produce machine-checkable evidence for latest run quality gating.

- [ ] Export/verify latest run artifact bundle includes:
  - `metrics.json`
  - `data_health_report.json`
  - parseable `run_info.json`
  - scanner/selection summary
- [ ] Write normalized summary JSON to `D:\OpenClawWorkspace\handoff\evidence\latest_candidate_gate.json`
- [ ] Confirm gate outcome (`ACCEPT`/`REJECT`/`PENDING`) with explicit reasons.

## P1 — Backend runtime health proof
**Goal:** Confirm backend can serve key health and ML status endpoints.

- [ ] Start backend (if not running)
- [ ] Capture responses for:
  - `GET /health`
  - `GET /ml/status`
- [ ] Save raw outputs under `D:\OpenClawWorkspace\handoff\evidence\`.

## P2 — Integration sanity (frontend↔backend stream)
**Goal:** Validate no immediate regressions in terminal stream path.

- [ ] Run backend tests (`backend/app/tests`)
- [ ] Boot frontend+backend once and capture websocket connect log snippet
- [ ] Record pass/fail in handoff report.

## Execution rule for labour cron loops
1. Always pick the first unchecked item in the highest non-empty priority section.
2. If blocked, record `[blocked]` reason + exact missing dependency, then take next item.
3. Every loop must update `D:\OpenClawWorkspace\handoff\latest_labour_report.md` with evidence paths.
