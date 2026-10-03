# Futures Terminal

Desktop-first local trading terminal for Binance USD-M Futures, with:
- Python backend engine (market data, signal pipeline, execution/risk plumbing)
- React + TypeScript frontend terminal UI
- Local SQLite + replay/snapshot storage

## Scope (v1)
- One symbol: BTCUSDT
- One exchange: Binance USD-M Futures
- One market type: perpetual futures
- One terminal page + one replay page
- Basic order execution primitives only (market/limit/cancel/close/reduce-only/leverage)

## Project layout
- `backend/`: market ingestion, state engine, risk/execution, storage
- `frontend/`: terminal UI (DOM, tape, heatmap, chart, panels)
- `config/`: tunable JSON configs for symbols, risk, signal weights, layout
- `docs/`: architecture, mapping, risk and release notes
- `scripts/`: setup and run helpers for Windows

## Quick start (Windows PowerShell)
1. Copy env template:
   - `Copy-Item .env.example .env`
2. Backend setup:
   - `python -m venv .venv`
   - `.\.venv\Scripts\Activate.ps1`
   - `pip install -r requirements.txt`
3. Frontend setup:
   - `cd frontend`
   - `npm install`
4. Run both:
   - Back to repo root, run `./scripts/run_terminal.ps1`

## One-click launch (from `D:\PROJECTS\20 sec`)
- Double-click `LAUNCH_FUTURES_TERMINAL.bat`
- This starts backend + frontend and opens `http://127.0.0.1:5173`

## One-click launch (from the project folder)
- Double-click `futures-terminal/LAUNCH_FUTURES_TERMINAL.bat`

## Phase 1 behavior
Current scaffold includes:
- Binance public stream subscriber for BTCUSDT
- Capture of `depth`, `aggTrade`, `bookTicker`, `markPrice@1s`, `kline_1m`, `forceOrder`
- Raw event persistence to SQLite and JSONL replay files
- FastAPI REST health/settings/trading placeholders
- FastAPI WebSocket broadcast pipe for frontend

## Safety notes
- Use testnet keys for development.
- Keep `kill_switch_enabled` true in `config/risk.json` while iterating.
- Do not enable automation or unsupported order types in v1.
