# Futures Terminal

Futures Terminal is a local trading dashboard project for Binance USD-M Futures.

In simple words, this project is made to collect market data, show it in a web dashboard, and prepare the basic structure for trading tools. It has a Python backend and a React frontend.

This project should be treated as a research and learning project. It is not ready to be used for real-money trading without careful testing.

## Current Scope

- Main symbol: BTCUSDT
- Exchange: Binance USD-M Futures
- Market type: perpetual futures
- Pages: terminal view and replay view
- Basic order actions only, such as market, limit, cancel, close, reduce-only, and leverage

## Project Layout

- `backend/`: Python code for market data, risk logic, storage, and API work.
- `frontend/`: React and TypeScript user interface.
- `config/`: JSON settings for symbols, risk, signal weights, and layout.
- `docs/`: notes about architecture, risk, and releases.
- `scripts/`: helper scripts for running the app on Windows.

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

## What It Does Right Now

The current version includes:

- A Binance public stream subscriber for BTCUSDT.
- Market data capture for order book, trades, mark price, candles, and liquidations.
- Local saving with SQLite and JSONL replay files.
- FastAPI endpoints for health, settings, and trading placeholders.
- A WebSocket connection so the frontend can receive live updates.

## Safety Notes

- Use testnet keys while developing.
- Keep `kill_switch_enabled` set to `true` in `config/risk.json`.
- Do not connect real funds until the project is fully reviewed and tested.
- Trading is risky. This code is for development and research, not financial advice.
