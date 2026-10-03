# Futures Terminal

Futures Terminal is a full-stack trading terminal project for crypto futures research, signal visualization, replay, and market-event analysis. The source lives under `futures-terminal` and includes a Python backend, a Vite/React frontend, scripts, configuration files, documentation, and research tooling.

## Highlights

- Trading terminal UI with heatmap, candles, order entry, positions, and signal panels
- Python backend services for REST/WebSocket data flow
- Replay and logging pages
- Market-event and feature analysis tools
- Configurable symbols, risk rules, signal weights, exchange selection, and UI layout
- Documentation for architecture, signal definitions, risk rules, and release workflow
- PowerShell scripts for setup, backend/frontend runs, backups, and health checks

## Tech Stack

- Python backend
- React + TypeScript frontend
- Vite
- WebSocket/REST service layer
- PowerShell operational scripts
- JSON configuration files

## Project Layout

```text
futures-terminal/
├── backend/      # Python backend services and tooling
├── frontend/     # React/TypeScript terminal UI
├── config/       # Symbols, risk, layout, and exchange config
├── docs/         # Architecture and project notes
├── scripts/      # Setup and run scripts
├── agents/       # Agent prompts and review checklists
└── README.md
```

## Setup

```powershell
cd futures-terminal
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
cd frontend
npm install
```

## Run

Use the provided scripts when possible:

```powershell
cd futures-terminal
.\scripts\run_backend.ps1
.\scripts\run_frontend.ps1
```

Or run backend and frontend manually according to the existing project docs inside `futures-terminal`.

## Notes

This project appears to be a research/development terminal, not production financial advice or a guaranteed trading system. Review exchange settings, risk configuration, and execution paths carefully before connecting it to real accounts.

