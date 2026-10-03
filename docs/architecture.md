# Architecture

## Components
1. UI (React/TypeScript): visualization and controls
2. Engine (Python): market ingestion, state, signals, execution guardrails
3. Storage: SQLite + JSONL replay/session artifacts

## Data flow
- Binance Public WS -> Event bus -> State services -> Signal services -> WS server -> UI
- Event bus -> Recorder -> SQLite + replay files
- UI actions -> REST trading routes -> execution service (future phase)

## Boundaries
- Market data, private account stream, and execution service are separate modules.
- Any order send path must pass risk checks first.
