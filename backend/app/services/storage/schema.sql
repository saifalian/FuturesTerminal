CREATE TABLE IF NOT EXISTS raw_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    stream TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    pair_symbol TEXT,
    source_exchange_id TEXT,
    ts_exchange_ms INTEGER,
    ts_receive_ms INTEGER,
    capture_mode TEXT
);

CREATE INDEX IF NOT EXISTS idx_raw_events_ts ON raw_events(ts);
CREATE INDEX IF NOT EXISTS idx_raw_events_stream ON raw_events(stream);
CREATE INDEX IF NOT EXISTS idx_raw_events_pair_ts ON raw_events(pair_symbol, ts_receive_ms);
CREATE INDEX IF NOT EXISTS idx_raw_events_capture_mode ON raw_events(capture_mode);

CREATE TABLE IF NOT EXISTS market_event_chunks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pair_symbol TEXT NOT NULL,
    source_exchange_id TEXT NOT NULL,
    date_key TEXT NOT NULL,
    hour_key TEXT NOT NULL,
    ts_start_ms INTEGER NOT NULL,
    ts_end_ms INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    capture_mode TEXT NOT NULL,
    dataset_quality TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_market_event_chunks_pair_ts ON market_event_chunks(pair_symbol, ts_start_ms, ts_end_ms);
CREATE INDEX IF NOT EXISTS idx_market_event_chunks_pair_ex_hour ON market_event_chunks(pair_symbol, source_exchange_id, date_key, hour_key);
CREATE UNIQUE INDEX IF NOT EXISTS idx_market_event_chunks_file_path ON market_event_chunks(file_path);
