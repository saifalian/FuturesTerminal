CREATE TABLE IF NOT EXISTS ml_feature_manifests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pair_symbol TEXT NOT NULL,
    tab_id TEXT NOT NULL,
    date_key TEXT NOT NULL,
    file_path TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    ts_start_ms INTEGER NOT NULL,
    ts_end_ms INTEGER NOT NULL,
    schema_version TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ml_feature_manifests_pair_date ON ml_feature_manifests(pair_symbol, date_key);
CREATE INDEX IF NOT EXISTS idx_ml_feature_manifests_pair_ts ON ml_feature_manifests(pair_symbol, ts_start_ms, ts_end_ms);

CREATE TABLE IF NOT EXISTS ml_pair_profiles (
    pair_symbol TEXT PRIMARY KEY,
    config_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ml_runs (
    run_id TEXT PRIMARY KEY,
    pair_symbol TEXT NOT NULL,
    instance_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    stage TEXT NOT NULL,
    stage_progress REAL NOT NULL,
    device TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    ended_at TEXT,
    config_json TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    error_text TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ml_runs_pair ON ml_runs(pair_symbol, created_at);
CREATE INDEX IF NOT EXISTS idx_ml_runs_status ON ml_runs(status, updated_at);

CREATE TABLE IF NOT EXISTS ml_run_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    ts TEXT NOT NULL,
    level TEXT NOT NULL,
    message TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ml_run_logs_run ON ml_run_logs(run_id, id);

CREATE TABLE IF NOT EXISTS ml_models (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pair_symbol TEXT NOT NULL,
    instance_id TEXT NOT NULL DEFAULT '',
    run_id TEXT NOT NULL,
    model_path TEXT NOT NULL,
    normalizer_path TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    approved_at TEXT NOT NULL,
    is_current INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ml_models_pair ON ml_models(pair_symbol, approved_at);

CREATE TABLE IF NOT EXISTS ml_bot_state (
    pair_symbol TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    position_side TEXT NOT NULL,
    entry_price REAL NOT NULL,
    qty REAL NOT NULL,
    unrealized_pnl REAL NOT NULL,
    realized_pnl REAL NOT NULL,
    last_signal_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ml_instances (
    instance_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    pair_symbol TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    archived INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ml_instance_profiles (
    instance_id TEXT PRIMARY KEY,
    pair_symbol TEXT NOT NULL,
    config_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ml_instance_bot_state (
    instance_id TEXT PRIMARY KEY,
    pair_symbol TEXT NOT NULL,
    status TEXT NOT NULL,
    position_side TEXT NOT NULL,
    active_session_id TEXT NOT NULL DEFAULT '',
    entry_price REAL NOT NULL,
    qty REAL NOT NULL,
    unrealized_pnl REAL NOT NULL,
    realized_pnl REAL NOT NULL,
    last_signal_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ml_bot_trades (
    trade_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL DEFAULT '',
    session_id TEXT NOT NULL DEFAULT '',
    pair_symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    qty REAL NOT NULL,
    leverage REAL NOT NULL,
    commission_fee_pct REAL NOT NULL,
    entry_price REAL NOT NULL,
    exit_price REAL NOT NULL,
    entry_notional REAL NOT NULL,
    exit_notional REAL NOT NULL,
    entry_fee REAL NOT NULL,
    exit_fee REAL NOT NULL,
    gross_pnl REAL NOT NULL,
    net_pnl REAL NOT NULL,
    opened_at TEXT NOT NULL,
    closed_at TEXT NOT NULL,
    duration_seconds INTEGER NOT NULL,
    close_reason TEXT NOT NULL,
    decision_context_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ml_bot_trades_instance ON ml_bot_trades(instance_id, created_at);
CREATE INDEX IF NOT EXISTS idx_ml_bot_trades_pair ON ml_bot_trades(pair_symbol, created_at);

CREATE TABLE IF NOT EXISTS ml_bot_trade_sessions (
    session_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    name TEXT NOT NULL,
    is_default INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    archived INTEGER NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ml_bot_trade_sessions_default
ON ml_bot_trade_sessions(instance_id, is_default)
WHERE is_default = 1;
CREATE UNIQUE INDEX IF NOT EXISTS idx_ml_bot_trade_sessions_name_ci
ON ml_bot_trade_sessions(instance_id, lower(name))
WHERE archived = 0;
CREATE INDEX IF NOT EXISTS idx_ml_bot_trade_sessions_instance
ON ml_bot_trade_sessions(instance_id, archived, updated_at);
