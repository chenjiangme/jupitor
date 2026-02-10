-- Initial schema for transactional data (SQLite)

CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,          -- 'buy' or 'sell'
    type TEXT NOT NULL,          -- 'market', 'limit', 'stop', 'stop_limit'
    time_in_force TEXT NOT NULL, -- 'day', 'gtc', 'ioc', 'fok'
    qty REAL NOT NULL,
    limit_price REAL,
    stop_price REAL,
    status TEXT NOT NULL,        -- 'pending', 'submitted', 'filled', 'partial', 'cancelled', 'rejected'
    filled_qty REAL DEFAULT 0,
    filled_avg_price REAL,
    strategy_id TEXT,
    broker_order_id TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS positions (
    symbol TEXT PRIMARY KEY,
    qty REAL NOT NULL,
    avg_entry_price REAL NOT NULL,
    market_value REAL,
    side TEXT NOT NULL,           -- 'long' or 'short'
    opened_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    type TEXT NOT NULL,           -- 'buy', 'sell', 'hold'
    strength REAL NOT NULL,      -- -1.0 to 1.0
    metadata TEXT,               -- JSON
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_strategy ON orders(strategy_id);
CREATE INDEX IF NOT EXISTS idx_signals_strategy ON signals(strategy_id);
CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);
