-- ============================================================
-- PostgreSQL Schema for Fraud Detection System
-- ============================================================
-- Run: psql -U fraud_user -d fraud_detection -f init.sql

-- Transactions table
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(64) UNIQUE NOT NULL,
    timestamp TIMESTAMP DEFAULT NOW(),
    cardholder_id VARCHAR(32) NOT NULL,
    merchant_id VARCHAR(32),
    merchant_name VARCHAR(128),
    amount DECIMAL(12,2) NOT NULL,
    currency VARCHAR(8) DEFAULT 'USD',
    mcc INTEGER,
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    channel VARCHAR(16),
    card_type VARCHAR(16),
    is_international BOOLEAN DEFAULT FALSE,
    fraud_score DECIMAL(6,4),
    classification VARCHAR(16),
    processing_time_ms DECIMAL(8,2),
    features JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_txn_cardholder ON transactions(cardholder_id);
CREATE INDEX IF NOT EXISTS ix_txn_timestamp ON transactions(timestamp);
CREATE INDEX IF NOT EXISTS ix_txn_classification ON transactions(classification);
CREATE INDEX IF NOT EXISTS ix_txn_created ON transactions(created_at);

-- Alerts table
CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL,
    cardholder_id VARCHAR(32) NOT NULL,
    amount DECIMAL(12,2),
    merchant_name VARCHAR(128),
    fraud_score DECIMAL(6,4),
    classification VARCHAR(16),
    severity VARCHAR(16) DEFAULT 'MEDIUM',
    channel VARCHAR(16),
    status VARCHAR(16) DEFAULT 'OPEN',
    reviewed_by VARCHAR(64),
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_alert_created ON alerts(created_at);
CREATE INDEX IF NOT EXISTS ix_alert_status ON alerts(status);
CREATE INDEX IF NOT EXISTS ix_alert_severity ON alerts(severity);

-- Model metadata table
CREATE TABLE IF NOT EXISTS model_metadata (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(64) NOT NULL,
    version VARCHAR(32) NOT NULL,
    algorithm VARCHAR(64),
    accuracy DECIMAL(6,4),
    precision_score DECIMAL(6,4),
    recall DECIMAL(6,4),
    f1_score DECIMAL(6,4),
    auprc DECIMAL(6,4),
    training_samples INTEGER,
    feature_count INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    deployed_at TIMESTAMP DEFAULT NOW(),
    notes TEXT
);

-- Seed model metadata
INSERT INTO model_metadata (model_name, version, algorithm, accuracy, precision_score, recall, f1_score, auprc, is_active, notes)
VALUES ('fraud_model', '1.0', 'RandomForest', 0.981, 0.94, 0.89, 0.91, 0.92, TRUE, 'Initial production model trained on Kaggle Credit Card dataset')
ON CONFLICT DO NOTHING;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO fraud_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO fraud_user;
