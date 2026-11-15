DROP TABLE IF EXISTS fraud_alerts;
CREATE TABLE fraud_alerts (
    alert_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) NOT NULL,
    client_id VARCHAR(20) NOT NULL,
    client_name VARCHAR(100),
    account_id VARCHAR(20),
    amount NUMERIC(12, 2) NOT NULL,
    avg_client_spend NUMERIC(12, 2),
    event_timestamp TIMESTAMP NOT NULL,
    country VARCHAR(50),
    merchant VARCHAR(100),
    anomaly_type VARCHAR(50) NOT NULL,
    risk_score INT NOT NULL,
    risk_level VARCHAR(20) NOT NULL,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_tx_anomaly UNIQUE (transaction_id, anomaly_type)
);

CREATE TABLE IF NOT EXISTS stream_metrics (
    batch_id INT PRIMARY KEY,
    processed_tx_count INT NOT NULL,
    total_volume_dh NUMERIC(14, 2) NOT NULL,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
