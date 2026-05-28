CREATE TABLE IF NOT EXISTS spark_sync_watermark (
    table_name   TEXT      PRIMARY KEY,
    last_updated BIGINT    NOT NULL DEFAULT 0,
    updated_at   TIMESTAMP DEFAULT NOW()
);


INSERT INTO spark_sync_watermark (table_name, last_updated)
VALUES ('app_user_visits_fact', 0)
ON CONFLICT (table_name) DO NOTHING; 
