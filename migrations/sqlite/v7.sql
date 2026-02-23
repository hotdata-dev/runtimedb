CREATE TABLE metrics_request_rollup_minute (
    minute           TEXT    NOT NULL,
    path             TEXT    NOT NULL,
    request_count    INTEGER NOT NULL DEFAULT 0,
    error_count      INTEGER NOT NULL DEFAULT 0,
    total_latency_ms INTEGER NOT NULL DEFAULT 0,
    min_latency_ms   INTEGER,
    max_latency_ms   INTEGER,
    PRIMARY KEY (minute, path)
);
