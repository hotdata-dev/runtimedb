pub mod worker;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;

/// Channel capacity for the metrics event channel.
/// Events are silently dropped when the channel is full (hot path uses try_send).
pub const METRICS_CHANNEL_CAPACITY: usize = 1_000;

/// An HTTP request event captured by the metrics middleware.
pub enum MetricsEvent {
    HttpRequest {
        timestamp: DateTime<Utc>,
        /// Matched route template, e.g. "/query" or "/connections/{connection_id}"
        path: String,
        method: String,
        status_code: u16,
        /// Wall-clock time from request receipt to response ready, in milliseconds
        latency_ms: u64,
    },
}

/// Convenience type alias for the metrics sender half.
pub type MetricsSender = mpsc::Sender<MetricsEvent>;

/// Accumulated metrics for a single (minute, path) bucket.
#[derive(Debug, Clone)]
pub struct RollupBucket {
    pub request_count: i64,
    pub error_count: i64,
    pub total_latency_ms: i64,
    pub min_latency_ms: i64,
    pub max_latency_ms: i64,
}

impl RollupBucket {
    pub fn new(latency_ms: u64, is_error: bool) -> Self {
        let lat = latency_ms as i64;
        Self {
            request_count: 1,
            error_count: if is_error { 1 } else { 0 },
            total_latency_ms: lat,
            min_latency_ms: lat,
            max_latency_ms: lat,
        }
    }

    pub fn accumulate(&mut self, latency_ms: u64, is_error: bool) {
        let lat = latency_ms as i64;
        self.request_count += 1;
        if is_error {
            self.error_count += 1;
        }
        self.total_latency_ms += lat;
        if lat < self.min_latency_ms {
            self.min_latency_ms = lat;
        }
        if lat > self.max_latency_ms {
            self.max_latency_ms = lat;
        }
    }
}
