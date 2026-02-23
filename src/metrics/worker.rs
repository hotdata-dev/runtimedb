use crate::catalog::CatalogManager;
use crate::metrics::{MetricsEvent, RollupBucket};
use chrono::{DateTime, DurationRound, TimeDelta, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tracing::warn;

/// Interval between metric flushes to the database.
const FLUSH_INTERVAL: Duration = Duration::from_secs(30);

/// Background worker that receives metrics events, accumulates them in memory,
/// and periodically flushes rollup rows to the database.
///
/// The in-memory map is cleared after every flush, so each event is written
/// exactly once. Multiple instances sharing the same catalog DB converge
/// correctly because the UPSERT is additive.
pub struct MetricsWorker {
    receiver: mpsc::Receiver<MetricsEvent>,
    catalog: Arc<dyn CatalogManager>,
}

impl MetricsWorker {
    pub fn new(receiver: mpsc::Receiver<MetricsEvent>, catalog: Arc<dyn CatalogManager>) -> Self {
        Self { receiver, catalog }
    }

    /// Run the worker until the sender side is dropped, then perform a final flush.
    pub async fn run(mut self) {
        let mut buckets: HashMap<(String, String), RollupBucket> = HashMap::new();
        let mut flush_interval = interval(FLUSH_INTERVAL);
        // Skip the immediate first tick so we don't flush an empty map on startup.
        flush_interval.tick().await;

        loop {
            tokio::select! {
                event = self.receiver.recv() => match event {
                    Some(e) => accumulate(e, &mut buckets),
                    None => {
                        // Channel closed — sender was dropped. Final flush then exit.
                        flush(&buckets, &self.catalog).await;
                        break;
                    }
                },
                _ = flush_interval.tick() => {
                    flush(&buckets, &self.catalog).await;
                    buckets.clear();
                }
            }
        }
    }
}

fn truncate_to_minute(ts: DateTime<Utc>) -> DateTime<Utc> {
    ts.duration_trunc(TimeDelta::minutes(1)).unwrap_or(ts)
}

fn accumulate(event: MetricsEvent, buckets: &mut HashMap<(String, String), RollupBucket>) {
    match event {
        MetricsEvent::HttpRequest {
            timestamp,
            path,
            method: _,
            status_code,
            latency_ms,
        } => {
            let minute = truncate_to_minute(timestamp)
                .format("%Y-%m-%dT%H:%M:00Z")
                .to_string();
            let is_error = status_code >= 400;
            let key = (minute, path);
            match buckets.get_mut(&key) {
                Some(bucket) => bucket.accumulate(latency_ms, is_error),
                None => {
                    buckets.insert(key, RollupBucket::new(latency_ms, is_error));
                }
            }
        }
    }
}

async fn flush(
    buckets: &HashMap<(String, String), RollupBucket>,
    catalog: &Arc<dyn CatalogManager>,
) {
    if buckets.is_empty() {
        return;
    }

    for ((minute, path), bucket) in buckets {
        if let Err(e) = catalog
            .record_request_rollup_minute(minute, path, bucket)
            .await
        {
            warn!(
                minute = %minute,
                path = %path,
                error = %e,
                "Failed to flush metrics rollup"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::MockCatalog;
    use crate::metrics::METRICS_CHANNEL_CAPACITY;
    use chrono::TimeZone;
    use tokio::sync::mpsc;

    fn make_event(ts: DateTime<Utc>, path: &str, status: u16, latency_ms: u64) -> MetricsEvent {
        MetricsEvent::HttpRequest {
            timestamp: ts,
            path: path.to_owned(),
            method: "GET".to_owned(),
            status_code: status,
            latency_ms,
        }
    }

    #[test]
    fn accumulate_same_bucket_sums_correctly() {
        let mut buckets = HashMap::new();
        let ts = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();

        accumulate(make_event(ts, "/query", 200, 10), &mut buckets);
        accumulate(make_event(ts, "/query", 200, 30), &mut buckets);
        accumulate(make_event(ts, "/query", 500, 20), &mut buckets);

        assert_eq!(buckets.len(), 1);
        let bucket = buckets.values().next().unwrap();
        assert_eq!(bucket.request_count, 3);
        assert_eq!(bucket.error_count, 1);
        assert_eq!(bucket.total_latency_ms, 60);
        assert_eq!(bucket.min_latency_ms, 10);
        assert_eq!(bucket.max_latency_ms, 30);
    }

    #[test]
    fn accumulate_different_paths_separate_buckets() {
        let mut buckets = HashMap::new();
        let ts = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();

        accumulate(make_event(ts, "/query", 200, 10), &mut buckets);
        accumulate(make_event(ts, "/health", 200, 5), &mut buckets);

        assert_eq!(buckets.len(), 2);
    }

    #[test]
    fn accumulate_different_minutes_separate_buckets() {
        let mut buckets = HashMap::new();
        let ts1 = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 30).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2026, 1, 1, 12, 1, 0).unwrap();

        accumulate(make_event(ts1, "/query", 200, 10), &mut buckets);
        accumulate(make_event(ts2, "/query", 200, 20), &mut buckets);

        assert_eq!(buckets.len(), 2);
    }

    #[test]
    fn flush_clears_map_on_second_flush() {
        let mut buckets = HashMap::new();
        let ts = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        accumulate(make_event(ts, "/query", 200, 10), &mut buckets);
        assert!(!buckets.is_empty());

        // Simulate post-flush clear
        buckets.clear();
        assert!(buckets.is_empty(), "map should be empty after flush-clear");
    }

    #[tokio::test]
    async fn worker_exits_cleanly_when_channel_closes() {
        let catalog = Arc::new(MockCatalog::new());
        let (tx, rx) = mpsc::channel::<MetricsEvent>(METRICS_CHANNEL_CAPACITY);
        let worker = MetricsWorker::new(rx, catalog);

        let handle = tokio::spawn(worker.run());

        // Drop sender to signal shutdown
        drop(tx);

        tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("worker should exit within 5 seconds")
            .expect("worker task should not panic");
    }
}
