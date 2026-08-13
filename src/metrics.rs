// SPDX-License-Identifier: Apache-2.0

//! Process counters and the interval line they are printed on.
//!
//! Same idea as gRParse and grPOIc: a handful of monotonic counters and one
//! line on stdout every N seconds. There is no Prometheus endpoint yet — a
//! sidecar scraping the line is the fleet's current answer, and inventing a
//! second metrics contract here would be worse than not having one.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Counters shared by every in-flight parse.
///
/// Relaxed ordering throughout: these are counters read by a printer thread,
/// not synchronization between the parses that bump them.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Parse streams opened.
    pub parses_started: AtomicU64,
    /// Parse streams that reached their trailer.
    pub parses_completed: AtomicU64,
    /// Parse streams that ended with a gRPC error.
    pub parses_failed: AtomicU64,
    /// Parse streams refused before starting, by the concurrency cap.
    pub parses_rejected: AtomicU64,
    /// Records emitted as rows.
    pub records_emitted: AtomicU64,
    /// Input bytes received across all parses.
    pub bytes_received: AtomicU64,
    /// Parses stopped by the byte cap.
    pub byte_cap_hits: AtomicU64,
}

impl Metrics {
    /// A fresh, zeroed counter set.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Add to a counter.
    pub fn add(counter: &AtomicU64, amount: u64) {
        counter.fetch_add(amount, Ordering::Relaxed);
    }

    /// Bump a counter by one.
    pub fn bump(counter: &AtomicU64) {
        Self::add(counter, 1);
    }

    /// The one-line snapshot printed on the interval.
    #[must_use]
    pub fn line(&self) -> String {
        let read = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        format!(
            "grpc-ebcdic metrics: parses{{started={},completed={},failed={},rejected={}}} \
             records={} bytes={} byte_cap_hits={}",
            read(&self.parses_started),
            read(&self.parses_completed),
            read(&self.parses_failed),
            read(&self.parses_rejected),
            read(&self.records_emitted),
            read(&self.bytes_received),
            read(&self.byte_cap_hits),
        )
    }

    /// Print [`Self::line`] every `interval` until the process exits.
    ///
    /// A zero interval disables the reporter, which is what a container that
    /// scrapes nothing wants.
    pub fn spawn_reporter(self: &Arc<Self>, interval: Duration) {
        if interval.is_zero() {
            return;
        }
        let metrics = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // The first tick fires immediately; skip it so startup does not
            // print a line of zeroes.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                println!("{}", metrics.line());
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::Metrics;

    #[test]
    fn the_line_carries_every_counter() {
        let metrics = Metrics::new();
        Metrics::bump(&metrics.parses_started);
        Metrics::add(&metrics.records_emitted, 17);
        Metrics::add(&metrics.bytes_received, 4096);
        let line = metrics.line();
        assert!(line.starts_with("grpc-ebcdic metrics:"), "{line}");
        for needle in [
            "started=1",
            "completed=0",
            "failed=0",
            "rejected=0",
            "records=17",
            "bytes=4096",
            "byte_cap_hits=0",
        ] {
            assert!(line.contains(needle), "{needle} missing from {line}");
        }
    }
}
