// SPDX-License-Identifier: Apache-2.0

//! Binary entry point for the grpc-ebcdic collector.
//!
//! Every knob is an environment variable, all optional:
//!
//! - `GRPC_EBCDIC_ADDR` — listen address (default `0.0.0.0:50063`).
//! - `GRPC_EBCDIC_WORKERS` — tokio worker threads (default: CPU count).
//! - `GRPC_EBCDIC_MAX_DOCUMENT_MIB` — byte cap per parse when the request does
//!   not set one (default 512). Exceeding it is `RESOURCE_EXHAUSTED`.
//! - `GRPC_EBCDIC_MAX_CONCURRENT_PARSES` — parse streams admitted at once
//!   (default 64). Past the cap a call is refused, not queued.
//! - `GRPC_EBCDIC_IDLE_TIMEOUT_SECONDS` — longest wait for the next request
//!   frame, or for the client to take the next event, before the parse ends
//!   with `DEADLINE_EXCEEDED` (default 30, minimum 1).
//! - `GRPC_EBCDIC_METRICS_INTERVAL_SECONDS` — seconds between metrics lines on
//!   stdout (default 60; `0` disables them).
//! - `GRPC_EBCDIC_WINDOW_BYTES` — HTTP/2 initial stream and connection window
//!   (default 16 MiB). Uploads are bulk transfers, and hyper's 1 MiB default
//!   paces them at one window per round trip over any real link.

// `Duration::from_mins` would satisfy clippy here but is still unstable, and
// the keepalive numbers below are the fleet's, not ours to round.
#![allow(clippy::duration_suboptimal_units)]

use std::time::Duration;

use tonic::transport::Server;

use grpc_ebcdic::{EbcdicGrpc, Metrics, server};

/// TCP keepalive probe interval on an idle connection.
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);

/// HTTP/2 ping interval on an idle connection.
const HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Deadline for an HTTP/2 keepalive ping before the connection is dropped.
const HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Default listen address when `GRPC_EBCDIC_ADDR` is not set.
const DEFAULT_ADDR: &str = "0.0.0.0:50063";

/// Default HTTP/2 initial window, for both the stream and the connection.
const DEFAULT_WINDOW_BYTES: u32 = 16 * 1024 * 1024;

/// Read a whole-number environment variable, falling back to `default` only
/// when it is not set.
///
/// A variable that is set to something else fails startup. Falling back
/// would start the server on a limit the operator did not ask for, with no
/// line anywhere to say so.
fn env_setting<T>(name: &str, default: T) -> Result<T, String>
where
    T: TryFrom<u64>,
{
    match std::env::var(name) {
        Ok(value) => parse_setting(name, Some(&value), default),
        Err(std::env::VarError::NotPresent) => parse_setting(name, None, default),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("{name} is set but is not text")),
    }
}

/// Resolve one setting from its raw value. Unset and empty mean the default;
/// anything else has to be a whole number that fits the setting's type.
fn parse_setting<T>(name: &str, value: Option<&str>, default: T) -> Result<T, String>
where
    T: TryFrom<u64>,
{
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(default);
    };
    let number: u64 = value
        .parse()
        .map_err(|_| format!("{name} is set to {value:?}, which is not a whole number"))?;
    T::try_from(number).map_err(|_| format!("{name} is set to {value}, which is out of range"))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workers = env_setting(
        "GRPC_EBCDIC_WORKERS",
        std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get),
    )?
    .max(1);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(workers)
        .build()?;
    runtime.block_on(serve())
}

/// Build the server and run it until a shutdown signal arrives.
async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("GRPC_EBCDIC_ADDR")
        .unwrap_or_else(|_| DEFAULT_ADDR.to_string())
        .parse()?;

    let metrics = Metrics::new();
    metrics.spawn_reporter(Duration::from_secs(env_setting(
        "GRPC_EBCDIC_METRICS_INTERVAL_SECONDS",
        60_u64,
    )?));

    let max_document_mib: u32 = env_setting(
        "GRPC_EBCDIC_MAX_DOCUMENT_MIB",
        grpc_ebcdic::service::DEFAULT_MAX_DOCUMENT_MIB,
    )?;
    let max_parses: usize = env_setting(
        "GRPC_EBCDIC_MAX_CONCURRENT_PARSES",
        grpc_ebcdic::service::DEFAULT_MAX_CONCURRENT_PARSES,
    )?;

    let idle_timeout = Duration::from_secs(
        env_setting(
            "GRPC_EBCDIC_IDLE_TIMEOUT_SECONDS",
            grpc_ebcdic::service::DEFAULT_IDLE_TIMEOUT_SECONDS,
        )?
        .max(1),
    );

    let grpc = EbcdicGrpc::new(std::sync::Arc::clone(&metrics))
        .with_max_document_mib(max_document_mib)
        .with_max_concurrent_parses(max_parses)
        .with_idle_timeout(idle_timeout);

    let window: u32 = env_setting("GRPC_EBCDIC_WINDOW_BYTES", DEFAULT_WINDOW_BYTES)?;

    eprintln!(
        "grpc-ebcdic {} listening on {addr} (cap {max_document_mib} MiB, {max_parses} parses, \
         http2 window {window} bytes)",
        env!("CARGO_PKG_VERSION")
    );
    let builder = Server::builder()
        .tcp_nodelay(true)
        .tcp_keepalive(Some(TCP_KEEPALIVE))
        .http2_keepalive_interval(Some(HTTP2_KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(HTTP2_KEEPALIVE_TIMEOUT))
        .initial_stream_window_size(window)
        .initial_connection_window_size(window);
    server::router(builder, grpc)
        .await?
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;
    eprintln!("grpc-ebcdic shut down");
    Ok(())
}

/// Resolve when the process receives SIGINT or SIGTERM, so open streams drain
/// instead of being cut mid-record.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    tokio::select! {
        _ = ctrl_c => {}
        _ = sigterm.recv() => {}
    }
}

#[cfg(test)]
mod tests {
    use super::parse_setting;

    #[test]
    fn an_unset_or_empty_setting_takes_the_default() {
        assert_eq!(parse_setting("X", None, 7_u64), Ok(7));
        assert_eq!(parse_setting("X", Some(""), 7_u64), Ok(7));
        assert_eq!(parse_setting("X", Some("  "), 7_u64), Ok(7));
        assert_eq!(parse_setting("X", Some("12"), 7_u64), Ok(12));
        assert_eq!(parse_setting("X", Some(" 12 "), 7_u32), Ok(12));
    }

    #[test]
    fn a_setting_that_is_set_but_unusable_fails_startup_and_names_itself() {
        for value in ["512MB", "eight", "-1", "1.5"] {
            let err = parse_setting("GRPC_EBCDIC_MAX_DOCUMENT_MIB", Some(value), 7_u32)
                .expect_err("a value that is not a whole number is refused");
            assert!(err.contains("GRPC_EBCDIC_MAX_DOCUMENT_MIB"), "{err}");
            assert!(err.contains(value), "{err}");
        }
        // Too large for the setting is refused too, not swapped for the default.
        let err = parse_setting("GRPC_EBCDIC_WINDOW_BYTES", Some("99999999999"), 7_u32)
            .expect_err("a value past u32 is refused");
        assert!(err.contains("out of range"), "{err}");
    }
}
