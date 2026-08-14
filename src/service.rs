// SPDX-License-Identifier: Apache-2.0

//! The `EbcdicParseService` gRPC implementation.
//!
//! The whole file is one shape: read a frame, decode whatever records that
//! frame completed, send them, repeat. There is no collecting phase and no
//! place where a whole document exists, which is both the streaming contract
//! and the memory bound.
//!
//! The one exception is asked for explicitly: with `emit_document` set, every
//! outbound event also goes through a [`DocumentFold`], which does accumulate
//! — that is what a Document is — and is therefore off by default and capped
//! when on. See [`crate::document_fold`].

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::codec::{self, Codec};
use crate::document_fold::DocumentFold;
use crate::error::ParseError;
use crate::layout;
use crate::metrics::Metrics;
use crate::proto::v1 as pb;
use crate::proto::v1::ebcdic_parse_service_server::{EbcdicParseService, EbcdicParseServiceServer};
use crate::stream::{DecodeOptions, RecordStream};

/// Default byte cap on one parse: 512 MiB.
///
/// Not a limit on how much this service can decode — the walk is incremental
/// and a terabyte would stream fine — but a limit on how much one caller may
/// push through one stream before the operator has said it is expected.
pub const DEFAULT_MAX_DOCUMENT_MIB: u32 = 512;

/// Default number of parse streams admitted at once.
///
/// Each in-flight parse holds at most one record plus a footer, so the bound
/// is about fairness and file descriptors rather than heap. Past it a call is
/// refused rather than queued: a queued parse that eventually runs is worse for
/// a live view than one that fails fast.
pub const DEFAULT_MAX_CONCURRENT_PARSES: usize = 64;

/// Events buffered between the decoder and the wire.
///
/// Small on purpose. A deep queue would let the decoder run ahead of a slow
/// consumer and turn the row stream back into a batch held in server memory,
/// which is the failure mode this service is built to avoid.
const EVENT_CHANNEL_CAPACITY: usize = 32;

/// The service implementation.
pub struct EbcdicGrpc {
    /// Byte cap applied when the request does not set one.
    max_document_bytes: u64,
    /// Admission control for concurrent parses.
    permits: Arc<Semaphore>,
    /// How many permits the semaphore was built with, for `GetServiceInfo`.
    max_concurrent_parses: usize,
    /// Process counters.
    metrics: Arc<Metrics>,
}

impl EbcdicGrpc {
    /// Build the service with the fleet defaults.
    #[must_use]
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            max_document_bytes: u64::from(DEFAULT_MAX_DOCUMENT_MIB) << 20,
            permits: Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_PARSES)),
            max_concurrent_parses: DEFAULT_MAX_CONCURRENT_PARSES,
            metrics,
        }
    }

    /// Override the default byte cap, in mebibytes.
    #[must_use]
    pub fn with_max_document_mib(mut self, mib: u32) -> Self {
        self.max_document_bytes = u64::from(mib.max(1)) << 20;
        self
    }

    /// Override how many parses may run at once.
    #[must_use]
    pub fn with_max_concurrent_parses(mut self, parses: usize) -> Self {
        self.max_concurrent_parses = parses.max(1);
        self.permits = Arc::new(Semaphore::new(self.max_concurrent_parses));
        self
    }

    /// Wrap the implementation in its generated tonic server.
    #[must_use]
    pub fn into_service(self) -> EbcdicParseServiceServer<Self> {
        EbcdicParseServiceServer::new(self)
    }

    /// Resolve the byte cap for one request.
    fn byte_cap(&self, options: &pb::ParseOptions) -> u64 {
        if options.max_document_mib == 0 {
            self.max_document_bytes
        } else {
            u64::from(options.max_document_mib) << 20
        }
    }
}

/// The server-streaming half of `ParseEbcdic`.
type EventStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<pb::ParseEbcdicResponse, Status>> + Send>>;

#[tonic::async_trait]
impl EbcdicParseService for EbcdicGrpc {
    type ParseEbcdicStream = EventStream;

    async fn parse_ebcdic(
        &self,
        request: Request<Streaming<pb::ParseEbcdicRequest>>,
    ) -> Result<Response<Self::ParseEbcdicStream>, Status> {
        // Admission first: refusing before any work is what makes the cap
        // meaningful, and `try_acquire_owned` never queues.
        let permit = Arc::clone(&self.permits).try_acquire_owned().map_err(|_| {
            Metrics::bump(&self.metrics.parses_rejected);
            Status::resource_exhausted(format!(
                "{} parses are already running; retry when one finishes",
                self.max_concurrent_parses
            ))
        })?;

        let mut inbound = request.into_inner();

        // The options frame is read before the response stream opens, so a bad
        // layout is a plain unary error with no half-open stream behind it.
        let first = inbound.message().await?.ok_or_else(|| {
            Status::invalid_argument("the request stream ended before its options frame")
        })?;
        let options = match first.frame {
            Some(pb::parse_ebcdic_request::Frame::Options(options)) => options,
            Some(pb::parse_ebcdic_request::Frame::Chunk(_)) => {
                return Err(Status::invalid_argument(
                    "the first frame must carry options, not data: the server cannot decode bytes \
                     before it has the layout",
                ));
            }
            None => return Err(Status::invalid_argument("the first frame is empty")),
        };

        let codec = Codec::resolve(&options.encoding).map_err(ParseError::from)?;
        let layout = layout::resolve(&options)?;
        let decode_options = DecodeOptions {
            codec,
            strip_control_characters: options.strip_control_characters.unwrap_or(true),
            max_records: options.max_records,
            abort_on_error: options.abort_on_error,
        };
        let layout_info = layout.to_layout_info(codec.name());
        let byte_cap = self.byte_cap(&options);
        // Built here or not at all: with `emit_document` unset there is no
        // fold, so the row path costs exactly what it cost before.
        let fold = options
            .emit_document
            .then(|| DocumentFold::new(env!("CARGO_PKG_VERSION")));

        Metrics::bump(&self.metrics.parses_started);
        let metrics = Arc::clone(&self.metrics);
        let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);

        tokio::spawn(async move {
            // Held for the life of the parse so the permit is released exactly
            // when the stream ends, however it ends.
            let _permit = permit;
            let outcome = run_parse(
                &mut inbound,
                RecordStream::new(layout, decode_options),
                layout_info,
                byte_cap,
                EventSink { tx: &tx, fold },
                &metrics,
            )
            .await;
            match outcome {
                Ok(()) => Metrics::bump(&metrics.parses_completed),
                Err(error) => {
                    Metrics::bump(&metrics.parses_failed);
                    if error.code() == tonic::Code::ResourceExhausted {
                        Metrics::bump(&metrics.byte_cap_hits);
                    }
                    // A send failure here only means the client is gone, which
                    // is not something the server can or should report.
                    let _ = tx.send(Err(Status::from(error))).await;
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as EventStream
        ))
    }

    async fn get_service_info(
        &self,
        _request: Request<pb::GetServiceInfoRequest>,
    ) -> Result<Response<pb::GetServiceInfoResponse>, Status> {
        Ok(Response::new(pb::GetServiceInfoResponse {
            service: "grpc-ebcdic".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            encodings: codec::supported_encodings()
                .into_iter()
                .map(String::from)
                .collect(),
            default_encoding: codec::DEFAULT_ENCODING.to_string(),
            default_max_document_mib: u32::try_from(self.max_document_bytes >> 20)
                .unwrap_or(u32::MAX),
            max_concurrent_parses: u32::try_from(self.max_concurrent_parses).unwrap_or(u32::MAX),
            supported_field_types: vec![
                pb::FieldType::String as i32,
                pb::FieldType::Integer as i32,
                pb::FieldType::UnsignedInteger as i32,
                pb::FieldType::PackedDecimal as i32,
                pb::FieldType::ZonedDecimal as i32,
                pb::FieldType::Skip as i32,
            ],
            copybook_compiler: true,
        }))
    }
}

/// The outbound half of one parse: the channel, and the fold when the request
/// asked for a Document.
///
/// Every event goes out through here, which is what makes "the fold sees
/// exactly what the client sees" a property of the code rather than of a
/// convention someone has to remember.
struct EventSink<'a> {
    /// Where events go.
    tx: &'a mpsc::Sender<Result<pb::ParseEbcdicResponse, Status>>,
    /// The Document fold, when `emit_document` was set.
    fold: Option<DocumentFold>,
}

impl EventSink<'_> {
    /// Fold one event and send it, treating a closed channel as a cancelled
    /// call.
    async fn send(&mut self, event: pb::parse_ebcdic_response::Event) -> Result<(), ParseError> {
        if let Some(fold) = self.fold.as_mut() {
            fold.consume(&event);
        }
        self.tx
            .send(Ok(pb::ParseEbcdicResponse { event: Some(event) }))
            .await
            .map_err(|_| ParseError::internal("the client stopped reading the event stream"))
    }

    /// Send the trailer, preceded by the Document when one was asked for.
    ///
    /// The order is the contract: the fold's own truncation warnings are
    /// merged into the trailer *before* the fold sees it, the document event
    /// goes out immediately before the trailer, and the trailer stays last.
    async fn finish(&mut self, mut status: pb::ParseStatus) -> Result<(), ParseError> {
        let Some(mut fold) = self.fold.take() else {
            return self
                .send(pb::parse_ebcdic_response::Event::Status(status))
                .await;
        };
        status.warnings.extend(fold.truncation_warnings());
        let status = pb::parse_ebcdic_response::Event::Status(status);
        fold.consume(&status);
        self.send(pb::parse_ebcdic_response::Event::Document(fold.take()))
            .await?;
        self.send(status).await
    }
}

/// Drive one parse from the first data frame to the trailer.
///
/// Every row is awaited onto the channel before the next is decoded, so a
/// consumer that stops reading stops the parse rather than filling the server
/// with rows it has not asked for.
async fn run_parse(
    inbound: &mut Streaming<pb::ParseEbcdicRequest>,
    mut walk: RecordStream,
    layout_info: pb::LayoutInfo,
    byte_cap: u64,
    mut sink: EventSink<'_>,
    metrics: &Metrics,
) -> Result<(), ParseError> {
    sink.send(pb::parse_ebcdic_response::Event::LayoutInfo(layout_info))
        .await?;

    loop {
        let frame = match inbound.message().await {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            // The client hung up or cancelled. Nothing to report to it.
            Err(status) => {
                return Err(ParseError::internal(format!(
                    "request stream failed: {status}"
                )));
            }
        };
        let chunk = match frame.frame {
            Some(pb::parse_ebcdic_request::Frame::Chunk(chunk)) => chunk,
            Some(pb::parse_ebcdic_request::Frame::Options(_)) => {
                return Err(ParseError::invalid(
                    "options may only appear in the first frame of a parse stream",
                ));
            }
            None => continue,
        };
        if walk.received() + chunk.len() as u64 > byte_cap {
            return Err(ParseError::exhausted(format!(
                "the input exceeds the {byte_cap}-byte cap for this stream"
            )));
        }
        Metrics::add(&metrics.bytes_received, chunk.len() as u64);
        walk.push(&chunk);
        drain(&mut walk, &mut sink, metrics).await?;
    }

    walk.finish_input();
    drain(&mut walk, &mut sink, metrics).await?;
    let status = walk.status()?;
    sink.finish(status).await
}

/// Emit every record the walk can produce right now.
async fn drain(
    walk: &mut RecordStream,
    sink: &mut EventSink<'_>,
    metrics: &Metrics,
) -> Result<(), ParseError> {
    while let Some(row) = walk.next_record()? {
        Metrics::bump(&metrics.records_emitted);
        sink.send(pb::parse_ebcdic_response::Event::Record(row))
            .await?;
    }
    Ok(())
}
