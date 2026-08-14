// SPDX-License-Identifier: Apache-2.0

//! A gRPC collector that decodes copybook-driven EBCDIC records and streams
//! one typed row per record as the byte walk progresses.
//!
//! Design rules, in the order they constrain the code:
//!
//! - **The live stream is the product.** [`LayoutInfo`](proto::v1::LayoutInfo)
//!   goes out before a single input byte is read, each
//!   [`RecordRow`](proto::v1::RecordRow) goes out the moment that record's last
//!   byte arrives, and [`ParseStatus`](proto::v1::ParseStatus) is a trailer of
//!   counts. Nothing is batched and nothing is held back. A mainframe extract
//!   is routinely tens of gigabytes; the alternative is a server that dies and
//!   a UI that shows nothing until it does.
//! - **Diskless.** Bytes live in memory and never touch a filesystem. The only
//!   buffer that persists across a chunk is the partial record at its tail plus
//!   the declared footer, so memory is bounded by the record size and not by
//!   the file size.
//! - **Types survive the wire.** COMP-3 and zoned decimals become an exact
//!   base-10 [`Decimal`](proto::v1::Decimal), binary COMP fields become
//!   integers, and character fields are decoded with the requested EBCDIC code
//!   page. There is no float anywhere on the value path and no JSON blob
//!   standing in for a schema.
//! - **Docling parity.** The layout model, the decoders, and the selector
//!   semantics mirror Docling's `EbcdicDocumentBackend` so the same bytes and
//!   the same layout yield the same values through either implementation. What
//!   differs is the shape of the delivery, deliberately.
//! - **The Document plane is opt-in and bounded.** [`document_fold`] projects
//!   the parse into one `ai.pipestream.document.v1.Document` — flat under the
//!   body, one table per record schema that matched a record, exactly as
//!   docling's own backend builds it — when the request asks for it, emitted
//!   immediately before
//!   the trailer. It is a fold over this crate's own events rather than a
//!   second parser, it is off by default, and because a Document is one message
//!   it caps its rows per schema and reports what it dropped instead of
//!   quietly shortening a table.

pub mod codec;
mod codepages;
/// The COBOL copybook subset compiler. Internal: its output is the
/// crate-private raw layout shape, and callers reach it through
/// [`layout::resolve`].
mod copybook;
pub mod decode;
pub mod document_fold;
pub mod error;
pub mod layout;
pub mod metrics;
pub mod proto;
pub mod server;
pub mod service;
pub mod stream;

pub use document_fold::DocumentFold;
pub use error::ParseError;
pub use metrics::Metrics;
pub use service::EbcdicGrpc;
pub use stream::RecordStream;
