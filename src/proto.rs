// SPDX-License-Identifier: Apache-2.0

//! Generated protobuf code for the `ai.pipestream.ebcdic.v1` and
//! `ai.pipestream.document.v1` packages.
//!
//! Produced at build time by `build.rs` (tonic-prost-build) from the contracts
//! under `proto/`, which `buf` lints and breaking-checks. Nothing here is
//! committed, so the generated code can never drift from the `.proto` files.
//!
//! The two packages are nested to mirror their protobuf package paths, which is
//! not cosmetic: the response oneof carries a `Document`, and prost writes that
//! cross-package reference as a path relative to the package nesting. The
//! flatter names the rest of the crate uses are re-exports of the same modules.

/// Generated code, laid out as `ai::pipestream::<package>::v1` so that prost's
/// relative cross-package references resolve.
mod generated {
    /// The `ai` protobuf namespace.
    pub mod ai {
        /// The `ai.pipestream` protobuf namespace.
        pub mod pipestream {
            /// The `ai.pipestream.document` protobuf namespace.
            pub mod document {
                /// Messages and enums of the `ai.pipestream.document.v1`
                /// package, vendored from gRParse.
                #[allow(clippy::all, clippy::pedantic, clippy::nursery, missing_docs)]
                pub mod v1 {
                    tonic::include_proto!("ai.pipestream.document.v1");
                }
            }

            /// The `ai.pipestream.ebcdic` protobuf namespace.
            pub mod ebcdic {
                /// Messages, enums, client, and server of the
                /// `ai.pipestream.ebcdic.v1` package.
                #[allow(clippy::all, clippy::pedantic, clippy::nursery, missing_docs)]
                pub mod v1 {
                    tonic::include_proto!("ai.pipestream.ebcdic.v1");
                }
            }
        }
    }
}

/// Messages, enums, client, and server for the `ai.pipestream.ebcdic.v1`
/// protobuf package.
///
/// Wire-level documentation lives in the `.proto` files, where buf enforces a
/// comment on every element; prost carries it into the generated Rust where it
/// can.
pub use generated::ai::pipestream::ebcdic::v1;

/// The gRParse `ai.pipestream.document.v1` Document plane, whose schema is
/// vendored byte-identical from the gRParse repo that owns it.
///
/// This crate only reads that schema; the one place that writes it is
/// [`crate::document_fold`].
pub use generated::ai::pipestream::document;

/// Serialized `FileDescriptorSet` for both packages, served by the gRPC
/// reflection service so `grpcurl` and friends need no local copy of the
/// protos.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("ebcdic_descriptor");
