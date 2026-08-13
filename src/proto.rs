// SPDX-License-Identifier: Apache-2.0

//! Generated protobuf code for the `ai.pipestream.ebcdic.v1` package.
//!
//! Produced at build time by `build.rs` (tonic-prost-build) from the contracts
//! under `proto/`, which `buf` lints and breaking-checks. Nothing here is
//! committed, so the generated code can never drift from the `.proto` files.

/// Messages, enums, client, and server for the `ai.pipestream.ebcdic.v1`
/// protobuf package.
///
/// Wire-level documentation lives in the `.proto` files, where buf enforces a
/// comment on every element; prost carries it into the generated Rust where it
/// can.
#[allow(clippy::all, clippy::pedantic, clippy::nursery, missing_docs)]
pub mod v1 {
    tonic::include_proto!("ai.pipestream.ebcdic.v1");
}

/// Serialized `FileDescriptorSet` for the package, served by the gRPC
/// reflection service so `grpcurl` and friends need no local copy of the
/// protos.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("ebcdic_descriptor");
