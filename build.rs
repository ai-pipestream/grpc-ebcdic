// SPDX-License-Identifier: Apache-2.0

//! Build script: compiles the buf-managed protobuf contracts under `proto`
//! into Rust server and client stubs with `tonic-prost-build`, and writes the
//! `FileDescriptorSet` the reflection service serves.
//!
//! `buf` owns linting and breaking-change detection (`buf.yaml`); it is not on
//! the build path, so a checkout without `buf` installed still compiles. The
//! optional `buf.gen.yaml` produces the same stubs out of tree for anyone who
//! prefers committed generated code, but this file is the canonical route.
//!
//! Clients are generated as well as servers: the integration tests drive the
//! real server over a real socket with the generated client.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = "proto";
    let protos = [
        // Vendored byte-identical from the gRParse repo, which owns it. The
        // Document projection is optional on the wire but the schema is not
        // optional at build time: the response oneof references it.
        "proto/ai/pipestream/document/v1/document.proto",
        "proto/ai/pipestream/ebcdic/v1/ebcdic.proto",
        "proto/ai/pipestream/ebcdic/v1/ebcdic_service.proto",
    ];
    for proto in &protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("ebcdic_descriptor.bin"))
        .compile_protos(&protos, &[proto_root])?;
    Ok(())
}
