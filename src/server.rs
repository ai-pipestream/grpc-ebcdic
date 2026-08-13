// SPDX-License-Identifier: Apache-2.0

//! Assembly of the three services this process serves.
//!
//! It lives in the library rather than in `main.rs` so the integration tests
//! exercise the same wiring the binary does. Health and reflection registered
//! only in `main` are health and reflection nobody ever checks.

use tonic::transport::Server;
use tonic::transport::server::Router;

use crate::proto;
use crate::proto::v1::ebcdic_parse_service_server::EbcdicParseServiceServer;
use crate::service::EbcdicGrpc;

/// Add the parse service, `grpc.health.v1.Health`, and server reflection to a
/// configured server builder.
///
/// The health service is set `SERVING` for the parse service by name, so an
/// orchestrator can wait on the contract rather than on "a process is
/// listening".
///
/// # Errors
///
/// Fails only if the compiled-in file descriptor set cannot be decoded, which
/// would mean the build produced a corrupt artifact.
pub async fn router(
    mut builder: Server,
    grpc: EbcdicGrpc,
) -> Result<Router, Box<dyn std::error::Error>> {
    // The health contract is registered for reflection too, not only served.
    // Without it `grpcurl -plaintext host:port grpc.health.v1.Health/Check`
    // reports the service as absent even though it answers, because grpcurl
    // discovers methods through reflection before it calls them.
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<EbcdicParseServiceServer<EbcdicGrpc>>()
        .await;

    Ok(builder
        .add_service(grpc.into_service())
        .add_service(reflection)
        .add_service(health_service))
}
