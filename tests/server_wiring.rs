// SPDX-License-Identifier: Apache-2.0

//! The two services that are not the product but are the contract with an
//! orchestrator: `grpc.health.v1.Health` and server reflection.
//!
//! These go through `grpc_ebcdic::server::router`, which is the same function
//! `main.rs` calls, so a registration removed from the binary fails here.

use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Endpoint, Server};
use tonic_health::pb::health_client::HealthClient;
use tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient;
use tonic_reflection::pb::v1::server_reflection_request::MessageRequest;
use tonic_reflection::pb::v1::server_reflection_response::MessageResponse;

use grpc_ebcdic::{EbcdicGrpc, Metrics, server};

/// Fully qualified name of the parse service, as it appears on the wire.
const SERVICE_NAME: &str = "ai.pipestream.ebcdic.v1.EbcdicParseService";

/// Start the full server wiring on an ephemeral port and return its address.
async fn start() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().unwrap();
    let router = server::router(Server::builder(), EbcdicGrpc::new(Metrics::new()))
        .await
        .expect("the descriptor set is valid");
    tokio::spawn(async move {
        router
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("server runs");
    });
    format!("http://{addr}")
}

/// Connect a channel to a running server.
async fn connect(addr: &str) -> tonic::transport::Channel {
    Endpoint::from_shared(addr.to_string())
        .unwrap()
        .connect()
        .await
        .expect("connect")
}

#[tokio::test]
async fn the_health_service_reports_the_parse_service_as_serving() {
    let addr = start().await;
    let mut health = HealthClient::new(connect(&addr).await);

    // The empty service name is the whole-process check an orchestrator
    // probe uses by default.
    let overall = health
        .check(tonic_health::pb::HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("the process reports its own health")
        .into_inner();
    assert_eq!(
        overall.status,
        tonic_health::pb::health_check_response::ServingStatus::Serving as i32
    );

    let named = health
        .check(tonic_health::pb::HealthCheckRequest {
            service: SERVICE_NAME.to_string(),
        })
        .await
        .expect("the parse service is registered by name")
        .into_inner();
    assert_eq!(
        named.status,
        tonic_health::pb::health_check_response::ServingStatus::Serving as i32
    );

    // A service that does not exist is NOT_FOUND, not a silent SERVING.
    let missing = health
        .check(tonic_health::pb::HealthCheckRequest {
            service: "nope.v1.Nope".to_string(),
        })
        .await
        .expect_err("an unknown service is not healthy by default");
    assert_eq!(missing.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn reflection_lists_the_parse_service_so_grpcurl_needs_no_protos() {
    let addr = start().await;
    let mut reflection = ServerReflectionClient::new(connect(&addr).await);

    let request = tonic_reflection::pb::v1::ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    };
    let mut stream = reflection
        .server_reflection_info(tokio_stream::iter(vec![request]))
        .await
        .expect("reflection is registered")
        .into_inner();

    let response = stream.message().await.unwrap().expect("one response");
    let Some(MessageResponse::ListServicesResponse(list)) = response.message_response else {
        panic!("expected a service list, got {response:?}");
    };
    let names: Vec<&str> = list.service.iter().map(|s| s.name.as_str()).collect();
    assert!(names.contains(&SERVICE_NAME), "{names:?}");
    // The health contract is discoverable too, so `grpcurl ... Health/Check`
    // works against a bare deployment with no protos on the client side.
    assert!(names.contains(&"grpc.health.v1.Health"), "{names:?}");
}

#[tokio::test]
async fn reflection_serves_the_file_that_defines_the_parse_service() {
    let addr = start().await;
    let mut reflection = ServerReflectionClient::new(connect(&addr).await);

    let request = tonic_reflection::pb::v1::ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::FileContainingSymbol(
            SERVICE_NAME.to_string(),
        )),
    };
    let mut stream = reflection
        .server_reflection_info(tokio_stream::iter(vec![request]))
        .await
        .expect("reflection is registered")
        .into_inner();

    let response = stream.message().await.unwrap().expect("one response");
    let Some(MessageResponse::FileDescriptorResponse(files)) = response.message_response else {
        panic!("expected file descriptors, got {response:?}");
    };
    assert!(
        !files.file_descriptor_proto.is_empty(),
        "the descriptor set compiled into the binary is empty"
    );
}
