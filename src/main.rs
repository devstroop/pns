use std::net::SocketAddr;

use axum::{Router, routing::get, Json};
use serde_json::json;
use tokio::net::TcpListener;
use tonic::{transport::Server, Request, Response, Status, Streaming};
use futures::StreamExt;

pub mod push {
    tonic::include_proto!("devstroop.push.v1");
}

use push::{push_service_server::{PushService, PushServiceServer}, ClientMessage, ServerMessage, Notification, Pong};

#[derive(Default)]
struct PushSvc;

#[tonic::async_trait]
impl PushService for PushSvc {
    type StreamStream = tokio_stream::wrappers::ReceiverStream<Result<ServerMessage, Status>>;

    async fn stream(
        &self,
        request: Request<Streaming<ClientMessage>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(64);

        tokio::spawn(async move {
            while let Some(msg) = inbound.next().await {
                match msg {
                    Ok(m) => {
                        if let Some(push::client_message::Payload::Ping(_)) = m.payload {
                            let _ = tx.send(Ok(ServerMessage {
                                payload: Some(push::server_message::Payload::Pong(Pong {})),
                            })).await;
                        }
                        // TODO: auth via Redis NS:{token}, route notifications
                    }
                    Err(e) => {
                        tracing::warn!("stream error: {e}");
                        break;
                    }
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({"ok": true, "service": "devstroop-push", "version": "0.1.0"}))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let grpc_addr: SocketAddr = "0.0.0.0:50051".parse()?;
    let http_addr: SocketAddr = "0.0.0.0:8080".parse()?;

    let grpc = Server::builder()
        .add_service(PushServiceServer::new(PushSvc))
        .serve(grpc_addr);

    let app = Router::new().route("/health", get(health));
    let http = async {
        let listener = TcpListener::bind(http_addr).await?;
        axum::serve(listener, app).await
    };

    tracing::info!("devstroop-push listening gRPC :50051, HTTP :8080");
    tokio::select! {
        r = grpc => r?,
        r = http => r?,
    }
    Ok(())
}
