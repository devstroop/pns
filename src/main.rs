use std::net::SocketAddr;
use std::sync::Arc;

use axum::{Router, routing::get, Json};
use redis::aio::ConnectionManager;
use serde_json::json;
use tokio::net::TcpListener;
use tonic::{transport::Server, Request, Response, Status, Streaming};
use futures::StreamExt;

pub mod push {
    tonic::include_proto!("devstroop.push.v1");
}

use push::{
    push_service_server::{PushService, PushServiceServer},
    ClientMessage, ServerMessage, Pong,
};

#[derive(Clone)]
struct PushSvc {
    redis: Arc<ConnectionManager>,
}

#[tonic::async_trait]
impl PushService for PushSvc {
    type StreamStream = tokio_stream::wrappers::ReceiverStream<Result<ServerMessage, Status>>;

    async fn stream(
        &self,
        request: Request<Streaming<ClientMessage>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let mut inbound = request.into_inner();
        let redis = self.redis.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(64);

        tokio::spawn(async move {
            // Expect first message with token for auth.
            let token = match inbound.next().await {
                Some(Ok(m)) if !m.token.is_empty() => m.token,
                _ => {
                    let _ = tx.send(Err(Status::unauthenticated("missing token"))).await;
                    return;
                }
            };

            // Validate session in Redis: NS:{token} -> {tenant}:{userId}
            let mut conn = redis.as_ref().clone();
            let session: Option<String> = redis::cmd("GET")
                .arg(format!("NS:{token}"))
                .query_async(&mut conn)
                .await
                .unwrap_or(None);

            let Some(session) = session else {
                let _ = tx.send(Err(Status::unauthenticated("invalid or expired session"))).await;
                return;
            };

            tracing::info!("client authenticated: {session}");

            // Send a welcome notification.
            let _ = tx.send(Ok(ServerMessage {
                payload: Some(push::server_message::Payload::Notification(push::Notification {
                    id: "welcome".to_string(),
                    title: "Connected".to_string(),
                    body: format!("Session {session}"),
                    data: String::new(),
                    created_at: chrono_now(),
                })),
            })).await;

            // Handle subsequent messages (ping/ack).
            while let Some(msg) = inbound.next().await {
                match msg {
                    Ok(m) => {
                        if let Some(push::client_message::Payload::Ping(_)) = m.payload {
                            let _ = tx.send(Ok(ServerMessage {
                                payload: Some(push::server_message::Payload::Pong(Pong {})),
                            })).await;
                        }
                        // TODO: handle Ack -> XDEL stream entry
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

fn chrono_now() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({"ok": true, "service": "devstroop-push", "version": "0.1.0"}))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let client = redis::Client::open(redis_url)?;
    let conn = client.get_connection_manager().await?;

    let grpc_addr: SocketAddr = "0.0.0.0:50051".parse()?;
    let http_addr: SocketAddr = "0.0.0.0:8080".parse()?;

    let svc = PushSvc { redis: Arc::new(conn) };

    let grpc = Server::builder()
        .add_service(PushServiceServer::new(svc))
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
