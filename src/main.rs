use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Router, Json,
    routing::{get, post},
    http::{HeaderMap, StatusCode},
};
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpListener;
use tonic::{transport::Server, Request, Response, Status, Streaming};
use futures::StreamExt;
use futures::TryStreamExt;

pub mod push {
    tonic::include_proto!("devstroop.push.v1");
}

use push::{
    push_service_server::{PushService, PushServiceServer},
    ClientMessage, ServerMessage, Pong,
};

#[derive(Clone)]
struct PushSvc {
    redis_url: String,
}

#[derive(Deserialize)]
struct PushBody {
    user_id: String,
    title: String,
    body: String,
    #[serde(default)]
    data: String,
}

#[derive(Serialize)]
struct PushResponse {
    ok: bool,
    id: String,
}

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

fn api_key() -> String {
    std::env::var("PUSH_API_KEY").unwrap_or_default()
}

fn stream_key(user_id: &str) -> String {
    format!("N{user_id}0")
}

#[tonic::async_trait]
impl PushService for PushSvc {
    type StreamStream = tokio_stream::wrappers::ReceiverStream<Result<ServerMessage, Status>>;

    async fn stream(
        &self,
        request: Request<Streaming<ClientMessage>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let mut inbound = request.into_inner();
        let redis_url = self.redis_url.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(64);

        tokio::spawn(async move {
            // Auth: first message must carry token.
            let token = match inbound.next().await {
                Some(Ok(m)) if !m.token.is_empty() => m.token,
                _ => {
                    let _ = tx.send(Err(Status::unauthenticated("missing token"))).await;
                    return;
                }
            };

            let mut conn = match redis::Client::open(redis_url.as_str()) {
                Ok(c) => match c.get_async_connection().await {
                    Ok(c) => c,
                    Err(_) => {
                        let _ = tx.send(Err(Status::internal("redis connect failed"))).await;
                        return;
                    }
                },
                Err(_) => {
                    let _ = tx.send(Err(Status::internal("redis url invalid"))).await;
                    return;
                }
            };

            let session: Option<String> = redis::cmd("GET")
                .arg(format!("NS:{token}"))
                .query_async(&mut conn)
                .await
                .unwrap_or(None);

            let Some(session) = session else {
                let _ = tx.send(Err(Status::unauthenticated("invalid or expired session"))).await;
                return;
            };

            // session is "{tenant}:{userId}" — use userId for stream key.
            let user_id = session.split(':').nth(1).unwrap_or(&session).to_string();
            let key = stream_key(&user_id);

            tracing::info!("client authenticated: {session}, stream={key}");

            let _ = tx.send(Ok(ServerMessage {
                payload: Some(push::server_message::Payload::Notification(push::Notification {
                    id: "welcome".to_string(),
                    title: "Connected".to_string(),
                    body: format!("Session {session}"),
                    data: String::new(),
                    created_at: chrono_now(),
                })),
            })).await;

            // Delivery loop: blocking XREAD for new entries.
            let mut last_id = "$".to_string();
            loop {
                // Check for client messages (ping/ack) without blocking forever.
                // We use a short timeout read.
                let mut read_conn = match redis::Client::open(redis_url.as_str()) {
                    Ok(c) => match c.get_async_connection().await {
                        Ok(c) => c,
                        Err(_) => break,
                    },
                    Err(_) => break,
                };

                let result: redis::RedisResult<redis::Value> = redis::cmd("XREAD")
                    .arg("BLOCK").arg(1000)
                    .arg("COUNT").arg(10)
                    .arg("STREAMS").arg(&key).arg(&last_id)
                    .query_async(&mut read_conn)
                    .await;

                if let Ok(redis::Value::Array(streams)) = result {
                    for stream in streams {
                        if let redis::Value::Array(entries) = stream {
                            // entries: [stream_name, [ [id, [field, value, ...]], ... ]]
                            if entries.len() >= 2 {
                                if let redis::Value::Array(msgs) = &entries[1] {
                                    for msg in msgs {
                                        if let redis::Value::Array(fields) = msg {
                                            if fields.is_empty() { continue; }
                                            let id = match &fields[0] {
                                                redis::Value::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                                                _ => continue,
                                            };
                                            // parse fields
                                            let mut title = String::new();
                                            let mut body = String::new();
                                            let mut data = String::new();
                                            if fields.len() > 1 {
                                                if let redis::Value::Array(kv) = &fields[1] {
                                                    let mut i = 0;
                                                    while i + 1 < kv.len() {
                                                        let k = match &kv[i] { redis::Value::BulkString(b) => String::from_utf8_lossy(b).to_string(), _ => String::new() };
                                                        let v = match &kv[i+1] { redis::Value::BulkString(b) => String::from_utf8_lossy(b).to_string(), _ => String::new() };
                                                        match k.as_str() {
                                                            "title" => title = v,
                                                            "body" => body = v,
                                                            "data" => data = v,
                                                            _ => {}
                                                        }
                                                        i += 2;
                                                    }
                                                }
                                            }
                                            let _ = tx.send(Ok(ServerMessage {
                                                payload: Some(push::server_message::Payload::Notification(push::Notification {
                                                    id: id.clone(),
                                                    title,
                                                    body,
                                                    data,
                                                    created_at: chrono_now(),
                                                })),
                                            })).await;
                                            last_id = id;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Drain inbound pings/acks (non-blocking).
                while let Ok(Some(res)) = inbound.try_next().await {
                    let msg = res;
                    if let Some(push::client_message::Payload::Ping(_)) = msg.payload {
                        let _ = tx.send(Ok(ServerMessage {
                            payload: Some(push::server_message::Payload::Pong(Pong {})),
                        })).await;
                    }
                    // TODO: ack -> XDEL
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

async fn publish(
    headers: HeaderMap,
    Json(body): Json<PushBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    // API key check.
    let key = headers.get("x-api-key").and_then(|v| v.to_str().ok()).unwrap_or("");
    if !api_key().is_empty() && key != api_key() {
        return (StatusCode::UNAUTHORIZED, Json(json!({"ok": false, "error": "unauthorized"})));
    }

    let client = match redis::Client::open(redis_url().as_str()) {
        Ok(c) => c,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"ok": false, "error": "redis"}))),
    };
    let mut conn = match client.get_async_connection().await {
        Ok(c) => c,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"ok": false, "error": "redis"}))),
    };

    let key_name = stream_key(&body.user_id);
    let id: String = redis::cmd("XADD")
        .arg(&key_name)
        .arg("*")
        .arg("title").arg(&body.title)
        .arg("body").arg(&body.body)
        .arg("data").arg(&body.data)
        .query_async(&mut conn)
        .await
        .unwrap_or_default();

    (StatusCode::CREATED, Json(json!({"ok": true, "id": id})))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let grpc_addr: SocketAddr = "0.0.0.0:50051".parse()?;
    let http_addr: SocketAddr = "0.0.0.0:8080".parse()?;

    let svc = PushSvc { redis_url: redis_url() };

    let grpc = Server::builder()
        .add_service(PushServiceServer::new(svc))
        .serve(grpc_addr);

    let app = Router::new()
        .route("/health", get(health))
        .route("/push", post(publish));
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
