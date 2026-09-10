//! WebSocket transport for ACP protocol.
//! 
//! Provides WebSocket server functionality for acp-bridge,
//! allowing remote clients to connect via WebSocket instead of stdin/stdout.

use crate::engine::{self, AppState, Notification};
use crate::protocol::{AcpError, JsonRpcRequest};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info};
use tokio_tungstenite::tungstenite::Message;

/// WebSocket server configuration
#[derive(Debug, Clone)]
pub struct WebSocketConfig {
    /// Host to bind to (default: "127.0.0.1")
    pub host: String,
    /// Port to listen on (default: 8765)
    pub port: u16,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8765,
        }
    }
}

impl WebSocketConfig {
    /// Create config from environment variables
    pub fn from_env() -> Self {
        let host = std::env::var("WS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port = std::env::var("WS_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8765);
        
        Self { host, port }
    }
    
    /// Get the full address string
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Run WebSocket server for ACP protocol
pub async fn run_websocket_server(state: Arc<AppState>, ws_config: WebSocketConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = ws_config.address();
    let listener = TcpListener::bind(&addr).await?;
    
    info!(
        host = %ws_config.host,
        port = %ws_config.port,
        "WebSocket server listening on ws://{}",
        addr
    );
    
    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                debug!(%peer_addr, "New TCP connection accepted");
                let state_clone = Arc::clone(&state);
                tokio::spawn(async move {
                    if let Err(e) = handle_websocket_connection(stream, state_clone).await {
                        error!(error = %e, "WebSocket connection error");
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "Failed to accept connection");
            }
        }
    }
}

/// Handle a single WebSocket connection
async fn handle_websocket_connection(
    stream: TcpStream,
    state: Arc<AppState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ws_stream = tokio_tungstenite::accept_async(stream).await?;
    let (mut write, mut read) = ws_stream.split();
    
    info!("WebSocket connection established");
    
    // Set up notification channel for this connection
    let (notify_tx, mut notify_rx) = mpsc::unbounded_channel::<Notification>();
    
    // Spawn task to forward notifications to WebSocket client
    let write_handle = tokio::spawn(async move {
        while let Some(notification) = notify_rx.recv().await {
            let rpc_msg = match notification {
                Notification::Thinking => {
                    json!({
                        "jsonrpc": "2.0",
                        "method": "session/thinking",
                        "params": {}
                    })
                }
                Notification::ToolStart(tool_name) => {
                    json!({
                        "jsonrpc": "2.0",
                        "method": "session/toolStart",
                        "params": {
                            "tool": tool_name
                        }
                    })
                }
                Notification::ToolDone(tool_name, output) => {
                    json!({
                        "jsonrpc": "2.0",
                        "method": "session/toolDone",
                        "params": {
                            "tool": tool_name,
                            "output": output
                        }
                    })
                }
                Notification::TextChunk(text) => {
                    json!({
                        "jsonrpc": "2.0",
                        "method": "session/textChunk",
                        "params": {
                            "text": text
                        }
                    })
                }
            };
            
            let msg_str = rpc_msg.to_string();
            if let Err(e) = write.send(Message::Text(msg_str.into())).await {
                error!(error = %e, "Failed to send notification");
                break;
            }
        }
    });
    
    // Read and process incoming messages
    while let Some(msg_result) = read.next().await {
        match msg_result {
            Ok(msg) => match msg {
                Message::Text(text) => {
                    let trimmed = text.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    
                    let request: JsonRpcRequest = match serde_json::from_str(trimmed) {
                        Ok(r) => r,
                        Err(e) => {
                            debug!(error = %e, "Skipping invalid JSON-RPC message");
                            continue;
                        }
                    };
                    
                    let id_opt = request.id;
                    let method = request.method.as_str();
                    let params = request.params.clone().unwrap_or(json!({}));
                    
                    debug!(?id_opt, method, "Received WebSocket message");
                    
                    // Handle notifications (no id)
                    if id_opt.is_none() {
                        match method {
                            "session/cancel" => {
                                let sid = params
                                    .get("sessionId")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                info!(
                                    session_id = %sid,
                                    "Received session/cancel notification"
                                );
                            }
                            _ => {
                                debug!(method, "Ignoring unknown notification");
                            }
                        }
                        continue;
                    }
                    
                    let id = id_opt.unwrap();
                    
                    // Process request and send response
                    let response = process_request(id, method, &params, &state, notify_tx.clone()).await;
                    let response_str = serde_json::to_string(&response).unwrap_or_else(|e| {
                        error!(error = %e, "Failed to serialize response");
                        "{\"error\":\"internal error\"}".to_string()
                    });
                    
                    if let Err(e) = write.send(Message::Text(response_str.into())).await {
                        error!(error = %e, "Failed to send response");
                        break;
                    }
                }
                Message::Close(_) => {
                    info!("WebSocket connection closed by client");
                    break;
                }
                Message::Ping(data) => {
                    if let Err(e) = write.send(Message::Pong(data)).await {
                        error!(error = %e, "Failed to send pong");
                        break;
                    }
                }
                Message::Binary(_) | Message::Frame(_) | Message::Pong(_) => {
                    debug!("Ignoring non-text message");
                }
            },
            Err(e) => {
                error!(error = %e, "WebSocket read error");
                break;
            }
        }
    }
    
    // Cancel the notification sender task
    write_handle.abort();
    info!("WebSocket connection handler finished");
    
    Ok(())
}

/// Process a JSON-RPC request (same logic as stdin/stdout but adapted for WebSocket)
async fn process_request(
    id: u64,
    method: &str,
    params: &Value,
    state: &Arc<AppState>,
    notify_tx: mpsc::UnboundedSender<Notification>,
) -> Value {
    match method {
        "initialize" => {
            let result = engine::initialize(&state.config);
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": result
            })
        }
        "session/new" => {
            let raw_cwd = params.get("cwd").and_then(|v| v.as_str()).unwrap_or(".");
            if let Some(servers) = params.get("mcpServers").and_then(|v| v.as_array()) {
                if !servers.is_empty() {
                    debug!(count = servers.len(), "Ignoring mcpServers param");
                }
            }
            match engine::session_new(state, raw_cwd) {
                Ok(session_id) => {
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {"sessionId": session_id}
                    })
                }
                Err(e) => {
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {"code": e.code(), "message": e.to_string()}
                    })
                }
            }
        }
        "session/prompt" => {
            handle_websocket_prompt(id, params, state, notify_tx).await
        }
        "session/end" => {
            let session_id = params.get("sessionId").and_then(|v| v.as_str()).unwrap_or("");
            if session_id.is_empty() {
                let err = AcpError::MissingParam { field: "sessionId".into() };
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {"code": err.code(), "message": err.to_string()}
                })
            } else {
                match engine::session_end(state, session_id) {
                    Ok(()) => {
                        json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": {"status": "ended"}
                        })
                    }
                    Err(e) => {
                        json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "error": {"code": e.code(), "message": e.to_string()}
                        })
                    }
                }
            }
        }
        "session/load" | "session/resume" => {
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32601,
                    "message": "session/load and session/resume are not supported by acp-bridge"
                }
            })
        }
        "session/set_mode" => {
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32601,
                    "message": "session/set_mode is not supported by acp-bridge"
                }
            })
        }
        _ => {
            let err = AcpError::MethodNotFound { method: method.to_string() };
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {"code": err.code(), "message": err.to_string()}
            })
        }
    }
}

/// Handle session/prompt for WebSocket (similar to ACP but with WebSocket-specific handling)
async fn handle_websocket_prompt(
    id: u64,
    params: &Value,
    state: &Arc<AppState>,
    notify_tx: mpsc::UnboundedSender<Notification>,
) -> Value {
    let session_id = match params.get("sessionId").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => {
            let err = AcpError::MissingParam {
                field: "sessionId".into(),
            };
            return json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {"code": err.code(), "message": err.to_string()}
            });
        }
    };
    
    let prompt_value = params.get("prompt").cloned().unwrap_or(Value::Null);
    let raw_user_text = engine::extract_user_text_from_prompt(&prompt_value);
    let (user_text, _sender_context) = engine::strip_sender_context(&raw_user_text);
    let user_images = engine::extract_user_images_from_prompt(&prompt_value);
    
    if user_text.trim().is_empty() && user_images.is_empty() {
        let err = AcpError::MissingParam {
            field: "prompt (expected non-empty text or image content)".into(),
        };
        return json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": err.code(), "message": err.to_string()}
        });
    }
    
    // Spawn the engine prompt in a task
    let state_clone = Arc::clone(state);
    let sid = session_id.clone();
    let handle = tokio::spawn(async move {
        engine::session_prompt(
            &state_clone,
            &sid,
            &user_text,
            &user_images,
            Some(notify_tx),
        )
        .await
    });
    
    // Wait for completion
    let result = handle.await;
    
    match result {
        Ok(final_output) => {
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": final_output
            })
        }
        Err(e) => {
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {"code": -32000, "message": format!("Task failed: {}", e)}
            })
        }
    }
}
