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
use tokio_rustls::TlsAcceptor;
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use rustls::ServerConfig;
use std::fs::File;
use std::io::BufReader;
use tokio_tungstenite::MaybeTlsStream;

/// WebSocket server configuration
#[derive(Debug, Clone)]
pub struct WebSocketConfig {
    /// Host to bind to (default: "127.0.0.1")
    pub host: String,
    /// Port to listen on (default: 7777)
    pub port: u16,
    /// Path to TLS certificate file (optional, enables WSS if provided)
    pub tls_cert_path: Option<String>,
    /// Path to TLS private key file (optional, enables WSS if provided)
    pub tls_key_path: Option<String>,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 7777,
            tls_cert_path: None,
            tls_key_path: None,
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
            .unwrap_or(7777);
        let tls_cert_path = std::env::var("WS_TLS_CERT").ok();
        let tls_key_path = std::env::var("WS_TLS_KEY").ok();
        
        Self { host, port, tls_cert_path, tls_key_path }
    }
    
    /// Get the full address string
    pub fn address(&self) -> String {
        format!("{}:{}/acp", self.host, self.port)
    }
    
    /// Check if TLS is enabled
    pub fn is_tls_enabled(&self) -> bool {
        self.tls_cert_path.is_some() && self.tls_key_path.is_some()
    }
    
    /// Get the protocol prefix (ws:// or wss://)
    pub fn protocol(&self) -> &'static str {
        if self.is_tls_enabled() {
            "wss://"
        } else {
            "ws://"
        }
    }
}

/// Run WebSocket server for ACP protocol
pub async fn run_websocket_server(state: Arc<AppState>, ws_config: WebSocketConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = ws_config.address();
    let listener = TcpListener::bind(&addr).await?;
    
    info!(
        host = %ws_config.host,
        port = %ws_config.port,
        protocol = %ws_config.protocol(),
        "WebSocket server listening on {}{}",
        ws_config.protocol(),
        addr
    );
    
    // Load TLS certificate and key if provided
    let tls_acceptor = if let (Some(cert_path), Some(key_path)) = (&ws_config.tls_cert_path, &ws_config.tls_key_path) {
        let cert_file = File::open(cert_path)?;
        let key_file = File::open(key_path)?;
        
        let mut cert_reader = BufReader::new(cert_file);
        let mut key_reader = BufReader::new(key_file);
        
        let certs_vec = certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()?;
        
        // Try to load PKCS8 key first, then RSA key
        let key = pkcs8_private_keys(&mut key_reader)
            .filter_map(|r| r.ok())
            .next()
            .or_else(|| {
                let mut key_reader2 = BufReader::new(File::open(key_path).ok()?);
                rsa_private_keys(&mut key_reader2)
                    .filter_map(|r| r.ok())
                    .next()
            })
            .ok_or("No private key found")?;
        
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs_vec, rustls::pki_types::PrivateKeyDer::Pkcs8(key))?;
        
        Some(TlsAcceptor::from(Arc::new(config)))
    } else {
        None
    };
    
    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                debug!(%peer_addr, "New TCP connection accepted");
                let state_clone = Arc::clone(&state);
                let tls_acceptor_clone = tls_acceptor.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_websocket_connection(stream, state_clone, tls_acceptor_clone).await {
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
    tls_acceptor: Option<TlsAcceptor>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Upgrade to TLS if acceptor is provided
    let tcp_stream = if let Some(acceptor) = &tls_acceptor {
        let tls_stream = acceptor.accept(stream).await?;
        info!("TLS handshake completed");
        MaybeTlsStream::Rustls(tls_stream)
    } else {
        MaybeTlsStream::Plain(stream)
    };
    
    let ws_stream = tokio_tungstenite::server::handshake(tcp_stream).await?;
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
