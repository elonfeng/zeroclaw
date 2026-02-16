use super::traits::{Channel, ChannelMessage};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

/// Default WebSocket reconnect interval in seconds.
const DEFAULT_RECONNECT_INTERVAL: u64 = 30;

/// Ring buffer capacity for message deduplication.
const DEDUP_RING_SIZE: usize = 1024;

/// WebSocket write half type alias to reduce complexity.
type WsSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Message,
>;

/// OneBot V11 channel — connects to any OneBotV11-compliant WebSocket server
/// (e.g., go-cqhttp, NapCat, Lagrange) for QQ messaging.
pub struct OneBotChannel {
    ws_url: String,
    access_token: Option<String>,
    reconnect_interval: u64,
    group_trigger_prefix: Vec<String>,
    allowed_users: Vec<String>,
    /// Shared WebSocket writer for sending messages.
    ws_writer: Arc<Mutex<Option<WsSink>>>,
    /// Monotonic echo counter for request tracking.
    echo_counter: Arc<AtomicU64>,
}

/// Fixed-size ring buffer for O(1) message deduplication.
struct DedupRing {
    set: HashSet<String>,
    ring: Vec<String>,
    idx: usize,
}

impl DedupRing {
    fn new() -> Self {
        Self {
            set: HashSet::with_capacity(DEDUP_RING_SIZE),
            ring: vec![String::new(); DEDUP_RING_SIZE],
            idx: 0,
        }
    }

    /// Returns true if the ID was already seen.
    fn is_duplicate(&mut self, id: &str) -> bool {
        if id.is_empty() {
            return false;
        }

        if self.set.contains(id) {
            return true;
        }

        // Evict the oldest entry from the ring slot
        if !self.ring[self.idx].is_empty() {
            self.set.remove(&self.ring[self.idx]);
        }

        self.ring[self.idx] = id.to_string();
        self.set.insert(id.to_string());
        self.idx = (self.idx + 1) % DEDUP_RING_SIZE;
        false
    }
}

impl OneBotChannel {
    pub fn new(
        ws_url: String,
        access_token: Option<String>,
        reconnect_interval: u64,
        group_trigger_prefix: Vec<String>,
        allowed_users: Vec<String>,
    ) -> Self {
        Self {
            ws_url,
            access_token,
            reconnect_interval: if reconnect_interval == 0 {
                DEFAULT_RECONNECT_INTERVAL
            } else {
                reconnect_interval
            },
            group_trigger_prefix,
            allowed_users,
            ws_writer: Arc::new(Mutex::new(None)),
            echo_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    fn is_user_allowed(&self, user_id: &str) -> bool {
        self.allowed_users.iter().any(|u| u == "*" || u == user_id)
    }

    /// Check if a group message should trigger the bot.
    /// Returns true if the message starts with a trigger prefix or mentions the bot.
    fn should_trigger_group(&self, content: &str, is_bot_mentioned: bool) -> bool {
        if is_bot_mentioned {
            return true;
        }

        self.group_trigger_prefix
            .iter()
            .any(|prefix| content.starts_with(prefix.as_str()))
    }

    /// Strip the trigger prefix from the message content.
    fn strip_trigger_prefix<'a>(&self, content: &'a str) -> &'a str {
        for prefix in &self.group_trigger_prefix {
            if let Some(stripped) = content.strip_prefix(prefix.as_str()) {
                return stripped.trim_start();
            }
        }
        content
    }

    /// Parse a user_id from flexible JSON (can be number or string).
    fn parse_user_id(value: &serde_json::Value) -> Option<String> {
        if let Some(n) = value.as_i64() {
            Some(n.to_string())
        } else if let Some(n) = value.as_u64() {
            Some(n.to_string())
        } else {
            value.as_str().map(|s| s.to_string())
        }
    }

    /// Extract text content from OneBotV11 message format.
    /// Handles both string format and CQ code array format.
    fn extract_content(message: &serde_json::Value, self_id: &str) -> (String, bool) {
        let mut is_bot_mentioned = false;

        // Try string format first (raw_message or message as string)
        if let Some(raw) = message
            .get("raw_message")
            .or_else(|| message.get("message"))
            .and_then(|m| m.as_str())
        {
            let (text, mentioned) = Self::strip_cq_at(raw, self_id);
            return (text, mentioned);
        }

        // Try array format (message as array of segments)
        if let Some(segments) = message.get("message").and_then(|m| m.as_array()) {
            let mut text_parts = Vec::new();

            for seg in segments {
                let seg_type = seg.get("type").and_then(|t| t.as_str()).unwrap_or("");
                let data = seg.get("data");

                match seg_type {
                    "text" => {
                        if let Some(text) =
                            data.and_then(|d| d.get("text")).and_then(|t| t.as_str())
                        {
                            text_parts.push(text.to_string());
                        }
                    }
                    "at" => {
                        if let Some(qq) = data.and_then(|d| d.get("qq")).and_then(|q| q.as_str()) {
                            if qq == self_id || qq == "all" {
                                is_bot_mentioned = true;
                            }
                        }
                    }
                    _ => {}
                }
            }

            return (text_parts.join("").trim().to_string(), is_bot_mentioned);
        }

        (String::new(), false)
    }

    /// Strip CQ:at codes from a string message and detect bot mentions.
    fn strip_cq_at(raw: &str, self_id: &str) -> (String, bool) {
        let mut is_mentioned = false;
        let mut result = raw.to_string();

        // Match [CQ:at,qq=xxx] patterns
        let at_self = format!("[CQ:at,qq={self_id}]");
        if result.contains(&at_self) {
            is_mentioned = true;
            result = result.replace(&at_self, "");
        }

        // Also handle [CQ:at,qq=all]
        if result.contains("[CQ:at,qq=all]") {
            is_mentioned = true;
            result = result.replace("[CQ:at,qq=all]", "");
        }

        (result.trim().to_string(), is_mentioned)
    }
}

#[async_trait]
impl Channel for OneBotChannel {
    fn name(&self) -> &str {
        "onebot"
    }

    async fn send(&self, message: &str, recipient: &str) -> anyhow::Result<()> {
        let mut guard = self.ws_writer.lock().await;
        let writer = guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("OneBot: WebSocket not connected"))?;

        let echo = format!("send_{}", self.echo_counter.fetch_add(1, Ordering::Relaxed));

        // Parse recipient format: "private:{user_id}" or "group:{group_id}"
        let action = if let Some(group_id) = recipient.strip_prefix("group:") {
            let group_id: i64 = group_id
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid group ID: {group_id}"))?;
            json!({
                "action": "send_group_msg",
                "params": {
                    "group_id": group_id,
                    "message": message,
                },
                "echo": echo,
            })
        } else {
            let user_id_str = recipient.strip_prefix("private:").unwrap_or(recipient);
            let user_id: i64 = user_id_str
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid user ID: {user_id_str}"))?;
            json!({
                "action": "send_private_msg",
                "params": {
                    "user_id": user_id,
                    "message": message,
                },
                "echo": echo,
            })
        };

        writer
            .send(Message::Text(action.to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("OneBot: failed to send message: {e}"))?;

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn listen(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        tracing::info!("OneBot: connecting to {}", self.ws_url);

        // Build WebSocket request with optional auth header
        let mut request =
            tokio_tungstenite::tungstenite::http::Request::builder().uri(&self.ws_url);

        if let Some(ref token) = self.access_token {
            request = request.header("Authorization", format!("Bearer {token}"));
        }

        let request = request
            .body(())
            .map_err(|e| anyhow::anyhow!("Failed to build WebSocket request: {e}"))?;

        let (ws_stream, _) = tokio_tungstenite::connect_async(request).await?;
        let (write, mut read) = ws_stream.split();

        // Store the writer for send()
        {
            let mut guard = self.ws_writer.lock().await;
            *guard = Some(write);
        }

        tracing::info!("OneBot: connected, listening for messages...");

        let mut dedup = DedupRing::new();

        // Detect bot's self_id from the first lifecycle event
        let mut self_id = String::new();

        while let Some(msg) = read.next().await {
            let msg = match msg {
                Ok(Message::Text(t)) => t,
                Ok(Message::Close(_)) => break,
                Err(e) => {
                    tracing::warn!("OneBot WebSocket error: {e}");
                    break;
                }
                _ => continue,
            };

            let event: serde_json::Value = match serde_json::from_str(&msg) {
                Ok(v) => v,
                Err(_) => continue,
            };

            // Skip API responses (they have an "echo" field)
            if event.get("echo").is_some() {
                continue;
            }

            let post_type = event
                .get("post_type")
                .and_then(|p| p.as_str())
                .unwrap_or("");

            match post_type {
                "meta_event" => {
                    // Extract self_id from lifecycle or heartbeat events
                    if self_id.is_empty() {
                        if let Some(id) = event.get("self_id").and_then(Self::parse_user_id) {
                            self_id = id;
                            tracing::debug!("OneBot: detected self_id = {}", self_id);
                        }
                    }
                    continue;
                }
                "message" => {}
                _ => continue,
            }

            // Extract self_id if not yet known
            if self_id.is_empty() {
                if let Some(id) = event.get("self_id").and_then(Self::parse_user_id) {
                    self_id = id;
                }
            }

            let message_type = event
                .get("message_type")
                .and_then(|m| m.as_str())
                .unwrap_or("");

            let message_id = event
                .get("message_id")
                .map(|m| m.to_string())
                .unwrap_or_default();

            // Deduplication
            if dedup.is_duplicate(&message_id) {
                continue;
            }

            // Extract user_id (handles both string and number formats)
            let user_id = event
                .get("user_id")
                .and_then(Self::parse_user_id)
                .or_else(|| {
                    event
                        .get("sender")
                        .and_then(|s| s.get("user_id"))
                        .and_then(Self::parse_user_id)
                })
                .unwrap_or_default();

            if !self.is_user_allowed(&user_id) {
                tracing::warn!("OneBot: ignoring message from unauthorized user: {user_id}");
                continue;
            }

            let (content, is_bot_mentioned) = Self::extract_content(&event, &self_id);

            if content.is_empty() {
                continue;
            }

            match message_type {
                "private" => {
                    let chat_id = format!("private:{user_id}");

                    let channel_msg = ChannelMessage {
                        id: Uuid::new_v4().to_string(),
                        sender: chat_id,
                        content,
                        channel: "onebot".to_string(),
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                    };

                    if tx.send(channel_msg).await.is_err() {
                        tracing::warn!("OneBot: message channel closed");
                        break;
                    }
                }
                "group" => {
                    let group_id = event
                        .get("group_id")
                        .and_then(Self::parse_user_id)
                        .unwrap_or_default();

                    // Check group trigger conditions
                    if (!self.group_trigger_prefix.is_empty() || !is_bot_mentioned)
                        && !self.should_trigger_group(&content, is_bot_mentioned)
                    {
                        continue;
                    }

                    let final_content = self.strip_trigger_prefix(&content).to_string();
                    if final_content.is_empty() {
                        continue;
                    }

                    let chat_id = format!("group:{group_id}");

                    let channel_msg = ChannelMessage {
                        id: Uuid::new_v4().to_string(),
                        sender: chat_id,
                        content: final_content,
                        channel: "onebot".to_string(),
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                    };

                    if tx.send(channel_msg).await.is_err() {
                        tracing::warn!("OneBot: message channel closed");
                        break;
                    }
                }
                _ => {}
            }
        }

        // Clean up writer on disconnect
        {
            let mut guard = self.ws_writer.lock().await;
            *guard = None;
        }

        anyhow::bail!("OneBot WebSocket connection closed")
    }

    async fn health_check(&self) -> bool {
        // Try to establish a WebSocket connection to verify the server is reachable
        let mut request =
            tokio_tungstenite::tungstenite::http::Request::builder().uri(&self.ws_url);

        if let Some(ref token) = self.access_token {
            request = request.header("Authorization", format!("Bearer {token}"));
        }

        let Ok(request) = request.body(()) else {
            return false;
        };

        match tokio_tungstenite::connect_async(request).await {
            Ok((ws, _)) => {
                let (mut write, _) = ws.split();
                let _ = write.close().await;
                true
            }
            Err(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name() {
        let ch = OneBotChannel::new("ws://localhost:8080".into(), None, 30, vec![], vec![]);
        assert_eq!(ch.name(), "onebot");
    }

    #[test]
    fn test_user_allowed_wildcard() {
        let ch = OneBotChannel::new(
            "ws://localhost:8080".into(),
            None,
            30,
            vec![],
            vec!["*".into()],
        );
        assert!(ch.is_user_allowed("anyone"));
    }

    #[test]
    fn test_user_allowed_specific() {
        let ch = OneBotChannel::new(
            "ws://localhost:8080".into(),
            None,
            30,
            vec![],
            vec!["123456".into()],
        );
        assert!(ch.is_user_allowed("123456"));
        assert!(!ch.is_user_allowed("789"));
    }

    #[test]
    fn test_user_denied_empty() {
        let ch = OneBotChannel::new("ws://localhost:8080".into(), None, 30, vec![], vec![]);
        assert!(!ch.is_user_allowed("anyone"));
    }

    #[test]
    fn test_dedup_ring_basic() {
        let mut ring = DedupRing::new();
        assert!(!ring.is_duplicate("msg1"));
        assert!(ring.is_duplicate("msg1"));
        assert!(!ring.is_duplicate("msg2"));
        assert!(ring.is_duplicate("msg2"));
    }

    #[test]
    fn test_dedup_ring_empty_id() {
        let mut ring = DedupRing::new();
        assert!(!ring.is_duplicate(""));
        assert!(!ring.is_duplicate(""));
    }

    #[test]
    fn test_dedup_ring_eviction() {
        let mut ring = DedupRing::new();
        // Fill the ring completely
        for i in 0..DEDUP_RING_SIZE {
            assert!(!ring.is_duplicate(&format!("msg_{i}")));
        }
        // First entry should still be there
        assert!(ring.is_duplicate("msg_0"));

        // Adding one more should evict msg_0
        assert!(!ring.is_duplicate("overflow"));
        assert!(!ring.is_duplicate("msg_0")); // msg_0 was evicted
    }

    #[test]
    fn test_strip_cq_at() {
        let (text, mentioned) = OneBotChannel::strip_cq_at("[CQ:at,qq=12345]hello world", "12345");
        assert_eq!(text, "hello world");
        assert!(mentioned);
    }

    #[test]
    fn test_strip_cq_at_not_mentioned() {
        let (text, mentioned) = OneBotChannel::strip_cq_at("hello world", "12345");
        assert_eq!(text, "hello world");
        assert!(!mentioned);
    }

    #[test]
    fn test_strip_cq_at_other_user() {
        let (text, mentioned) = OneBotChannel::strip_cq_at("[CQ:at,qq=99999]hello", "12345");
        assert_eq!(text, "[CQ:at,qq=99999]hello");
        assert!(!mentioned);
    }

    #[test]
    fn test_extract_content_string_format() {
        let event = serde_json::json!({
            "raw_message": "[CQ:at,qq=111]hello world",
        });
        let (content, mentioned) = OneBotChannel::extract_content(&event, "111");
        assert_eq!(content, "hello world");
        assert!(mentioned);
    }

    #[test]
    fn test_extract_content_array_format() {
        let event = serde_json::json!({
            "message": [
                {"type": "at", "data": {"qq": "111"}},
                {"type": "text", "data": {"text": " hello world"}},
            ],
        });
        let (content, mentioned) = OneBotChannel::extract_content(&event, "111");
        assert_eq!(content, "hello world");
        assert!(mentioned);
    }

    #[test]
    fn test_group_trigger_prefix() {
        let ch = OneBotChannel::new(
            "ws://localhost:8080".into(),
            None,
            30,
            vec!["/".into(), "!".into()],
            vec![],
        );
        assert!(ch.should_trigger_group("/help", false));
        assert!(ch.should_trigger_group("!info", false));
        assert!(!ch.should_trigger_group("hello", false));
        assert!(ch.should_trigger_group("hello", true)); // bot mentioned
    }

    #[test]
    fn test_strip_trigger_prefix() {
        let ch = OneBotChannel::new(
            "ws://localhost:8080".into(),
            None,
            30,
            vec!["/".into(), "!".into()],
            vec![],
        );
        assert_eq!(ch.strip_trigger_prefix("/help"), "help");
        assert_eq!(ch.strip_trigger_prefix("!info"), "info");
        assert_eq!(ch.strip_trigger_prefix("hello"), "hello");
    }

    #[test]
    fn test_parse_user_id_number() {
        let v = serde_json::json!(12345);
        assert_eq!(OneBotChannel::parse_user_id(&v), Some("12345".into()));
    }

    #[test]
    fn test_parse_user_id_string() {
        let v = serde_json::json!("12345");
        assert_eq!(OneBotChannel::parse_user_id(&v), Some("12345".into()));
    }

    #[test]
    fn test_config_serde() {
        let toml_str = r#"
ws_url = "ws://127.0.0.1:3001"
access_token = "my_token"
reconnect_interval = 10
group_trigger_prefix = ["/", "!"]
allowed_users = ["123456", "*"]
"#;
        let config: crate::config::schema::OneBotConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.ws_url, "ws://127.0.0.1:3001");
        assert_eq!(config.access_token, Some("my_token".into()));
        assert_eq!(config.reconnect_interval, 10);
        assert_eq!(config.group_trigger_prefix, vec!["/", "!"]);
    }

    #[test]
    fn test_config_serde_defaults() {
        let toml_str = r#"
ws_url = "ws://localhost:8080"
"#;
        let config: crate::config::schema::OneBotConfig = toml::from_str(toml_str).unwrap();
        assert!(config.access_token.is_none());
        assert_eq!(config.reconnect_interval, 30);
        assert!(config.group_trigger_prefix.is_empty());
        assert!(config.allowed_users.is_empty());
    }
}
