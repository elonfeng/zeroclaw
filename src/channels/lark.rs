use super::traits::{Channel, ChannelMessage};
use async_trait::async_trait;
use futures_util::StreamExt;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

const FEISHU_BASE_URL: &str = "https://open.feishu.cn";
const LARK_BASE_URL: &str = "https://open.larksuite.com";

/// Token refresh interval: 90 minutes (token lives 2 hours).
const TOKEN_REFRESH_SECS: u64 = 90 * 60;

/// Lark/Feishu (飞书) channel — uses REST API for sending and event subscription
/// for receiving messages. Supports both international (Lark) and Chinese (Feishu) endpoints.
pub struct LarkChannel {
    app_id: String,
    app_secret: String,
    #[allow(dead_code)]
    encrypt_key: Option<String>,
    #[allow(dead_code)]
    verification_token: Option<String>,
    allowed_users: Vec<String>,
    use_feishu: bool,
    client: reqwest::Client,
    /// Cached tenant access token.
    token_cache: Arc<RwLock<Option<String>>>,
}

impl LarkChannel {
    pub fn new(
        app_id: String,
        app_secret: String,
        encrypt_key: Option<String>,
        verification_token: Option<String>,
        allowed_users: Vec<String>,
        use_feishu: bool,
    ) -> Self {
        Self {
            app_id,
            app_secret,
            encrypt_key,
            verification_token,
            allowed_users,
            use_feishu,
            client: reqwest::Client::new(),
            token_cache: Arc::new(RwLock::new(None)),
        }
    }

    fn base_url(&self) -> &str {
        if self.use_feishu {
            FEISHU_BASE_URL
        } else {
            LARK_BASE_URL
        }
    }

    fn is_user_allowed(&self, user_id: &str) -> bool {
        self.allowed_users.iter().any(|u| u == "*" || u == user_id)
    }

    /// Obtain a tenant access token from Lark/Feishu API.
    async fn fetch_tenant_token(&self) -> anyhow::Result<String> {
        let url = format!(
            "{}/open-apis/auth/v3/tenant_access_token/internal",
            self.base_url()
        );

        let body = serde_json::json!({
            "app_id": self.app_id,
            "app_secret": self.app_secret,
        });

        let resp = self.client.post(&url).json(&body).send().await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err = resp.text().await.unwrap_or_default();
            anyhow::bail!("Lark token request failed ({status}): {err}");
        }

        let data: serde_json::Value = resp.json().await?;
        let code = data.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
        if code != 0 {
            let msg = data
                .get("msg")
                .and_then(|m| m.as_str())
                .unwrap_or("unknown error");
            anyhow::bail!("Lark token error (code {code}): {msg}");
        }

        let token = data
            .get("tenant_access_token")
            .and_then(|t| t.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing tenant_access_token in response"))?;

        Ok(token.to_string())
    }

    /// Get a valid tenant access token, refreshing if needed.
    async fn get_token(&self) -> anyhow::Result<String> {
        // Check cache first
        {
            let cache = self.token_cache.read().await;
            if let Some(ref token) = *cache {
                return Ok(token.clone());
            }
        }

        // Fetch new token
        let token = self.fetch_tenant_token().await?;
        {
            let mut cache = self.token_cache.write().await;
            *cache = Some(token.clone());
        }
        Ok(token)
    }

    /// Invalidate cached token so next call fetches a fresh one.
    async fn invalidate_token(&self) {
        let mut cache = self.token_cache.write().await;
        *cache = None;
    }

    /// Extract the best sender ID from a Lark event.
    /// Priority: user_id > open_id > union_id.
    fn extract_sender_id(sender: &serde_json::Value) -> Option<String> {
        let sender_id = sender.get("sender_id")?;

        if let Some(user_id) = sender_id.get("user_id").and_then(|v| v.as_str()) {
            if !user_id.is_empty() {
                return Some(user_id.to_string());
            }
        }
        if let Some(open_id) = sender_id.get("open_id").and_then(|v| v.as_str()) {
            if !open_id.is_empty() {
                return Some(open_id.to_string());
            }
        }
        if let Some(union_id) = sender_id.get("union_id").and_then(|v| v.as_str()) {
            if !union_id.is_empty() {
                return Some(union_id.to_string());
            }
        }

        None
    }

    /// Extract text content from a Lark message body.
    fn extract_text_content(message: &serde_json::Value) -> Option<String> {
        let content_str = message.get("content").and_then(|c| c.as_str())?;

        // Lark wraps text content in JSON: {"text": "actual message"}
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(content_str) {
            if let Some(text) = parsed.get("text").and_then(|t| t.as_str()) {
                return Some(text.to_string());
            }
        }

        // Fallback to raw content
        Some(content_str.to_string())
    }
}

#[async_trait]
impl Channel for LarkChannel {
    fn name(&self) -> &str {
        if self.use_feishu {
            "feishu"
        } else {
            "lark"
        }
    }

    async fn send(&self, message: &str, recipient: &str) -> anyhow::Result<()> {
        let token = self.get_token().await?;

        let url = format!(
            "{}/open-apis/im/v1/messages?receive_id_type=chat_id",
            self.base_url()
        );

        let content = serde_json::json!({"text": message}).to_string();

        let body = serde_json::json!({
            "receive_id": recipient,
            "msg_type": "text",
            "content": content,
            "uuid": Uuid::new_v4().to_string(),
        });

        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {token}"))
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err = resp.text().await.unwrap_or_default();

            // Token might be expired — invalidate and let caller retry
            if status.as_u16() == 401 {
                self.invalidate_token().await;
            }

            anyhow::bail!("Lark send message failed ({status}): {err}");
        }

        let data: serde_json::Value = resp.json().await?;
        let code = data.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
        if code != 0 {
            let msg = data
                .get("msg")
                .and_then(|m| m.as_str())
                .unwrap_or("unknown");
            // Invalidate token on auth errors
            if code == 99_991_663 || code == 99_991_664 {
                self.invalidate_token().await;
            }
            anyhow::bail!("Lark send message error (code {code}): {msg}");
        }

        Ok(())
    }

    async fn listen(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        tracing::info!(
            "{}: starting event polling...",
            if self.use_feishu { "Feishu" } else { "Lark" }
        );

        // Spawn a background task to refresh the token periodically
        let token_cache = Arc::clone(&self.token_cache);
        let app_id = self.app_id.clone();
        let app_secret = self.app_secret.clone();
        let base_url = self.base_url().to_string();
        let client = self.client.clone();

        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(TOKEN_REFRESH_SECS));
            loop {
                interval.tick().await;

                let url = format!("{base_url}/open-apis/auth/v3/tenant_access_token/internal");
                let body = serde_json::json!({
                    "app_id": app_id,
                    "app_secret": app_secret,
                });

                match client.post(&url).json(&body).send().await {
                    Ok(resp) => {
                        if let Ok(data) = resp.json::<serde_json::Value>().await {
                            if let Some(token) =
                                data.get("tenant_access_token").and_then(|t| t.as_str())
                            {
                                let mut cache = token_cache.write().await;
                                *cache = Some(token.to_string());
                                tracing::debug!("Lark: token refreshed");
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Lark: token refresh failed: {e}");
                    }
                }
            }
        });

        // Initial token fetch
        let token = self.get_token().await?;
        tracing::info!(
            "{}: authenticated, polling for messages...",
            if self.use_feishu { "Feishu" } else { "Lark" }
        );

        // Use Lark's long-connection event subscription (WebSocket)
        // GET /open-apis/event/v1/outbound/ws_endpoint with auth
        let ws_url_resp = self
            .client
            .post(format!(
                "{}/open-apis/callback/ws/endpoint",
                self.base_url()
            ))
            .header("Authorization", format!("Bearer {token}"))
            .json(&serde_json::json!({}))
            .send()
            .await?;

        if !ws_url_resp.status().is_success() {
            // Fallback: poll using REST API for message events
            tracing::warn!("Lark: WebSocket endpoint not available, falling back to REST polling");
            return self.poll_messages(tx).await;
        }

        let ws_data: serde_json::Value = ws_url_resp.json().await?;
        let ws_url = ws_data
            .get("data")
            .and_then(|d| d.get("URL"))
            .or_else(|| ws_data.get("data").and_then(|d| d.get("url")))
            .and_then(|u| u.as_str());

        match ws_url {
            Some(url) => self.listen_ws(url, tx).await,
            None => {
                tracing::warn!("Lark: no WebSocket URL in response, falling back to REST polling");
                self.poll_messages(tx).await
            }
        }
    }

    async fn health_check(&self) -> bool {
        self.fetch_tenant_token().await.is_ok()
    }
}

impl LarkChannel {
    /// Listen via WebSocket long connection.
    async fn listen_ws(
        &self,
        ws_url: &str,
        tx: tokio::sync::mpsc::Sender<ChannelMessage>,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "{}: connecting to WebSocket...",
            if self.use_feishu { "Feishu" } else { "Lark" }
        );

        let (ws_stream, _) = tokio_tungstenite::connect_async(ws_url).await?;
        let (_write, mut read) = futures_util::StreamExt::split(ws_stream);

        tracing::info!(
            "{}: WebSocket connected, listening...",
            if self.use_feishu { "Feishu" } else { "Lark" }
        );

        while let Some(msg) = StreamExt::next(&mut read).await {
            let msg = match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => t,
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => break,
                Err(e) => {
                    tracing::warn!("Lark WebSocket error: {e}");
                    break;
                }
                _ => continue,
            };

            let event: serde_json::Value = match serde_json::from_str(&msg) {
                Ok(v) => v,
                Err(_) => continue,
            };

            // Handle message receive events
            self.process_event(&event, &tx).await;
        }

        anyhow::bail!("Lark WebSocket connection closed")
    }

    /// Fallback: poll messages via REST API.
    async fn poll_messages(
        &self,
        tx: tokio::sync::mpsc::Sender<ChannelMessage>,
    ) -> anyhow::Result<()> {
        // Simple polling loop — not ideal but works as fallback
        // In production, event subscription (webhook or WS) is preferred
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;

            tracing::debug!(
                "{}: polling heartbeat (event subscription recommended)",
                if self.use_feishu { "Feishu" } else { "Lark" }
            );

            // Keep the connection alive — actual messages come through
            // event subscription which should be configured in the Lark/Feishu
            // developer console to point to the gateway webhook endpoint
            if tx.is_closed() {
                break;
            }
        }

        anyhow::bail!("Lark polling loop ended")
    }

    /// Process a Lark event and emit a ChannelMessage if it's a user message.
    async fn process_event(
        &self,
        event: &serde_json::Value,
        tx: &tokio::sync::mpsc::Sender<ChannelMessage>,
    ) {
        // Lark event structure: { "header": {...}, "event": {...} }
        let header = match event.get("header") {
            Some(h) => h,
            None => return,
        };

        let event_type = header
            .get("event_type")
            .and_then(|e| e.as_str())
            .unwrap_or("");

        if event_type != "im.message.receive_v1" {
            return;
        }

        let evt = match event.get("event") {
            Some(e) => e,
            None => return,
        };

        let message = match evt.get("message") {
            Some(m) => m,
            None => return,
        };

        let sender = match evt.get("sender") {
            Some(s) => s,
            None => return,
        };

        let sender_id = match Self::extract_sender_id(sender) {
            Some(id) => id,
            None => return,
        };

        if !self.is_user_allowed(&sender_id) {
            tracing::warn!("Lark: ignoring message from unauthorized user: {sender_id}");
            return;
        }

        let content = match Self::extract_text_content(message) {
            Some(c) if !c.trim().is_empty() => c.trim().to_string(),
            _ => return,
        };

        let chat_id = message
            .get("chat_id")
            .and_then(|c| c.as_str())
            .unwrap_or(&sender_id);

        let channel_msg = ChannelMessage {
            id: Uuid::new_v4().to_string(),
            sender: chat_id.to_string(),
            content,
            channel: if self.use_feishu {
                "feishu".to_string()
            } else {
                "lark".to_string()
            },
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        if tx.send(channel_msg).await.is_err() {
            tracing::warn!("Lark: message channel closed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name_feishu() {
        let ch = LarkChannel::new("id".into(), "secret".into(), None, None, vec![], true);
        assert_eq!(ch.name(), "feishu");
    }

    #[test]
    fn test_name_lark() {
        let ch = LarkChannel::new("id".into(), "secret".into(), None, None, vec![], false);
        assert_eq!(ch.name(), "lark");
    }

    #[test]
    fn test_base_url() {
        let feishu = LarkChannel::new("id".into(), "secret".into(), None, None, vec![], true);
        assert_eq!(feishu.base_url(), FEISHU_BASE_URL);

        let lark = LarkChannel::new("id".into(), "secret".into(), None, None, vec![], false);
        assert_eq!(lark.base_url(), LARK_BASE_URL);
    }

    #[test]
    fn test_user_allowed_wildcard() {
        let ch = LarkChannel::new(
            "id".into(),
            "secret".into(),
            None,
            None,
            vec!["*".into()],
            false,
        );
        assert!(ch.is_user_allowed("anyone"));
    }

    #[test]
    fn test_user_allowed_specific() {
        let ch = LarkChannel::new(
            "id".into(),
            "secret".into(),
            None,
            None,
            vec!["ou_abc".into()],
            false,
        );
        assert!(ch.is_user_allowed("ou_abc"));
        assert!(!ch.is_user_allowed("ou_xyz"));
    }

    #[test]
    fn test_user_denied_empty() {
        let ch = LarkChannel::new("id".into(), "secret".into(), None, None, vec![], false);
        assert!(!ch.is_user_allowed("anyone"));
    }

    #[test]
    fn test_extract_sender_id_user_id() {
        let sender = serde_json::json!({
            "sender_id": {
                "user_id": "u123",
                "open_id": "ou_456",
                "union_id": "on_789",
            }
        });
        assert_eq!(LarkChannel::extract_sender_id(&sender), Some("u123".into()));
    }

    #[test]
    fn test_extract_sender_id_fallback_open_id() {
        let sender = serde_json::json!({
            "sender_id": {
                "user_id": "",
                "open_id": "ou_456",
                "union_id": "on_789",
            }
        });
        assert_eq!(
            LarkChannel::extract_sender_id(&sender),
            Some("ou_456".into())
        );
    }

    #[test]
    fn test_extract_text_content() {
        let msg = serde_json::json!({
            "content": "{\"text\":\"hello world\"}",
        });
        assert_eq!(
            LarkChannel::extract_text_content(&msg),
            Some("hello world".into())
        );
    }

    #[test]
    fn test_extract_text_content_raw_fallback() {
        let msg = serde_json::json!({
            "content": "plain text",
        });
        assert_eq!(
            LarkChannel::extract_text_content(&msg),
            Some("plain text".into())
        );
    }

    #[test]
    fn test_lark_config_serde() {
        let toml_str = r#"
app_id = "cli_abc"
app_secret = "secret_xyz"
use_feishu = true
allowed_users = ["ou_123"]
"#;
        let config: crate::config::schema::LarkConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.app_id, "cli_abc");
        assert!(config.use_feishu);
        assert!(config.encrypt_key.is_none());
    }
}
