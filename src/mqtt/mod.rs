pub(crate) mod clients;

use super::Opts;
use rumq_client::{self, Publish};
use std::{convert::TryFrom, fmt::Display};

pub const LOG_THE_TIME_TOPIC: &str = "homeqtt/log/time";
pub const HEARTBEAT_TOPICS: &str = "homeqtt/heartbeats/+";

enum Message {
    LogTheTime(LogTheTime),
    HeartBeat(ClientInfo),
}

impl Display for MessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match &self.kind {
                MessageErrorKind::UnsupportedTopic =>
                    format!("unsupported topic: '{}'", self.topic),
                MessageErrorKind::InvalidUtf8 => format!("invalid utf8 on topic: '{}'", self.topic),
                MessageErrorKind::InvalidPayload(_e) =>
                    format!("invalid payload on topic: '{}'", self.topic),
            }
        )
    }
}

impl TryFrom<Publish> for Message {
    type Error = MessageError;
    fn try_from(value: Publish) -> Result<Self, Self::Error> {
        let topic = value.topic_name.to_owned();
        let utf8_payload = String::from_utf8(value.payload)
            .map_err(|_| Self::Error::new(topic.clone(), MessageErrorKind::InvalidUtf8))?;
        Ok(match topic.as_ref() {
            LOG_THE_TIME_TOPIC => Message::LogTheTime(
                serde_json::from_str(&utf8_payload)
                    .map_err(|e| Self::Error::new(topic, MessageErrorKind::InvalidPayload(e)))?,
            ),
            HEARTBEAT_TOPICS => Message::HeartBeat(
                serde_json::from_str(&utf8_payload)
                    .map_err(|e| Self::Error::new(topic, MessageErrorKind::InvalidPayload(e)))?,
            ),
            _invalid => Err(Self::Error::new(topic, MessageErrorKind::UnsupportedTopic))?,
        })
    }
}

struct MessageError {
    topic: String,
    kind: MessageErrorKind,
}

impl MessageError {
    pub fn new(topic: String, kind: MessageErrorKind) -> Self {
        Self { topic, kind }
    }
}

enum MessageErrorKind {
    UnsupportedTopic,
    InvalidUtf8,
    InvalidPayload(serde_json::Error),
}

#[derive(serde::Deserialize)]
struct LogTheTime {
    id: String,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct ClientInfo {
    id: String,
}
