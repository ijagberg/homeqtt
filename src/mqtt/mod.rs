use super::Opts;
use chrono::prelude::*;
use rumq_client::{self, MqttOptions, Notification, Publish, QoS, Request, Subscribe};
use std::{convert::TryFrom, fmt::Display, time};
use tokio::stream::StreamExt;
use tokio::sync::mpsc;

const LOG_THE_TIME_TOPIC: &str = "homeqtt/log/time";
const HEARTBEAT_TOPICS: &str = "homeqtt/heartbeats/+";

pub(super) async fn listen(opts: Opts) {
    info!("starting listener...");
    let (mut requests_tx, requests_rx) = mpsc::channel(100);
    let mut mqtt_options = MqttOptions::new("master", opts.mqtt_url, 1883);
    mqtt_options
        .set_keep_alive(5)
        .set_throttle(time::Duration::from_secs(1))
        .set_clean_session(true)
        .set_max_packet_size(100_000);
    let mut eventloop = rumq_client::eventloop(mqtt_options, requests_rx);

    // main listener loop
    loop {
        error!("sleeping for 5 seconds before connecting...");
        tokio::time::delay_for(time::Duration::from_secs(5)).await;

        // subscribe to all relevant topics
        let sub_request = Subscribe::new(LOG_THE_TIME_TOPIC, QoS::AtLeastOnce);
        if let Err(err) = requests_tx.send(Request::Subscribe(sub_request)).await {
            error!("failed to subscribe: '{}'", err);
            continue;
        }

        let mut stream = match eventloop.connect().await {
            Ok(stream) => stream,
            Err(err) => {
                error!("failed to connect to event loop: '{}'", err);
                continue;
            }
        };

        while let Some(notification) = stream.next().await {
            match notification {
                Notification::Publish(publish) => handle_publish(publish),
                _ => (),
            }
        }
    }
}

fn handle_publish(publish: Publish) {
    let now = chrono::Utc::now();
    match Message::try_from(publish) {
        Ok(Message::LogTheTime(data)) => info!(
            "{} wants to log current time: '{}'",
            data.id,
            Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
        ),
        Ok(Message::HeartBeat(client_info)) => info!(
            "received heartbeat from '{}' at {}",
            client_info.id,
            now.to_rfc3339_opts(SecondsFormat::Millis, true)
        ),
        Err(error) => error!("error handling publish: '{}'", error),
    }
}

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

#[derive(serde::Deserialize)]
struct ClientInfo {
    id: String,
}
