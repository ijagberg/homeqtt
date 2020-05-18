use super::Opts;
use chrono::prelude::*;
use rumq_client::{self, MqttOptions, Notification, Publish, QoS, Request, Subscribe};
use std::{convert::TryFrom, fmt::Display, time};
use tokio::stream::StreamExt;
use tokio::sync::mpsc;

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
        // subscribe to all relevant topics
        let sub_request = Subscribe::new("hello/world", QoS::AtLeastOnce);
        if let Err(err) = requests_tx.send(Request::Subscribe(sub_request)).await {
            error!("failed to subscribe: '{}'", err);
            error!("sleeping for 5 seconds before reconnecting...");
            tokio::time::delay_for(time::Duration::from_secs(5)).await;
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
    match Message::try_from(publish) {
        Ok(Message::LogTheTime(data)) => info!(
            "{} wants to log current time: '{}'",
            data.id,
            Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
        ),
        Err(error) => error!("error handling notification: '{}'", error),
    }
}

enum Message {
    LogTheTime(LogTheTime),
}

impl Display for MessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::UnsupportedTopic(topic) => format!("unsupported topic: '{}'", topic),
                Self::InvalidUtf8 => "invalid utf8".to_string(),
                Self::InvalidPayload => "invalid payload".to_string(),
            }
        )
    }
}

impl TryFrom<Publish> for Message {
    type Error = MessageError;
    fn try_from(value: Publish) -> Result<Self, Self::Error> {
        Ok(match value.topic_name.as_ref() {
            "log/time" => Self::LogTheTime(
                serde_json::from_str(
                    &String::from_utf8(value.payload).map_err(|_| Self::Error::InvalidUtf8)?,
                )
                .map_err(|_| Self::Error::InvalidPayload)?,
            ),
            invalid => Err(Self::Error::UnsupportedTopic(invalid.to_owned()))?,
        })
    }
}

enum MessageError {
    UnsupportedTopic(String),
    InvalidUtf8,
    InvalidPayload,
}

#[derive(serde::Deserialize)]
struct LogTheTime {
    id: String,
}
