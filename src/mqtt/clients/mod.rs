use std::time::Duration;

use super::{ClientInfo, Message};
use crate::WorkerOpts;
use chrono::prelude::*;
use rumqttc::{Client, Incoming, MqttOptions, Publish, QoS};
use std::convert::TryFrom;

pub(crate) struct Main {
    #[allow(unused)]
    client_info: ClientInfo,
    mqtt_opts: MqttOptions,
}

impl Main {
    pub(crate) fn new(client_info: ClientInfo, mqtt_opts: MqttOptions) -> Self {
        Self {
            client_info,
            mqtt_opts,
        }
    }

    pub fn run(self) {
        let (mut client, mut connection) = Client::new(self.mqtt_opts, 10);
        Main::subscribe(&mut client, super::LOG_THE_TIME_TOPIC);
        Main::subscribe(&mut client, super::HEARTBEAT_TOPICS);

        for (_i, notification) in connection.iter().enumerate() {
            trace!("notification: '{:?}'", notification);
            if let Ok((Some(Incoming::Publish(p)), _)) = notification {
                Main::handle_publish(p);
            }
        }
    }

    fn subscribe(client: &mut Client, topic: &str) {
        info!("subscribing to '{}'", topic);
        if let Err(e) = client.subscribe(topic, QoS::AtMostOnce) {
            error!("failed to subscribe to '{}': '{}'", topic, e);
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
}

pub(crate) struct Worker {
    client_info: ClientInfo,
    opts: WorkerOpts,
    mqtt_opts: MqttOptions,
}

impl Worker {
    pub(crate) fn new(client_info: ClientInfo, opts: WorkerOpts, mqtt_opts: MqttOptions) -> Self {
        Self {
            client_info,
            opts,
            mqtt_opts,
        }
    }

    pub fn run(self) {
        let (mut client, _connection) = Client::new(self.mqtt_opts, 10);

        loop {
            info!("sending heartbeat...");
            client
                .publish(
                    format!("homeqtt/heartbeats/{}", self.client_info.id),
                    QoS::AtLeastOnce,
                    false,
                    serde_json::to_string(&self.client_info).unwrap().as_bytes(),
                )
                .unwrap();
            info!("sleeping for {}ms...", self.opts.heartbeat_timer_ms);
            std::thread::sleep(Duration::from_millis(self.opts.heartbeat_timer_ms));
        }
    }
}
