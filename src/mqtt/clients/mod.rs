use super::{ClientInfo, LOG_THE_TIME_TOPIC};
use rumq_client::{MqttEventLoop, Notification, Publish, QoS, Request, Subscribe};
use std::time::Duration;
use tokio::sync::mpsc::Sender;

use super::Message;
use crate::WorkerOpts;
use chrono::prelude::*;
use std::{convert::TryFrom, time};
use tokio::stream::StreamExt;

pub(crate) struct Main {
    #[allow(unused)]
    client_info: ClientInfo,
    tx: Sender<Request>,
    eventloop: MqttEventLoop,
}

impl Main {
    pub(crate) fn new(
        client_info: ClientInfo,
        tx: Sender<Request>,
        eventloop: MqttEventLoop,
    ) -> Self {
        Self {
            client_info,
            tx,
            eventloop,
        }
    }

    pub async fn run(mut self) {
        info!("starting listener...");
        // main listener loop
        loop {
            info!("sleeping for 5 seconds before connecting...");
            tokio::time::delay_for(time::Duration::from_secs(5)).await;

            // subscribe to all relevant topics
            let log_time_sub = Subscribe::new(LOG_THE_TIME_TOPIC, QoS::AtLeastOnce);
            if let Err(err) = self.tx.send(Request::Subscribe(log_time_sub)).await {
                error!("failed to subscribe: '{}'", err);
                continue;
            }
            let heartbeat_sub = Subscribe::new(super::HEARTBEAT_TOPICS, QoS::AtLeastOnce);
            if let Err(err) = self.tx.send(Request::Subscribe(heartbeat_sub)).await {
                error!("failed to subscribe: '{}'", err);
                continue;
            }

            let mut stream = match self.eventloop.connect().await {
                Ok(stream) => stream,
                Err(err) => {
                    error!("failed to connect to event loop: '{}'", err);
                    continue;
                }
            };

            loop {
                self.tx
                    .send(Request::Publish(Publish::new(
                        "homeqtt/test",
                        QoS::AtLeastOnce,
                        "tjena",
                    )))
                    .await
                    .unwrap();
                tokio::time::delay_for(Duration::from_secs(1)).await;
            }

            info!("connected");

            while let Some(notification) = stream.next().await {
                match notification {
                    Notification::Publish(publish) => Main::handle_publish(publish),
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
}

pub(crate) struct Worker {
    client_info: ClientInfo,
    opts: WorkerOpts,
    tx: Sender<Request>,
    eventloop: MqttEventLoop,
}

impl Worker {
    pub(crate) fn new(
        client_info: ClientInfo,
        opts: WorkerOpts,
        tx: Sender<Request>,
        eventloop: MqttEventLoop,
    ) -> Self {
        Self {
            client_info,
            opts,
            tx,
            eventloop,
        }
    }

    pub async fn run(mut self) {
        'connection: loop {
            info!("sleeping for 5 seconds before connecting...");
            tokio::time::delay_for(time::Duration::from_secs(5)).await;

            let _stream = match self.eventloop.connect().await {
                Ok(stream) => stream,
                Err(err) => {
                    error!("failed to connect to event loop: '{}'", err);
                    continue;
                }
            };
            info!("connected");
            loop {
                info!("sending heartbeat...");
                if let Err(e) = self
                    .tx
                    .send(Request::Publish(Publish::new(
                        format!("homeqtt/heartbeats/{}", self.client_info.id),
                        QoS::AtLeastOnce,
                        serde_json::to_string(&self.client_info).unwrap().as_bytes(),
                    )))
                    .await
                {
                    error!("failed to publish heartbeat: '{}'", e);
                    continue 'connection;
                }
                info!("sleeping for {}ms...", self.opts.heartbeat_timer_ms);
                tokio::time::delay_for(Duration::from_millis(self.opts.heartbeat_timer_ms)).await;
            }
        }
    }
}
