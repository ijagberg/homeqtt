use super::Opts;
use rumq_client::{self, MqttOptions, Notification, QoS, Request, Subscribe};
use std::time;
use tokio::stream::StreamExt;
use tokio::sync::mpsc;

pub(super) async fn listen(opts: Opts) {
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
                Notification::Publish(publish) => info!(
                    "received publish notification: '{}'",
                    String::from_utf8(publish.payload).unwrap()
                ),
                _ => (),
            }
        }
    }
}
