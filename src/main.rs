mod mqtt;

use dotenv;
use rumq_client::MqttOptions;
use std::time;
use structopt::StructOpt;
use tokio::sync::mpsc;

#[macro_use]
extern crate log;

#[derive(StructOpt)]
struct Opts {
    #[structopt(long, env = "MQTT_URL")]
    mqtt_url: String,
    #[structopt(subcommand)]
    client: Client,
}

#[derive(StructOpt)]
enum Client {
    Main,
    Worker(WorkerOpts),
}

#[tokio::main(basic_scheduler)]
async fn main() {
    dotenv::dotenv().ok();
    pretty_env_logger::init();
    let opts = Opts::from_args();

    match opts.client {
        Client::Main => {
            let id = String::from("main");
            let (tx, rx) = mpsc::channel(100);
            let mut mqtt_options = MqttOptions::new(&id, opts.mqtt_url, 1883);
            mqtt_options
                .set_keep_alive(5)
                .set_throttle(time::Duration::from_secs(1))
                .set_clean_session(true)
                .set_max_packet_size(100_000);
            let eventloop = rumq_client::eventloop(mqtt_options, rx);

            let client = mqtt::clients::Main::new(id, tx, eventloop);
            client.run().await;
        }
        Client::Worker(worker_opts) => {
            let id = String::from("worker");
            let (tx, rx) = mpsc::channel(100);
            let mut mqtt_options = MqttOptions::new(&id, opts.mqtt_url, 1883);
            mqtt_options
                .set_keep_alive(5)
                .set_throttle(time::Duration::from_secs(1))
                .set_clean_session(true)
                .set_max_packet_size(100_000);
            let eventloop = rumq_client::eventloop(mqtt_options, rx);
            let client = mqtt::clients::Worker::new(id, worker_opts, tx, eventloop);
            client.run().await;
        }
    }
}

#[derive(StructOpt)]
struct WorkerOpts {
    #[structopt(long, default_value = "10000")]
    heartbeat_timer_ms: u64,
}
