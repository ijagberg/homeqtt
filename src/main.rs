mod mqtt;

use dotenv;
use mqtt::ClientInfo;
use rumqttc::MqttOptions;
use std::time;
use structopt::StructOpt;

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
            let client_info = ClientInfo::new(String::from("worker"));
            let mut mqtt_options = MqttOptions::new(client_info.id(), opts.mqtt_url, 1883);
            mqtt_options
                .set_keep_alive(5)
                .set_throttle(time::Duration::from_secs(1))
                .set_clean_session(true)
                .set_max_packet_size(100_000);

            let client = mqtt::clients::Main::new(client_info, mqtt_options);
            client.run().await;
        }
        Client::Worker(worker_opts) => {
            let client_info = ClientInfo::new(String::from("worker"));
            let mut mqtt_options = MqttOptions::new(client_info.id(), opts.mqtt_url, 1883);
            mqtt_options
                .set_keep_alive(5)
                .set_throttle(time::Duration::from_secs(1))
                .set_clean_session(true)
                .set_max_packet_size(100_000);
            let client = mqtt::clients::Worker::new(client_info, worker_opts, mqtt_options);
            client.run().await;
        }
    }
}

#[derive(StructOpt)]
struct WorkerOpts {
    #[structopt(long, default_value = "10000")]
    heartbeat_timer_ms: u64,
}
