mod mqtt;

use dotenv;
use structopt::StructOpt;

#[macro_use]
extern crate log;

#[derive(StructOpt)]
struct Opts {
    #[structopt(long, env = "MQTT_URL")]
    mqtt_url: String,
}

fn main() {
    dotenv::dotenv().ok();
    pretty_env_logger::init();
    let opts = Opts::from_args();
}
