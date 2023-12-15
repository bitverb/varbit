use std::str::FromStr;

use anyhow::Ok;
use clap::Parser;
use log::info;

#[derive(Parser, Debug)]
#[command(author = "xx", version = "0.1.0", about, long_about = None, next_line_help = true)]
struct Args {
    /// app name
    #[arg(short, long, default_value = "varbit")]
    name: String,

    /// listen port
    #[arg(short, long, default_value_t = 3000)]
    port: u16,

    /// log level
    #[arg(short, long, default_value = "debug")]
    level: String,

    /// config path
    #[arg(long, default_value = "example/etc/config.toml", short)]
    conf: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Args = Args::parse();
    let level = log::LevelFilter::from_str(&args.level).unwrap();
    vlog::init_log(env_logger::Target::Stdout, level);
    info!("args {:?}", args);
    let c = conf::from_path(args.conf.to_owned());
    info!("config {:?}", c);
    let _ = api::start(c.clone()).await;
    Ok(())
}
