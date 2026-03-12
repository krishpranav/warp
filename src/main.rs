//! eventide — server entry point

use anyhow::Context;
use clap::Parser;
use tracing::info;

use eventide_core::{Config, net};

#[derive(Debug, Parser)]
#[command(name = "eventide", version, about)]
struct Args {
    #[arg(short, long, default_value = "eventide.toml", env = "EVENTIDE_CONFIG")]
    config: std::path::PathBuf,

    #[arg(long, env = "EVENTIDE_LOG")]
    log: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    init_tracing(args.log.as_deref());

    let config = Config::load(&args.config)
        .with_context(|| format!("failed to load config: {}", args.config.display()))?;

    info!(
        version = env!("CARGO_PKG_VERSION"),
        data_dir = %config.storage.data_dir.display(),
        bind     = %config.net.bind,
        "eventide starting"
    );

    net::Server::new(config)
        .await
        .context("failed to initialise server")?
        .run()
        .await
        .context("server exited with error")
}

fn init_tracing(level_override: Option<&str>) {
    use tracing_subscriber::{EnvFilter, fmt, prelude::*};

    let filter = level_override
        .map(|l| EnvFilter::new(l))
        .unwrap_or_else(|| {
            EnvFilter::from_env("EVENTIDE_LOG")
                .unwrap_or_else(|_| EnvFilter::new("info"))
        });

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_target(true).compact())
        .init();
}