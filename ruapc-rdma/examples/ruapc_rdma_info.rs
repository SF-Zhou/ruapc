use clap::Parser;
use ruapc_rdma::{Result, *};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// device filter by name.
    #[arg(long)]
    pub device_filter: Vec<String>,

    /// enable gid type filter (IB or RoCE v2).
    #[arg(long, default_value_t = false)]
    pub gid_type_filter: bool,

    /// RoCE v2 skip link local address.
    #[arg(long, default_value_t = false)]
    pub skip_link_local_addr: bool,

    /// enable port state filter.
    #[arg(long, default_value_t = false)]
    pub skip_inactive_port: bool,

    /// enable verbose logging.
    #[arg(long, short, default_value_t = false)]
    pub verbose: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_max_level(if args.verbose {
            tracing::Level::DEBUG
        } else {
            tracing::Level::INFO
        })
        .with_timer(tracing_subscriber::fmt::time::ChronoLocal::rfc_3339())
        .init();

    let mut config = DeviceConfig::default();
    config.device_filter.extend(args.device_filter);
    if args.gid_type_filter {
        config.gid_type_filter = [GidType::IB, GidType::RoCEv2].into();
    }
    if args.skip_link_local_addr {
        config.roce_v2_skip_link_local_addr = true;
    }

    let devices = Devices::open(&config)?;
    let device_infos = devices
        .iter()
        .map(|device| device.info())
        .collect::<Vec<_>>();
    println!("{}", serde_json::to_string_pretty(&device_infos).unwrap());

    Ok(())
}
