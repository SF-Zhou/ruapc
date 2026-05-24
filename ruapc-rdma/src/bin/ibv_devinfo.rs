use std::{path::Path, process::exit};

use clap::Parser;
use ruapc_rdma::{ActiveDevice, GidType, Port, ibv_mtu, ibv_transport_type};

#[derive(Parser)]
#[command(name = "ibv_devinfo", about = "Query RDMA devices")]
struct Args {
    #[arg(short = 'd', long = "ib-dev")]
    device: Option<String>,

    #[arg(short = 'l', long)]
    list: bool,

    #[arg(short, long)]
    verbose: bool,

    #[arg(short = 'p', long)]
    port: Option<u8>,
}

fn main() {
    let args = Args::parse();

    let devices = match ActiveDevice::available() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Failed to query RDMA devices: {e}");
            exit(1);
        }
    };

    if args.list {
        let plural = if devices.len() == 1 { "" } else { "s" };
        println!("{} HCA{} found:", devices.len(), plural);
        for dev in &devices {
            println!("\t{}", dev.info().name);
        }
        println!();
        return;
    }

    for dev in &devices {
        if let Some(ref name) = args.device
            && dev.info().name != *name
        {
            continue;
        }
        print_device(dev, args.verbose, args.port);
    }
}

fn read_sysfs(path: &Path, file: &str) -> String {
    std::fs::read_to_string(path.join(file))
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

fn mtu_raw(m: ibv_mtu) -> u32 {
    unsafe { std::mem::transmute::<ibv_mtu, i32>(m) as u32 }
}

fn mtu_bytes(raw: u32) -> u32 {
    match raw {
        1 => 256,
        2 => 512,
        3 => 1024,
        4 => 2048,
        5 => 4096,
        _ => 0,
    }
}

fn transport_str(t: ibv_transport_type) -> &'static str {
    match t {
        ibv_transport_type::IBV_TRANSPORT_IB => "InfiniBand",
        ibv_transport_type::IBV_TRANSPORT_IWARP => "iWARP",
        ibv_transport_type::IBV_TRANSPORT_USNIC => "usNIC",
        ibv_transport_type::IBV_TRANSPORT_USNIC_UDP => "usNIC UDP",
        ibv_transport_type::IBV_TRANSPORT_UNSPECIFIED => "unspecified",
        _ => "invalid transport",
    }
}

fn active_width_str(w: u8) -> &'static str {
    match w {
        1 => "1",
        2 => "4",
        4 => "8",
        8 => "12",
        16 => "2",
        _ => "invalid width",
    }
}

fn active_speed_str(s: u8) -> &'static str {
    match s {
        1 => "2.5 Gbps",
        2 => "5.0 Gbps",
        4 | 8 => "10.0 Gbps",
        16 => "14.0 Gbps",
        32 => "25.0 Gbps",
        64 => "50.0 Gbps",
        128 => "100.0 Gbps",
        _ => "invalid speed",
    }
}

fn phys_state_str(s: u8) -> &'static str {
    match s {
        1 => "SLEEP",
        2 => "POLLING",
        3 => "DISABLED",
        4 => "PORT_CONFIGURATION TRAINNING",
        5 => "LINK_UP",
        6 => "LINK_ERROR_RECOVERY",
        7 => "PHY TEST",
        _ => "invalid physical state",
    }
}

fn vendor_id_display(vendor_id: u32) -> String {
    format!("0x{vendor_id:04x}")
}

fn hw_ver_display(hw_ver: u32) -> String {
    format!("0x{hw_ver:x}")
}

fn gid_type_display(t: &GidType) -> String {
    match t {
        GidType::IB => "IB".to_string(),
        GidType::RoCEv1 => "RoCE v1".to_string(),
        GidType::RoCEv2 => "RoCE v2".to_string(),
        GidType::Other(s) => s.clone(),
    }
}

fn gid_v6_expanded(gid: &ruapc_rdma::ibv_gid) -> String {
    let bits = gid.as_bits();
    format!(
        "{:04x}:{:04x}:{:04x}:{:04x}:{:04x}:{:04x}:{:04x}:{:04x}",
        (bits >> 112) as u16,
        (bits >> 96) as u16,
        (bits >> 80) as u16,
        (bits >> 64) as u16,
        (bits >> 48) as u16,
        (bits >> 32) as u16,
        (bits >> 16) as u16,
        bits as u16,
    )
}

fn print_device(dev: &ActiveDevice, verbose: bool, port_filter: Option<u8>) {
    let info = dev.info();
    let attr = &info.device_attr;

    println!("hca_id:\t{}", info.name);

    let t = info.transport_type;
    println!("\ttransport:\t\t\t{} ({})", transport_str(t), t as i32);

    println!("\tfw_ver:\t\t\t\t{}", attr.fw_ver);
    println!("\tnode_guid:\t\t\t{}", attr.node_guid);
    println!("\tsys_image_guid:\t\t\t{}", attr.sys_image_guid);
    println!("\tvendor_id:\t\t\t{}", vendor_id_display(attr.vendor_id));
    println!("\tvendor_part_id:\t\t\t{}", attr.vendor_part_id);
    println!("\thw_ver:\t\t\t\t{}", hw_ver_display(attr.hw_ver));

    let board_id = read_sysfs(&info.ibdev_path, "board_id");
    println!("\tboard_id:\t\t\t{board_id}");

    println!("\tphys_port_cnt:\t\t\t{}", attr.phys_port_cnt);

    if verbose {
        print_verbose_device_attrs(attr);
        let num_comp_vectors = unsafe { (*dev.context().as_ptr()).num_comp_vectors };
        println!("\tnum_comp_vectors:\t\t{num_comp_vectors}");
    }

    for port in info
        .ports
        .iter()
        .filter(|p| port_filter.is_none_or(|pf| p.port_num == pf))
    {
        print_port(port, verbose);
    }

    println!();
}

fn print_verbose_device_attrs(attr: &ruapc_rdma::ibv_device_attr) {
    use ruapc_rdma::ibv_device_cap_flags;

    println!("\tmax_mr_size:\t\t\t0x{:x}", attr.max_mr_size);
    println!("\tpage_size_cap:\t\t\t0x{:x}", attr.page_size_cap);
    println!("\tmax_qp:\t\t\t\t{}", attr.max_qp);
    println!("\tmax_qp_wr:\t\t\t{}", attr.max_qp_wr);

    let dev_flags = attr.device_cap_flags;
    println!("\tdevice_cap_flags:\t\t{dev_flags}");
    for bit in (0..32).map(|i| 1u32.wrapping_shl(i as u32)) {
        if dev_flags.0 & bit != 0
            && let Some(name) = ibv_device_cap_flags::flag_name(bit)
        {
            println!("\t\t\t\t\t{name}");
        }
    }

    println!("\tmax_sge:\t\t\t{}", attr.max_sge);
    println!("\tmax_sge_rd:\t\t\t{}", attr.max_sge_rd);
    println!("\tmax_cq:\t\t\t\t{}", attr.max_cq);
    println!("\tmax_cqe:\t\t\t{}", attr.max_cqe);
    println!("\tmax_mr:\t\t\t\t{}", attr.max_mr);
    println!("\tmax_pd:\t\t\t\t{}", attr.max_pd);
    println!("\tmax_qp_rd_atom:\t\t\t{}", attr.max_qp_rd_atom);
    println!("\tmax_ee_rd_atom:\t\t\t{}", attr.max_ee_rd_atom);
    println!("\tmax_res_rd_atom:\t\t{}", attr.max_res_rd_atom);
    println!("\tmax_qp_init_rd_atom:\t\t{}", attr.max_qp_init_rd_atom);
    println!("\tmax_ee_init_rd_atom:\t\t{}", attr.max_ee_init_rd_atom);

    let ac_name = format!("{:?}", attr.atomic_cap).replacen("IBV_", "", 1);
    println!("\tatomic_cap:\t\t\t{ac_name} ({})", attr.atomic_cap as i32);

    println!("\tmax_ee:\t\t\t\t{}", attr.max_ee);
    println!("\tmax_rdd:\t\t\t{}", attr.max_rdd);
    println!("\tmax_mw:\t\t\t\t{}", attr.max_mw);
    println!("\tmax_raw_ipv6_qp:\t\t{}", attr.max_raw_ipv6_qp);
    println!("\tmax_raw_ethy_qp:\t\t{}", attr.max_raw_ethy_qp);
    println!("\tmax_mcast_grp:\t\t\t{}", attr.max_mcast_grp);
    println!("\tmax_mcast_qp_attach:\t\t{}", attr.max_mcast_qp_attach);
    println!(
        "\tmax_total_mcast_qp_attach:\t{}",
        attr.max_total_mcast_qp_attach
    );
    println!("\tmax_ah:\t\t\t\t{}", attr.max_ah);
    println!("\tmax_fmr:\t\t\t{}", attr.max_fmr);
    println!("\tmax_srq:\t\t\t{}", attr.max_srq);
    println!("\tmax_srq_wr:\t\t\t{}", attr.max_srq_wr);
    println!("\tmax_srq_sge:\t\t\t{}", attr.max_srq_sge);
    println!("\tmax_pkeys:\t\t\t{}", attr.max_pkeys);
    println!("\tlocal_ca_ack_delay:\t\t{}", attr.local_ca_ack_delay);
}

fn print_port(port: &Port, verbose: bool) {
    let pa = &port.port_attr;

    println!("\t\tport:\t{}", port.port_num);

    let state_name = format!("{:?}", pa.state).replacen("IBV_", "", 1);
    println!("\t\t\tstate:\t\t\t{} ({})", state_name, pa.state as i32);

    let max_mtu = mtu_raw(pa.max_mtu);
    let active_mtu = mtu_raw(pa.active_mtu);
    println!("\t\t\tmax_mtu:\t\t{} ({max_mtu})", mtu_bytes(max_mtu));
    println!(
        "\t\t\tactive_mtu:\t\t{} ({active_mtu})",
        mtu_bytes(active_mtu)
    );

    println!("\t\t\tsm_lid:\t\t\t{}", pa.sm_lid);
    println!("\t\t\tport_lid:\t\t{}", pa.lid);
    println!("\t\t\tport_lmc:\t\t0x{:02x}", pa.lmc);
    println!("\t\t\tlink_layer:\t\t{}", pa.link_layer);

    if verbose {
        println!("\t\t\tmax_msg_sz:\t\t0x{:x}", pa.max_msg_sz);
        println!("\t\t\tport_cap_flags:\t\t{}", pa.port_cap_flags);
        println!("\t\t\tport_cap_flags2:\t{}", pa.port_cap_flags2);

        if pa.max_vl_num == 0 {
            println!("\t\t\tmax_vl_num:\t\tinvalid value (0)");
        } else {
            println!("\t\t\tmax_vl_num:\t\t{}", pa.max_vl_num);
        }

        println!("\t\t\tbad_pkey_cntr:\t\t0x{:x}", pa.bad_pkey_cntr);
        println!("\t\t\tqkey_viol_cntr:\t\t0x{:x}", pa.qkey_viol_cntr);
        println!("\t\t\tsm_sl:\t\t\t{}", pa.sm_sl);
        println!("\t\t\tpkey_tbl_len:\t\t{}", pa.pkey_tbl_len);
        println!("\t\t\tgid_tbl_len:\t\t{}", pa.gid_tbl_len);
        println!("\t\t\tsubnet_timeout:\t\t{}", pa.subnet_timeout);
        println!("\t\t\tinit_type_reply:\t{}", pa.init_type_reply);

        println!(
            "\t\t\tactive_width:\t\t{}X ({})",
            active_width_str(pa.active_width),
            pa.active_width
        );
        println!(
            "\t\t\tactive_speed:\t\t{} ({})",
            active_speed_str(pa.active_speed),
            pa.active_speed
        );
        println!(
            "\t\t\tphys_state:\t\t{} ({})",
            phys_state_str(pa.phys_state),
            pa.phys_state
        );

        for gid in &port.gids {
            let gtype = gid_type_display(&gid.gid_type);
            let gip = match gid.gid_type {
                GidType::IB | GidType::RoCEv1 => gid_v6_expanded(&gid.gid),
                _ => gid.gid.as_ipv6().to_string(),
            };
            println!("\t\t\tGID[{:3}]:\t\t{gip}, {gtype}", gid.index);
        }
    }
}
