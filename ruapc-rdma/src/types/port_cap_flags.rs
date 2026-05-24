impl crate::ibv_port_cap_flags {
    pub fn flag_name(bit: u32) -> Option<&'static str> {
        match bit {
            0x00000002 => Some("SM"),
            0x00000004 => Some("NOTICE_SUP"),
            0x00000008 => Some("TRAP_SUP"),
            0x00000010 => Some("OPT_IPD_SUP"),
            0x00000020 => Some("AUTO_MIGR_SUP"),
            0x00000040 => Some("SL_MAP_SUP"),
            0x00000080 => Some("MKEY_NVRAM"),
            0x00000100 => Some("PKEY_NVRAM"),
            0x00000200 => Some("LED_INFO_SUP"),
            0x00000800 => Some("SYS_IMAGE_GUID_SUP"),
            0x00001000 => Some("PKEY_SW_EXT_PORT_TRAP_SUP"),
            0x00004000 => Some("EXTENDED_SPEEDS_SUP"),
            0x00008000 => Some("CAP_MASK2_SUP"),
            0x00010000 => Some("CM_SUP"),
            0x00020000 => Some("SNMP_TUNNEL_SUP"),
            0x00040000 => Some("REINIT_SUP"),
            0x00080000 => Some("DEVICE_MGMT_SUP"),
            0x00100000 => Some("VENDOR_CLASS_SUP"),
            0x00200000 => Some("DR_NOTICE_SUP"),
            0x00400000 => Some("CAP_MASK_NOTICE_SUP"),
            0x00800000 => Some("BOOT_MGMT_SUP"),
            0x01000000 => Some("LINK_LATENCY_SUP"),
            0x02000000 => Some("CLIENT_REG_SUP"),
            0x04000000 => Some("IP_BASED_GIDS"),
            _ => None,
        }
    }
}

impl std::fmt::Display for crate::ibv_port_cap_flags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x{:08x}", self.0)
    }
}
