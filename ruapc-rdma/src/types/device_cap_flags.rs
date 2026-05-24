impl crate::ibv_device_cap_flags {
    pub fn flag_name(bit: u32) -> Option<&'static str> {
        match bit {
            0x00000001 => Some("RESIZE_MAX_WR"),
            0x00000002 => Some("BAD_PKEY_CNTR"),
            0x00000004 => Some("BAD_QKEY_CNTR"),
            0x00000008 => Some("RAW_MULTI"),
            0x00000010 => Some("AUTO_PATH_MIG"),
            0x00000020 => Some("CHANGE_PHY_PORT"),
            0x00000040 => Some("UD_AV_PORT_ENFORCE"),
            0x00000080 => Some("CURR_QP_STATE_MOD"),
            0x00000100 => Some("SHUTDOWN_PORT"),
            0x00000200 => Some("INIT_TYPE"),
            0x00000400 => Some("PORT_ACTIVE_EVENT"),
            0x00000800 => Some("SYS_IMAGE_GUID"),
            0x00001000 => Some("RC_RNR_NAK_GEN"),
            0x00002000 => Some("SRQ_RESIZE"),
            0x00004000 => Some("N_NOTIFY_CQ"),
            0x00020000 => Some("MEM_WINDOW"),
            0x00040000 => Some("UD_IP_CSUM"),
            0x00100000 => Some("XRC"),
            0x00200000 => Some("MEM_MGT_EXTENSIONS"),
            0x00800000 => Some("MEM_WINDOW_TYPE_2A"),
            0x01000000 => Some("MEM_WINDOW_TYPE_2B"),
            0x02000000 => Some("RC_IP_CSUM"),
            0x04000000 => Some("RAW_IP_CSUM"),
            0x20000000 => Some("MANAGED_FLOW_STEERING"),
            _ => None,
        }
    }
}

impl std::fmt::Display for crate::ibv_device_cap_flags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x{:08x}", self.0)
    }
}
