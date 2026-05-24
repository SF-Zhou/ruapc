impl crate::ibv_port_cap_flags2 {
    pub fn flag_name(bit: u16) -> Option<&'static str> {
        match bit {
            0x0001 => Some("SET_NODE_DESC_SUP"),
            0x0002 => Some("INFO_EXT_SUP"),
            0x0004 => Some("VIRT_SUP"),
            0x0008 => Some("SWITCH_PORT_STATE_TABLE_SUP"),
            0x0010 => Some("LINK_WIDTH_2X_SUP"),
            0x0020 => Some("LINK_SPEED_HDR_SUP"),
            0x0400 => Some("LINK_SPEED_NDR_SUP"),
            _ => None,
        }
    }
}

impl std::fmt::Display for crate::ibv_port_cap_flags2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x{:04x}", self.0)
    }
}
