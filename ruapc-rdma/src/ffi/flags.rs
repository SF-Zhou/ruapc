//! Typed accessors and names for capability flag enums.

macro_rules! impl_flag_name {
    ($ty:ty, $prefix:literal) => {
        impl $ty {
            /// Returns the C flag name without its common prefix.
            pub fn name(self) -> &'static str {
                let name: &'static str = self.into();
                name.strip_prefix($prefix).unwrap_or(name)
            }
        }
    };
}

impl_flag_name!(crate::ibv_device_cap_flags, "IBV_DEVICE_");
impl_flag_name!(crate::ibv_port_cap_flags, "IBV_PORT_");
impl_flag_name!(crate::ibv_port_cap_flags2, "IBV_PORT_");

#[cfg(test)]
mod tests {
    use enumflags2::BitFlags;

    use crate::{ibv_device_cap_flags, ibv_port_attr, ibv_port_cap_flags2};

    #[test]
    fn test_flag_names_and_iteration() {
        let flags = ibv_device_cap_flags::IBV_DEVICE_RESIZE_MAX_WR
            | ibv_device_cap_flags::IBV_DEVICE_BAD_QKEY_CNTR;
        let names: Vec<_> = flags.iter().map(ibv_device_cap_flags::name).collect();
        assert_eq!(names, ["RESIZE_MAX_WR", "BAD_QKEY_CNTR"]);
    }

    #[test]
    fn test_flags_serde_roundtrip() {
        let flags = ibv_device_cap_flags::IBV_DEVICE_RESIZE_MAX_WR
            | ibv_device_cap_flags::IBV_DEVICE_BAD_QKEY_CNTR;
        let encoded = serde_json::to_string(&flags).unwrap();
        let decoded: BitFlags<ibv_device_cap_flags> = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, flags);
    }

    #[test]
    fn test_port_cap_flags2_abi_layout() {
        fn assert_u16_field(attr: &ibv_port_attr) -> &BitFlags<ibv_port_cap_flags2, u16> {
            &attr.port_cap_flags2
        }

        assert_eq!(size_of::<ibv_port_cap_flags2>(), 2);
        assert_eq!(size_of::<BitFlags<ibv_port_cap_flags2>>(), 2);
        let _ = assert_u16_field;
        assert_eq!(std::mem::offset_of!(ibv_port_attr, port_cap_flags2), 48);
    }
}
