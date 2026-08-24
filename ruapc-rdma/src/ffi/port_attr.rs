//! Helpers for interpreting encoded RDMA port link attributes.

use crate::ibv_port_attr;

const fn speed_bps(speed: u32) -> Option<u64> {
    match speed {
        1 => Some(2_500_000_000),
        2 => Some(5_000_000_000),
        4 | 8 => Some(10_000_000_000),
        16 => Some(14_000_000_000),
        32 => Some(25_000_000_000),
        64 => Some(50_000_000_000),
        128 => Some(100_000_000_000),
        256 => Some(200_000_000_000),
        _ => None,
    }
}

impl ibv_port_attr {
    /// Returns the number of active physical lanes.
    ///
    /// `active_width` uses the `IBV_LINK_WIDTH_*` encoding rather than storing
    /// the lane count directly. Unknown encodings return `None`.
    pub const fn active_width_lanes(&self) -> Option<u8> {
        match self.active_width {
            1 => Some(1),
            2 => Some(4),
            4 => Some(8),
            8 => Some(12),
            16 => Some(2),
            _ => None,
        }
    }

    /// Returns the encoded active speed, preferring the extended field when
    /// provided by the installed rdma-core.
    pub const fn active_speed_raw(&self) -> u32 {
        #[cfg(ruapc_ibv_port_attr_has_active_speed_ex)]
        if self.active_speed_ex != 0 {
            return self.active_speed_ex;
        }

        self.active_speed as u32
    }

    /// Returns the active per-lane signaling rate in bits per second.
    ///
    /// Unknown `IBV_LINK_SPEED_*` encodings return `None`.
    pub const fn active_speed_bps(&self) -> Option<u64> {
        speed_bps(self.active_speed_raw())
    }

    /// Returns the aggregate active link bandwidth in bits per second.
    ///
    /// This is the nominal signaling rate (`active_width * active_speed`), not
    /// the payload throughput after protocol encoding and framing overhead.
    pub const fn bandwidth_bps(&self) -> Option<u64> {
        match (self.active_width_lanes(), self.active_speed_bps()) {
            (Some(width), Some(speed)) => Some(width as u64 * speed),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_active_width() {
        let mut attr = ibv_port_attr::default();
        for (raw, lanes) in [(1, 1), (2, 4), (4, 8), (8, 12), (16, 2)] {
            attr.active_width = raw;
            assert_eq!(attr.active_width_lanes(), Some(lanes));
        }

        attr.active_width = 0;
        assert_eq!(attr.active_width_lanes(), None);
    }

    #[test]
    fn decodes_active_speed() {
        let mut attr = ibv_port_attr::default();
        for (raw, bps) in [
            (1, 2_500_000_000),
            (2, 5_000_000_000),
            (4, 10_000_000_000),
            (8, 10_000_000_000),
            (16, 14_000_000_000),
            (32, 25_000_000_000),
            (64, 50_000_000_000),
            (128, 100_000_000_000),
        ] {
            attr.active_speed = raw;
            assert_eq!(attr.active_speed_bps(), Some(bps));
        }

        attr.active_speed = 0;
        assert_eq!(attr.active_speed_bps(), None);

        assert_eq!(speed_bps(256), Some(200_000_000_000));
    }

    #[test]
    fn calculates_aggregate_bandwidth() {
        let mut attr = ibv_port_attr {
            active_width: 2,
            active_speed: 32,
            ..Default::default()
        };
        assert_eq!(attr.bandwidth_bps(), Some(100_000_000_000));

        attr.active_speed = 0;
        assert_eq!(attr.bandwidth_bps(), None);
    }
}
