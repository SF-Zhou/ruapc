//! RDMA verbs bindings and wrapper types.
//! This module provides safe abstractions over the raw RDMA verbs interface.

#![allow(dead_code)]
#![allow(deref_nullptr)]
#![allow(non_snake_case, non_camel_case_types, non_upper_case_globals)]
#![allow(clippy::missing_safety_doc, clippy::too_many_arguments)]

use std::{borrow::Cow, net::Ipv6Addr, os::raw::c_int};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// Constants
pub const ACCESS_FLAGS: u32 = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0
    | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0
    | ibv_access_flags::IBV_ACCESS_REMOTE_READ.0
    | ibv_access_flags::IBV_ACCESS_RELAXED_ORDERING.0;

/// Wrapper for pthread mutex
#[repr(transparent)]
pub struct pthread_mutex_t(pub libc::pthread_mutex_t);

impl std::fmt::Debug for pthread_mutex_t {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("pthread_mutex_t").finish()
    }
}

/// Wrapper for pthread condition variable
#[repr(transparent)]
pub struct pthread_cond_t(pub libc::pthread_cond_t);

impl std::fmt::Debug for pthread_cond_t {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("pthread_cond_t").finish()
    }
}

/// Represents different types of network link layers
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum IBV_LINK_LAYER {
    UNSPECIFIED = 0,
    INFINIBAND = 1,
    ETHERNET = 2,
}

// Include generated bindings
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

/// Request notification for completion queue events
#[inline(always)]
pub unsafe fn ibv_req_notify_cq(cq: *mut ibv_cq, solicited_only: c_int) -> c_int {
    unsafe { (*(*cq).context).ops.req_notify_cq.unwrap_unchecked()(cq, solicited_only) }
}

#[inline(always)]
pub unsafe fn ibv_poll_cq(cq: *mut ibv_cq, num_entries: c_int, wc: *mut ibv_wc) -> c_int {
    unsafe { (*(*cq).context).ops.poll_cq.unwrap_unchecked()(cq, num_entries, wc) }
}

#[inline(always)]
pub unsafe fn ibv_post_send(
    qp: *mut ibv_qp,
    wr: *mut ibv_send_wr,
    bad_wr: *mut *mut ibv_send_wr,
) -> c_int {
    unsafe { (*(*qp).context).ops.post_send.unwrap_unchecked()(qp, wr, bad_wr) }
}

#[inline(always)]
pub unsafe fn ibv_post_recv(
    qp: *mut ibv_qp,
    wr: *mut ibv_recv_wr,
    bad_wr: *mut *mut ibv_recv_wr,
) -> c_int {
    unsafe { (*(*qp).context).ops.post_recv.unwrap_unchecked()(qp, wr, bad_wr) }
}

impl ibv_gid {
    pub fn as_raw(&self) -> &[u8; 16] {
        unsafe { &self.raw }
    }

    pub fn as_bits(&self) -> u128 {
        u128::from_be_bytes(unsafe { self.raw })
    }

    pub fn as_ipv6(&self) -> std::net::Ipv6Addr {
        Ipv6Addr::from_bits(self.as_bits())
    }

    pub fn subnet_prefix(&self) -> u64 {
        u64::from_be(unsafe { self.global.subnet_prefix })
    }

    pub fn interface_id(&self) -> u64 {
        u64::from_be(unsafe { self.global.interface_id })
    }

    pub fn is_null(&self) -> bool {
        self.interface_id() == 0
    }
}

impl std::fmt::Debug for ibv_gid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ipv6())
    }
}

impl Serialize for ibv_gid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.as_ipv6().to_string())
    }
}

impl<'de> Deserialize<'de> for ibv_gid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        let addr = s
            .parse::<Ipv6Addr>()
            .map_err(|_| D::Error::custom("invalid IPv6 address format"))?;
        let bits = addr.to_bits();
        let mut gid = ibv_gid::default();
        gid.global.subnet_prefix = ((bits >> 64) as u64).to_be();
        gid.global.interface_id = (bits as u64).to_be();
        Ok(gid)
    }
}

impl JsonSchema for ibv_gid {
    fn schema_name() -> Cow<'static, str> {
        "GID".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "pattern": "^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$",
            "description": "IPv6 address format GID"
        })
    }
}

impl ibv_wc {
    pub fn is_recv(&self) -> bool {
        self.wr_id.get_type() == WCType::Recv
    }

    pub fn is_send_data(&self) -> bool {
        self.wr_id.get_type() == WCType::SendData
    }

    pub fn is_send_imm(&self) -> bool {
        self.wr_id.get_type() == WCType::SendImm
    }

    pub fn is_rdma_read(&self) -> bool {
        self.wr_id.get_type() == WCType::RdmaRead
    }

    pub fn succ(&self) -> bool {
        self.status == ibv_wc_status::IBV_WC_SUCCESS
    }

    pub fn imm(&self) -> Option<u32> {
        if ibv_wc_flags(self.wc_flags) & ibv_wc_flags::IBV_WC_WITH_IMM != ibv_wc_flags(0) {
            Some(u32::from_be(unsafe { self.__bindgen_anon_1.imm_data }))
        } else {
            None
        }
    }
}

impl std::fmt::Debug for ibv_wc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ibv_wc")
            .field("wr_id", &self.wr_id)
            .field("status", &self.status)
            .field("opcode", &self.opcode)
            .field("vendor_err", &self.vendor_err)
            .field("byte_len", &self.byte_len)
            .field("imm_data", &self.imm())
            .finish()
    }
}

/// Work completion ID with type information
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WRID(pub u64);

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WCType {
    Recv = 0,
    SendData = 1,
    SendImm = 2,
    RdmaRead = 3,
}

impl WRID {
    pub const TYPE_BITS: u32 = 62;
    pub const TYPE_MASK: u64 = ((1 << (u64::BITS - Self::TYPE_BITS)) - 1) << Self::TYPE_BITS;

    pub fn new(wc_type: WCType, id: u64) -> Self {
        assert!(id & Self::TYPE_MASK == 0, "ID too large");
        Self(((wc_type as u64) << Self::TYPE_BITS) | id)
    }

    pub fn recv(id: u64) -> Self {
        Self::new(WCType::Recv, id)
    }

    pub fn send_data(id: u64) -> Self {
        Self::new(WCType::SendData, id)
    }

    pub fn send_imm(id: u64) -> Self {
        Self::new(WCType::SendImm, id)
    }

    pub fn rdma_read(id: u64) -> Self {
        Self::new(WCType::RdmaRead, id)
    }

    pub fn get_type(&self) -> WCType {
        match (self.0 & Self::TYPE_MASK) >> Self::TYPE_BITS {
            0 => WCType::Recv,
            1 => WCType::SendData,
            2 => WCType::SendImm,
            3 => WCType::RdmaRead,
            _ => unreachable!(),
        }
    }

    pub fn get_id(&self) -> u64 {
        self.0 & !Self::TYPE_MASK
    }
}

impl std::fmt::Debug for WRID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.get_type() {
            WCType::Recv => write!(f, "Recv({})", self.get_id()),
            WCType::SendData => write!(f, "SendData({})", self.get_id()),
            WCType::SendImm => write!(f, "SendImm({})", self.get_id()),
            WCType::RdmaRead => write!(f, "RdmaRead({})", self.get_id()),
        }
    }
}

/// Globally Unique Identifier for RDMA devices
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct Guid(pub u64);

impl Guid {
    fn as_u64(&self) -> u64 {
        u64::from_be(self.0)
    }
}

impl std::fmt::Display for Guid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guid = self.as_u64();
        write!(
            f,
            "{:04x}:{:04x}:{:04x}:{:04x}",
            (guid >> 48) & 0xFFFF,
            (guid >> 32) & 0xFFFF,
            (guid >> 16) & 0xFFFF,
            guid & 0xFFFF
        )
    }
}

impl std::fmt::Debug for Guid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl Serialize for Guid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Guid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        let parts: Vec<_> = s.split(':').collect();
        if parts.len() != 4 {
            return Err(D::Error::custom("invalid GUID format"));
        }
        let mut guid: u64 = 0;
        for (i, part) in parts.iter().enumerate() {
            let value = u16::from_str_radix(part, 16)
                .map_err(|_| D::Error::custom("invalid hexadecimal value"))?;
            guid |= (value as u64) << (48 - i * 16);
        }
        Ok(Guid(guid.to_be()))
    }
}

impl JsonSchema for Guid {
    fn schema_name() -> Cow<'static, str> {
        "Guid".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "pattern": "^[0-9a-fA-F]{4}:[0-9a-fA-F]{4}:[0-9a-fA-F]{4}:[0-9a-fA-F]{4}$"
        })
    }
}

/// Firmware version information
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct FwVer(pub [u8; 64usize]);

impl std::fmt::Display for FwVer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                self.0.as_ptr(),
                self.0.iter().position(|&c| c == 0).unwrap_or(64),
            ))
        };
        f.write_str(s)
    }
}

impl std::fmt::Debug for FwVer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl Serialize for FwVer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

impl<'de> Deserialize<'de> for FwVer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let mut fw_ver = [0u8; 64];
        let bytes = s.as_bytes();
        for (i, &b) in bytes.iter().take(63).enumerate() {
            fw_ver[i] = b as _;
        }
        Ok(FwVer(fw_ver))
    }
}

impl JsonSchema for FwVer {
    fn schema_name() -> Cow<'static, str> {
        "FwVer".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
        })
    }
}
