//! Completed writes and shared RPC replies with local buffer recovery.

use crate::{Buffer, Error};

/// Whether an attached buffer set can be recovered locally.
///
/// `Available` includes an explicitly attached empty set. `Unavailable` does
/// not assert that DMA is still active: buffers may also have been recycled.
/// Recovery after an error permits reuse, but does not establish valid contents.
#[derive(Debug, Default)]
pub enum BufferAvailability {
    /// No buffer set was attached, or the set was already taken.
    #[default]
    NotAttached,
    /// The complete set is locally owned and available for reuse.
    Available(Vec<Buffer>),
    /// An attached set cannot be recovered at this point.
    Unavailable,
}

impl BufferAvailability {
    /// Borrows an available set, preserving the distinction from no attachment.
    #[must_use]
    pub fn available(&self) -> Option<&[Buffer]> {
        match self {
            Self::Available(buffers) => Some(buffers),
            Self::NotAttached | Self::Unavailable => None,
        }
    }

    /// Consumes the state and returns the set only when it is available.
    #[must_use]
    pub fn into_available(self) -> Option<Vec<Buffer>> {
        match self {
            Self::Available(buffers) => Some(buffers),
            Self::NotAttached | Self::Unavailable => None,
        }
    }

    /// Takes the current recovery state, leaving `NotAttached` in its place.
    #[must_use]
    pub fn take(&mut self) -> Self {
        std::mem::take(self)
    }
}

/// Receipt for a completed remote write, or an explicit no-transfer result.
///
/// Transfer success is separate from recovery of the server's source buffers.
/// This receipt does not prove how much of the destination was filled, nor
/// associate the completed write with a particular application response.
#[derive(Debug)]
pub struct WriteReceipt {
    sources: BufferAvailability,
}

impl WriteReceipt {
    pub(crate) fn new(sources: BufferAvailability) -> Self {
        Self { sources }
    }

    /// Local source ownership after the completed transfer.
    #[must_use]
    pub fn source_buffers(&self) -> &BufferAvailability {
        &self.sources
    }

    /// Recovers local sources; the receipt remains valid for `into_reply`.
    #[must_use]
    pub fn take_sources(&mut self) -> BufferAvailability {
        self.sources.take()
    }

    /// Builds a server reply and releases any retained source buffers.
    #[must_use]
    pub fn into_reply<T>(self, response: T) -> BufferReply<T> {
        BufferReply::new(response, BufferAvailability::NotAttached)
    }
}

/// RPC response with the recovery state of locally attached destinations.
///
/// A service declares `Result<BufferReply<T>, E>` and both its handler and client
/// return that exact type through the same service trait. The handler constructs
/// the reply from a completed [`WriteReceipt`]; its destinations are `NotAttached`
/// because they belong to the client. The client receives the response together
/// with the recovery state of its local destination buffers.
///
/// Only `T` is serialized. Local buffers are never part of the wire payload.
/// This type does not implement `Deserialize`: client calls attach their own
/// destination recovery state after decoding `T`.
#[derive(Debug)]
pub struct BufferReply<T> {
    response: T,
    destinations: BufferAvailability,
}

impl<T: serde::Serialize> serde::Serialize for BufferReply<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.response.serialize(serializer)
    }
}

impl<T: schemars::JsonSchema> schemars::JsonSchema for BufferReply<T> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        T::schema_name()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        T::schema_id()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        T::json_schema(generator)
    }

    fn inline_schema() -> bool {
        T::inline_schema()
    }
}

impl<T> BufferReply<T> {
    pub(crate) fn new(response: T, destinations: BufferAvailability) -> Self {
        Self {
            response,
            destinations,
        }
    }

    /// The application response.
    #[must_use]
    pub fn response(&self) -> &T {
        &self.response
    }

    /// Recovery of this call's locally attached destination set.
    ///
    /// Replies created from a [`WriteReceipt`] on the server report
    /// [`BufferAvailability::NotAttached`]. On the client this reports the
    /// availability of the destinations attached to the call.
    #[must_use]
    pub fn destinations(&self) -> &BufferAvailability {
        &self.destinations
    }

    /// Separates the response from destination recovery.
    #[must_use]
    pub fn into_parts(self) -> (T, BufferAvailability) {
        (self.response, self.destinations)
    }
}

/// Shared service and client return alias following a completed remote write.
pub type ResultBufferReply<T, E = Error> = Result<BufferReply<T>, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_write_with_unavailable_sources_still_builds_a_wire_reply() {
        let receipt = WriteReceipt::new(BufferAvailability::Unavailable);
        assert!(matches!(
            receipt.source_buffers(),
            BufferAvailability::Unavailable
        ));
        let reply = receipt.into_reply(17_u32);
        assert_eq!(*reply.response(), 17);
        assert!(matches!(
            reply.destinations(),
            BufferAvailability::NotAttached
        ));
        assert_eq!(
            rmp_serde::to_vec_named(&reply).unwrap(),
            rmp_serde::to_vec_named(&17_u32).unwrap()
        );
        assert_eq!(
            schemars::schema_for!(BufferReply<u32>),
            schemars::schema_for!(u32)
        );
    }

    #[test]
    fn taking_sources_preserves_the_completed_write_receipt() {
        let mut receipt = WriteReceipt::new(BufferAvailability::Available(Vec::new()));
        assert!(
            matches!(receipt.take_sources(), BufferAvailability::Available(buffers) if buffers.is_empty())
        );
        assert!(matches!(
            receipt.source_buffers(),
            BufferAvailability::NotAttached
        ));
        assert_eq!(serde_json::to_value(receipt.into_reply(7)).unwrap(), 7);
    }

    #[test]
    fn local_destination_recovery_is_not_serialized() {
        for destinations in [
            BufferAvailability::NotAttached,
            BufferAvailability::Available(Vec::new()),
            BufferAvailability::Unavailable,
        ] {
            let reply = BufferReply::new(23_u32, destinations);
            assert_eq!(serde_json::to_value(&reply).unwrap(), 23);
            assert_eq!(
                rmp_serde::to_vec_named(&reply).unwrap(),
                rmp_serde::to_vec_named(&23_u32).unwrap()
            );
        }
    }
}
