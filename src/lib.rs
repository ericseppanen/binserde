//! Utilities for binary serialization/deserialization
//!
//! The [`BinSerDe`] trait allows us to define data structures
//! that can match data structures that are sent over the wire
//! in big-endian form with no packing.

use bincode::Options;
use serde::{Deserialize, Serialize};
use std::io::Write;

/// A shortcut that defines our method of binary serialization
///
/// Properties:
/// - Big endian
/// - Fixed integer encoding (i.e. 1u32 is 00000001 not 01)
/// - Allow trailing bytes: this means we don't throw an error
///   if the deserializer is passed a buffer with more data
///   past the end.
pub fn coder() -> impl Options {
    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}

/// Binary serialize/deserialize helper functions
///
pub trait BinSerDe<'de>: Serialize + Deserialize<'de> + Sized {
    /// Serialize into an existing buffer
    ///
    /// tip: `&mut [u8]` implements `Write`
    fn bser_into<W: Write>(&self, w: W) -> bincode::Result<()>;

    /// Serialize into a new buffer
    fn bser(&self) -> bincode::Result<Vec<u8>>;

    /// Deserialize
    fn bdes(buf: &'de [u8]) -> bincode::Result<Self>;
}

impl<'de, T> BinSerDe<'de> for T
where
    T: Serialize + Deserialize<'de> + Sized,
{
    /// Serialize into an existing buffer
    fn bser_into<W: Write>(&self, w: W) -> bincode::Result<()> {
        coder().serialize_into(w, &self)
    }

    /// Serialize into a new heap-allocated buffer
    fn bser(&self) -> bincode::Result<Vec<u8>> {
        coder().serialize(&self)
    }

    /// Deserialize
    fn bdes(buf: &'de [u8]) -> bincode::Result<Self> {
        coder().deserialize(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    pub struct ShortStruct {
        pub a: u8,
        pub b: u32,
    }

    #[test]
    fn short() {
        let x = ShortStruct { a: 7, b: 65536 };

        let encoded = x.bser().unwrap();

        assert_eq!(encoded, vec![7, 0, 1, 0, 0]);

        let raw = [8u8, 7, 3, 0, 0];
        let decoded: ShortStruct = coder().deserialize(&raw).unwrap();

        assert_eq!(
            decoded,
            ShortStruct {
                a: 8,
                b: 0x07030000
            }
        );

        // has trailing data
        let raw = [8u8, 7, 3, 0, 0, 0xFF, 0xFF, 0xFF];
        let _: ShortStruct = coder().deserialize(&raw).unwrap();
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    pub struct PgReplicationMsg {
        pub tag: u8,
        pub blockpos: u64,
        pub last_flush_position: u64,
        pub apply: u64,
        pub timestamp: u64,
        pub reply_requested: u8,
    }

    #[derive(Debug)]
    pub struct KeepAliveReply {
        pub blockpos: u64,
        pub timestamp: u64,
    }

    impl From<KeepAliveReply> for PgReplicationMsg {
        fn from(keepalive: KeepAliveReply) -> PgReplicationMsg {
            PgReplicationMsg {
                tag: 'r' as u8,
                blockpos: keepalive.blockpos,
                timestamp: keepalive.timestamp,
                last_flush_position: 0,
                apply: 0,
                reply_requested: 0,
            }
        }
    }

    #[test]
    fn keepalive_reply() {
        let msg = KeepAliveReply {
            blockpos: 0x1234,
            timestamp: 0x5678,
        };
        let msg = PgReplicationMsg::from(msg);

        let encoded = msg.bser().unwrap();

        #[rustfmt::skip] // organize the bytes one field at a time.
        let expected = [
            'r' as u8,
            0, 0, 0, 0, 0, 0, 18, 52,
            0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 86, 120,
            0,
        ];

        assert_eq!(expected.len(), 34);

        assert_eq!(encoded, expected);
    }
}
