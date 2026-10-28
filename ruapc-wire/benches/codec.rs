//! Device-free measurements through the wire crate's public APIs.
//!
//! Run with `cargo bench -p ruapc-wire --bench codec`. Each selected case uses
//! `RUAPC_CODEC_BENCH_WARMUP` warmups (default 1,000), followed by
//! `RUAPC_CODEC_BENCH_ITERS` measured operations (default 100,000).
//! `RUAPC_CODEC_BENCH_FILTER` optionally selects a case-name substring.
//!
//! Fixtures and correctness checks are outside timing. Encoding allocates its
//! final frame on every operation, including any growth. Message decoding
//! clones a pre-encoded body, parses borrowed bytes, and advances the owned
//! byte view to the payload. It includes metadata/payload allocations and
//! their destruction; it excludes stream-header parsing. Chunk decoding creates
//! a fresh decoder each time: whole frames share input storage, split headers
//! allocate only partial-header storage, and split bodies use the decoder's
//! actual assembly allocation and copies. Input chunks are cloned without
//! copying their bytes. Every case includes destruction of its outputs.
//! RDMA decoding measures one four-frame aggregate per operation, without
//! parsing RPC metadata, collecting frames or posting work.
//! Times are wall time per operation, not latency distributions or RPC timings.

use std::{hint::black_box, time::Instant};

use bytes::{Buf, Bytes};
use ruapc_wire::{CodecError, MsgFlags, MsgMeta, deserialize_payload, frame, parse_body};

const STREAM_HEADER_LEN: usize = 8;
const AGGREGATE_FRAMES: usize = 4;

struct Config {
    iters: usize,
    warmup: usize,
    filter: Option<String>,
}

impl Config {
    fn from_env() -> Self {
        let iters = read_usize("RUAPC_CODEC_BENCH_ITERS", 100_000);
        assert!(iters > 0, "RUAPC_CODEC_BENCH_ITERS must be nonzero");
        Self {
            iters,
            warmup: read_usize("RUAPC_CODEC_BENCH_WARMUP", 1_000),
            filter: optional_env("RUAPC_CODEC_BENCH_FILTER"),
        }
    }

    fn measure(&self, name: &str, mut operation: impl FnMut()) -> usize {
        if self
            .filter
            .as_deref()
            .is_some_and(|filter| !name.contains(filter))
        {
            return 0;
        }
        for _ in 0..self.warmup {
            operation();
        }
        let start = Instant::now();
        for _ in 0..self.iters {
            operation();
        }
        #[allow(clippy::cast_precision_loss)]
        let ns_per_op = start.elapsed().as_secs_f64() * 1e9 / self.iters as f64;
        println!("{name:<35} {ns_per_op:>10.2} ns/op");
        1
    }
}

fn optional_env(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => panic!("invalid {name}: {error}"),
    }
}

fn read_usize(name: &str, default: usize) -> usize {
    optional_env(name).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a nonnegative integer"))
    })
}

fn metadata(msgpack: bool) -> MsgMeta {
    MsgMeta {
        method: "Echo/echo".into(),
        flags: MsgFlags::IsReq
            | if msgpack {
                MsgFlags::UseMessagePack
            } else {
                MsgFlags::empty()
            },
        msgid: 42,
        timeout_ms: 1_500,
        ..Default::default()
    }
}

fn benchmark_messages(config: &Config) -> usize {
    let mut cases = 0;
    for (format, msgpack) in [("msgpack", true), ("json", false)] {
        let meta = metadata(msgpack);
        for size in [16, 4096] {
            let payload = "x".repeat(size);
            let encoded = frame::encode(&meta, &payload).unwrap();
            let body = encoded.slice(STREAM_HEADER_LEN..);
            let mut owned = body.clone();
            let (parsed_meta, payload_offset) = parse_body(&owned).unwrap();
            owned.advance(payload_offset);
            assert_eq!(parsed_meta, meta);
            assert_eq!(
                deserialize_payload::<String>(&owned, parsed_meta.flags).unwrap(),
                payload
            );
            drop(parsed_meta);
            drop(owned);

            cases += config.measure(&format!("encode/{format}/{size}B"), || {
                drop(black_box(
                    frame::encode(black_box(&meta), black_box(&payload)).unwrap(),
                ));
            });
            cases += config.measure(&format!("decode/{format}/{size}B"), || {
                let mut owned = black_box(body.clone());
                let (parsed_meta, payload_offset) = parse_body(&owned).unwrap();
                owned.advance(payload_offset);
                let decoded =
                    deserialize_payload::<String>(&owned, black_box(&parsed_meta).flags).unwrap();
                drop(black_box(decoded));
                drop(parsed_meta);
                drop(owned);
            });
        }
    }
    cases
}

fn decode_whole(encoded: &Bytes) -> Bytes {
    let mut decoder = frame::FrameDecoder::default();
    let mut input = encoded.clone();
    decoder.decode(black_box(&mut input)).unwrap().unwrap()
}

fn decode_split(first: &Bytes, rest: &Bytes) -> Bytes {
    let mut decoder = frame::FrameDecoder::default();
    let mut first = first.clone();
    let mut rest = rest.clone();
    drop(black_box(decoder.decode(black_box(&mut first)).unwrap()));
    decoder.decode(black_box(&mut rest)).unwrap().unwrap()
}

fn benchmark_chunks(config: &Config) -> usize {
    let encoded = frame::encode(&metadata(true), &"x".repeat(4096)).unwrap();
    let expected = encoded.slice(STREAM_HEADER_LEN..);
    let whole = decode_whole(&encoded);
    assert_eq!(whole, expected);
    assert_eq!(whole.as_ptr(), expected.as_ptr());
    drop(whole);

    let header_first = encoded.slice(..3);
    let header_rest = encoded.slice(3..);
    let split_header = decode_split(&header_first, &header_rest);
    assert_eq!(split_header, expected);
    assert_eq!(split_header.as_ptr(), expected.as_ptr());
    drop(split_header);

    let body_first = encoded.slice(..encoded.len() / 2);
    let body_rest = encoded.slice(encoded.len() / 2..);
    assert_eq!(decode_split(&body_first, &body_rest), expected);

    let mut cases = config.measure("chunks/whole/4096B", || {
        drop(black_box(decode_whole(black_box(&encoded))));
    });
    cases += config.measure("chunks/header_split/4096B", || {
        drop(black_box(decode_split(
            black_box(&header_first),
            black_box(&header_rest),
        )));
    });
    cases += config.measure("chunks/body_split/4096B", || {
        drop(black_box(decode_split(
            black_box(&body_first),
            black_box(&body_rest),
        )));
    });
    cases
}

fn benchmark_rdma(config: &Config) -> usize {
    let encoded = frame::encode(&metadata(true), &"x".repeat(16)).unwrap();
    let body = encoded.slice(STREAM_HEADER_LEN..);
    let mut aggregate = Vec::with_capacity(AGGREGATE_FRAMES * encoded.len());
    for _ in 0..AGGREGATE_FRAMES {
        aggregate.extend_from_slice(&encoded);
    }
    let aggregate = Bytes::from(aggregate);
    let mut decoded = 0;
    frame::decode_frames::<CodecError>(&aggregate, |message| {
        assert_eq!(message, body);
        decoded += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(decoded, AGGREGATE_FRAMES);

    config.measure("frame/decode/4_frames", || {
        frame::decode_frames::<CodecError>(black_box(&aggregate), |frame| {
            drop(black_box(frame));
            Ok(())
        })
        .unwrap();
    })
}

pub fn main() {
    let config = Config::from_env();
    println!(
        "Wire codecs: {} iterations, {} warmups; filter={:?}",
        config.iters, config.warmup, config.filter,
    );
    println!(
        "Encode includes final-frame allocation/growth/drop; message decode includes owned decoding/drop."
    );
    println!(
        "Chunk cases create a decoder per operation; only fragmented storage is allocated. RDMA op = four frames."
    );
    let cases = benchmark_messages(&config) + benchmark_chunks(&config) + benchmark_rdma(&config);
    assert!(cases > 0, "RUAPC_CODEC_BENCH_FILTER matched no cases");
}
