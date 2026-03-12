//! # eventide — universal event engine
//!
//! One binary. Replaces Kafka, Redis Streams, RabbitMQ, and your webhook
//! dispatcher. Designed for teams that want durable event infrastructure
//! without the operational horror.
//!
//! ## Architecture
//!
//! ```text
//!  ┌──────────────────────────────────────────────────────┐
//!  │                    eventide server                   │
//!  │                                                      │
//!  │  ┌──────────┐  ┌──────────┐  ┌────────────────────┐ │
//!  │  │  broker  │  │  queue   │  │  webhook dispatch  │ │
//!  │  │ pub/sub  │  │  jobs    │  │  durable delivery  │ │
//!  │  └────┬─────┘  └────┬─────┘  └────────┬───────────┘ │
//!  │       └─────────────┴─────────────────┘             │
//!  │                      │                               │
//!  │               ┌──────▼──────┐                        │
//!  │               │  commit log │  ← mmap'd segments     │
//!  │               │  (durable)  │                        │
//!  │               └─────────────┘                        │
//!  │                                                      │
//!  │  ┌──────────────────────────────────────────────┐   │
//!  │  │          WASM plugin host (optional)          │   │
//!  │  └──────────────────────────────────────────────┘   │
//!  └──────────────────────────────────────────────────────┘
//!        ▲                              ▲
//!   HTTP/2 + msgpack              HTTP/1.1 webhooks
//! ```

#![deny(
    missing_docs,
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    unsafe_op_in_unsafe_fn
)]
#![allow(
    clippy::module_name_repetitions,
    clippy::must_use_candidate
)]

pub mod broker;
pub mod log;
pub mod net;
pub mod queue;
pub mod webhook;
pub mod plugin;

mod util;

pub use broker::{Broker, Topic, Subscription};
pub use log::{CommitLog, Offset, Record, SegmentConfig};
pub use queue::{JobQueue, Job, JobId, JobState};
pub use webhook::{WebhookDispatcher, Endpoint, DeliveryPolicy};
pub use util::error::{Error, Result};

pub use crate::net::config::Config;