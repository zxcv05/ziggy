//! Ziggy - High-performance MPMC ring buffer
//!
//! A thread-safe multi-producer multi-consumer ring buffer using
//! sequence-per-slot design (LMAX Disruptor style).

const ring_buffer = @import("ring_buffer.zig");

pub const RingBuffer = ring_buffer.RingBuffer;
pub const Waiter = ring_buffer.Waiter;
pub const cache_line = ring_buffer.cache_line;

test {
    @import("std").testing.refAllDecls(@This());
    _ = ring_buffer;
}
