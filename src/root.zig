const ring_buffer = @import("ring_buffer.zig");

pub const RingBuffer = ring_buffer.RingBuffer;
pub const cache_line = ring_buffer.cache_line;

test {
    _ = ring_buffer;
}
