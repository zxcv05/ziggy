const std = @import("std");

// 128 bytes to handle x86 adjacent-line prefetch (effectively 128-byte coherence unit)
const cache_line = 128;

fn RingBuffer(comptime T: type) type {
    return struct {
        const Self = @This();

        // Pad entry to cache line to prevent false sharing between adjacent slots
        const Entry = struct {
            value: T align(cache_line) = undefined,
            version: u64 = 0,
        };

        ring: []Entry,
        allocator: std.mem.Allocator,
        mask: u32, // For fast modulo with power-of-2 sizes
        encoded_version: u64 align(cache_line) = 0,
        consumed_index: u64 align(cache_line) = 0,
        shutdown: bool align(cache_line) = false,

        pub fn init(allocator: std.mem.Allocator, size: u32) !Self {
            // Ensure power of 2 for fast masking
            std.debug.assert(size > 0 and (size & (size - 1)) == 0);
            const ring = try allocator.alloc(Entry, size);
            @memset(ring, Entry{});
            return .{
                .ring = ring,
                .allocator = allocator,
                .mask = size - 1,
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.ring);
            self.* = undefined;
        }

        pub fn produce(self: *Self, value: T) void {
            const slot = while (true) {
                const current = @atomicLoad(u64, &self.encoded_version, .monotonic);
                const write_idx: u32 = @truncate(current);
                const version: u32 = @truncate(current >> 32);

                const consumed: u32 = @truncate(@atomicLoad(u64, &self.consumed_index, .monotonic));
                if (write_idx -% consumed > self.mask) {
                    std.atomic.spinLoopHint();
                    continue;
                }

                const new_idx = write_idx +% 1;
                const new_version = version +% 1;
                const new_encoded = @as(u64, new_idx) | (@as(u64, new_version) << 32);

                if (@cmpxchgWeak(
                    u64,
                    &self.encoded_version,
                    current,
                    new_encoded,
                    .acquire,
                    .monotonic,
                ) == null) {
                    break .{ .idx = write_idx, .ver = new_version };
                }
            };

            const entry = &self.ring[slot.idx & self.mask];
            entry.value = value;
            @atomicStore(u64, &entry.version, slot.ver, .release);
        }

        pub fn close(self: *Self) void {
            @atomicStore(bool, &self.shutdown, true, .release);
        }

        pub fn tryConsume(self: *Self) ?T {
            while (true) {
                const consumed: u64 = @atomicLoad(u64, &self.consumed_index, .monotonic);
                const consumed_idx: u32 = @truncate(consumed);
                const write_state = @atomicLoad(u64, &self.encoded_version, .monotonic);
                const write_idx: u32 = @truncate(write_state);

                if (consumed_idx == write_idx) {
                    return null;
                }

                if (@cmpxchgWeak(
                    u64,
                    &self.consumed_index,
                    consumed,
                    consumed +% 1,
                    .acquire,
                    .monotonic,
                ) == null) {
                    const slot_idx = consumed_idx & self.mask;
                    const expected_version: u64 = consumed_idx +% 1;

                    const entry = &self.ring[slot_idx];
                    while (@atomicLoad(u64, &entry.version, .acquire) < expected_version) {
                        std.atomic.spinLoopHint();
                    }

                    return entry.value;
                }
                // CAS failed, retry immediately
            }
        }

        pub fn consume(self: *Self) ?T {
            // Fast path - try immediately
            if (self.tryConsume()) |value| {
                return value;
            }

            // Spin until data available or shutdown
            while (!@atomicLoad(bool, &self.shutdown, .acquire)) {
                if (self.tryConsume()) |value| return value;
                std.atomic.spinLoopHint();
            }

            return self.tryConsume();
        }
    };
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    std.debug.print("=== Ring Buffer Benchmark ===\n\n", .{});

    try runBenchmark(allocator, 20, 20, 1_000_000, 512);
}

const Ring = RingBuffer(u64);

fn runBenchmark(
    allocator: std.mem.Allocator,
    num_producers: u32,
    num_consumers: u32,
    total_items: u32,
    ring_size: u32,
) !void {
    var ring = try Ring.init(allocator, ring_size);
    defer ring.deinit();

    var consumed = std.atomic.Value(u32).init(0);
    const items_per_producer = total_items / num_producers;
    const actual_total = items_per_producer * num_producers;

    const producer_threads = try allocator.alloc(std.Thread, num_producers);
    defer allocator.free(producer_threads);
    const consumer_threads = try allocator.alloc(std.Thread, num_consumers);
    defer allocator.free(consumer_threads);

    const start = std.time.nanoTimestamp();

    for (producer_threads) |*t| {
        t.* = try std.Thread.spawn(.{}, benchProducer, .{ &ring, items_per_producer });
    }

    for (consumer_threads) |*t| {
        t.* = try std.Thread.spawn(.{}, benchConsumer, .{ &ring, &consumed, actual_total });
    }

    for (producer_threads) |t| t.join();
    ring.close(); // Signal consumers to stop after all producers done
    for (consumer_threads) |t| t.join();

    const end = std.time.nanoTimestamp();
    const elapsed_ns: u64 = @intCast(end - start);
    const elapsed_ms = elapsed_ns / 1_000_000;
    const ops_per_sec = if (elapsed_ns > 0) (@as(u64, actual_total) * 1_000_000_000) / elapsed_ns else 0;

    std.debug.print("{d}P/{d}C: {d} items in {d}ms = {d} ops/sec\n", .{
        num_producers,
        num_consumers,
        actual_total,
        elapsed_ms,
        ops_per_sec,
    });
}

fn benchProducer(ring: *Ring, count: u32) void {
    for (0..count) |i| {
        ring.produce(i);
    }
}

fn benchConsumer(ring: *Ring, consumed: *std.atomic.Value(u32), total: u32) void {
    while (consumed.load(.monotonic) < total) {
        if (ring.consume()) |_| {
            _ = consumed.fetchAdd(1, .monotonic);
        }
    }
}
