const std = @import("std");

const cache_line = 128;

fn RingBuffer(comptime T: type) type {
    return struct {
        const Self = @This();

        // Each slot has its own sequence number - reduces global contention
        const Slot = struct {
            sequence: u64 align(cache_line) = 0,
            value: T = undefined,
        };

        slots: []Slot,
        mask: u64,
        head: u64 align(cache_line) = 0,
        tail: u64 align(cache_line) = 0,
        shutdown: bool align(cache_line) = false,
        allocator: std.mem.Allocator,

        pub fn init(allocator: std.mem.Allocator, size: u32) !Self {
            std.debug.assert(size > 0 and (size & (size - 1)) == 0);
            const slots = try allocator.alloc(Slot, size);
            for (slots, 0..) |*slot, i| {
                slot.sequence = i;
                slot.value = undefined;
            }
            return .{
                .slots = slots,
                .mask = size - 1,
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.slots);
            self.* = undefined;
        }

        pub fn produce(self: *Self, value: T) void {
            var backoff: u8 = 0;
            while (true) {
                const head = @atomicLoad(u64, &self.head, .monotonic);
                const slot = &self.slots[head & self.mask];

                const seq = @atomicLoad(u64, &slot.sequence, .acquire);
                const diff = @as(i64, @bitCast(seq)) - @as(i64, @bitCast(head));

                if (diff == 0) {
                    if (@cmpxchgWeak(u64, &self.head, head, head +% 1, .monotonic, .monotonic) == null) {
                        slot.value = value;
                        @atomicStore(u64, &slot.sequence, head +% 1, .release);
                        return;
                    }
                } else if (diff < 0) {
                    if (backoff < 6) {
                        for (0..(@as(u32, 1) << @as(u5, @intCast(backoff)))) |_| std.atomic.spinLoopHint();
                        backoff += 1;
                    } else {
                        std.atomic.spinLoopHint();
                    }
                }
            }
        }

        pub fn close(self: *Self) void {
            @atomicStore(bool, &self.shutdown, true, .release);
        }

        pub fn tryConsume(self: *Self) ?T {
            const tail = @atomicLoad(u64, &self.tail, .monotonic);
            const slot = &self.slots[tail & self.mask];

            const seq = @atomicLoad(u64, &slot.sequence, .acquire);
            const diff = @as(i64, @bitCast(seq)) - @as(i64, @bitCast(tail +% 1));

            if (diff == 0) {
                if (@cmpxchgWeak(u64, &self.tail, tail, tail +% 1, .monotonic, .monotonic) == null) {
                    const value = slot.value;
                    @atomicStore(u64, &slot.sequence, tail +% self.mask +% 1, .release);
                    return value;
                }
            }
            return null;
        }

        pub fn consume(self: *Self) ?T {
            if (self.tryConsume()) |v| return v;

            var backoff: u8 = 0;
            while (!@atomicLoad(bool, &self.shutdown, .acquire)) {
                if (self.tryConsume()) |v| return v;

                if (backoff < 6) {
                    for (0..(@as(u32, 1) << @as(u5, @intCast(backoff)))) |_| std.atomic.spinLoopHint();
                    backoff += 1;
                } else {
                    std.atomic.spinLoopHint();
                    backoff = 4;
                }
            }
            return self.tryConsume();
        }
    };
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    std.debug.print("=== Ring Buffer Benchmark ===\n\n", .{});
    try runBenchmark(allocator, 1, 1, 1_000_000, 256);
    try runBenchmark(allocator, 4, 4, 1_000_000, 256);
    try runBenchmark(allocator, 20, 20, 1_000_000, 512);
    try runBenchmark(allocator, 100, 100, 1_000_000, 1024);
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
    ring.close();
    for (consumer_threads) |t| t.join();

    const end = std.time.nanoTimestamp();
    const elapsed_ns: u64 = @intCast(end - start);
    const elapsed_ms = elapsed_ns / 1_000_000;
    const ops_per_sec = if (elapsed_ns > 0) (@as(u64, actual_total) * 1_000_000_000) / elapsed_ns else 0;

    std.debug.print("{d}P/{d}C: {d} items in {d}ms = {d} ops/sec\n", .{
        num_producers, num_consumers, actual_total, elapsed_ms, ops_per_sec,
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
