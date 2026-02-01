const std = @import("std");
const ziggy = @import("ziggy");

const RingBuffer = ziggy.RingBuffer;

// ============================================================================
// BENCHMARKS (run with: zig build run -Doptimize=ReleaseFast)
// ============================================================================

const TimestampedMsg = struct {
    send_time: i128,
    id: u64,
};

const BenchRing = RingBuffer(TimestampedMsg);
const ThroughputRing = RingBuffer(u64);

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    const cpus = std.Thread.getCpuCount() catch 4;
    const threads_per_side: u32 = @intCast(@max(1, cpus / 2));

    std.debug.print("=== {d} CPUs detected, using {d}P/{d}C (no oversubscription) ===\n\n", .{cpus, threads_per_side, threads_per_side});

    std.debug.print("=== THROUGHPUT ===\n", .{});
    try runThroughputBenchmark(allocator, threads_per_side, threads_per_side, 1_000_000, 512);

    std.debug.print("\n=== LATENCY ===\n", .{});
    try runLatencyBenchmark(allocator, threads_per_side, threads_per_side, 100_000, 512);
}

fn runThroughputBenchmark(allocator: std.mem.Allocator, num_producers: u32, num_consumers: u32, total_items: u32, ring_size: u32) !void {
    var ring = try ThroughputRing.init(allocator, ring_size);
    defer ring.deinit();
    var consumed = std.atomic.Value(u32).init(0);
    const items_per_producer = total_items / num_producers;
    const actual_total = items_per_producer * num_producers;
    const producer_threads = try allocator.alloc(std.Thread, num_producers);
    defer allocator.free(producer_threads);
    const consumer_threads = try allocator.alloc(std.Thread, num_consumers);
    defer allocator.free(consumer_threads);
    const start = std.time.nanoTimestamp();
    for (producer_threads) |*t| t.* = try std.Thread.spawn(.{}, throughputProducer, .{ &ring, items_per_producer });
    for (consumer_threads) |*t| t.* = try std.Thread.spawn(.{}, throughputConsumer, .{ &ring, &consumed, actual_total });
    for (producer_threads) |t| t.join();
    ring.close();
    for (consumer_threads) |t| t.join();
    const elapsed_ns: u64 = @intCast(std.time.nanoTimestamp() - start);
    const ops_per_sec = if (elapsed_ns > 0) (@as(u64, actual_total) * 1_000_000_000) / elapsed_ns else 0;
    std.debug.print("{d}P/{d}C: {d} ops/sec\n", .{num_producers, num_consumers, ops_per_sec});
}

fn throughputProducer(ring: *ThroughputRing, count: u32) void {
    for (0..count) |i| ring.produce(i);
}

fn throughputConsumer(ring: *ThroughputRing, consumed: *std.atomic.Value(u32), total: u32) void {
    while (consumed.load(.monotonic) < total) {
        if (ring.consume()) |_| _ = consumed.fetchAdd(1, .monotonic);
    }
}

fn runLatencyBenchmark(allocator: std.mem.Allocator, num_producers: u32, num_consumers: u32, total_items: u32, ring_size: u32) !void {
    var ring = try BenchRing.init(allocator, ring_size);
    defer ring.deinit();

    const max_samples = total_items;
    var latencies = try allocator.alloc(i128, max_samples);
    defer allocator.free(latencies);
    @memset(latencies, 0);

    var latency_idx = std.atomic.Value(u32).init(0);
    var consumed = std.atomic.Value(u32).init(0);

    const items_per_producer = total_items / num_producers;
    const actual_total = items_per_producer * num_producers;

    const producer_threads = try allocator.alloc(std.Thread, num_producers);
    defer allocator.free(producer_threads);
    const consumer_threads = try allocator.alloc(std.Thread, num_consumers);
    defer allocator.free(consumer_threads);

    const start = std.time.nanoTimestamp();

    for (producer_threads) |*t| {
        t.* = try std.Thread.spawn(.{}, latencyProducer, .{ &ring, items_per_producer });
    }

    for (consumer_threads) |*t| {
        t.* = try std.Thread.spawn(.{}, latencyConsumer, .{ &ring, &consumed, actual_total, latencies, &latency_idx });
    }

    for (producer_threads) |t| t.join();
    ring.close();
    for (consumer_threads) |t| t.join();

    const end = std.time.nanoTimestamp();

    const num_samples = @min(latency_idx.load(.monotonic), max_samples);
    if (num_samples == 0) {
        std.debug.print("{d}P/{d}C: No samples collected\n", .{ num_producers, num_consumers });
        return;
    }

    std.mem.sort(i128, latencies[0..num_samples], {}, std.sort.asc(i128));

    const min_lat = latencies[0];
    const p50 = latencies[num_samples / 2];
    const p99 = latencies[(num_samples * 99) / 100];
    const p999 = latencies[(num_samples * 999) / 1000];
    const max_lat = latencies[num_samples - 1];

    var sum: i128 = 0;
    for (latencies[0..num_samples]) |l| sum += l;
    const avg = @divTrunc(sum, num_samples);

    const elapsed_ms = @divTrunc(end - start, 1_000_000);

    std.debug.print("{d}P/{d}C ({d} samples, {d}ms total):\n", .{num_producers, num_consumers, num_samples, elapsed_ms});
    std.debug.print("  min: {d}µs, avg: {d}µs, p50: {d}µs\n", .{@divTrunc(min_lat, 1000), @divTrunc(avg, 1000), @divTrunc(p50, 1000)});
    std.debug.print("  p99: {d}µs, p99.9: {d}µs, max: {d}µs\n\n", .{@divTrunc(p99, 1000), @divTrunc(p999, 1000), @divTrunc(max_lat, 1000)});
}

fn latencyProducer(ring: *BenchRing, count: u32) void {
    for (0..count) |i| {
        ring.produce(.{ .send_time = std.time.nanoTimestamp(), .id = i });
    }
}

fn latencyConsumer(ring: *BenchRing, consumed: *std.atomic.Value(u32), total: u32, latencies: []i128, latency_idx: *std.atomic.Value(u32)) void {
    while (consumed.load(.monotonic) < total) {
        if (ring.consume()) |msg| {
            const idx = latency_idx.fetchAdd(1, .monotonic);
            if (idx < latencies.len) latencies[idx] = std.time.nanoTimestamp() - msg.send_time;
            _ = consumed.fetchAdd(1, .monotonic);
        }
    }
}
