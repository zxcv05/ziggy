<div align="center">
  <img src="./assets/ziggy.svg"
       alt="ziggy"
       width="15%">
    <div>Ziggy: High performance lock free MPMC channel implementation </div>
</div>

## Internals

lock free MPMC using sequence-per-slot (LMAX Disruptor style).

see [Dmitry Vyukov’s bounded MPMC spec](https://web.archive.org/web/20210803180930/https://www.1024cores.net/home/lock-free-algorithms/queues)

### memory layout

```
┌─────────────────────────────────────────────────────┐
│ head (producer cursor)          [cache line 0]      │
│ tail (consumer cursor)          [cache line 1]      │
│ epoch (wakeup counter)          [cache line 2]      │
│ shutdown flag                   [cache line 3]      │
│ waiter (mutex + cond + parked)  [cache line 4]      │
├─────────────────────────────────────────────────────┤
│ slots[0]: sequence | value      [cache line N]      │
│ slots[1]: sequence | value      [cache line N+1]    │
│ ...                                                 │
└─────────────────────────────────────────────────────┘
```

128-byte alignment prevents false sharing.

### slot lifecycle

1. `slot.seq == head`  → slot is writable (empty)
2. producer CAS head, writes value, sets `seq = head+1`
3. `slot.seq == tail+1` → slot is readable (has data)
4. consumer CAS tail, reads value, sets `seq = tail+mask+1`

### parking (epoch-based)

prevents lost wakeups without mutex in hot path:

- producer: write data → increment epoch → notify
- consumer: read epoch → lock → check (shutdown/data/epoch) → sleep or return

## Usage

```zig
const RingBuffer = @import("ziggy").RingBuffer;

var ring = try RingBuffer(u64).init(allocator, 512);
defer ring.deinit();

// producer
ring.produce(42);

// consumer
if (ring.tryConsume()) |val| { ... }
// or blocking:
if (ring.consume()) |val| { ... }

ring.close();
```

## build

```
zig build -Doptimize=ReleaseFast   # build
zig build test                      # run tests
zig build run -Doptimize=ReleaseFast  # run benchmarks
```

## Benchmarks

hardware details:

```
- CPU: Intel Core i5-13500 (13th Gen)
- Cores: 14 (6 P-cores + 8 E-cores)
- Threads: 20 (hyperthreading on P-cores)
- Max freq: 4.8 GHz
- L3 cache: 24 MiB
- RAM: 62 GB
```

10P/10C on 20-core machine, 100 runs each:

| metric     | ziggy     | crossbeam | diff |
|------------|-----------|-----------|------|
| throughput | 6.06 M/s  | 6.36 M/s  | -5%  |
| p50        | 0.046 ms  | 0.051 ms  | -9%  |
| p99        | 0.087 ms  | 0.090 ms  | -3%  |
| p99.9      | 0.151 ms  | 0.157 ms  | -4%  |
| p99.99     | 0.172 ms  | 0.176 ms  | -2%  |
| p99.999    | 1.052 ms  | 0.844 ms  | +25% |

![latency](benchmarks/latency.png)

<img src="benchmarks/throughput.png" width="50%">

The core channel code is relatively lean (< 200 LOC)

ziggy wins on latency (p50-p99.99), crossbeam wins on throughput and extreme tail (p99.999).

to run crossbeam benchmark:
```
cd benchmarks/crossbeam
cargo run --release
```
