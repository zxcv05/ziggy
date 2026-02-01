# Ring Buffer Benchmark Results

## Current Implementation (mutex + condvar for parking)

| Config | Items | Time | Throughput |
|--------|-------|------|------------|
| 1P/1C | 1M | 22ms | 44M ops/sec |
| 4P/4C | 1M | 263ms | 3.8M ops/sec |
| 100P/100C | 1M | 404ms | 2.5M ops/sec |

## Failed Experiment: Lock-free Futex

Tried replacing mutex+condvar with atomic counter + raw futex:

| Config | Mutex+CV | Lock-free Futex |
|--------|----------|-----------------|
| 1P/1C | 44M | 30M (-32%) |
| 4P/4C | 3.8M | 1M (-74%) |
| 100P/100C | 2.5M | 0.7M (-72%) |

**Why slower:** The kernel's condvar is highly optimized. Our manual futex approach had overhead from atomic counter updates on every notify().

## Hardware
- Apple Silicon (12 cores)
- macOS

## Design
- Lock-free MPMC ring buffer
- Two-phase commit: reserve slot via CAS, write, publish via version
- Cache line alignment on head/tail to avoid false sharing
- Consumers park via condvar when ring empty, wake on notify
