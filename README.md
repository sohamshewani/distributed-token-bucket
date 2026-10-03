# distributed-token-bucket
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/distributed-token-bucket/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/distributed-token-bucket/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/distributed-token-bucket) [![Stars](https://img.shields.io/github/stars/sohamshewani/distributed-token-bucket?style=flat)](https://github.com/sohamshewani/distributed-token-bucket)

## Executive Summary & Problem Space

`distributed-token-bucket` is a high-throughput, low-latency rate-limiting engine engineered in modern C++20 for distributed infrastructure. In modern high-scale distributed systems, traditional rate-limiting mechanisms face severe performance cliffs. Centralized redis-backed token buckets or naive mutex-protected global counters introduce unacceptable p99 latency inflation, network serialization overhead, and thread synchronization bottlenecks.

This library resolves these issues by implementing a decentralized, lock-free per-core token bucket topology paired with an asynchronous reconciliation engine. By utilizing hardware-aligned ring buffers, zero-allocation memory allocators, and atomic batch synchronization, `distributed-token-bucket` achieves linear scalability across multi-socket, multi-core NUMA architectures while maintaining strict mathematical invariants over token distribution.

---

## Structural ASCII Architecture / Dataflow Diagram

```
 +-----------------------------------------------------------------------------------+
 |                              Client Request Threads                               |
 +-----------------------------------------------------------------------------------+
           | (Concurrent Token Acquisition via CAS / Atomic Operations)
           v
 +-----------------------------------------------------------------------------------+
 |                         include/engine.hpp (Core Engine)                          |
 |  +--------------------+     +---------------------+     +----------------------+  |
 |  | Local Thread Cache | --> | include/concurrency | --> | include/ring_buffer  |  |
 |  +--------------------+     +---------------------+     +----------------------+  |
 +-----------------------------------------------------------------------------------+
           | (Zero-Copy Batch Flush & State Reconciliation)
           v
 +-----------------------------------------------------------------------------------+
 |                       include/allocator.hpp (Arena Allocator)                     |
 +-----------------------------------------------------------------------------------+
           | (Hardware-Aligned Memory Pools: include/types.hpp)
           v
 +-----------------------------------------------------------------------------------+
 |                         src/driver.cpp & Target System                            |
 +-----------------------------------------------------------------------------------+
```

---

## Concurrency Model & Memory Invariants

The concurrency model of `distributed-token-bucket` is engineered around strict lock-free primitives and hardware-conscious memory layouts to eliminate cache thrashing:

* **Lock-Free Synchronization (`include/concurrency.hpp`)**: Relies exclusively on `std::atomic` operations with sequentially consistent or acquire-release memory orderings. Hot paths avoid blocking synchronization primitives (such as `std::mutex` or `std::shared_mutex`) entirely, preventing priority inversion and OS-level scheduler context switches.
* **Cache-Line Alignment (`include/types.hpp`)**: All per-core bucket states and ring buffer head/tail pointers are padded to the CPU cache line boundary (64 bytes) using `alignas(64)` to completely eliminate false sharing across adjacent hardware threads.
* **Memory Ownership & Zero-Allocation (`include/allocator.hpp`)**: The engine enforces a deterministic memory lifetime model. Pre-allocated arenas manage buffers without invoking the system allocator (`malloc`/`free`) during steady-state request processing, guaranteeing deterministic tail latencies.
* **Atomic Invariants**: The global and local token pools maintain the invariant that at any given instant, $\sum \text{LocalBuckets} + \text{GlobalReserve} \le \text{MaxCapacity}$, preventing token inflation under concurrent refill and consumption cycles.

---

## Performance Characteristics & Algorithmic Complexity Table

| Operation | Path Type | Time Complexity | Space Complexity | Estimated Latency (p99) |
| :--- | :--- | :--- | :--- | :--- |
| **Token Acquisition** | Local Fast-Path | $O(1)$ (Lock-free CAS) | $O(1)$ | $< 15\text{ns}$ |
| **Token Replenishment** | Batch Sync | $O(1)$ | $O(1)$ | $< 45\text{ns}$ |
| **Ring Buffer Push/Pop** | Lock-Free Queue | $O(1)$ | $O(N)$ | $< 20\text{ns}$ |
| **State Reconciliation** | Background Merge | $O(C)$ ($C = \text{Cores}$) | $O(C)$ | $< 120\text{ns}$ |

---

## Security Architecture & Threat Model

`distributed-token-bucket` is designed for deployment in hostile multi-tenant environments where rate-limiting mechanisms themselves can become vectors for denial-of-service (DoS) or resource exhaustion attacks.

* **Input Boundaries & Validation (`include/types.hpp`)**: All runtime configurations, token request weights, and burst parameters undergo rigorous bounds checking. Negative allocations, integer overflows during token replenishment, and malformed capacity requests trigger immediate safe degradation or exception propagation.
* **Memory Corruption Protections**: The codebase is compiled with strict compiler hardening flags (-D_FORTIFY_SOURCE=2, `-fstack-protector-strong`, `-Werror`, `-Wconversion`). Use of raw pointers is prohibited in favor of smart pointers, explicit span bounds, and arena-backed allocations.
* **Side-Channel & Race Mitigation**: Lock-free state transitions utilize explicit memory barriers (`std::memory_order_acq_rel`) to prevent compiler and processor instruction reordering, protecting against subtle race conditions that could lead to token leakage or bucket exhaustion bypasses.

---

## Build, Verification & Benchmarking Recipes

Ensure you have a C++20 compliant compiler (GCC 11+, Clang 13+) and GNU Make installed.

### 1. Build the Production Binaries and Drivers
```bash
make clean
make all
```

### 2. Execute the Invariant & Unit Test Suite
Verify thread safety, concurrency bounds, and algorithmic correctness using the test harness:
```bash
make test
```

### 3. Run the Micro-Benchmarks
Execute the Google Benchmark suite to profile throughput, CPU cycles per operation, and tail latencies under heavy concurrency:
```bash
make benchmark