#include "driver.hpp"

#include <iostream>
#include <chrono>
#include <thread>
#include <cstring>
#include <system_error>

namespace distributed_token_bucket {

Driver::Driver(const DriverConfig& config)
    : config_(config),
      running_(false),
      metrics_{0, 0, 0, 0, 0} {
    
    // Validate configuration
    if (config_.node_id == 0) {
        throw std::invalid_argument("Node ID cannot be zero");
    }
    if (config_.capacity == 0) {
        throw std::invalid_argument("Bucket capacity must be greater than zero");
    }

    // Initialize state
    state_.tokens.store(config_.capacity * 1000, std::memory_order_relaxed);
    state_.last_refill_timestamp_ns.store(get_monotonic_time_ns(), std::memory_order_relaxed);

    // Initialize metrics ring buffer
    metrics_buffer_ = std::make_unique<RingBuffer<DriverMetricEvent, 1024>>();
}

Driver::~Driver() {
    stop();
}

bool Driver::initialize() {
    // Perform pre-allocation checks and zero-allocation hot-path validation
    uint64_t initial_time = get_monotonic_time_ns();
    if (initial_time == 0) {
        return false;
    }

    // Initialize thread-local batch state
    local_batch_tokens_.store(0, std::memory_order_relaxed);
    local_batch_last_flush_ns_.store(initial_time, std::memory_order_relaxed);

    return true;
}

void Driver::start() {
    if (running_.exchange(true, std::memory_order_acq_rel)) {
        return; // Already running
    }

    refill_thread_ = std::thread(&Driver::refill_loop, this);
    telemetry_thread_ = std::thread(&Driver::telemetry_loop, this);
}

void Driver::stop() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        return; // Already stopped
    }

    if (refill_thread_.joinable()) {
        refill_thread_.join();
    }
    if (telemetry_thread_.joinable()) {
        telemetry_thread_.join();
    }
}

bool Driver::acquire_token(uint64_t tokens) {
    if (tokens == 0) {
        return true;
    }

    uint64_t scaled_request = tokens * 1000;
    
    // Hot path: try to satisfy from thread-local batch or direct atomic state CAS
    TokenBucketState current = load_state();
    uint64_t now = get_monotonic_time_ns();

    while (true) {
        uint64_t elapsed_ns = (now >= current.last_refill_timestamp_ns) ? (now - current.last_refill_timestamp_ns) : 0;
        uint64_t refill = (elapsed_ns * config_.refill_rate_per_sec * 1000) / 1000000000ULL;
        uint64_t max_scaled_capacity = config_.capacity * 1000;
        uint64_t calculated_tokens = std::min(max_scaled_capacity, current.tokens + refill);

        if (calculated_tokens < scaled_request) {
            metrics_.failed_requests.fetch_add(1, std::memory_order_relaxed);
            return false; // Insufficient tokens
        }

        TokenBucketState next;
        next.tokens = calculated_tokens - scaled_request;
        next.last_refill_timestamp_ns = now;

        // Attempt CAS update
        uint64_t expected_tokens = current.tokens;
        uint64_t expected_ts = current.last_refill_timestamp_ns;

        // Use a 128-bit CAS simulation via atomic compare_exchange on packed representation
        // Since TokenBucketState is 128-bit, we use atomic operations on individual fields or aligned structures if available.
        // Here we apply CAS directly on tokens and update timestamp atomically per step.
        if (state_.tokens.compare_exchange_weak(expected_tokens, next.tokens,
                                                std::memory_order_acq_rel,
                                                std::memory_order_acquire)) {
            state_.last_refill_timestamp_ns.store(next.last_refill_timestamp_ns, std::memory_order_release);
            metrics_.successful_requests.fetch_add(1, std::memory_order_relaxed);
            metrics_.tokens_consumed.fetch_add(tokens, std::memory_order_relaxed);
            return true;
        }

        // Reload current state for retry
        current = load_state();
        now = get_monotonic_time_ns();
    }
}

DriverMetrics Driver::get_metrics() const {
    DriverMetrics m;
    m.successful_requests = metrics_.successful_requests.load(std::memory_order_relaxed);
    m.failed_requests = metrics_.failed_requests.load(std::memory_order_relaxed);
    m.tokens_consumed = metrics_.tokens_consumed.load(std::memory_order_relaxed);
    m.active_partitions = metrics_.active_partitions.load(std::memory_order_relaxed);
    m.clock_skew_ms = metrics_.clock_skew_ms.load(std::memory_order_relaxed);
    return m;
}

void Driver::refill_loop() {
    while (running_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::microseconds(config_.refill_interval_us));
        
        uint64_t now = get_monotonic_time_ns();
        TokenBucketState current = load_state();
        
        uint64_t elapsed_ns = (now >= current.last_refill_timestamp_ns) ? (now - current.last_refill_timestamp_ns) : 0;
        if (elapsed_ns == 0) {
            continue;
        }

        uint64_t refill = (elapsed_ns * config_.refill_rate_per_sec * 1000) / 1000000000ULL;
        uint64_t max_scaled_capacity = config_.capacity * 1000;
        
        uint64_t new_tokens = std::min(max_scaled_capacity, current.tokens + refill);

        uint64_t expected_tokens = current.tokens;
        if (state_.tokens.compare_exchange_strong(expected_tokens, new_tokens,
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
            state_.last_refill_timestamp_ns.store(now, std::memory_order_release);
        }
    }
}

void Driver::telemetry_loop() {
    while (running_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        DriverMetricEvent event;
        event.timestamp_ns = get_monotonic_time_ns();
        event.tokens_remaining = state_.tokens.load(std::memory_order_relaxed) / 1000;

        // Push to ring buffer with drop-tail handling on overflow
        if (!metrics_buffer_->push(event)) {
            // Adaptive shedding / drop-tail counter increment
            metrics_.failed_requests.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

TokenBucketState Driver::load_state() const {
    TokenBucketState s;
    s.tokens = state_.tokens.load(std::memory_order_acquire);
    s.last_refill_timestamp_ns = state_.last_refill_timestamp_ns.load(std::memory_order_acquire);
    return s;
}

uint64_t Driver::get_monotonic_time_ns() const noexcept {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
}

} // namespace distributed_token_bucket