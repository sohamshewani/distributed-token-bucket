#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <mutex>
#include <map>
#include <cassert>
#include <cmath>
#include <algorithm>
#include <random>
#include <sstream>

// ============================================================================
// SYSTEM UNDER TEST: distributed-token-bucket (Inline Implementation for Test)
// ============================================================================

class MockClock {
public:
    virtual ~MockClock() = default;
    virtual uint64_t now_ms() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }
};

class SimulatedClock : public MockClock {
private:
    mutable std::mutex mtx_;
    uint64_t current_time_ms_;

public:
    explicit SimulatedClock(uint64_t start_time_ms = 1000000) 
        : current_time_ms_(start_time_ms) {}

    uint64_t now_ms() const override {
        std::lock_guard<std::mutex> lock(mtx_);
        return current_time_ms_;
    }

    void advance(uint64_t ms) {
        std::lock_guard<std::mutex> lock(mtx_);
        current_time_ms_ += ms;
    }

    void set(uint64_t ms) {
        std::lock_guard<std::mutex> lock(mtx_);
        current_time_ms_ = ms;
    }
};

struct TokenBucketConfig {
    double capacity;
    double refill_rate_per_sec;
    std::string node_id;
};

class DistributedTokenBucket {
private:
    TokenBucketConfig config_;
    std::shared_ptr<MockClock> clock_;
    mutable std::mutex mtx_;
    
    double tokens_;
    uint64_t last_update_ms_;
    uint64_t version_;
    bool is_failed_;

    void replenish_unlocked(uint64_t now) {
        if (now <= last_update_ms_) return;
        uint64_t delta_ms = now - last_update_ms_;
        double generated = (delta_ms / 1000.0) * config_.refill_rate_per_sec;
        tokens_ = std::min(config_.capacity, tokens_ + generated);
        last_update_ms_ = now;
    }

public:
    DistributedTokenBucket(TokenBucketConfig config, std::shared_ptr<MockClock> clock)
        : config_(std::move(config)),
          clock_(std::move(clock)),
          tokens_(config_.capacity),
          last_update_ms_(clock_->now_ms()),
          version_(0),
          is_failed_(false) {}

    bool consume(double count) {
        if (count <= 0.0) return true;
        
        std::lock_guard<std::mutex> lock(mtx_);
        if (is_failed_) return false;

        uint64_t now = clock_->now_ms();
        replenish_unlocked(now);

        if (tokens_ >= count) {
            tokens_ -= count;
            version_++;
            return true;
        }
        return false;
    }

    void force_sync(double external_tokens, uint64_t external_timestamp, uint64_t external_version) {
        std::lock_guard<std::mutex> lock(mtx_);
        if (external_version >= version_) {
            tokens_ = std::clamp(external_tokens, 0.0, config_.capacity);
            last_update_ms_ = std::max(last_update_ms_, external_timestamp);
            version_ = external_version + 1;
        }
    }

    void set_failed(bool failed) {
        std::lock_guard<std::mutex> lock(mtx_);
        is_failed_ = failed;
    }

    double get_tokens() const {
        std::lock_guard<std::mutex> lock(mtx_);
        // Note: const method observing state lazily replenishes if we cast away const,
        // but to keep it pure or testable, we simulate the math locally without mutating.
        uint64_t now = clock_->now_ms();
        if (now <= last_update_ms_) return tokens_;
        uint64_t delta_ms = now - last_update_ms_;
        double generated = (delta_ms / 1000.0) * config_.refill_rate_per_sec;
        return std::min(config_.capacity, tokens_ + generated);
    }

    uint64_t get_version() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return version_;
    }

    bool is_failed() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return is_failed_;
    }
};

// ============================================================================
// TEST SUITE FRAMEWORK & ASSERTIONS
// ============================================================================

#define RUN_TEST(test_func) \
    do { \
        std::cout << "[ RUN      ] " << #test_func << std::endl; \
        try { \
            test_func(); \
            std::cout << "[       OK ] " << #test_func << std::endl; \
        } catch (const std::exception& e) { \
            std::cerr << "[  FAILED  ] " << #test_func << " with exception: " << e.what() << std::endl; \
            std::exit(1); \
        } catch (...) { \
            std::cerr << "[  FAILED  ] " << #test_func << " with unknown exception." << std::endl; \
            std::exit(1); \
        } \
    } while(0)

// ============================================================================
// UNIT TESTS: Boundary & Edge-Cases
// ============================================================================

void test_initialization_and_capacity() {
    auto clock = std::make_shared<SimulatedClock>(1000);
    TokenBucketConfig config{100.0, 10.0, "node-1"};
    DistributedTokenBucket bucket(config, clock);

    assert(bucket.get_tokens() == 100.0);
    assert(bucket.get_version() == 0);
    assert(!bucket.is_failed());
}

void test_zero_and_negative_consumption() {
    auto clock = std::make_shared<SimulatedClock>(1000);
    TokenBucketConfig config{10.0, 1.0, "node-1"};
    DistributedTokenBucket bucket(config, clock);

    // Consume zero tokens (should always succeed, no state change)
    assert(bucket.consume(0.0) == true);
    assert(bucket.get_tokens() == 10.0);
    assert(bucket.get_version() == 0);

    // Consume negative tokens (should be treated as 0 or handled gracefully)
    assert(bucket.consume(-5.0) == true);
    assert(bucket.get_tokens() == 10.0);
}

void test_exact_capacity_consumption() {
    auto clock = std::make_shared<SimulatedClock>(1000);
    TokenBucketConfig config{50.0, 5.0, "node-1"};
    DistributedTokenBucket bucket(config, clock);

    // Consume exact capacity
    assert(bucket.consume(50.0) == true);
    assert(bucket.get_tokens() == 0.0);
    assert(bucket.get_version() == 1);

    // Further consumption must fail immediately
    assert(bucket.consume(0.1) == false);
}

void test_refill_mechanics() {
    auto clock = std::make_shared<SimulatedClock>(1000);
    // 10 tokens per second -> 0.01 tokens per ms
    TokenBucketConfig config{100.0, 10.0, "node-1"};
    DistributedTokenBucket bucket(config, clock);

    assert(bucket.consume(100.0) == true);
    assert(bucket.get_tokens() == 0.0);

    // Advance clock by 500ms -> should refill 5 tokens
    clock->advance(500);
    assert(std::abs(bucket.get_tokens() - 5.0) < 1e-5);

    // Advance clock by another 1500ms -> total 2000ms -> 20 tokens, capped at capacity if overfilled
    clock->advance(1500);
    // Total elapsed 2000ms since empty = 20 tokens
    assert(std::abs(bucket.get_tokens() - 20.0) < 1e-5);
}

void test_capacity_ceiling() {
    auto clock = std::make_shared<SimulatedClock>(1000);
    TokenBucketConfig config{10.0, 100.0, "node-1"}; // 100 tokens/sec refill
    DistributedTokenBucket bucket(config, clock);

    // Wait a long time, tokens should not exceed capacity (10.0)
    clock->advance(10000); // 10 seconds
    assert(bucket.get_tokens() == 10.0);
}

// ============================================================================
// INVARIANT TESTS: Consistency & Synchronization
// ============================================================================

void test_monotonic_versioning() {
    auto clock = std::make_shared<SimulatedClock>(1000);
    TokenBucketConfig config{100.0, 10.0, "node-1"};
    DistributedTokenBucket bucket(config, clock);

    uint64_t prev_version = bucket.get_version();
    for (int i = 0; i < 10; ++i) {
        bool success = bucket.consume(1.0);
        assert(success);
        uint64_t curr_version = bucket.get_version();
        assert(curr_version == prev_version + 1);
        prev_version = curr_version;
    }
}

void test_distributed_state_sync() {
    auto clock = std::make_shared<SimulatedClock>(1000);
    TokenBucketConfig config{100.0, 10.0, "node-1"};
    DistributedTokenBucket bucket(config, clock);

    // Force sync with older version should be ignored
    bucket.force_sync(50.0, 1000, 0);
    // Since internal version is 0, equal/greater version updates. Let's update version 5.
    bucket.force_sync(40.0, 1000, 5);
    assert(bucket.get_tokens() == 40.0);
    assert(bucket.get_version() == 6); // internal version increments on sync

    // Try syncing with an older external version (e.g., version 2)
    bucket.force_sync(90.0, 1000, 2);
    // Should be ignored, tokens remain 40.0
    assert(bucket.get_tokens() == 40.0);
}

// ============================================================================
// FAILURE & RECOVERY STATE TESTS
// ============================================================================

void test_failure_mode_blocking() {
    auto clock = std::make_shared<SimulatedClock>(1000);
    TokenBucketConfig config{100.0, 10.0, "node-1"};
    DistributedTokenBucket bucket(config, clock);

    assert(bucket.consume(10.0) == true);

    // Trigger failure state (e.g., partition / coordinator loss)
    bucket.set_failed(true);
    assert(bucket.is_failed() == true);

    // All consumption must fail during failure state
    assert(bucket.consume(1.0) == false);
    assert(bucket.consume(0.0) == true); // Zero consumption might bypass or still fail depending on design; here let's check standard behavior: fail if failed.
    // In our implementation: if (is_failed_) return false; even for zero? Let's check: consume returns false if failed.

    // Recover from failure state
    bucket.set_failed(false);
    assert(bucket.is_failed() == false);