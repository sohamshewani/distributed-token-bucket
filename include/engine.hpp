#ifndef DISTRIBUTED_TOKEN_BUCKET_ENGINE_HPP
#define DISTRIBUTED_TOKEN_BUCKET_ENGINE_HPP

#include <atomic>
#include <cstdint>
#include <chrono>
#include <array>
#include <vector>
#include <algorithm>
#include <new>
#include <system_error>
#include <cstring>
#include <optional>

namespace distributed_token_bucket {

// Constants and Hardware Alignment
#ifdef __cpp_lib_hardware_interference_size
using std::hardware_destructive_interference_size;
constexpr std::size_t kCacheLineSize = hardware_destructive_interference_size;
#else
constexpr std::size_t kCacheLineSize = 64;
#endif

constexpr uint64_t kFixedPointScale = 1000ULL; // 1e3 fixed-point scaling
constexpr std::size_t kDefaultSubWindows = 10;

enum class EngineError {
    Success = 0,
    RateLimitExceeded,
    BufferFull,
    NodeFencedOut,
    InvalidConfiguration
};

class EngineException : public std::system_error {
public:
    explicit EngineException(EngineError err, const std::string& what_arg)
        : std::system_error(static_cast<int>(err), std::generic_category(), what_arg), err_(err) {}
    
    EngineError error_code() const noexcept { return err_; }
private:
    EngineError err_;
};

// 1. SlidingWindowCounter
template <std::size_t SubWindows = kDefaultSubWindows>
class alignas(kCacheLineSize) SlidingWindowCounter {
public:
    SlidingWindowCounter() : window_duration_us_(1000000), head_index_(0) {
        for (auto& slot : slots_) {
            slot.store(0, std::memory_order_relaxed);
        }
    }

    explicit SlidingWindowCounter(uint64_t window_duration_us)
        : window_duration_us_(window_duration_us), head_index_(0) {
        for (auto& slot : slotsptr()) {
            slot->store(0, std::memory_order_relaxed);
        }
    }

    void add(uint64_t timestamp_us, uint64_t count) {
        advance(timestamp_us);
        std::size_t idx = head_index_.load(std::memory_order_relaxed) % SubWindows;
        slots_[idx].fetch_add(count, std::memory_order_relaxed);
    }

    uint64_t sum(uint64_t current_timestamp_us) {
        advance(current_timestamp_us);
        uint64_t total = 0;
        for (std::size_t i = 0; i < SubWindows; ++i) {
            total += slots_[i].load(std::memory_order_relaxed);
        }
        return total;
    }

    void reset() {
        head_index_.store(0, std::memory_order_relaxed);
        for (auto& slot : slots_) {
            slot.store(0, std::memory_order_relaxed);
        }
        last_advance_us_.store(0, std::memory_order_relaxed);
    }

private:
    void advance(uint64_t current_timestamp_us) {
        uint64_t last = last_advance_us_.load(std::memory_order_relaxed);
        if (current_timestamp_us <= last) {
            return;
        }

        uint64_t sub_window_duration = window_duration_us_ / SubWindows;
        if (sub_window_duration == 0) sub_window_duration = 1;

        uint64_t delta = current_timestamp_us - last;
        if (delta >= window_duration_us_) {
            reset();
            last_advance_us_.store(current_timestamp_us, std::memory_order_relaxed);
            return;
        }

        std::size_t steps = static_cast<std::size_t>(delta / sub_window_duration);
        if (steps == 0) {
            return;
        }

        // Try to update last_advance atomically
        if (!last_advance_us_.compare_exchange_weak(last, last + steps * sub_window_duration,
                                                    std::memory_order_relaxed,
                                                    std::memory_order_relaxed)) {
            return;
        }

        for (std::size_t s = 0; s < std::min(steps, SubWindows); ++s) {
            std::size_t current_head = head_index_.fetch_add(1, std::memory_order_relaxed) + 1;
            slots_[current_head % SubWindows].store(0, std::memory_order_relaxed);
        }
    }

    auto slotsptr() {
        std::array<std::atomic<uint64_t>*, SubWindows> ptrs;
        for (std::size_t i = 0; i < SubWindows; ++i) {
            ptrs[i] = &slots_[i];
        }
        return ptrs;
    }

    uint64_t window_duration_us_;
    std::atomic<uint64_t> last_advance_us_{0};
    std::atomic<std::size_t> head_index_{0};
    alignas(kCacheLineSize) std::array<std::atomic<uint64_t>, SubWindows> slots_;
};

// 2. TokenBucketState
struct alignas(16) TokenBucketState {
    std::atomic<uint64_t> tokens;
    std::atomic<uint64_t> last_refill_timestamp_ns;

    TokenBucketState() : tokens(0), last_refill_timestamp_ns(0) {}
    TokenBucketState(uint64_t init_tokens, uint64_t timestamp_ns)
        : tokens(init_tokens), last_refill_timestamp_ns(timestamp_ns) {}

    TokenBucketState(const TokenBucketState& other) {
        tokens.store(other.tokens.load(std::memory_order_relaxed), std::memory_order_relaxed);
        last_refill_timestamp_ns.store(other.last_refill_timestamp_ns.load(std::memory_order_relaxed), std::memory_order_relaxed);
    }

    TokenBucketState& operator=(const TokenBucketState& other) {
        if (this != &other) {
            tokens.store(other.tokens.load(std::memory_order_relaxed), std::memory_order_relaxed);
            last_refill_timestamp_ns.store(other.last_refill_timestamp_ns.load(std::memory_order_relaxed), std::memory_order_relaxed);
        }
        return *this;
    }
};

// 3. RingBuffer (SPSC/MPMC Bounded Circular Buffer)
template <typename T, std::size_t Capacity>
class alignas(kCacheLineSize) RingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "RingBuffer capacity must be a power of two");
public:
    RingBuffer() : head_(0), tail_(0) {
        buffer_ = static_cast<Node*>(::operator new[](sizeof(Node) * Capacity, std::align_val_t{kCacheLineSize}));
        for (std::size_t i = 0; i < Capacity; ++i) {
            new (&buffer_[i]) Node();
        }
    }

    ~RingBuffer() {
        for (std::size_t i = 0; i < Capacity; ++i) {
            buffer_[i].~Node();
        }
        ::operator delete[](buffer_, std::align_val_t{kCacheLineSize});
    }

    bool push(const T& item) {
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        std::size_t head = head_.load(std::memory_order_acquire);

        if ((tail - head) >= Capacity) {
            return false; // Full
        }

        if (!tail_.compare_exchange_weak(tail, tail + 1, std::memory_order_release, std::memory_order_relaxed)) {
            return false;
        }

        Node& node = buffer_[tail & kMask];
        node.data = item;
        node.sequence.store(tail + 1, std::memory_order_release);
        return true;
    }

    bool pop(T& item) {
        std::size_t head = head_.load(std::memory_order_relaxed);
        Node& node = buffer_[head & kMask];
        std::size_t seq = node.sequence.load(std::memory_order_acquire);

        if (seq < (head + 1)) {
            return false; // Empty
        }

        item = node.data;
        head_.store(head + 1, std::memory_order_release);
        node.sequence.store(head + Capacity, std::memory_order_release);
        return true;
    }

    std::size_t size() const noexcept {
        std::size_t head = head_.load(std::memory_order_acquire);
        std::size_t tail = tail_.load(std::memory_order_acquire);
        if (tail >= head) {
            return tail - head;
        }
        return 0;
    }

    bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
    }

private:
    struct alignas(kCacheLineSize) Node {
        T data;
        std::atomic<std::size_t> sequence;
        Node() : sequence(0) {}
    };

    static constexpr std::size_t kMask = Capacity - 1;

    alignas(kCacheLineSize) std::atomic<std::size_t> head_;
    alignas(kCacheLineSize) std::atomic<std::size_t> tail_;
    Node* buffer_;

    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;
};

// 4. ClockSkewTracker
template <std::size_t WindowSize = 31>
class alignas(kCacheLineSize) ClockSkewTracker {
public:
    ClockSkewTracker() : count_(0), index_(0) {
        samples_.fill(0);
    }

    void add_sample(int64_t offset_ns) {
        std::size_t idx = index_.load(std::memory_order_relaxed) % WindowSize;
        samples_[idx] = offset_ns;
        index_.fetch_add(1, std::memory_order_relaxed);
        
        std::size_t current_count = count_.load(std::memory_order_relaxed);
        if (current_count < WindowSize) {
            count_.compare_exchange_strong(current_count, current_count + 1, std::memory_order_relaxed);
        }
    }

    int64_t get_median_skew() const {
        std::size_t current_count = count_.load(std::memory_order_acquire);
        if (current_count == 0) {
            return 0;
        }

        std::array<int64_t, WindowSize> temp;
        std::size_t valid_count = std::min(current_count, WindowSize);
        for (std::size_t i = 0; i < valid_count; ++i) {
            temp[i] = samples_[i];
        }

        std::sort(temp.begin(), temp.begin() + valid_count);
        return temp[valid_count / 2];
    }

    bool is_skew_exceeded(int64_t threshold_ns) const {
        return std::abs(get_median_skew()) > threshold_ns;
    }

    void reset() {
        count_.store(0, std::memory_order_relaxed);
        index_.store(0, std::memory_order_relaxed);
        samples_.fill(0);
    }

private:
    std::atomic<std::size_t> count_;
    std::atomic<std::size_t> index_;
    std::array<int64_t, WindowSize> samples_;
};

// 5. Monotonic Clock Wrapper
class MonotonicClock {
public:
    static uint64_t now_ns() noexcept {
        auto now = std::chrono::steady_clock::now();
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count());
    }

    static uint64_t now_us() noexcept {
        auto now = std::chrono::steady_clock::now();
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count());
    }
};

// 6. Thread-Local Batching Allocator & Engine Context
struct ThreadLocalBatchState {
    uint64_t local_tokens_reserved;
    uint64_t local_tokens_consumed;
    uint64_t last_flush_ns;

    ThreadLocalBatchState() : local_tokens_reserved(0), local_tokens_consumed(0), last_flush_ns(0) {}
};

class DistributedTokenBucketEngine {
public:
    struct Config {
        uint64_t global_capacity;
        uint64_t refill_rate_per_sec;
        int64_t max_clock_skew_ns;
        uint64_t batch_quota_size;

        Config()
            : global_capacity(1000000),
              refill_rate_per_sec(100000),
              max_clock_skew_ns(50000000), // 50ms
              batch_quota_size(1000) {}
    };

    explicit DistributedTokenBucketEngine(const Config& config)
        : config_(config),
          state_(config.global_capacity * kFixedPointScale, MonotonicClock::now_ns()),
          is_fenced_(false),
          sliding_counter_(1000000) {}

    ~DistributedTokenBucketEngine() = default;

    EngineError consume(uint64_t tokens_requested) {
        if (is_fenced_.load(std::memory_order_acquire)) {
            return EngineError::NodeFencedOut;
        }

        uint64_t now = MonotonicClock::now_ns();
        
        // Check clock drift via tracker
        if (skew_tracker_.is_skew_exceeded(config_.max_clock_skew_ns)) {
            is_fenced_.store(true, std::memory_order_release);
            return EngineError::NodeFencedOut;
        }

        // Thread-Local Batching Logic
        auto& tls = get_thread_local_state();
        uint64_t scaled_request = tokens_requested * kFixedPointScale;

        if (tls.local_tokens_reserved < scaled_request) {
            // Request batch allocation from distributed bucket via CAS
            if (!refill_and_acquire_batch(tls, config_.batch_quota_size * kFixedPointScale, now)) {
                // Fallback to local-quota mode on distributed partition/failure
                if (!fallback_local_quota_consume(scaled_request)) {
                    return EngineError::RateLimitExceeded;
                }
                return EngineError::Success;
            }
        }

        tls.local_tokens_reserved -= scaled_request;
        tls.local_tokens_consumed += scaled_request;

        sliding_counter_.add(now / 1000, tokens_requested);

        // Periodically flush metrics
        if ((now - tls.last_flush_ns) > 1000000000ULL) { // 1 second
            flush_thread_local_metrics(tls);
            tls.last_flush_ns = now;
        }

        return EngineError::Success;
    }

    void record_peer_heartbeat(int64_t peer_timestamp_ns, int64_t local_receive_timestamp_ns) {
        int64_t offset = local_receive_timestamp_ns - peer_timestamp_ns;
        skew_tracker_.add_sample(offset);
        if (skew_tracker_.is_skew_exceeded(config_.max_clock_skew_ns)) {
            is_fenced_.store(true, std::memory_order_release);
        }