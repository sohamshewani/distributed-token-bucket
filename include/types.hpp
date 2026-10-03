#ifndef DISTRIBUTED_TOKEN_BUCKET_TYPES_HPP
#define DISTRIBUTED_TOKEN_BUCKET_TYPES_HPP

#include <atomic>
#include <cstdint>
#include <chrono>
#include <array>
#include <new>
#include <algorithm>
#include <optional>
#include <system_error>

namespace distributed_token_bucket {

// Hardware alignment constants
#ifdef __cpp_lib_hardware_interference_size
using std::hardware_destructive_interference_size;
constexpr std::size_t kCacheLineSize = hardware_destructive_interference_size;
#else
constexpr std::size_t kCacheLineSize = 64;
#endif

// Fixed-point scaling factor for tokens to maintain fractional precision without floats
constexpr uint64_t kTokenScale = 1000ULL;

enum class ErrorCode : uint32_t {
    Success = 0,
    RateLimitExceeded,
    BufferFull,
    BufferEmpty,
    ClockDriftExceeded,
    NodeFenced,
    InvalidConfiguration
};

class ErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override { return "DistributedTokenBucket"; }
    std::string message(int ev) const override {
        switch (static_cast<ErrorCode>(ev)) {
            case ErrorCode::Success: return "Success";
            case ErrorCode::RateLimitExceeded: return "Rate limit exceeded";
            case ErrorCode::BufferFull: return "Ring buffer full";
            case ErrorCode::BufferEmpty: return "Ring buffer empty";
            case ErrorCode::ClockDriftExceeded: return "Clock drift threshold exceeded (>50ms)";
            case ErrorCode::NodeFenced: return "Node fenced out due to synchronization failure";
            case ErrorCode::InvalidConfiguration: return "Invalid configuration parameters";
            default: return "Unknown error";
        }
    }
};

inline const std::error_category& get_error_category() noexcept {
    static ErrorCategory instance;
    return instance;
}

inline std::error_code make_error_code(ErrorCode e) noexcept {
    return {static_cast<int>(e), get_error_category()};
}

} // namespace distributed_token_bucket

namespace std {
template <>
struct is_error_code_enum<distributed_token_bucket::ErrorCode> : true_type {};
}

namespace distributed_token_bucket {

// 1. SlidingWindowCounter: Cache-aligned (64-byte) structure using a fixed-size array
// of atomic 64-bit integers representing microsecond-resolution sub-windows with ring-indexing.
template <std::size_t NumSubWindows = 10>
class alignas(kCacheLineSize) SlidingWindowCounter {
private:
    static_assert(NumSubWindows > 0, "NumSubWindows must be greater than zero");
    
    struct alignas(kCacheLineSize) SubWindow {
        std::atomic<uint64_t> count{0};
        std::atomic<uint64_t> timestamp_us{0};
    };

    std::array<SubWindow, NumSubWindows> windows_;
    std::atomic<uint64_t> window_duration_us_;
    std::atomic<std::size_t> current_index_{0};

    std::size_t get_index_for_time(uint64_t now_us) const noexcept {
        uint64_t duration = window_duration_us_.load(std::memory_order_relaxed);
        if (duration == 0) return 0;
        return (now_us / duration) % NumSubWindows;
    }

public:
    explicit SlidingWindowCounter(uint64_t window_duration_us = 1000000ULL) 
        : window_duration_us_(window_duration_us) {
        uint64_t init_time = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        for (std::size_t i = 0; i < NumSubWindows; ++i) {
            windows_[i].timestamp_us.store(init_time, std::memory_order_relaxed);
            windows_[i].count.store(0, std::memory_order_relaxed);
        }
    }

    void add(uint64_t count, uint64_t now_us) noexcept {
        std::size_t target_idx = get_index_for_time(now_us);
        
        // Atomically reset stale sub-windows if time has advanced past them
        uint64_t expected_time = windows_[target_idx].timestamp_us.load(std::memory_order_acquire);
        uint64_t duration = window_duration_us_.load(std::memory_order_relaxed);
        
        if (now_us >= expected_time + (duration * NumSubWindows)) {
            uint64_t desired_time = (now_us / duration) * duration;
            if (windows_[target_idx].timestamp_us.compare_exchange_strong(
                    expected_time, desired_time, std::memory_order_acq_rel, std::memory_order_relaxed)) {
                windows_[target_idx].count.store(count, std::memory_order_release);
                return;
            }
        }
        
        // Otherwise, add to the current sub-window
        windows_[target_idx].count.fetch_add(count, std::memory_order_relaxed);
    }

    uint64_t sum(uint64_t now_us) const noexcept {
        uint64_t total = 0;
        uint64_t duration = window_duration_us_.load(std::memory_order_relaxed);
        uint64_t window_span = duration * NumSubWindows;

        for (std::size_t i = 0; i < NumSubWindows; ++i) {
            uint64_t ts = windows_[i].timestamp_us.load(std::memory_order_acquire);
            if (now_us >= ts && (now_us - ts) < window_span) {
                total += windows_[i].count.load(std::memory_order_relaxed);
            }
        }
        return total;
    }

    void reset() noexcept {
        for (std::size_t i = 0; i < NumSubWindows; ++i) {
            windows_[i].count.store(0, std::memory_order_relaxed);
            windows_[i].timestamp_us.store(0, std::memory_order_relaxed);
        }
    }
};

// 2. TokenBucketState: Compact 128-bit layout pack storing tokens and timestamp.
struct alignas(16) TokenBucketState {
    std::atomic<uint64_t> tokens{0};
    std::atomic<uint64_t> last_refill_timestamp_ns{0};

    TokenBucketState() = default;
    
    TokenBucketState(uint64_t initial_tokens, uint64_t timestamp_ns) 
        : tokens(initial_tokens), last_refill_timestamp_ns(timestamp_ns) {}

    TokenBucketState(const TokenBucketState& other) noexcept {
        tokens.store(other.tokens.load(std::memory_order_relaxed), std::memory_order_relaxed);
        last_refill_timestamp_ns.store(other.last_refill_timestamp_ns.load(std::memory_order_relaxed), std::memory_order_relaxed);
    }

    TokenBucketState& operator=(const TokenBucketState& other) noexcept {
        if (this != &other) {
            tokens.store(other.tokens.load(std::memory_order_relaxed), std::memory_order_relaxed);
            last_refill_timestamp_ns.store(other.last_refill_timestamp_ns.load(std::memory_order_relaxed), std::memory_order_relaxed);
        }
        return *this;
    }
};

// 3. RingBuffer (MPMC/SPSC): Power-of-two bounded circular buffer with cache line separation.
template <typename T, std::size_t Capacity>
class RingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");
    static_assert(Capacity > 0, "Capacity must be greater than zero");

private:
    struct Node {
        std::atomic<size_t> sequence{0};
        T data;
    };

    static constexpr size_t kBufferMask = Capacity - 1;

    alignas(kCacheLineSize) Node buffer_[Capacity];
    alignas(kCacheLineSize) std::atomic<size_t> enqueue_pos_{0};
    alignas(kCacheLineSize) std::atomic<size_t> dequeue_pos_{0};

    char pad_[kCacheLineSize]; // Eliminate false sharing between head and tail

public:
    RingBuffer() {
        for (size_t i = 0; i < Capacity; ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    ~RingBuffer() = default;

    bool push(const T& item) noexcept {
        Node* node;
        size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
        for (;;) {
            node = &buffer_[pos & kBufferMask];
            size_t seq = node->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
            if (dif == 0) {
                if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false; // Buffer full
            } else {
                pos = enqueue_pos_.load(std::memory_order_relaxed);
            }
        }
        node->data = item;
        node->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    bool pop(T& item) noexcept {
        Node* node;
        size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
        for (;;) {
            node = &buffer_[pos & kBufferMask];
            size_t seq = node->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);
            if (dif == 0) {
                if (dequeue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false; // Buffer empty
            } else {
                pos = dequeue_pos_.load(std::memory_order_relaxed);
            }
        }
        item = std::move(node->data);
        node->sequence.store(pos + kBufferMask + 1, std::memory_order_release);
        return true;
    }

    size_t size() const noexcept {
        size_t head = enqueue_pos_.load(std::memory_order_acquire);
        size_t tail = dequeue_pos_.load(std::memory_order_acquire);
        if (head >= tail) {
            return head - tail;
        }
        return 0;
    }

    bool empty() const noexcept {
        return enqueue_pos_.load(std::memory_order_acquire) == dequeue_pos_.load(std::memory_order_acquire);
    }
};

// 4. ClockSkewTracker: Running median filter over peer heartbeat samples for propagation and drift.
template <std::size_t WindowSize = 31>
class ClockSkewTracker {
    static_assert(WindowSize % 2 == 1, "WindowSize must be odd for valid median calculation");

private:
    std::array<int64_t, WindowSize> samples_{};
    std::size_t index_{0};
    std::size_t count_{0};
    int64_t last_offset_ns_{0};
    bool fenced_{false};
    uint64_t max_allowed_drift_ns_;

public:
    explicit ClockSkewTracker(uint64_t max_allowed_drift_ns = 50000000ULL) // Default 50ms
        : max_allowed_drift_ns_(max_allowed_drift_ns) {}

    void update(int64_t offset_ns) noexcept {
        samples_[index_] = offset_ns;
        index_ = (index_ + 1) % WindowSize;
        if (count_ < WindowSize) {
            count_++;
        }

        int64_t current_median = calculate_median();
        last_offset_ns_ = current_median;

        // Byzantine/Stale Clocks enforcement (> 50ms threshold)
        if (static_cast<uint64_t>(std::abs(current_median)) > max_allowed_drift_ns_) {
            fenced_ = true;
        } else {
            fenced_ = false;
        }
    }

    int64_t get_median_offset() const noexcept {
        return last_offset_ns_;
    }

    bool is_fenced() const noexcept {
        return fenced_;
    }

    void reset() noexcept {
        samples_.fill(0);
        index_ = 0;
        count_ = 0;
        last_offset_ns_ = 0;
        fenced_ = false;
    }

private:
    int64_t calculate_median() const noexcept {
        if (count_ == 0) return 0;
        
        // Copy active samples for sorting without modifying state
        std::array<int64_t, WindowSize> temp;
        for (std::size_t i = 0; i < count_; ++i) {
            temp[i] = samples_[i];
        }
        
        std::sort(temp.begin(), temp.begin() + count_);
        return temp[count_ / 2];
    }
};

// Monotonic time wrapper to mitigate NTP step-back anomalies
class MonotonicClock {
public:
    static uint64_t now_ns() noexcept {
        static thread_local uint64_t last_ns = 0;
        auto duration = std::chrono::steady_clock::now().time_since_epoch();
        uint64_t current_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
        
        // Guarantee strict monotonicity
        if (current_ns <= last_ns) {
            current_ns = last_ns + 1;
        }
        last_ns = current_ns;
        return current_ns;
    }

    static uint64_t now_us() noexcept {
        return now_ns() / 1000ULL;
    }
};

} // namespace distributed_token_bucket

#endif // DISTRIBUTED_TOKEN_BUCKET_TYPES_HPP