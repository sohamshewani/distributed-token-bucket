#ifndef INCLUDE_CONCURRENCY_HPP
#define INCLUDE_CONCURRENCY_HPP

#include <atomic>
#include <cstdint>
#include <chrono>
#include <array>
#include <vector>
#include <algorithm>
#include <new>
#include <stdexcept>
#include <system_error>

namespace distributed_token_bucket {

#ifdef __cpp_lib_hardware_interference_size
using std::hardware_destructive_interference_size;
#else
constexpr std::size_t hardware_destructive_interference_size = 64;
#endif

// ============================================================================
// 1. CLOCK & TIME INVARIANTS (Monotonic Time Provision)
// ============================================================================
class MonotonicClock {
public:
    static inline uint64_t now_ns() noexcept {
        auto duration = std::chrono::steady_clock::now().time_since_epoch();
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count());
    }

    static inline uint64_t now_us() noexcept {
        auto duration = std::chrono::steady_clock::now().time_since_epoch();
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(duration).count());
    }
};

// ============================================================================
// 2. TOKEN BUCKET STATE (Compact 128-bit layout pack)
// ============================================================================
struct alignas(16) TokenBucketState {
    std::atomic<uint64_t> tokens{0};
    std::atomic<uint64_t> last_refill_timestamp_ns{0};

    TokenBucketState() noexcept = default;
    
    TokenBucketState(uint64_t initial_tokens, uint64_t timestamp_ns) noexcept
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

static_assert(sizeof(TokenBucketState) == 16, "TokenBucketState must be exactly 128 bits (16 bytes)");

// ============================================================================
// 3. SLIDING WINDOW COUNTER (Cache-aligned, ring-indexed, microsecond resolution)
// ============================================================================
template <size_t NumSubWindows = 10>
class alignas(hardware_destructive_interference_size) SlidingWindowCounter {
private:
    static_assert((NumSubWindows & (NumSubWindows - 1)) == 0 || NumSubWindows > 0, "NumSubWindows must be valid");
    
    struct alignas(8) SubWindow {
        std::atomic<uint64_t> timestamp_us{0};
        std::atomic<uint64_t> count{0};
    };

    const uint64_t window_span_us_;
    const uint64_t sub_window_span_us_;
    std::array<SubWindow, NumSubWindows> sub_windows_;
    char pad_[hardware_destructive_interference_size - ((sizeof(uint64_t) * 2 + sizeof(SubWindow) * NumSubWindows) % hardware_destructive_interference_size)];

public:
    explicit SlidingWindowCounter(uint64_t window_span_ms = 1000) noexcept
        : window_span_us_(window_span_ms * 1000),
          sub_window_span_us_(window_span_us_ / NumSubWindows) {
        uint64_t current_time = MonotonicClock::now_us();
        for (size_t i = 0; i < NumSubWindows; ++i) {
            sub_windows_[i].timestamp_us.store(current_time - (i * sub_window_span_us_), std::memory_order_relaxed);
            sub_windows_[i].count.store(0, std::memory_order_relaxed);
        }
    }

    void add(uint64_t count_val) noexcept {
        uint64_t now = MonotonicClock::now_us();
        size_t index = (now / sub_window_span_us_) % NumSubWindows;

        SubWindow& slot = sub_windows_[index];
        uint64_t slot_ts = slot.timestamp_us.load(std::memory_order_acquire);

        if (now - slot_ts >= window_span_us_) {
            // Attempt to reset stale slot
            uint64_t expected_ts = slot_ts;
            if (slot.timestamp_us.compare_exchange_strong(expected_ts, now, std::memory_order_acq_rel, std::memory_order_relaxed)) {
                slot.count.store(count_val, std::memory_order_release);
                return;
            }
        }

        // If current slot matches window time or CAS failed because another thread updated it
        if ((now / sub_window_span_us_) == (slot.timestamp_us.load(std::memory_order_acquire) / sub_window_span_us_)) {
            slot.count.fetch_add(count_val, std::memory_order_relaxed);
        } else {
            // Fallback retry recursively or force update
            add(count_val);
        }
    }

    uint64_t get_current_count() noexcept {
        uint64_t now = MonotonicClock::now_us();
        uint64_t total_count = 0;
        uint64_t threshold = (now > window_span_us_) ? (now - window_span_us_) : 0;

        for (size_t i = 0; i < NumSubWindows; ++i) {
            uint64_t slot_ts = sub_windows_[i].timestamp_us.load(std::memory_order_acquire);
            if (slot_ts >= threshold) {
                total_count += sub_windows_[i].count.load(std::memory_order_relaxed);
            }
        }
        return total_count;
    }
};

// ============================================================================
// 4. MPMC RING BUFFER (Power-of-two bounded circular buffer, zero false sharing)
// ============================================================================
template <typename T, size_t Capacity>
class MPMCRingBuffer {
private:
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");

    struct Cell {
        std::atomic<size_t> sequence;
        T data;
    };

    static constexpr size_t kCacheLineSize = hardware_destructive_interference_size;

    alignas(kCacheLineSize) Cell* buffer_;
    const size_t capacity_mask_;
    
    alignas(kCacheLineSize) std::atomic<size_t> enqueue_pos_{0};
    alignas(kCacheLineSize) std::atomic<size_t> dequeue_pos_{0};

    char pad_[kCacheLineSize];

public:
    explicit MPMCRingBuffer() 
        : capacity_mask_(Capacity - 1) {
        buffer_ = static_cast<Cell*>(::operator new[](sizeof(Cell) * Capacity, std::align_val_t{kCacheLineSize}));
        for (size_t i = 0; i < Capacity; ++i) {
            new (&buffer_[i].data) T();
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    ~MPMCRingBuffer() {
        for (size_t i = 0; i < Capacity; ++i) {
            buffer_[i].data.~T();
        }
        ::operator delete[](buffer_, std::align_val_t{kCacheLineSize});
    }

    MPMCRingBuffer(const MPMCRingBuffer&) = delete;
    MPMCRingBuffer& operator=(const MPMCRingBuffer&) = delete;

    bool enqueue(const T& item) noexcept {
        Cell* cell;
        size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
        for (;;) {
            cell = &buffer_[pos & capacity_mask_];
            size_t seq = cell->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
            
            if (dif == 0) {
                if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                // Buffer is full (Drop-tail adaptive shedding policy)
                return false;
            } else {
                pos = enqueue_pos_.load(std::memory_order_relaxed);
            }
        }

        cell->data = item;
        cell->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    bool dequeue(T& item) noexcept {
        Cell* cell;
        size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
        for (;;) {
            cell = &buffer_[pos & capacity_mask_];
            size_t seq = cell->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);
            
            if (dif == 0) {
                if (dequeue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                // Buffer is empty
                return false;
            } else {
                pos = dequeue_pos_.load(std::memory_order_relaxed);
            }
        }

        item = cell->data;
        cell->sequence.store(pos + capacity_mask_ + 1, std::memory_order_release);
        return true;
    }

    size_t approximate_size() const noexcept {
        size_t enq = enqueue_pos_.load(std::memory_order_relaxed);
        size_t deq = dequeue_pos_.load(std::memory_order_relaxed);
        if (enq >= deq) {
            return enq - deq;
        }
        return 0;
    }
};

// ============================================================================
// 5. CLOCK SKEW TRACKER (Running Median Filter for Drift & Propagation Delay)
// ============================================================================
template <size_t WindowSize = 15>
class ClockSkewTracker {
private:
    static_assert(WindowSize > 0 && (WindowSize % 2 != 0), "WindowSize must be an odd positive integer");
    
    std::array<int64_t, WindowSize> samples_{};
    size_t insert_index_{0};
    size_t sample_count_{0};
    int64_t current_median_{0};
    int64_t max_drift_threshold_ns_{50000000}; // 50ms default threshold

public:
    explicit ClockSkewTracker(int64_t max_drift_threshold_ms = 50) noexcept
        : max_drift_threshold_ns_(max_drift_threshold_ms * 1000000LL) {
        samples_.fill(0);
    }

    void update(int64_t skew_sample_ns) noexcept {
        samples_[insert_index_] = skew_sample_ns;
        insert_index_ = (insert_index_ + 1) % WindowSize;
        if (sample_count_ < WindowSize) {
            sample_count_++;
        }

        // Compute running median
        std::array<int64_t, WindowSize> temp;
        for (size_t i = 0; i < sample_count_; ++i) {
            temp[i] = samples_[i];
        }
        std::sort(temp.begin(), temp.begin() + sample_count_);
        current_median_ = temp[sample_count_ / 2];
    }

    int64_t get_skew_median_ns() const noexcept {
        return current_median_;
    }

    bool is_fenced() const noexcept {
        return std::abs(current_median_) > max_drift_threshold_ns_;
    }

    void set_drift_threshold_ms(int64_t threshold_ms) noexcept {
        max_drift_threshold_ns_ = threshold_ms * 1000000LL;
    }
};

// ============================================================================
// 6. THREAD-LOCAL PERMIT CACHE (Batching tokens to eliminate hot-path CAS contention)
// ============================================================================
class DistributedTokenBucketRef;

class ThreadLocalPermitCache {
private:
    uint64_t local_balance_{0};
    uint64_t batch_size_;
    int64_t pending_delta_{0};
    DistributedTokenBucketRef* parent_bucket_;

public:
    ThreadLocalPermitCache(DistributedTokenBucketRef* bucket, uint64_t batch_size) noexcept
        : batch_size_(batch_size), parent_bucket_(bucket) {}

    ~ThreadLocalPermitCache() noexcept;

    bool acquire(uint64_t tokens) noexcept;
    void flush() noexcept;
};

// ============================================================================
// 7. DISTRIBUTED TOKEN BUCKET CONSENSUS INTERFACE
// ============================================================================
class DistributedTokenBucketRef {
private:
    TokenBucketState state_;
    uint64_t capacity_;
    uint64_t refill_rate_per_sec_;
    std::atomic<int64_t> global_delta_accumulator_{0};

public:
    DistributedTokenBucketRef(uint64_t capacity, uint64_t refill_rate_per_sec) noexcept
        : capacity_(capacity), 
          refill_rate_per_sec_(refill_rate_per_sec),
          state_(capacity * 1000, MonotonicClock::now_ns()) {}

    bool consume_atomic(uint64_t tokens_to_consume) noexcept {
        uint64_t scaled_consume = tokens_to_consume * 1000;
        uint64_t scaled_capacity = capacity_ * 1000;

        TokenBucketState current = state_;
        uint64_t now = MonotonicClock::now_ns();

        for (;;) {
            uint64_t last_ts = current.last_refill_timestamp_ns.load(std::memory_order_relaxed);
            uint64_t current_tokens = current.tokens.load(std::memory_order_relaxed);

            uint64_t elapsed_ns = (now > last_ts) ? (now - last_ts) : 0;
            uint64_t generated_tokens = (elapsed_ns * refill_rate_per_sec_) / 1000000; // Refill based on ns ratio

            uint64_t new_tokens = std::min(scaled_capacity, current_tokens + generated_tokens);

            if (new_tokens < scaled_consume) {
                return false; // Insufficient tokens
            }

            TokenBucketState next_state(new_tokens - scaled_consume, now);

            if (state_.tokens.compare_exchange_weak(current.tokens, next_state.tokens, std::memory_order_acq_rel, std::memory_order_relaxed)) {
                state_.last_refill_timestamp_ns.store(next_state.last_refill_timestamp_ns.release(), std::memory_order_release);
                return true;
            }