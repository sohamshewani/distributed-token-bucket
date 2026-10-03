#ifndef DISTRIBUTED_TOKEN_BUCKET_RING_BUFFER_HPP
#define DISTRIBUTED_TOKEN_BUCKET_RING_BUFFER_HPP

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace distributed_token_bucket {

// Hardware cache line size standard for modern x86_64 and ARM architectures
constexpr std::size_t kCacheLineSize = 64;

/**
 * @brief Exception thrown when RingBuffer operations fail due to capacity limits.
 */
class RingBufferException : public std::runtime_error {
public:
    explicit RingBufferException(const char* message) : std::runtime_error(message) {}
};

/**
 * @brief High-performance, lock-free MPMC (Multi-Producer Multi-Consumer) bounded circular buffer.
 * 
 * Implements a power-of-two bounded circular buffer backed by contiguous cache-aligned memory blocks.
 * Employs explicit padding to eliminate false sharing between producer and consumer cache lines, 
 * leveraging sequence numbers for wait-free/lock-free coordination without dynamic allocations on the hot path.
 * 
 * @tparam T Type of elements stored in the ring buffer. Must be trivially destructible or managed cleanly.
 * @tparam Capacity Must be a power of two.
 */
template <typename T, std::size_t Capacity>
class RingBuffer {
    static_assert(Capacity > 0, "RingBuffer capacity must be greater than zero.");
    static_assert((Capacity & (Capacity - 1)) == 0, "RingBuffer capacity must be a power of two.");

public:
    RingBuffer() 
        : head_(0), 
          tail_(0) 
    {
        // Allocate aligned storage for elements and sequence arrays
        buffer_ = static_5_storage_allocate();
        for (std::size_t i = 0; i < Capacity; ++i) {
            new (&node_seq_[i]) std::atomic<std::size_t>(i);
        }
    }

    ~RingBuffer() {
        // Drain remaining elements to ensure proper destruction if T is non-trivial
        T dummy;
        while (try_dequeue(dummy)) {
            // Drop elements
        }
        
        // Free raw aligned memory blocks
        ::operator delete[](buffer_, std::align_val_t{kCacheLineSize});
    }

    // Non-copyable, non-movable to preserve atomicity and memory alignment guarantees
    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;
    RingBuffer(RingBuffer&&) = delete;
    RingBuffer& operator=(RingBuffer&&) = delete;

    /**
     * @brief Enqueue an item into the ring buffer (Multi-Producer safe).
     * 
     * @param item The item to push. Perfect forwarded.
     * @return true if successful, false if the buffer is full.
     */
    template <typename U>
    bool try_enqueue(U&& item) {
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        for (;;) {
            std::atomic<std::size_t>& seq = node_seq_[tail & kIndexMask];
            std::size_t seq_val = seq.load(std::memory_order_acquire);
            intptr_t diff = static_cast<intptr_t>(seq_val) - static_cast<intptr_t>(tail);

            if (diff == 0) {
                // The cell is available. Try to claim it by advancing the tail index.
                if (tail_.compare_exchange_weak(tail, tail + 1, 
                                                std::memory_order_relaxed, 
                                                std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                // The buffer is full.
                return false;
            } else {
                // Another thread advanced the tail; reload.
                tail = tail_.load(std::memory_order_relaxed);
            }
        }

        // Construct the item in place within the pre-allocated buffer block
        new (&element_at(tail)) T(std::forward<U>(item));

        // Publish the element by updating the sequence number
        node_seq_[tail & kIndexMask].store(tail + 1, std::memory_order_release);
        return true;
    }

    /**
     * @brief Dequeue an item from the ring buffer (Multi-Consumer safe).
     * 
     * @param item Reference to write the dequeued item into.
     * @return true if successful, false if the buffer is empty.
     */
    bool try_dequeue(T& item) {
        std::size_t head = head_.load(std::memory_order_relaxed);
        for (;;) {
            std::atomic<std::size_t>& seq = node_seq_[head & kIndexMask];
            std::size_t seq_val = seq.load(std::memory_order_acquire);
            intptr_t diff = static_cast<intptr_t>(seq_val) - static_cast<intptr_t>(head + 1);

            if (diff == 0) {
                // The cell has an item ready. Try to claim it by advancing the head index.
                if (head_.compare_exchange_weak(head, head + 1, 
                                                std::memory_order_relaxed, 
                                                std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                // The buffer is empty.
                return false;
            } else {
                // Another thread advanced the head; reload.
                head = head_.load(std::memory_order_relaxed);
            }
        }

        // Move the element out of the buffer storage
        T* ptr = &element_at(head);
        item = std::move(*ptr);
        ptr->~T();

        // Reset the sequence number to allow reuse for the next wrap-around epoch
        node_seq_[head & kIndexMask].store(head + kCapacity, std::memory_order_release);
        return true;
    }

    /**
     * @brief Check if the ring buffer contains no elements.
     */
    [[nodiscard]] bool empty() const noexcept {
        std::size_t head = head_.load(std::memory_order_relaxed);
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        return head == tail;
    }

    /**
     * @brief Retrieve the exact capacity of the ring buffer.
     */
    [[nodiscard]] constexpr std::size_t capacity() const noexcept {
        return Capacity;
    }

    /**
     * @brief Estimate the current size (number of elements) in the buffer.
     * Note: Approximate in MPMC contexts due to concurrent modifications.
     */
    [[nodiscard]] std::size_t size_approx() const noexcept {
        std::size_t head = head_.load(std::memory_order_relaxed);
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail >= head) {
            return tail - head;
        }
        return (Capacity - (head - tail));
    }

private:
    static constexpr std::size_t kCapacity = Capacity;
    static constexpr std::size_t kIndexMask = Capacity - 1;

    // Explicit cache line padding to eliminate false sharing between head and tail pointers
    alignas(kCacheLineSize) std::atomic<std::size_t> head_;
    char pad_head_[kCacheLineSize - sizeof(std::atomic<std::size_t>)];

    alignas(kCacheLineSize) std::atomic<std::size_t> tail_;
    char pad_tail_[kCacheLineSize - sizeof(std::atomic<std::size_t>)];

    // Storage pointers initialized via aligned raw operator new
    alignas(kCacheLineSize) std::atomic<std::size_t> node_seq_[Capacity];
    void* buffer_;

    /**
     * @brief Helper to allocate raw cache-aligned storage.
     */
    void* static_5_storage_allocate() {
        std::size_t total_size = sizeof(T) * Capacity;
        // Ensure minimum alignment respects hardware cache boundaries and type alignment
        std::size_t alignment = (alignof(T) > kCacheLineSize) ? alignof(T) : kCacheLineSize;
        void* ptr = ::operator new[](total_size, std::align_val_t{alignment});
        if (!ptr) {
            throw RingBufferException("Failed to allocate aligned memory for RingBuffer data storage.");
        }
        return ptr;
    }

    /**
     * @brief Retrieve typed reference to element slot at absolute index.
     */
    [[nodiscard]] T& element_at(std::size_t index) noexcept {
        T* typed_array = static_cast<T*>(buffer_);
        return typed_array[index & kIndexMask];
    }
};

} // namespace distributed_token_bucket

#endif // DISTRIBUTED_TOKEN_BUCKET_RING_BUFFER_HPP