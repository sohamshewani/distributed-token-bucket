#ifndef DISTRIBUTED_TOKEN_BUCKET_ALLOCATOR_HPP
#define DISTRIBUTED_TOKEN_BUCKET_ALLOCATOR_HPP

#include <cstdint>
#include <cstddef>
#include <new>
#include <atomic>
#include <system_error>
#include <new>

namespace dtb {

enum class AllocatorError : uint8_t {
    Success = 0,
    OutOfMemory,
    InvalidAlignment,
    ExceededCapacity
};

class AllocatorErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override {
        return "dtb_allocator";
    }

    std::string message(int ev) const override {
        switch (static_cast<AllocatorError>(ev)) {
            case AllocatorError::Success: return "Success";
            case AllocatorError::OutOfMemory: return "Out of memory in arena";
            case AllocatorError::InvalidAlignment: return "Invalid alignment requested";
            case AllocatorError::ExceededCapacity: return "Exceeded maximum arena capacity";
            default: return "Unknown allocator error";
        }
    }
};

inline const std::error_category& allocator_error_category() noexcept {
    static AllocatorErrorCategory instance;
    return instance;
}

inline std::error_code make_error_code(AllocatorError e) noexcept {
    return {static_cast<int>(e), allocator_error_category()};
}

} // namespace dtb

namespace std {
template <>
struct is_error_code_enum<dtb::AllocatorError> : true_type {};
} // namespace std

namespace dtb {

class ArenaAllocator {
private:
    uint8_t* buffer_;
    size_t capacity_;
    std::atomic<size_t> offset_;

public:
    explicit ArenaAllocator(size_t capacity)
        : capacity_(capacity), offset_(0) {
        if (capacity_ == 0) {
            throw std::system_error(make_error_code(AllocatorError::ExceededCapacity));
        }
        
#if defined(_MSC_VER)
        buffer_ = static_cast<uint8_t*>(_aligned_malloc(capacity_, 64));
#else
        int result = posix_memalign(reinterpret_cast<void**>(&buffer_), 64, capacity_);
        if (result != 0) {
            buffer_ = nullptr;
        }
#endif
        if (buffer_ == nullptr) {
            throw std::bad_alloc();
        }
    }

    ~ArenaAllocator() noexcept {
        if (buffer_ != nullptr) {
#if defined(_MSC_VER)
            _aligned_free(buffer_);
#else
            free(buffer_);
#endif
            buffer_ = nullptr;
        }
    }

    ArenaAllocator(const ArenaAllocator&) = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;

    ArenaAllocator(ArenaAllocator&& other) noexcept
        : buffer_(other.buffer_), capacity_(other.capacity_), offset_(other.offset_.load(std::memory_order_relaxed)) {
        other.buffer_ = nullptr;
        other.capacity_ = 0;
        other.offset_.store(0, std::memory_order_relaxed);
    }

    ArenaAllocator& operator=(ArenaAllocator&& other) noexcept {
        if (this != &other) {
            if (buffer_ != nullptr) {
#if defined(_MSC_VER)
                _aligned_free(buffer_);
#else
                free(buffer_);
#endif
            }
            buffer_ = other.buffer_;
            capacity_ = other.capacity_;
            offset_.store(other.offset_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            
            other.buffer_ = nullptr;
            other.capacity_ = 0;
            other.offset_.store(0, std::memory_order_relaxed);
        }
        return *this;
    }

    template <typename T, typename... Args>
    T* allocate(Args&&... args) {
        size_t size = sizeof(T);
        size_t align = alignof(T);
        
        if (align > 64) {
            throw std::system_error(make_error_code(AllocatorError::InvalidAlignment));
        }

        size_t current_offset = offset_.load(std::memory_order_relaxed);
        void* ptr = nullptr;
        size_t space = capacity_ - current_offset;

        while (true) {
            current_offset = offset_.load(std::memory_order_relaxed);
            space = capacity_ - current_offset;
            ptr = buffer_ + current_offset;
            
            if (std::align(align, size, ptr, space) == nullptr) {
                throw std::system_error(make_error_code(AllocatorError::OutOfMemory));
            }

            size_t adjusted_offset = static_cast<uint8_t*>(ptr) - buffer_ + size;
            if (adjusted_offset > capacity_) {
                throw std::system_error(make_error_code(AllocatorError::OutOfMemory));
            }

            if (offset_.compare_exchange_weak(current_offset, adjusted_offset,
                                            std::memory_order_release,
                                            std::memory_order_relaxed)) {
                break;
            }
        }

        return ::new (ptr) T(std::forward<Args>(args)...);
    }

    void* allocate_bytes(size_t size, size_t align = 64) {
        if (align > 64) {
            throw std::system_error(make_error_code(AllocatorError::InvalidAlignment));
        }

        size_t current_offset = offset_.load(std::memory_order_relaxed);
        void* ptr = nullptr;
        size_t space = capacity_ - current_offset;

        while (true) {
            current_offset = offset_.load(std::memory_order_relaxed);
            space = capacity_ - current_offset;
            ptr = buffer_ + current_offset;
            
            if (std::align(align, size, ptr, space) == nullptr) {
                throw std::system_error(make_error_code(AllocatorError::OutOfMemory));
            }

            size_t adjusted_offset = static_cast<uint8_t*>(ptr) - buffer_ + size;
            if (adjusted_offset > capacity_) {
                throw std::system_error(make_error_code(AllocatorError::OutOfMemory));
            }

            if (offset_.compare_exchange_weak(current_offset, adjusted_offset,
                                            std::memory_order_release,
                                            std::memory_order_relaxed)) {
                break;
            }
        }

        return ptr;
    }

    void reset() noexcept {
        offset_.store(0, std::memory_order_release);
    }

    [[nodiscard]] size_t capacity() const noexcept {
        return capacity_;
    }

    [[nodiscard]] size_t allocated_bytes() const noexcept {
        return offset_.load(std::memory_order_relaxed);
    }
};

template <typename T>
class ArenaStlAdapter {
private:
    ArenaAllocator* arena_;

public:
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = size_t;
    using difference_type = ptrdiff_t;

    template <typename U>
    struct rebind {
        using other = ArenaStlAdapter<U>;
    };

    explicit ArenaStlAdapter(ArenaAllocator& arena) noexcept : arena_(&arena) {}

    template <typename U>
    ArenaStlAdapter(const ArenaStlAdapter<U>& other) noexcept : arena_(other.arena()) {}

    [[nodiscard]] pointer allocate(size_t n) {
        if (n > static_cast<size_t>(-1) / sizeof(T)) {
            throw std::bad_alloc();
        }
        return static_cast<pointer>(arena_->allocate_bytes(n * sizeof(T), alignof(T)));
    }

    void deallocate(pointer, size_t) noexcept {
        // Arena allocators do not individualize deallocations; no-op.
    }

    template <typename U, typename... Args>
    void construct(U* p, Args&&... args) {
        ::new (static_cast<void*>(p)) U(std::forward<Args>(args)...);
    }

    template <typename U>
    void destroy(U* p) noexcept {
        p->~U();
    }

    [[nodiscard]] ArenaAllocator* arena() const noexcept {
        return arena_;
    }

    template <typename U>
    bool operator==(const ArenaStlAdapter<U>& other) const noexcept {
        return arena_ == other.arena();
    }

    template <typename U>
    bool operator!=(const ArenaStlAdapter<U>& other) const noexcept {
        return arena_ != other.arena();
    }
};

} // namespace dtb

#endif // DISTRIBUTED_TOKEN_BUCKET_ALLOCATOR_HPP