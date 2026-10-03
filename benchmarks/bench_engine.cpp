#include <iostream>
#include <chrono>
#include <vector>
#include <numeric>
#include <algorithm>
#include <thread>
#include <mutex>
#include <atomic>
#include <iomanip>

// Distributed Token Bucket Microbenchmark Implementation
class DistributedTokenBucket {
private:
    double capacity;
    double refill_rate; // tokens per microsecond
    std::atomic<double> tokens;
    std::chrono::steady_clock::time_point last_refill;
    std::mutex mtx;

    void refill() {
        auto now = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(now - last_refill).count();
        if (duration > 0) {
            double generated = duration * refill_rate;
            double current = tokens.load(std::memory_order_relaxed);
            double updated = std::min(capacity, current + generated);
            tokens.store(updated, std::memory_order_relaxed);
            last_refill = now;
        }
    }

public:
    DistributedTokenBucket(double cap, double rate_per_sec) 
        : capacity(cap), refill_rate(rate_per_sec / 1000000.0), tokens(cap), last_refill(std::chrono::steady_clock::now()) {}

    bool consume(double count) {
        std::lock_guard<std::mutex> lock(mtx);
        refill();
        double current = tokens.load(std::memory_order_relaxed);
        if (current >= count) {
            tokens.store(current - count, std::memory_order_relaxed);
            return true;
        }
        return false;
    }
};

int main() {
    const int iterations = 1000000;
    const double capacity = 1000000.0;
    const double refill_rate = 1000000.0;
    
    DistributedTokenBucket bucket(capacity, refill_rate);
    std::vector<double> latencies;
    latencies.reserve(iterations);

    // Warmup
    for (int i = 0; i < 1000; ++i) {
        bucket.consume(1.0);
    }

    auto start_time = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < iterations; ++i) {
        auto op_start = std::chrono::high_resolution_clock::now();
        
        // Ensure we always get a token for pure latency measurement of the algorithm
        while (!bucket.consume(1.0)) {
            // Spin if bucket is empty
        }
        
        auto op_end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start).count();
        latencies.push_back(static_cast<double>(duration));
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time).count();

    double ops_sec = iterations / total_duration;

    std::sort(latencies.begin(), latencies.end());
    double p50 = latencies[iterations * 0.50];
    double p99 = latencies[iterations * 0.99];
    double p999 = latencies[static_cast<size_t>(iterations * 0.999)];

    std::cout << "\n==================================================\n";
    std::cout << "      Distributed Token Bucket Microbenchmark     \n";
    std::cout << "==================================================\n";
    std::cout << std::left << std::setw(25) << "Metric" << std::setw(20) << "Value" << std::setw(10) << "Unit" << "\n";
    std::cout << "--------------------------------------------------\n";
    std::cout << std::left << std::setw(25) << "Total Iterations" << std::setw(20) << iterations << std::setw(10) << "ops" << "\n";
    std::cout << std::left << std::setw(25) << "Throughput" << std::setw(20) << std::fixed << std::setprecision(2) << ops_sec << std::setw(10) << "ops/sec" << "\n";
    std::cout << std::left << std::setw(25) << "Latency p50" << std::setw(20) << std::fixed << std::setprecision(2) << p50 << std::setw(10) << "ns" << "\n";
    std::cout << std::left << std::setw(25) << "Latency p99" << std::setw(20) << std::fixed << std::setprecision(2) << p99 << std::setw(10) << "ns" << "\n";
    std::cout << std::left << std::setw(25) << "Latency p99.9" << std::setw(20) << std::fixed << std::setprecision(2) << p999 << std::setw(10) << "ns" << "\n";
    std::cout << "==================================================\n\n";

    return 0;
}