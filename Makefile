CXX := g++
CXXFLAGS := -std=c++17 -O3 -Wall -Wextra -pthread
LDFLAGS := -pthread

SRC_DIR := src
BUILD_DIR := build
TEST_DIR := tests
BENCH_DIR := benchmarks
INCLUDE_DIR := include

TARGET := $(BUILD_DIR)/distributed-token-bucket
TEST_TARGET := $(BUILD_DIR)/run_tests
BENCH_TARGET := $(BUILD_DIR)/run_benchmark

SRCS := $(wildcard $(SRC_DIR)/*.cpp)
OBJS := $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(SRCS))

TEST_SRCS := $(wildcard $(TEST_DIR)/*.cpp)
BENCH_SRCS := $(wildcard $(BENCH_DIR)/*.cpp)

.PHONY: all build test clean benchmark directories

all: directories build

directories:
	@mkdir -p $(BUILD_DIR)

build: directories $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -I$(INCLUDE_DIR) $^ -o $@ $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -I$(INCLUDE_DIR) -c $< -o $@

test: directories
	@if [ -d "$(TEST_DIR)" ] && [ -n "$(wildcard $(TEST_DIR)/*.cpp)" ]; then \
		$(CXX) $(CXXFLAGS) -I$(INCLUDE_DIR) $(TEST_SRCS) $(filter-out $(BUILD_DIR)/main.o, $(OBJS)) -o $(TEST_TARGET) $(LDFLAGS); \
		$(TEST_TARGET); \
	else \
		echo "No tests found in $(TEST_DIR). Skipping."; \
	fi

benchmark: directories
	@if [ -d "$(BENCH_DIR)" ] && [ -n "$(wildcard $(BENCH_DIR)/*.cpp)" ]; then \
		$(CXX) $(CXXFLAGS) -I$(INCLUDE_DIR) $(BENCH_SRCS) $(filter-out $(BUILD_DIR)/main.o, $(OBJS)) -o $(BENCH_TARGET) $(LDFLAGS); \
		$(BENCH_TARGET); \
	else \
		echo "No benchmarks found in $(BENCH_DIR). Skipping."; \
	fi

clean:
	@rm -rf $(BUILD_DIR)