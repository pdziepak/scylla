/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <atomic>
#include <memory>

#include <fmt/format.h>

#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>

#include "seastarx.hh"

namespace perf_tests::internal {

struct config;

using clock_type = std::chrono::steady_clock;

class performance_test {
    std::string _test_case;
    std::string _test_group;

    uint64_t _single_run_iterations = 0;
    std::atomic<uint64_t> _max_single_run_iterations;
private:
    void do_run(const config&);
protected:
    [[gnu::always_inline]] [[gnu::hot]]
    bool stop_iteration() const {
        return _single_run_iterations >= _max_single_run_iterations.load(std::memory_order_relaxed);
    }

    [[gnu::always_inline]] [[gnu::hot]]
    void next_iteration() {
        _single_run_iterations++;
    }

    virtual void set_up() = 0;
    virtual void tear_down() noexcept = 0;
    virtual future<clock_type::duration> do_single_run() = 0;
public:
    performance_test(const std::string& test_case, const std::string& test_group)
        : _test_case(test_case)
        , _test_group(test_group)
    { }

    virtual ~performance_test() = default;

    const std::string& test_case() const { return _test_case; }
    const std::string& test_group() const { return _test_group; }
    std::string name() const { return fmt::format("{}.{}", test_group(), test_case()); }

    void run(const config&);
public:
    static void register_test(std::unique_ptr<performance_test>);
};

// Helper for measuring time.
// Each microbenchmark can either use the default behaviour which measures
// only the start and stop time of the whole run or manually invoke
// start_measuring_time() and stop_measuring_time() in order to measure
// only parts of each iteration.
class time_measurement {
    clock_type::time_point _run_start_time;
    clock_type::time_point _start_time;
    clock_type::duration _total_time;
public:
    [[gnu::always_inline]] [[gnu::hot]]
    void start_run() {
        _total_time = { };
        auto t = clock_type::now();
        _run_start_time = t;
        _start_time = t;
    }

    [[gnu::always_inline]] [[gnu::hot]]
    clock_type::duration stop_run() {
        auto t = clock_type::now();
        if (_start_time == _run_start_time) {
            return t - _start_time;
        }
        return _total_time;
    }

    [[gnu::always_inline]] [[gnu::hot]]
    void start_iteration() {
        _start_time = clock_type::now();
    }
    
    [[gnu::always_inline]] [[gnu::hot]]
    void stop_iteration() {
        auto t = clock_type::now();
        _total_time += t - _start_time;
    }
};

extern time_measurement measure_time;

template<typename Test>
class concrete_performance_test final : public performance_test {
    std::optional<Test> _test;
protected:
    virtual void set_up() override {
        _test.emplace();
    }

    virtual void tear_down() noexcept override {
        _test.reset();
    }

    [[gnu::hot]]
    virtual future<clock_type::duration> do_single_run() override {
        if constexpr(is_future<decltype(_test->run())>::value) {
            measure_time.start_run();
            return do_until([this] { return stop_iteration(); }, [this] {
                next_iteration();
                return _test->run();
            }).then([] {
                return measure_time.stop_run();
            });
        } else {
            measure_time.start_run();
            while (!stop_iteration()) {
                next_iteration();
                _test->run();
            }
            return make_ready_future<clock_type::duration>(measure_time.stop_run());
        }
    }
public:
    using performance_test::performance_test;
};

void register_test(std::unique_ptr<performance_test>);

template<typename Test>
struct test_registrar {
    test_registrar(const std::string& test_group, const std::string& test_case) {
        auto test = std::make_unique<concrete_performance_test<Test>>(test_case, test_group);
        performance_test::register_test(std::move(test));
    }
};

}

namespace perf_tests {

[[gnu::always_inline]]
inline void start_measuring_time()
{
    internal::measure_time.start_iteration();
}

[[gnu::always_inline]]
inline void stop_measuring_time()
{
    internal::measure_time.stop_iteration();
}


template<typename T>
void do_not_optimize(T& v)
{
    asm volatile("" : : "r,m" (v));
}

}

#define PERF_TEST_F(test_group, test_case) \
    struct test_##test_group##_##test_case : test_group { \
        [[gnu::always_inline]] inline auto run(); \
    }; \
    static ::perf_tests::internal::test_registrar<test_##test_group##_##test_case> \
    test_##test_group##_##test_case##_registrar(#test_group, #test_case); \
    [[gnu::always_inline]] auto test_##test_group##_##test_case::run()

#define PERF_TEST(test_group, test_case) \
    struct test_##test_group##_##test_case { \
        [[gnu::always_inline]] inline auto run(); \
    }; \
    static ::perf_tests::internal::test_registrar<test_##test_group##_##test_case> \
    test_##test_group##_##test_case##_registrar(#test_group, #test_case); \
    [[gnu::always_inline]] auto test_##test_group##_##test_case::run()
