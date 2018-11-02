/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
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

#define BOOST_TEST_MODULE small_vector

#include <boost/test/included/unit_test.hpp>

#include "utils/small_vector.hh"

template<typename T, size_t N>
void check_equivalent(const utils::small_vector<T, N>& a, const std::vector<T>& b) {
    BOOST_REQUIRE_EQUAL(a.size(), b.size());
    BOOST_REQUIRE_GE(a.capacity(), a.size());
    for (auto i = 0u; i < b.size(); i++) {
        BOOST_REQUIRE(a[i] == b[i]);
    }
    auto idx = 0;
    for (auto&& v : a) {
        BOOST_REQUIRE(v == b[idx++]);
    }
}

template<typename T>
void test_random_walk(std::function<T()> make_element) {
    utils::small_vector<T, 8> actual;
    std::vector<T> expected;

    auto emplace_back = [&] (T x) {
        actual.emplace_back(x);
        expected.emplace_back(x);
        check_equivalent(actual, expected);
    };

    auto move_ctor = [&] {
        auto a = std::move(actual);
        check_equivalent(a, expected);
        return a;
    };

    auto move_assign = [&] {
        auto a = utils::small_vector<T, 8>();
        a = std::move(actual);
        check_equivalent(a, expected);
        return a;
    };

    for (auto i = 0; i < 64; i++) {
        emplace_back(make_element());
        auto a = move_ctor();
        actual = std::move(a);
        a = move_assign();
        actual = std::move(a);
    }

    auto another_actual = utils::small_vector<T, 8>(expected.begin(), expected.end());
    check_equivalent(another_actual, expected);

    for (auto i = 0u; i <= actual.size(); i++) {
        {
            auto a = actual;
            BOOST_CHECK(a == actual);

            auto e = expected;
            a.insert(a.begin() + i, actual.begin(), actual.begin());
            e.insert(e.begin() + i, actual.begin(), actual.begin());
            check_equivalent(a, e);

            BOOST_CHECK(a == actual);
        }

        {
            auto a = actual;
            BOOST_CHECK(a == actual);
            auto e = expected;

            a.insert(a.begin() + i, actual.begin(), actual.end());
            e.insert(e.begin() + i, actual.begin(), actual.end());
            check_equivalent(a, e);

            BOOST_CHECK(a != actual);
        }

        {
            auto a = actual;
            auto e = expected;
            a.insert(a.begin() + i, actual.begin(), actual.begin() + 1);
            e.insert(e.begin() + i, actual.begin(), actual.begin() + 1);
            check_equivalent(a, e);
        }

        {
            auto a = actual;
            auto e = expected;
            a.insert(a.begin() + i, actual[0]);
            e.insert(e.begin() + i, actual[0]);
            check_equivalent(a, e);
        }

        {
            auto a = actual;
            auto e = expected;
            a.erase(a.begin(), a.begin() + i);
            e.erase(e.begin(), e.begin() + i);
            check_equivalent(a, e);
        }

        if (i < actual.size()) {
            auto a = actual;
            auto e = expected;
            a.erase(a.begin() + i, a.end());
            e.erase(e.begin() + i, e.end());
            check_equivalent(a, e);
        }

        if (i < actual.size()) {
            auto a = actual;
            auto e = expected;
            a.erase(a.begin() + i);
            e.erase(e.begin() + i);
            check_equivalent(a, e);
        }
    }
}

BOOST_AUTO_TEST_CASE(random_walk_trivial) {
    test_random_walk<int>([x = 0] () mutable { return x++; });
}

BOOST_AUTO_TEST_CASE(random_walk_nontrivial) {
    test_random_walk<std::shared_ptr<int>>([x = 0] () mutable { return std::make_shared<int>(x++); });
}

template<typename T>
void test_insert(std::function<T()> make_element) {
    utils::small_vector<T, 8> actual;
    std::vector<T> expected;

    auto emplace_back = [&] {
        auto e = make_element();
        actual.emplace_back(e);
        expected.emplace_back(e);
    };

    auto insert_middle = [&] (size_t count) {
        auto a = actual;
        auto e = expected;
        check_equivalent(a, e);

        std::vector<T> vec;
        std::generate_n(std::back_inserter(vec), count, make_element);
        a.insert(a.begin() + 1, vec.begin(), vec.end());
        e.insert(e.begin() + 1, vec.begin(), vec.end());
        check_equivalent(a, e);
    };

    auto insert_end = [&] (size_t count) {
        auto a = actual;
        auto e = expected;
        check_equivalent(a, e);

        std::vector<T> vec;
        std::generate_n(std::back_inserter(vec), count, make_element);
        a.insert(a.end(), vec.begin(), vec.end());
        e.insert(e.end(), vec.begin(), vec.end());
        check_equivalent(a, e);
    };

    auto test_inserts = [&] {
        insert_middle(2);
        insert_middle(4);
        insert_middle(6);
        insert_middle(8);
        insert_middle(64);

        insert_end(2);
        insert_end(4);
        insert_end(6);
        insert_end(8);
        insert_end(64);
    };

    for (auto i = 0u; i < 2u; i++) {
        emplace_back();
    }

    test_inserts();

    for (auto i = 0u; i < 2u; i++) {
        emplace_back();
    }

    test_inserts();

    for (auto i = 0u; i < 4u; i++) {
        emplace_back();
    }

    test_inserts();
}

BOOST_AUTO_TEST_CASE(insert_trivial) {
    test_insert<int>([x = 0] () mutable { return x++; });
}

BOOST_AUTO_TEST_CASE(insert_nontrivial) {
    test_insert<std::shared_ptr<int>>([x = 0] () mutable { return std::make_shared<int>(x++); });
}

namespace  {

class fails_on_copy {
    size_t _counter;
public:
    explicit fails_on_copy(size_t counter) : _counter(counter) { }
    ~fails_on_copy() {
        // Make sure the compiler doesn't do anything clever
        *reinterpret_cast<volatile size_t*>(&_counter) = -1;
    }

    fails_on_copy(fails_on_copy&&) = default;
    fails_on_copy(const fails_on_copy& other) : _counter(other._counter - 1) {
        if (!_counter) {
            throw 1;
        }
    }
    fails_on_copy& operator=(fails_on_copy&&) = default;
    fails_on_copy& operator=(const fails_on_copy& other) {
        _counter = other._counter - 1;
        if (!_counter) {
            throw 1;
        }
        return *this;
    }

    size_t counter() const { return _counter; }
};

}

BOOST_AUTO_TEST_CASE(exception_safety) {
    std::vector<fails_on_copy> vec;
    vec.emplace_back(4);
    vec.emplace_back(1);

    BOOST_REQUIRE_THROW((utils::small_vector<fails_on_copy, 1>(vec.begin(), vec.end())), int);
    BOOST_REQUIRE_THROW((utils::small_vector<fails_on_copy, 4>(vec.begin(), vec.end())), int);

    utils::small_vector<fails_on_copy, 2> v;
    v.emplace_back(0);
    v.emplace_back(1);
    v.emplace_back(2);
    v.emplace_back(3);

    auto verify_unchanged = [&] {
        BOOST_REQUIRE_EQUAL(v.size(), 4);
        for (auto i = 0; i < 4; i++) {
            BOOST_REQUIRE_EQUAL(v[i].counter(), i);
        }
    };

    BOOST_REQUIRE_THROW(v.insert(v.begin(), vec.begin(), vec.end()), int);
    verify_unchanged();

    BOOST_REQUIRE_THROW(v.insert(v.end(), vec.begin(), vec.end()), int);
    verify_unchanged();

    vec.emplace_back(4);
    vec.emplace_back(4);
    vec.emplace_back(4);
    vec.emplace_back(4);
    BOOST_REQUIRE_THROW(v.insert(v.begin(), vec.begin(), vec.end()), int);
    verify_unchanged();

    vec.clear();
    vec.emplace_back(4);
    vec.emplace_back(4);
    vec.emplace_back(4);
    vec.emplace_back(4);
    vec.emplace_back(4);
    vec.emplace_back(1);
    BOOST_REQUIRE_THROW(v.insert(v.begin(), vec.begin(), vec.end()), int);
    verify_unchanged();

    auto fc = fails_on_copy(1);
    BOOST_REQUIRE_THROW(v.insert(v.begin(), fc), int);
    verify_unchanged();

    fc = fails_on_copy(1);
    BOOST_REQUIRE_THROW(v.insert(v.end(), fc), int);
    verify_unchanged();

    fc = fails_on_copy(1);
    BOOST_REQUIRE_THROW(v.push_back(fc), int);
    verify_unchanged();
}

BOOST_AUTO_TEST_CASE(resize) {
    auto vec = utils::small_vector<int, 4>();
    vec.emplace_back(1);
    vec.resize(1024);

    BOOST_REQUIRE_EQUAL(vec.size(), 1024);
    BOOST_REQUIRE_EQUAL(vec[0], 1);
    for (auto i = 0; i < 1023; i++) {
        BOOST_REQUIRE_EQUAL(vec[i + 1], 0);
    }

    vec.resize(1024);
    BOOST_REQUIRE_EQUAL(vec.size(), 1024);
    BOOST_REQUIRE_EQUAL(vec[0], 1);
    for (auto i = 0; i < 1023; i++) {
        BOOST_REQUIRE_EQUAL(vec[i + 1], 0);
    }

    vec.resize(512);
    BOOST_REQUIRE_EQUAL(vec.size(), 512);
    BOOST_REQUIRE_EQUAL(vec[0], 1);
    for (auto i = 0; i < 511; i++) {
        BOOST_REQUIRE_EQUAL(vec[i + 1], 0);
    }

    vec.resize(0);
    BOOST_REQUIRE_EQUAL(vec.size(), 0);
    for (auto&& v : vec) {
        (void)v;
        BOOST_FAIL("should not reach");
    }
}