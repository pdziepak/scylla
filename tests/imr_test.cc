/*
 * Copyright (C) 2017 ScyllaDB
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

#define BOOST_TEST_MODULE imr
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <random>

#include <boost/range/irange.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/generate.hpp>

#include "imr/fundamental.hh"
#include "imr/compound.hh"

static constexpr auto random_test_iteration_count = 20;

static std::random_device rd;
static std::default_random_engine gen(rd());

template<typename T>
T random_int() {
    std::uniform_int_distribution<T> dist;
    return dist(gen);
}

bytes random_bytes() {
    std::uniform_int_distribution<size_t> dist_length(0, 128 * 1024);
    bytes b(bytes::initialized_later(), dist_length(gen));
    boost::generate(b, [] { return random_int<bytes::value_type>(); });
    return b;
}

class A;
class B;
class C;

BOOST_AUTO_TEST_SUITE(fundamental);

template<typename FillAB, typename FillBC>
struct generate_flags_type;

template<size_t... IdxAB, size_t... IdxBC>
struct generate_flags_type<std::index_sequence<IdxAB...>, std::index_sequence<IdxBC...>> {
    using type = imr::flags<A, std::integral_constant<size_t, IdxAB>...,
                            B, std::integral_constant<ssize_t, IdxBC>..., C>;
};

BOOST_AUTO_TEST_CASE(test_flags) {
    using flags_type = generate_flags_type<std::make_index_sequence<7>, std::make_index_sequence<8>>::type;
    static constexpr size_t expected_size = 3;

    BOOST_CHECK_EQUAL(flags_type::size_when_serialized(), expected_size);
    BOOST_CHECK_EQUAL(flags_type::size_when_serialized(imr::set_flag<A>(),
                                                       imr::set_flag<B>(),
                                                       imr::set_flag<C>()), expected_size);

    uint8_t buffer[expected_size];
    std::fill_n(buffer, expected_size, 0xbe);
    BOOST_CHECK_EQUAL(flags_type::serialize(buffer, imr::set_flag<B>()), expected_size);

    auto mview = flags_type::make_view(buffer);
    BOOST_CHECK(!mview.get<A>());
    BOOST_CHECK(mview.get<B>());
    BOOST_CHECK(!mview.get<C>());

    mview.set<A>();
    mview.set<B>(false);
    BOOST_CHECK(mview.get<A>());
    BOOST_CHECK(!mview.get<B>());
    BOOST_CHECK(!mview.get<C>());

    flags_type::view view = mview;
    mview.set<C>();
    BOOST_CHECK(view.get<A>());
    BOOST_CHECK(!view.get<B>());
    BOOST_CHECK(view.get<C>());

    BOOST_CHECK_EQUAL(flags_type::serialized_object_size(buffer), expected_size);

    int some_context;
    BOOST_CHECK_EQUAL(flags_type::serialized_object_size(buffer, some_context), expected_size);

    std::fill_n(buffer, expected_size, 0xff);
    BOOST_CHECK_EQUAL(flags_type::serialize(buffer), expected_size);
    BOOST_CHECK(!mview.get<A>());
    BOOST_CHECK(!mview.get<B>());
    BOOST_CHECK(!mview.get<C>());
}

struct test_pod_type {
    int32_t x;
    uint64_t y;

    friend bool operator==(const test_pod_type& a, const test_pod_type& b) {
        return a.x == b.x && a.y == b.y;
    }
    friend std::ostream& operator<<(std::ostream& os, const test_pod_type& obj) {
        return os << "test_pod_type { x: " << obj.x << ", y: " << obj.y << " }";
    }
};

BOOST_AUTO_TEST_CASE(test_pod) {
    auto generate_object = [] {
        std::uniform_int_distribution<decltype(test_pod_type::x)> dist_x;
        std::uniform_int_distribution<decltype(test_pod_type::y)> dist_y;
        return test_pod_type { dist_x(gen), dist_y(gen) };
    };
    using pod_type = imr::pod<test_pod_type>;

    uint8_t buffer[pod_type::size];
    for (auto i = 0; i < random_test_iteration_count; i++) {
        auto obj = generate_object();

        BOOST_CHECK_EQUAL(pod_type::size_when_serialized(obj), pod_type::size);
        BOOST_CHECK_EQUAL(pod_type::serialize(buffer, obj), pod_type::size);


        BOOST_CHECK_EQUAL(pod_type::serialized_object_size(buffer), pod_type::size);
        int some_context;
        BOOST_CHECK_EQUAL(pod_type::serialized_object_size(buffer, some_context), pod_type::size);

        auto mview = pod_type::make_view(buffer);
        pod_type::view view = mview;

        BOOST_CHECK_EQUAL(mview.load(), obj);
        BOOST_CHECK_EQUAL(view.load(), obj);

        auto obj2 = generate_object();
        mview.store(obj2);

        BOOST_CHECK_EQUAL(mview.load(), obj2);
        BOOST_CHECK_EQUAL(view.load(), obj2);
    }
}

class test_buffer_context {
    size_t _size;
public:
    explicit test_buffer_context(size_t sz) : _size(sz) { }

    template<typename Tag>
    size_t size_of() const noexcept;
};

template<>
size_t test_buffer_context::size_of<A>() const noexcept {
    return _size;
}

BOOST_AUTO_TEST_CASE(test_buffer) {
    using buffer_type = imr::buffer<A>;

    auto test = [] (auto serialize) {
        auto data = random_bytes();
        auto size = data.size();

        auto buffer = std::make_unique<uint8_t[]>(size);

        serialize(buffer.get(), size, data);

        const auto ctx = test_buffer_context(size);
        BOOST_CHECK_EQUAL(buffer_type::serialized_object_size(buffer.get(), ctx), size);

        BOOST_CHECK(boost::range::equal(buffer_type::make_view(buffer.get(), ctx), data));
        BOOST_CHECK(boost::range::equal(buffer_type::make_view(const_cast<const uint8_t*>(buffer.get()), ctx), data));

        BOOST_CHECK_EQUAL(buffer_type::make_view(buffer.get(), ctx).size(), size);
    };

    for (auto i = 0; i < random_test_iteration_count; i++) {
        test([] (uint8_t* out, size_t size, const bytes& data) {
            BOOST_CHECK_EQUAL(buffer_type::size_when_serialized(data), size);
            BOOST_CHECK_EQUAL(buffer_type::serialize(out, data), size);
        });

        test([] (uint8_t* out, size_t size, const bytes& data) {
            auto serializer = [&data] (uint8_t* out) noexcept {
                boost::range::copy(data, out);
            };
            BOOST_CHECK_EQUAL(buffer_type::size_when_serialized(size, serializer), size);
            BOOST_CHECK_EQUAL(buffer_type::serialize(out, size, serializer), size);
        });
    }
}

BOOST_AUTO_TEST_SUITE_END();

BOOST_AUTO_TEST_SUITE(compound);

struct test_optional_context {
    template<typename Tag>
    bool is_present() const noexcept;

    template<typename Tag, typename... Args>
    decltype(auto) context_for(Args&&...) const noexcept { return *this; }
};
template<>
bool test_optional_context::is_present<A>() const noexcept {
    return true;
}
template<>
bool test_optional_context::is_present<B>() const noexcept {
    return false;
}

BOOST_AUTO_TEST_CASE(test_optional) {
    using optional_type1 = imr::optional<A, imr::pod<uint32_t>>;
    using optional_type2 = imr::optional<B, imr::pod<uint32_t>>;

    for (auto i = 0; i < random_test_iteration_count; i++) {
        auto value = random_int<uint32_t>();
        auto expected_size = imr::pod<uint32_t>::size_when_serialized(value);

        auto buffer = std::make_unique<uint8_t[]>(expected_size);

        BOOST_CHECK_EQUAL(optional_type1::size_when_serialized(value), expected_size);
        BOOST_CHECK_EQUAL(optional_type1::serialize(buffer.get(), value), expected_size);

        BOOST_CHECK_EQUAL(optional_type1::serialized_object_size(buffer.get(), test_optional_context()), expected_size);
        BOOST_CHECK_EQUAL(optional_type2::serialized_object_size(buffer.get(), test_optional_context()), 0);

        auto view = optional_type1::make_view(buffer.get());
        BOOST_CHECK_EQUAL(view.get().load(), value);
    }
}

BOOST_AUTO_TEST_SUITE_END();
