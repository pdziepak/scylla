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
#include "utils/make_visitor.hh"

static constexpr auto random_test_iteration_count = 20;

static std::random_device rd;
static std::default_random_engine gen(rd());

template<typename T>
T random_int() {
    std::uniform_int_distribution<T> dist;
    return dist(gen);
}

template<typename T>
T random_int(T max) {
    std::uniform_int_distribution<T> dist(0, max);
    return dist(gen);
}


bool random_bool() {
    static std::bernoulli_distribution dist;
    return dist(gen);
}

bytes random_bytes(size_t n) {
    bytes b(bytes::initialized_later(), n);
    boost::generate(b, [] { return random_int<bytes::value_type>(); });
    return b;
}

bytes random_bytes() {
    static std::uniform_int_distribution<size_t> dist_length(0, 128 * 1024);
    return random_bytes(dist_length(gen));
}

class A;
class B;
class C;
class D;

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


static constexpr auto data_size = 128;
using variant_type = imr::variant<A,
                                  imr::member<B, imr::pod<uint64_t>>,
                                  imr::member<C, imr::buffer<C>>,
                                  imr::member<D, imr::pod<int64_t>>>;

struct test_variant_context {
    unsigned _alternative_idx;
public:
    template<typename Tag>
    size_t size_of() const noexcept;

    template<typename Tag>
    auto active_alternative_of() const noexcept;

    template<typename Tag, typename... Args>
    decltype(auto) context_for(Args&&...) const noexcept { return *this; }
};

template<>
size_t test_variant_context::size_of<C>() const noexcept {
    return data_size;
}

template<>
auto test_variant_context::active_alternative_of<A>() const noexcept {
    switch (_alternative_idx) {
    case 0:
        return variant_type::index_for<B>();
    case 1:
        return variant_type::index_for<C>();
    case 2:
        return variant_type::index_for<D>();
    default:
        BOOST_FAIL("should not reach");
        abort();
    }
}

BOOST_AUTO_TEST_CASE(test_variant) {
    for (auto i = 0; i < random_test_iteration_count; i++) {
        unsigned alternative_idx = random_int<unsigned>(2);

        uint64_t uinteger = random_int<uint64_t>();
        int64_t integer = random_int<int64_t>();
        bytes data = random_bytes(data_size);

        const size_t expected_size = alternative_idx == 0
                                     ? imr::pod<uint64_t>::size_when_serialized(uinteger)
                                     : (alternative_idx == 1 ? data_size : sizeof(int64_t));

        auto buffer = std::make_unique<uint8_t[]>(expected_size);

        if (!alternative_idx) {
            BOOST_CHECK_EQUAL(variant_type::size_when_serialized<B>(uinteger), expected_size);
            BOOST_CHECK_EQUAL(variant_type::serialize<B>(buffer.get(), uinteger), expected_size);
        } else if (alternative_idx == 1) {
            BOOST_CHECK_EQUAL(variant_type::size_when_serialized<C>(data), expected_size);
            BOOST_CHECK_EQUAL(variant_type::serialize<C>(buffer.get(), data), expected_size);
        } else {
            BOOST_CHECK_EQUAL(variant_type::size_when_serialized<D>(integer), expected_size);
            BOOST_CHECK_EQUAL(variant_type::serialize<D>(buffer.get(), integer), expected_size);
        }

        auto ctx = test_variant_context { alternative_idx };

        BOOST_CHECK_EQUAL(variant_type::serialized_object_size(buffer.get(), ctx), expected_size);

        auto view = variant_type::make_view(buffer.get(), ctx);
        bool visitor_was_called = false;
        view.visit(utils::make_visitor(
                [&] (imr::pod<uint64_t>::view val) {
                    visitor_was_called = true;
                    if (alternative_idx == 0) {
                        BOOST_CHECK_EQUAL(val.load(), uinteger);
                    } else {
                        BOOST_FAIL("wrong variant alternative (B)");
                    }
                },
                [&] (imr::buffer<C>::view buf) {
                    visitor_was_called = true;
                    if (alternative_idx == 1) {
                        BOOST_CHECK(boost::equal(data, buf));
                    } else {
                        BOOST_FAIL("wrong variant alternative (C)");
                    }
                },
                [&] (imr::pod<int64_t>::view val) {
                    visitor_was_called = true;
                    if (alternative_idx == 2) {
                        BOOST_CHECK_EQUAL(val.load(), integer);
                    } else {
                        BOOST_FAIL("wrong variant alternative (D)");
                    }
                }
        ), ctx);
        BOOST_CHECK(visitor_was_called);
    }
}

BOOST_AUTO_TEST_CASE(test_structure_with_fixed) {
    using S = imr::structure<imr::member<A, imr::pod<uint8_t>>,
                             imr::member<B, imr::pod<int64_t>>,
                             imr::member<C, imr::pod<uint32_t>>>;
    static constexpr auto expected_size = sizeof(uint8_t) + sizeof(uint64_t)
                                          + sizeof(uint32_t);

    for (auto i = 0; i < random_test_iteration_count; i++) {
        auto a = random_int<uint8_t>();
        auto b = random_int<uint64_t>();
        auto c = random_int<uint32_t>();

        auto writer = [&] (auto&& serializer) noexcept {
            return serializer
                .serialize(a)
                .serialize(b)
                .serialize(c)
                .done();
        };

        uint8_t buffer[expected_size];

        BOOST_CHECK_EQUAL(S::size_when_serialized(writer), expected_size);
        BOOST_CHECK_EQUAL(S::serialize(buffer, writer), expected_size);
        BOOST_CHECK_EQUAL(S::serialized_object_size(buffer), expected_size);

        auto mview = S::make_view(buffer);
        BOOST_CHECK_EQUAL(mview.get<A>().load(), a);
        BOOST_CHECK_EQUAL(mview.get<B>().load(), b);
        BOOST_CHECK_EQUAL(mview.get<C>().load(), c);

        auto view = S::make_view(const_cast<const uint8_t*>(buffer));
        BOOST_CHECK_EQUAL(view.get<A>().load(), a);
        BOOST_CHECK_EQUAL(view.get<B>().load(), b);
        BOOST_CHECK_EQUAL(view.get<C>().load(), c);

        a = random_int<uint8_t>();
        b = random_int<uint64_t>();
        c = random_int<uint32_t>();
        mview.get<A>().store(a);
        mview.get<B>().store(b);
        mview.get<C>().store(c);

        BOOST_CHECK_EQUAL(view.get<A>().load(), a);
        BOOST_CHECK_EQUAL(view.get<B>().load(), b);
        BOOST_CHECK_EQUAL(view.get<C>().load(), c);
    }
}

class test_structure_context {
    bool _b_is_present;
    size_t _c_size_of;
public:
    test_structure_context(bool b_is_present, size_t c_size_of) noexcept
        : _b_is_present(b_is_present), _c_size_of(c_size_of) { }

    template<typename Tag>
    bool is_present() const noexcept;

    template<typename Tag>
    size_t size_of() const noexcept;

    template<typename Tag, typename... Args>
    decltype(auto) context_for(Args&&...) const noexcept { return *this; }
};

template<>
bool test_structure_context::is_present<B>() const noexcept {
    return _b_is_present;
}

template<>
size_t test_structure_context::size_of<C>() const noexcept {
    return _c_size_of;
}

BOOST_AUTO_TEST_CASE(test_structure_with_context) {
    using S = imr::structure<imr::member<A, imr::flags<B, C>>,
                             imr::optional_member<B, imr::pod<uint16_t>>,
                             imr::member<C, imr::buffer<C>>>;

    for (auto i = 0; i < random_test_iteration_count; i++) {
        auto b_value = random_int<uint16_t>();
        auto c_data = random_bytes();

        const auto expected_size = 1 + imr::pod<uint16_t>::size_when_serialized(b_value)
                                   + c_data.size();

        auto writer = [&] (auto&& serializer) noexcept {
            return serializer
                .serialize(imr::set_flag<B>())
                .serialize(b_value)
                .serialize(c_data)
                .done();
        };

        BOOST_CHECK_EQUAL(S::size_when_serialized(writer), expected_size);

        auto buffer = std::make_unique<uint8_t[]>(expected_size);
        BOOST_CHECK_EQUAL(S::serialize(buffer.get(), writer), expected_size);

        auto ctx = test_structure_context(true, c_data.size());
        BOOST_CHECK_EQUAL(S::serialized_object_size(buffer.get(), ctx), expected_size);

        auto mview = S::make_view(buffer.get(), ctx);
        BOOST_CHECK(mview.get<A>().get<B>());
        BOOST_CHECK(!mview.get<A>().get<C>());
        BOOST_CHECK_EQUAL(mview.get<B>().get().load(), b_value);
        BOOST_CHECK(boost::range::equal(mview.get<C>(ctx), c_data));

        auto view = S::view(mview);
        BOOST_CHECK(view.get<A>().get<B>());
        BOOST_CHECK(!view.get<A>().get<C>());
        BOOST_CHECK_EQUAL(view.get<B>().get().load(), b_value);
        BOOST_CHECK(boost::range::equal(view.get<C>(ctx), c_data));
    }
}

BOOST_AUTO_TEST_CASE(test_structure_get_element_without_view) {
    using S = imr::structure<imr::member<A, imr::flags<B, C>>,
                             imr::member<B, imr::pod<uint64_t>>,
                             imr::optional_member<C, imr::pod<uint16_t>>>;

    auto uinteger = random_int<uint64_t>();

    static constexpr auto expected_size = 1 + sizeof(uint64_t);

    auto writer = [&] (auto&& serializer) noexcept {
        return serializer
            .serialize(imr::set_flag<B>())
            .serialize(uinteger)
            .skip()
            .done();
    };

    BOOST_CHECK_EQUAL(S::size_when_serialized(writer), expected_size);

    uint8_t buffer[expected_size];
    BOOST_CHECK_EQUAL(S::serialize(buffer, writer), expected_size);

    auto fview = S::get_member<A>(buffer);
    BOOST_CHECK(fview.get<B>());
    BOOST_CHECK(!fview.get<C>());

    auto uview = S::get_member<B>(buffer);
    BOOST_CHECK_EQUAL(uview.load(), uinteger);
    // FIXME test offset
}

BOOST_AUTO_TEST_CASE(test_nested_structure) {
    using S1 = imr::structure<imr::optional_member<B, imr::pod<uint16_t>>,
                              imr::member<C, imr::buffer<C>>,
                              imr::member<A, imr::pod<uint8_t>>>;

    using S = imr::structure<imr::member<A, imr::pod<uint16_t>>,
                             imr::member<B, S1>,
                             imr::member<C, imr::pod<uint32_t>>>;

    for (auto i = 0; i < random_test_iteration_count; i++) {
        auto b1_value = random_int<uint16_t>();
        auto c1_data = random_bytes();
        auto a1_value = random_int<uint8_t>();

        const auto expected_size1 = imr::pod<uint16_t>::size_when_serialized(b1_value)
                                    + c1_data.size() + sizeof(uint8_t);

        auto a_value = random_int<uint16_t>();
        auto c_value = random_int<uint32_t>();

        const auto expected_size = sizeof(uint16_t) + expected_size1 + sizeof(uint32_t);

        auto writer1 = [&] (auto&& serializer) noexcept {
            return serializer
                    .serialize(b1_value)
                    .serialize(c1_data)
                    .serialize(a1_value)
                    .done();
        };

        auto writer = [&] (auto&& serializer) noexcept {
            return serializer
                    .serialize(a_value)
                    .serialize(writer1)
                    .serialize(c_value)
                    .done();
        };

        BOOST_CHECK_EQUAL(S::size_when_serialized(writer), expected_size);

        auto buffer = std::make_unique<uint8_t[]>(expected_size);
        BOOST_CHECK_EQUAL(S::serialize(buffer.get(), writer), expected_size);

        auto ctx = test_structure_context(true, c1_data.size());
        BOOST_CHECK_EQUAL(S::serialized_object_size(buffer.get(), ctx), expected_size);

        auto view = S::make_view(buffer.get(), ctx);
        BOOST_CHECK_EQUAL(view.get<A>().load(), a_value);
        BOOST_CHECK_EQUAL(view.get<B>(ctx).get<B>().get().load(), b1_value);
        BOOST_CHECK(boost::range::equal(view.get<B>(ctx).get<C>(ctx), c1_data));
        BOOST_CHECK_EQUAL(view.get<C>(ctx).load(), c_value);
    }
}

BOOST_AUTO_TEST_SUITE_END();
