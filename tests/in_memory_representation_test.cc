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

#define BOOST_TEST_MODULE in_mem_rep
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <random>

#include <boost/range/combine.hpp>
#include <boost/range/irange.hpp>
#include <boost/range/adaptor/sliced.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/generate.hpp>
#include <boost/range/algorithm/random_shuffle.hpp>
#include <boost/range/algorithm_ext/iota.hpp>

#include "in_memory_representation.hh"

static constexpr auto random_test_iteration_count = 20;

static std::random_device rd;
static std::default_random_engine gen(rd());

template<typename T>
T random_int() {
    static std::uniform_int_distribution<T> dist;
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

BOOST_AUTO_TEST_SUITE(meta);

template<typename T, typename = int>
struct do_has_value : std::false_type { };

template<typename T>
struct do_has_value<T, decltype((void)T::value, 0)> : std::true_type { };

template<typename T>
using has_value = do_has_value<T>;

template<typename T, typename = int>
struct has_type : std::false_type { };

template<typename T>
struct has_type<T, decltype(std::declval<typename T::type>(), 0)> : std::true_type { };

BOOST_AUTO_TEST_CASE(test_meta_find_if) {
    using Av = std::is_void<A>;
    using Bv = std::is_void<B>;
    using Cv = std::is_void<C>;

    BOOST_CHECK((has_value<imr::meta::find_if<has_value, Av>>::value));
    BOOST_CHECK_EQUAL((imr::meta::find_if<has_value, Av>::value), 0);
    BOOST_CHECK_EQUAL((imr::meta::find_if<has_value, Av, B, C>::value), 0);
    BOOST_CHECK_EQUAL((imr::meta::find_if<has_value, A, Bv, C>::value), 1);
    BOOST_CHECK_EQUAL((imr::meta::find_if<has_value, A, B, Cv>::value), 2);

    BOOST_CHECK((has_value<imr::meta::find_if<has_value, Av, Bv, Cv>>::value));
    BOOST_CHECK_EQUAL((imr::meta::find_if<has_value, Av, Bv, Cv>::value), 0);
    BOOST_CHECK_EQUAL((imr::meta::find_if<has_value, C, Av, Bv>::value), 1);
    BOOST_CHECK_EQUAL((imr::meta::find_if<has_value, B, B, Av>::value), 2);

    BOOST_CHECK(!(has_value<imr::meta::find_if<has_value>>::value));
    BOOST_CHECK(!(has_value<imr::meta::find_if<has_value, B, C>>::value));
    BOOST_CHECK(!(has_value<imr::meta::find_if<has_value, C, C>>::value));
}

BOOST_AUTO_TEST_CASE(test_meta_find) {
    BOOST_CHECK((has_value<imr::meta::find<A, A>>::value));
    BOOST_CHECK_EQUAL((imr::meta::find<A, A>::value), 0);
    BOOST_CHECK_EQUAL((imr::meta::find<A, A, B, C>::value), 0);
    BOOST_CHECK_EQUAL((imr::meta::find<B, A, B, C>::value), 1);
    BOOST_CHECK_EQUAL((imr::meta::find<C, A, B, C>::value), 2);

    BOOST_CHECK((has_value<imr::meta::find<A, A, A, A>>::value));
    BOOST_CHECK_EQUAL((imr::meta::find<A, A, A, A>::value), 0);
    BOOST_CHECK_EQUAL((imr::meta::find<A, C, A, A>::value), 1);
    BOOST_CHECK_EQUAL((imr::meta::find<A, B, B, A>::value), 2);

    BOOST_CHECK(!(has_value<imr::meta::find<A>>::value));
    BOOST_CHECK(!(has_value<imr::meta::find<A, B, C>>::value));
    BOOST_CHECK(!(has_value<imr::meta::find<A, C, C>>::value));
}

BOOST_AUTO_TEST_CASE(test_meta_head) {
    BOOST_CHECK(!(has_type<imr::meta::head<>>::value));
    BOOST_CHECK((has_type<imr::meta::head<A>>::value));
    BOOST_CHECK((std::is_same<imr::meta::head<A>::type, A>::value));
    BOOST_CHECK((std::is_same<imr::meta::head<A, B, C>::type, A>::value));
    BOOST_CHECK((std::is_same<imr::meta::head<A, A, A>::type, A>::value));
}

BOOST_AUTO_TEST_SUITE_END();

BOOST_AUTO_TEST_SUITE(imr);

template<typename FillAB, typename FillBC>
struct generate_flags_type;

template<size_t... IdxAB, size_t... IdxBC>
struct generate_flags_type<std::index_sequence<IdxAB...>, std::index_sequence<IdxBC...>> {
    using type = imr::flags<A, std::integral_constant<size_t, IdxAB>...,
                            B, std::integral_constant<ssize_t, IdxBC>..., C>;
};

BOOST_AUTO_TEST_CASE(test_flags) {
    using F = generate_flags_type<std::make_index_sequence<7>, std::make_index_sequence<8>>::type;
    static constexpr size_t expected_size = 3;

    BOOST_CHECK_EQUAL(F::size_when_serialized(), expected_size);
    BOOST_CHECK_EQUAL(F::size_when_serialized(imr::set_flag<A>(),
                                              imr::set_flag<B>(),
                                              imr::set_flag<C>()), expected_size);

    uint8_t buffer[expected_size];
    std::fill_n(buffer, expected_size, 0xbe);
    BOOST_CHECK_EQUAL(F::serialize(buffer, imr::set_flag<B>()), expected_size);

    auto mview = F::make_view(buffer);
    BOOST_CHECK(!mview.get<A>());
    BOOST_CHECK(mview.get<B>());
    BOOST_CHECK(!mview.get<C>());

    mview.set<A>();
    mview.set<B>(false);
    BOOST_CHECK(mview.get<A>());
    BOOST_CHECK(!mview.get<B>());
    BOOST_CHECK(!mview.get<C>());

    F::view view = mview;
    mview.set<C>();
    BOOST_CHECK(view.get<A>());
    BOOST_CHECK(!view.get<B>());
    BOOST_CHECK(view.get<C>());

    BOOST_CHECK_EQUAL(F::serialized_object_size(buffer), expected_size);

    int some_context;
    BOOST_CHECK_EQUAL(F::serialized_object_size(buffer, some_context), expected_size);

    std::fill_n(buffer, expected_size, 0xff);
    BOOST_CHECK_EQUAL(F::serialize(buffer), expected_size);
    BOOST_CHECK(!mview.get<A>());
    BOOST_CHECK(!mview.get<B>());
    BOOST_CHECK(!mview.get<C>());
}

struct test_fixed_size_value_type {
    int32_t x;
    uint64_t y;

    friend bool operator==(const test_fixed_size_value_type& a, const test_fixed_size_value_type& b) {
        return a.x == b.x && a.y == b.y;
    }
    friend std::ostream& operator<<(std::ostream& os, const test_fixed_size_value_type& obj) {
        return os << "test_fixed_size_value_type { x: " << obj.x << ", y: " << obj.y << " }";
    }
};

BOOST_AUTO_TEST_CASE(test_fixed_size_value) {
    auto generate_object = [] {
        std::uniform_int_distribution<decltype(test_fixed_size_value_type::x)> dist_x;
        std::uniform_int_distribution<decltype(test_fixed_size_value_type::y)> dist_y;
        return test_fixed_size_value_type { dist_x(gen), dist_y(gen) };
    };
    using FSV = imr::fixed_size_value<test_fixed_size_value_type>;

    uint8_t buffer[FSV::size];
    for (auto i : boost::irange(0, random_test_iteration_count)) {
        (void)i;
        auto obj = generate_object();

        BOOST_CHECK_EQUAL(FSV::size_when_serialized(obj), FSV::size);
        BOOST_CHECK_EQUAL(FSV::serialize(buffer, obj), FSV::size);


        BOOST_CHECK_EQUAL(FSV::serialized_object_size(buffer), FSV::size);
        int some_context;
        BOOST_CHECK_EQUAL(FSV::serialized_object_size(buffer, some_context), FSV::size);

        auto mview = FSV::make_view(buffer);
        FSV::view view = mview;

        BOOST_CHECK_EQUAL(mview.load(), obj);
        BOOST_CHECK_EQUAL(view.load(), obj);

        auto obj2 = generate_object();
        mview.store(obj2);

        BOOST_CHECK_EQUAL(mview.load(), obj2);
        BOOST_CHECK_EQUAL(view.load(), obj2);
    }
}

class test_fixed_buffer_context {
    size_t _size;
public:
    explicit test_fixed_buffer_context(size_t sz) : _size(sz) { }

    template<typename Tag>
    size_t size_of() const noexcept;
};

template<>
size_t test_fixed_buffer_context::size_of<A>() const noexcept {
    return _size;
}

BOOST_AUTO_TEST_CASE(test_fixed_buffer) {
    using FB = imr::fixed_buffer<A>;

    auto test = [] (auto serialize) {
        auto data = random_bytes();
        auto size = data.size();

        auto buffer = std::make_unique<uint8_t[]>(size);

        serialize(buffer.get(), size, data);

        const auto ctx = test_fixed_buffer_context(size);
        BOOST_CHECK_EQUAL(FB::serialized_object_size(buffer.get(), ctx), size);

        BOOST_CHECK(boost::range::equal(FB::make_view(buffer.get(), ctx), data));
        BOOST_CHECK(boost::range::equal(FB::make_view(const_cast<const uint8_t*>(buffer.get()), ctx), data));

        BOOST_CHECK_EQUAL(FB::make_view(buffer.get(), ctx).size(), size);
    };

    for (auto i : boost::irange(0, random_test_iteration_count)) {
        (void)i;

        test([] (uint8_t* out, size_t size, const bytes& data) {
            BOOST_CHECK_EQUAL(FB::size_when_serialized(data), size);
            BOOST_CHECK_EQUAL(FB::serialize(out, data), size);
        });

        test([] (uint8_t* out, size_t size, const bytes& data) {
            auto serializer = [&data] (uint8_t* out) noexcept {
                boost::range::copy(data, out);
            };
            BOOST_CHECK_EQUAL(FB::size_when_serialized(size, serializer), size);
            BOOST_CHECK_EQUAL(FB::serialize(out, size, serializer), size);
        });
    }
}

template<typename T>
void check_compressed_int(T value) {
    BOOST_TEST_MESSAGE("testing value: " << uintmax_t(value));

    using CI = imr::compressed_integer<T>;
    static constexpr size_t max_size = CI::maximum_size;
    static constexpr size_t buffer_size = max_size + 8;

    BOOST_CHECK_LE(CI::size_when_serialized(value), max_size);
    BOOST_CHECK_GT(CI::size_when_serialized(value), 0);
    auto size = CI::size_when_serialized(value);

    uint8_t buffer[buffer_size];
    std::fill_n(buffer, buffer_size, 0xbe);

    BOOST_CHECK_EQUAL(CI::serialize(buffer, value), size);
    BOOST_CHECK(std::all_of(buffer + size, buffer + buffer_size, [] (auto c) { return c == 0xbe; }));
    std::fill_n(buffer + size, buffer_size - size, 0xcd);

    BOOST_CHECK_EQUAL(CI::serialized_object_size(buffer), size);
    int some_context;
    BOOST_CHECK_EQUAL(CI::serialized_object_size(buffer, some_context), size);

    BOOST_CHECK_EQUAL(CI::make_view(buffer).load(), value);
}

template<typename T>
void test_compressed_ints() {
    check_compressed_int<T>(0);
    check_compressed_int<T>(std::numeric_limits<T>::min());
    check_compressed_int<T>(std::numeric_limits<T>::max());
    for (auto i : boost::irange(0, std::numeric_limits<T>::digits)) {
        check_compressed_int<T>(T(1) << i);
    }
    std::uniform_int_distribution<T> dist;
    for (auto i : boost::irange(0, random_test_iteration_count)) {
        (void)i;
        check_compressed_int<T>(random_int<T>());
    }
}

BOOST_AUTO_TEST_CASE(test_compressed_uint8_t) {
    test_compressed_ints<uint8_t>();
}

BOOST_AUTO_TEST_CASE(test_compressed_uint16_t) {
    test_compressed_ints<uint16_t>();
}

BOOST_AUTO_TEST_CASE(test_compressed_uint32_t) {
    test_compressed_ints<uint32_t>();
}

BOOST_AUTO_TEST_CASE(test_compressed_uint64_t) {
    test_compressed_ints<uint64_t>();
}

BOOST_AUTO_TEST_CASE(test_compressed_int8_t) {
        test_compressed_ints<int8_t>();
}

BOOST_AUTO_TEST_CASE(test_compressed_int16_t) {
        test_compressed_ints<int16_t>();
}

BOOST_AUTO_TEST_CASE(test_compressed_int32_t) {
        test_compressed_ints<int32_t>();
}

BOOST_AUTO_TEST_CASE(test_compressed_int64_t) {
        test_compressed_ints<int64_t>();
}

struct test_optional_context {
    template<typename Tag>
    bool is_present() const noexcept;
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
    using O1 = imr::optional<A, compressed_integer<uint32_t>>;
    using O2 = imr::optional<B, compressed_integer<uint32_t>>;

    for (auto i : boost::irange(0, random_test_iteration_count)) {
        (void)i;

        auto value = random_int<uint32_t>();
        auto expected_size = imr::compressed_integer<uint32_t>::size_when_serialized(value);
        BOOST_CHECK_GE(O1::underlying::maximum_size, expected_size);

        auto buffer = std::make_unique<uint8_t[]>(expected_size + 7);

        BOOST_CHECK_EQUAL(O1::size_when_serialized(value), expected_size);
        BOOST_CHECK_EQUAL(O1::serialize(buffer.get(), value), expected_size);

        BOOST_CHECK_EQUAL(O1::serialized_object_size(buffer.get(), test_optional_context()), expected_size);
        BOOST_CHECK_EQUAL(O2::serialized_object_size(buffer.get(), test_optional_context()), 0);

        auto view = O1::make_view(buffer.get());
        BOOST_CHECK_EQUAL(view.load(), value);
    }
}

static constexpr auto data_size = 128;
using V = imr::variant<A,
                       imr::member<B, imr::compressed_integer<uint64_t>>,
                       imr::member<C, imr::fixed_buffer<C>>>;

struct test_variant_context {
    bool _alternative_b;
public:
    template<typename Tag>
    size_t size_of() const noexcept;

    template<typename Tag>
    auto active_alternative_of() const noexcept;
};

template<>
size_t test_variant_context::size_of<C>() const noexcept {
    return data_size;
}

template<>
auto test_variant_context::active_alternative_of<A>() const noexcept {
    if (_alternative_b) {
        return V::index_for<B>();
    } else {
        return V::index_for<C>();
    }
}

template<typename... Functions>
struct do_build_visitor {
    void operator()();
};

template<typename Function, typename... Functions>
struct do_build_visitor<Function, Functions...> : Function, do_build_visitor<Functions...> {
    do_build_visitor(Function&& fn, Functions&&... fns)
        : Function(std::move(fn))
        , do_build_visitor<Functions...>(std::move(fns)...)
    { }

    using Function::operator();
    using do_build_visitor<Functions...>::operator();
};

template<typename... Functions>
auto build_visitor(Functions&&... fns) {
    return do_build_visitor<Functions...>(std::forward<Functions>(fns)...);
}

BOOST_AUTO_TEST_CASE(test_variant) {
    for (auto i : boost::irange(0, random_test_iteration_count)) {
        (void)i;

        bool alternative_b = random_bool();

        uint64_t integer = random_int<uint64_t>();
        bytes data = random_bytes(data_size);

        const size_t expected_size = alternative_b
                                     ? imr::compressed_integer<uint64_t>::size_when_serialized(integer)
                                     : data_size;

        auto buffer = std::make_unique<uint8_t[]>(expected_size);

        if (alternative_b) {
            BOOST_CHECK_EQUAL(V::size_when_serialized<B>(integer), expected_size);
            BOOST_CHECK_EQUAL(V::serialize<B>(buffer.get(), integer), expected_size);
        } else {
            BOOST_CHECK_EQUAL(V::size_when_serialized<C>(data), expected_size);
            BOOST_CHECK_EQUAL(V::serialize<C>(buffer.get(), data), expected_size);
        }

        auto ctx = test_variant_context { alternative_b };

        BOOST_CHECK_EQUAL(V::serialized_object_size(buffer.get(), ctx), expected_size);

        auto view = V::make_view(buffer.get(), ctx);
        view.visit(build_visitor(
            [&] (imr::fixed_buffer<C>::view buf) {
                if (alternative_b) {
                    BOOST_FAIL("wrong variant alternative (C, expected B)");
                } else {
                    BOOST_CHECK(boost::equal(data, buf));
                }
            },
            [&] (imr::compressed_integer<uint64_t>::view val) {
                if (alternative_b) {
                    BOOST_CHECK_EQUAL(val.load(), integer);
                } else {
                    BOOST_FAIL("wrong variant alternative (B, expected C)");
                }
            }
        ), ctx);
    }
}

BOOST_AUTO_TEST_CASE(test_structure_with_fixed) {
    using S = imr::structure<imr::member<A, imr::fixed_size_value<uint8_t>>,
                             imr::member<B, imr::fixed_size_value<int64_t>>,
                             imr::member<C, imr::fixed_size_value<uint32_t>>>;
    static constexpr auto expected_size = sizeof(uint8_t) + sizeof(uint64_t)
                                          + sizeof(uint32_t);

    for (auto i : boost::irange(0, random_test_iteration_count)) {
        (void)i;

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
                             imr::optional_member<B, imr::compressed_integer<uint16_t>>,
                             imr::member<C, fixed_buffer<C>>>;

    for (auto i : boost::irange(0, random_test_iteration_count)) {
        (void)i;

        auto b_value = random_int<uint16_t>();
        auto c_data = random_bytes();

        const auto expected_size = 1 + imr::compressed_integer<uint16_t>::size_when_serialized(b_value)
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
        BOOST_CHECK_EQUAL(mview.get<B>().load(), b_value);
        BOOST_CHECK(boost::range::equal(mview.get<C>(ctx), c_data));

        auto view = S::view(mview);
        BOOST_CHECK(view.get<A>().get<B>());
        BOOST_CHECK(!view.get<A>().get<C>());
        BOOST_CHECK_EQUAL(view.get<B>().load(), b_value);
        BOOST_CHECK(boost::range::equal(view.get<C>(ctx), c_data));
    }
}

BOOST_AUTO_TEST_CASE(test_structure_first_element) {
        using S = imr::structure<imr::member<A, imr::flags<B, C>>,
                                 imr::optional_member<B, imr::compressed_integer<uint16_t>>>;

        static constexpr auto expected_size = 1;

        auto writer = [&] (auto&& serializer) noexcept {
            return serializer
                    .serialize(imr::set_flag<B>())
                    .skip()
                    .done();
        };

        BOOST_CHECK_EQUAL(S::size_when_serialized(writer), expected_size);

        uint8_t buffer[expected_size];
        BOOST_CHECK_EQUAL(S::serialize(buffer, writer), expected_size);

        auto fview = S::get_first_member(buffer);
        BOOST_CHECK(fview.get<B>());
        BOOST_CHECK(!fview.get<C>());
}

BOOST_AUTO_TEST_CASE(test_nested_structure) {
    using S1 = imr::structure<imr::optional_member<B, imr::compressed_integer<uint16_t>>,
                              imr::member<C, fixed_buffer<C>>,
                              imr::member<A, fixed_size_value<uint8_t>>>;

    using S = imr::structure<imr::member<A, fixed_size_value<uint16_t>>,
                             imr::member<B, S1>,
                             imr::member<C, fixed_size_value<uint32_t>>>;

    for (auto i : boost::irange(0, random_test_iteration_count)) {
        (void)i;

        auto b1_value = random_int<uint16_t>();
        auto c1_data = random_bytes();
        auto a1_value = random_int<uint8_t>();

        const auto expected_size1 = imr::compressed_integer<uint16_t>::size_when_serialized(b1_value)
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
        BOOST_CHECK_EQUAL(view.get<B>(ctx).get<B>().load(), b1_value);
        BOOST_CHECK(boost::range::equal(view.get<B>(ctx).get<C>(ctx), c1_data));
        BOOST_CHECK_EQUAL(view.get<C>().load(), c_value);
    }
}

BOOST_AUTO_TEST_SUITE_END();

struct object_with_destructor {
    static size_t destruction_count;
    static uint64_t last_destroyed_one;

    static void reset() {
        destruction_count = 0;
        last_destroyed_one = 0;
    }

    uint64_t value;
};

size_t object_with_destructor::destruction_count = 0;
uint64_t object_with_destructor::last_destroyed_one = 0;

struct object_without_destructor {
    uint64_t value;
};

namespace imr {
namespace methods {

template<>
struct destructor<fixed_size_value<object_with_destructor>> {
    static void run(const uint8_t* ptr, ...) noexcept {
        object_with_destructor::destruction_count++;

        auto view = imr::fixed_size_value<object_with_destructor>::make_view(ptr);
        object_with_destructor::last_destroyed_one = view.load().value;
    }
};

}
}

BOOST_AUTO_TEST_SUITE(imr_methods);

BOOST_AUTO_TEST_CASE(test_simple_destructor) {
    object_with_destructor::reset();

    using O1 = imr::fixed_size_value<object_with_destructor>;
    using O2 = imr::fixed_size_value<object_without_destructor>;

    BOOST_CHECK(!imr::methods::is_trivially_destructible<O1>::value);
    BOOST_CHECK(imr::methods::is_trivially_destructible<O2>::value);

    static constexpr auto expected_size = sizeof(object_with_destructor);
    uint8_t buffer[expected_size];

    auto value = random_int<uint64_t>();
    BOOST_CHECK_EQUAL(O1::serialize(buffer, object_with_destructor { value }), expected_size);
    imr::methods::destroy<O1>(buffer);
    BOOST_CHECK_EQUAL(object_with_destructor::destruction_count, 1);
    BOOST_CHECK_EQUAL(object_with_destructor::last_destroyed_one, value);

    imr::methods::destroy<O2>(buffer);
    BOOST_CHECK_EQUAL(object_with_destructor::destruction_count, 1);
    BOOST_CHECK_EQUAL(object_with_destructor::last_destroyed_one, value);
}

BOOST_AUTO_TEST_CASE(test_structure_destructor) {
    object_with_destructor::reset();

    using S = imr::structure<imr::member<A, imr::fixed_size_value<object_with_destructor>>,
                             imr::member<B, imr::fixed_size_value<object_without_destructor>>,
                             imr::member<C, imr::fixed_size_value<object_with_destructor>>>;

    using S1 = imr::structure<imr::member<A, imr::fixed_size_value<object_without_destructor>>,
                              imr::member<B, imr::fixed_size_value<object_without_destructor>>,
                              imr::member<C, imr::fixed_size_value<object_without_destructor>>>;

    BOOST_CHECK(!imr::methods::is_trivially_destructible<S>::value);
    BOOST_CHECK(imr::methods::is_trivially_destructible<S1>::value);

    static constexpr auto expected_size = sizeof(object_with_destructor) * 3;
    uint8_t buffer[expected_size];

    auto a = random_int<uint64_t>();
    auto b = random_int<uint64_t>();
    auto c = random_int<uint64_t>();

    BOOST_CHECK_EQUAL(S::serialize(buffer, [&] (auto serializer) noexcept {
        return serializer
                .serialize(object_with_destructor { a })
                .serialize(object_without_destructor { b })
                .serialize(object_with_destructor { c })
                .done();
    }), expected_size);

    imr::methods::destroy<S>(buffer);
    BOOST_CHECK_EQUAL(object_with_destructor::destruction_count, 2);
    BOOST_CHECK_EQUAL(object_with_destructor::last_destroyed_one, c);

    imr::methods::destroy<S1>(buffer);
    BOOST_CHECK_EQUAL(object_with_destructor::destruction_count, 2);
    BOOST_CHECK_EQUAL(object_with_destructor::last_destroyed_one, c);
}

BOOST_AUTO_TEST_CASE(test_optional_destructor) {
        object_with_destructor::reset();

        using O1 = imr::optional<A, imr::fixed_size_value<object_with_destructor>>;
        using O2 = imr::optional<B, imr::fixed_size_value<object_with_destructor>>;
        using O3 = imr::optional<A, imr::fixed_size_value<object_without_destructor>>;

        BOOST_CHECK(!imr::methods::is_trivially_destructible<O1>::value);
        BOOST_CHECK(!imr::methods::is_trivially_destructible<O2>::value);
        BOOST_CHECK(imr::methods::is_trivially_destructible<O3>::value);

        static constexpr auto expected_size = sizeof(object_with_destructor);
        uint8_t buffer[expected_size];

        auto value = random_int<uint64_t>();

        BOOST_CHECK_EQUAL(O1::serialize(buffer, object_with_destructor { value }), expected_size);

        imr::methods::destroy<O2>(buffer, imr::test_optional_context());
        BOOST_CHECK_EQUAL(object_with_destructor::destruction_count, 0);
        BOOST_CHECK_EQUAL(object_with_destructor::last_destroyed_one, 0);

        imr::methods::destroy<O1>(buffer, imr::test_optional_context());
        BOOST_CHECK_EQUAL(object_with_destructor::destruction_count, 1);
        BOOST_CHECK_EQUAL(object_with_destructor::last_destroyed_one, value);

        imr::methods::destroy<O3>(buffer, imr::test_optional_context());
        BOOST_CHECK_EQUAL(object_with_destructor::destruction_count, 1);
        BOOST_CHECK_EQUAL(object_with_destructor::last_destroyed_one, value);
}

using V = imr::variant<A,
                       imr::member<B, imr::fixed_size_value<object_with_destructor>>,
                       imr::member<C, imr::fixed_size_value<object_without_destructor>>>;

struct test_variant_context {
    bool _alternative_b;
public:
    template<typename Tag>
    auto active_alternative_of() const noexcept;
};

template<>
auto test_variant_context::active_alternative_of<A>() const noexcept {
    if (_alternative_b) {
        return V::index_for<B>();
    } else {
        return V::index_for<C>();
    }
}

BOOST_AUTO_TEST_CASE(test_variant_destructor) {
    object_with_destructor::reset();

    using V1 = imr::variant<A, imr::member<B, imr::fixed_size_value<object_without_destructor>>>;

    BOOST_CHECK(!imr::methods::is_trivially_destructible<V>::value);
    BOOST_CHECK(imr::methods::is_trivially_destructible<V1>::value);

    static constexpr auto expected_size = sizeof(object_with_destructor);
    uint8_t buffer[expected_size];

    auto value = random_int<uint64_t>();

    BOOST_CHECK_EQUAL(V::serialize<B>(buffer, object_with_destructor { value }), expected_size);

    imr::methods::destroy<V>(buffer, test_variant_context { false });
    BOOST_CHECK_EQUAL(object_with_destructor::destruction_count, 0);
    BOOST_CHECK_EQUAL(object_with_destructor::last_destroyed_one, 0);

    imr::methods::destroy<V>(buffer, test_variant_context { true });
    BOOST_CHECK_EQUAL(object_with_destructor::destruction_count, 1);
    BOOST_CHECK_EQUAL(object_with_destructor::last_destroyed_one, value);
}

BOOST_AUTO_TEST_SUITE_END();

BOOST_AUTO_TEST_SUITE(imr_containers);

BOOST_AUTO_TEST_CASE(test_sparse_array) {
    struct dummy_context {
        int context_for_element(size_t) const { return 42; }
    } ctx;
    for (auto i : boost::irange(0, 10)) {
        (void)i;

        static constexpr auto value_count = 128;
        std::vector<uint16_t> values(value_count);
        boost::range::generate(values, random_int<uint16_t>);

        std::vector<size_t> indicies(value_count);
        boost::range::iota(indicies, 0);
        boost::range::random_shuffle(indicies);

        using SA = imr::containers::sparse_array<imr::compressed_integer<uint16_t>, value_count>;

        std::map<size_t, uint16_t> sorted;
        auto fill_array = [&] (auto serializer) {
            for (auto i_v : boost::range::combine(indicies, values)) {
                auto idx = boost::get<0>(i_v);
                auto value = boost::get<1>(i_v);
                serializer.emplace(idx, value);
                sorted[idx] = value;
            }
            return serializer.done();
        };

        auto total_size = SA::size_when_serialized(fill_array);
        BOOST_CHECK_LE(total_size, value_count * (sizeof(uint16_t) + sizeof(uint16_t) + 1) + sizeof(uint32_t));
        auto buffer = std::make_unique<uint8_t[]>(total_size + 7);
        auto buffer2 = std::make_unique<uint8_t[]>(total_size + 7);
        BOOST_CHECK_EQUAL(SA::serialize(buffer.get(), fill_array), total_size);

        size_t idx = 0;
        auto view = SA::make_view(buffer.get());
        for (auto it : view.elements_range(ctx)) {
            BOOST_CHECK_EQUAL(sorted.count(idx), 1);
            BOOST_CHECK_EQUAL(it.first, idx);
            BOOST_CHECK_EQUAL(it.second.load(), sorted[idx]);
            BOOST_CHECK_EQUAL(it.second.load(), view.elements_range(ctx)[idx].value().load());
            idx++;
        }
        BOOST_CHECK_EQUAL(idx, value_count);

        auto erase_all = [&] (auto serializer) {
            for (auto i : indicies) {
                serializer.erase(i);
            }
            return serializer.done();
        };
        BOOST_CHECK_EQUAL(SA::size_when_serialized(erase_all), sizeof(uint16_t) * 2);
        BOOST_CHECK_EQUAL(SA::serialize(buffer2.get(), buffer.get(), erase_all), sizeof(uint16_t) * 2);

        view = SA::make_view(buffer2.get());
        auto range = view.elements_range(ctx);
        BOOST_CHECK(range.begin() == range.end());

        boost::range::random_shuffle(indicies);
        boost::range::random_shuffle(values);
        sorted.clear();

        auto fill_with_some = [&] (auto serializer) {
            for (auto i_v : boost::range::combine(indicies, values) | boost::adaptors::sliced(0, 13)) {
                auto idx = boost::get<0>(i_v);
                auto value = boost::get<1>(i_v);
                serializer.emplace(idx, value);
                sorted[idx] = value;
            }
            return serializer.done();
        };
        SA::serialize(buffer.get(), buffer2.get(), fill_with_some);

        view = SA::make_view(buffer.get());
        for (auto it : view.elements_range(ctx)) {
            // test that we haven't missed anyting
            BOOST_CHECK_EQUAL(sorted.count(it.first), 1);
            BOOST_CHECK_EQUAL(it.second.load(), sorted[it.first]);
            BOOST_CHECK_EQUAL(it.second.load(), view.elements_range(ctx)[it.first].value().load());
        }

        auto fill_with_more = [&] (auto serializer) {
            for (auto i_v : boost::range::combine(indicies, values) | boost::adaptors::sliced(50, 72)) {
                auto idx = boost::get<0>(i_v);
                auto value = boost::get<1>(i_v);
                serializer.emplace(idx, value);
                sorted[idx] = value;
            }
            return serializer.done();
        };
        SA::serialize(buffer2.get(), buffer.get(), fill_with_more);

        view = SA::make_view(buffer2.get());
        for (auto it : view.elements_range(ctx)) {
            BOOST_CHECK_EQUAL(sorted.count(it.first), 1);
            BOOST_CHECK_EQUAL(it.second.load(), sorted[it.first]);
            BOOST_CHECK_EQUAL(it.second.load(), view.elements_range(ctx)[it.first].value().load());
        }

        std::vector<int> random(value_count);
        boost::generate(random, random_bool);

        auto erase_some = [&] (auto serializer) {
            for (auto idx : indicies | boost::adaptors::sliced(0, 72)) {
                if (random[idx]) {
                    serializer.erase(idx);
                    sorted.erase(idx);
                }
            }
            return serializer.done();
        };
        SA::serialize(buffer.get(), buffer2.get(), erase_some);

        // TODO: test destructors and movers

        view = SA::make_view(buffer.get());
        for (auto it : view.elements_range(ctx)) {
            BOOST_CHECK_EQUAL(sorted.count(it.first), 1);
            BOOST_CHECK_EQUAL(it.second.load(), sorted[it.first]);
            BOOST_CHECK_EQUAL(it.second.load(), view.elements_range(ctx)[it.first].value().load());
        }
    }
}

BOOST_AUTO_TEST_SUITE_END();
