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

BOOST_AUTO_TEST_SUITE(meta);

template<typename T, typename = int>
struct has_value : std::false_type { };

template<typename T>
struct has_value<T, decltype((void)T::value, 0)> : std::true_type { };

template<typename T, typename = int>
struct has_type : std::false_type { };

template<typename T>
struct has_type<T, decltype(std::declval<typename T::type>(), 0)> : std::true_type { };

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

        auto buffer = std::make_unique<uint8_t[]>(value_count * sizeof(uint16_t) * 4);
        imr::containers::sparse_array<imr::compressed_integer<uint16_t>, value_count> array(buffer.get());

        std::map<size_t, uint16_t> sorted;
        for (auto i_v : boost::range::combine(indicies, values)) {
            auto idx = boost::get<0>(i_v);
            auto value = boost::get<1>(i_v);
            auto size = imr::compressed_integer<uint16_t>::size_when_serialized(value);
            array.insert(idx, size, value);
            sorted[idx] = value;
        }

        size_t idx = 0;
        for (auto it : array.elements_range(ctx)) {
            BOOST_CHECK_EQUAL(sorted.count(idx), 1);
            BOOST_CHECK_EQUAL(it.first, idx);
            BOOST_CHECK_EQUAL(it.second.load(), sorted[idx]);
            BOOST_CHECK_EQUAL(it.second.load(), array.elements_range(ctx)[idx].value().load());
            idx++;
        }
        BOOST_CHECK_EQUAL(idx, value_count);

        for (auto i : indicies) {
            array.erase(i);
        }

        auto range = array.elements_range(ctx);
        BOOST_CHECK(range.begin() == range.end());

        boost::range::random_shuffle(indicies);
        boost::range::random_shuffle(values);
        sorted.clear();

        for (auto i_v : boost::range::combine(indicies, values) | boost::adaptors::sliced(0, 13)) {
            auto idx = boost::get<0>(i_v);
            auto value = boost::get<1>(i_v);
            auto size = imr::compressed_integer<uint16_t>::size_when_serialized(value);
            array.insert(idx, size, value);
            sorted[idx] = value;
        }

        for (auto it : array.elements_range(ctx)) {
            BOOST_CHECK_EQUAL(sorted.count(it.first), 1);
            BOOST_CHECK_EQUAL(it.second.load(), sorted[it.first]);
            BOOST_CHECK_EQUAL(it.second.load(), array.elements_range(ctx)[it.first].value().load());
        }
    }
}

BOOST_AUTO_TEST_SUITE_END();
