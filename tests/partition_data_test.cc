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

#define BOOST_TEST_MODULE partition_data
#include <boost/test/unit_test.hpp>

#include <random>

#include <boost/range/irange.hpp>
#include <boost/range/algorithm/generate.hpp>

#include "partition_data.hh"

// Don't make me link with the rest of Scylla because of a single symbol.
standard_allocation_strategy standard_allocation_strategy_instance;

// duplicated from imr_test.cc
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
//</duplicated>

BOOST_AUTO_TEST_CASE(test_live_cell_creation) {
    std::uniform_int_distribution<size_t> length_dist(0, data::cell::maximum_internal_storage_length * 2);

    for (auto i : boost::irange(0, random_test_iteration_count)) {
        (void)i;

        imr::utils::external_object_allocator allocator;

        auto value = random_bytes(length_dist(gen));
        auto timestamp = random_int<api::timestamp_type>();
        data::type_info ti;

        // Phase 1: determine sizes of all objects
        auto builder = data::cell::make_live(ti, timestamp, value);
        auto expected_size = data::cell::size_of(builder, allocator);
        BOOST_TEST_MESSAGE("cell size: " << expected_size);

        BOOST_CHECK_EQUAL(allocator.requested_allocations_count(),
                          value.size() > data::cell::maximum_internal_storage_length);

        // Phase 2: allocate necessary buffers
        allocator.allocate_all();
        // FIXME: the allocator will not free buffers it allocated, imr objects
        // need to be serialised and then destroyed.

        // Phase 3: serialise objects
        auto buffer = std::make_unique<uint8_t[]>(expected_size + 7);
        BOOST_CHECK_EQUAL(data::cell::serialize(buffer.get(), builder, allocator), expected_size);

        auto view = data::cell::make_view(ti, buffer.get());
        BOOST_CHECK(view.is_live());
        BOOST_CHECK_EQUAL(view.timestamp(), timestamp);
        BOOST_CHECK(boost::equal(view.value(), value));

        data::cell::destroy(ti, buffer.get());
    }
}
