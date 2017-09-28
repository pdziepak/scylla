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

#include "data/cell.hh"

#include "random-utils.hh"
#include "disk-error-handler.hh"

static constexpr auto random_test_iteration_count = 20;

BOOST_AUTO_TEST_CASE(test_live_cell_creation) {
    for (auto i = 0; i < random_test_iteration_count; i++) {
        bool fixed_size = tests::random::get_bool();
        auto size = tests::random::get_int<uint32_t>(1, fixed_size ? data::cell::maximum_internal_storage_length
                                                                   : data::cell::maximum_external_chunk_length * 3);
        auto value = tests::random::get_bytes(size);
        auto timestamp = tests::random::get_int<api::timestamp_type>();
        auto ti = [&] {
            if (fixed_size) {
                return data::type_info::make_fixed_size(size);
            } else {
                return data::type_info::make_variable_size();
            }
        }();

        imr::alloc::object_allocator allocator;

        auto builder = data::cell::make_live(ti, timestamp, value);
        auto expected_size = data::cell::size_of(builder, allocator);
        if (fixed_size) {
            BOOST_CHECK_GE(expected_size, size);
        }
        BOOST_TEST_MESSAGE("cell size: " << expected_size << ", value size: " << size);

        allocator.allocate_all();

        auto buffer = std::make_unique<uint8_t[]>(expected_size);
        BOOST_CHECK_EQUAL(data::cell::serialize(buffer.get(), builder, allocator), expected_size);

        auto view = data::cell::make_atomic_cell_view(ti, buffer.get());
        BOOST_CHECK(view.is_live());
        BOOST_CHECK_EQUAL(view.timestamp(), timestamp);
        BOOST_CHECK(boost::equal(view.value().linearize(), value));

        imr::methods::destroy<data::cell::structure>(buffer.get());
    }
}

// FIXME: We need more tests!

