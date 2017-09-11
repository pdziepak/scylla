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

#include <boost/test/unit_test.hpp>

#include <random>

#include <boost/range/irange.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm/generate.hpp>
#include <boost/range/algorithm/random_shuffle.hpp>
#include <boost/range/algorithm_ext/iota.hpp>
#include <boost/range/algorithm/sort.hpp>

#include "utils/logalloc.hh"

#include "schema.hh"
#include "partition_data.hh"

#include "disk-error-handler.hh"


#include "schema_builder.hh"
#include "mutation_partition2.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

// This shouldn't be here
thread_local imr::utils::context_factory<data::cell::last_chunk_context> lcc;
thread_local imr::utils::lsa_migrate_fn<data::cell::external_last_chunk,
        imr::utils::context_factory<data::cell::last_chunk_context>> data::cell::lsa_last_chunk_migrate_fn(lcc);
thread_local imr::utils::context_factory<data::cell::chunk_context> ecc;
thread_local imr::utils::lsa_migrate_fn<data::cell::external_chunk,
        imr::utils::context_factory<data::cell::chunk_context>> data::cell::lsa_chunk_migrate_fn(ecc);

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
        auto ti = random_bool() ? data::type_info(data::type_info::variable_size_tag())
                                : data::type_info(data::type_info::fixed_size_tag(), value.size());

        // Phase 1: determine sizes of all objects
        auto builder = data::cell::make_live(ti, timestamp, value);
        auto expected_size = data::cell::size_of(builder, allocator);
        BOOST_TEST_MESSAGE("cell size: " << expected_size);

        BOOST_CHECK_EQUAL(allocator.requested_allocations_count(),
                          value.size() > data::cell::maximum_internal_storage_length && !ti.is_fixed_size());

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

BOOST_AUTO_TEST_CASE(test_row) {
    std::uniform_int_distribution<size_t> cell_count_dist(1, data::row::max_cell_count * 3);
        // test for all cell count from 1 to max * 4
    std::uniform_int_distribution<size_t> length_dist(0, data::cell::maximum_internal_storage_length * 2);

    for (auto i : boost::irange(0, random_test_iteration_count)) {
        (void)i;

        imr::utils::external_object_allocator allocator;

        auto cell_count = cell_count_dist(gen);
        std::vector<column_id> ids(data::row::max_cell_count * 3);
        boost::range::iota(ids, 0);
        boost::range::random_shuffle(ids);
        ids.erase(ids.begin() + cell_count, ids.end());
        boost::range::sort(ids);

        std::vector<std::pair<bytes, api::timestamp_type>> cells;
        std::generate_n(std::back_inserter(cells), cell_count, [&] {
            return std::make_pair(random_bytes(length_dist(gen)), random_int<api::timestamp_type>());
        });
        auto sri = boost::copy_range<std::vector<data::schema_row_info>>(
            boost::irange<size_t>(0, (ids.back() + data::row::max_cell_count) / data::row::max_cell_count)
            | boost::adaptors::transformed([&] (auto idx) {
                auto start = idx * data::row::max_cell_count;
                auto end = std::min<size_t>(ids.back() + 1, start + data::row::max_cell_count);
                return data::schema_row_info(boost::copy_range<std::vector<data::type_info>>(
                    boost::irange<unsigned>(start, end)
                    | boost::adaptors::transformed([&] (auto id) {
                        if (random_bool()) {
                            auto it = boost::find(ids, id);
                            if (it != ids.end()) {
                                return data::type_info(data::type_info::fixed_size_tag(), cells[std::distance(ids.begin(), it)].first.size());
                            } else {
                                return data::type_info(data::type_info::fixed_size_tag(), random_int<uint8_t>());
                            }
                        } else {
                            return data::type_info(data::type_info::variable_size_tag());
                        }
                    })
                ));
            })
        );
        bool first = true;
        auto lsa_migrators = boost::copy_range<std::vector<std::unique_ptr<migrate_fn_type>>>(
            sri | boost::adaptors::transformed([&] (const data::schema_row_info& sri) -> std::unique_ptr<migrate_fn_type> {
                using context_factory = imr::utils::context_factory<data::row::context, data::schema_row_info>;
                if (first) {
                    first = false;
                    return std::make_unique<v2::rows_entry::lsa_migrator<context_factory>>(context_factory(sri));
                } else {
                    return std::make_unique<imr::utils::lsa_migrate_fn<data::row::external_chunk, context_factory>>(context_factory(sri));
                }
            })
        );

        auto writer = [&] (auto sizer, auto allocator) {
            for (auto i = 0u; i < cell_count; i++) {
                auto id = ids[i];
                sizer.set_live_cell(ids[i], data::cell::make_live(sri[id / data::row::max_cell_count].type_info_for(id % data::row::max_cell_count), cells[i].second, cells[i].first), allocator);
            }
            return sizer.done();
        };

        // Phase 1: determine sizes of all objects
        data::row::builder_state state;
        auto aszr = allocator.get_sizer();
        auto expected_size = data::row::size_of(state, lsa_migrators, writer, aszr);
        BOOST_TEST_MESSAGE("row size: " << expected_size);

        // Phase 2: allocate necessary buffers
        auto buffer = std::make_unique<uint8_t[]>(expected_size + 7);
        allocator.allocate_all();

        // Phase 3: serialise objects
        auto aser = allocator.get_serializer();
        BOOST_CHECK_EQUAL(data::row::serialize(buffer.get(), state, lsa_migrators, writer, aser), expected_size);

        auto view = data::row::make_view(sri, buffer.get());
        size_t idx = 0;
        data::row::for_each(view, [&] (auto&& i_a_c) {
            BOOST_CHECK_EQUAL(ids[idx], i_a_c.first);
            BOOST_CHECK_EQUAL(cells[idx].second, i_a_c.second.timestamp());
            BOOST_CHECK(boost::equal(cells[idx].first, i_a_c.second.value()));
            idx++;
        });
        BOOST_CHECK_EQUAL(idx, cell_count);

        data::row::destroy(sri, buffer.get());
    }
}


BOOST_AUTO_TEST_CASE(test_rows_entry) {
    auto s = schema_builder("ks", "cf")
        .with_column("pk", bytes_type, column_kind::partition_key)
        .with_column("v1", int32_type)
        .with_column("v2", bytes_type)
        .with_column("v3", bytes_type)
        .with_column("v4", int32_type)
        .with_column("v5", long_type)
        .with_column("v6", int32_type)
        .with_column("v7", bytes_type)
        .with_column("v8", bytes_type)
        .with_column("v9", bytes_type)
        .with_column("v10", bytes_type)
        .with_column("v12", bytes_type)
        .with_column("v13", bytes_type)
        .with_column("v14", bytes_type)
        .with_column("v15", bytes_type)
        .with_column("v16", bytes_type)
        .with_column("v17", bytes_type)
        .with_column("v18", bytes_type)
        .with_column("v19", bytes_type)
        .with_column("v20", bytes_type)
        .with_column("v22", bytes_type)
        .with_column("v23", bytes_type)
        .with_column("v24", bytes_type)
        .with_column("v25", bytes_type)
        .with_column("v26", bytes_type)
        .build();

    auto v1 = random_int<int32_t>();
    auto v3 = random_int<uint8_t>();
    auto v5 = random_int<int64_t>();

    // owning rows_entry_ptr with a schema?
    auto ts = api::new_timestamp();
    auto blob = random_bytes(v3);

    auto re = v2::rows_entry_ptr::make(*s, clustering_key::make_empty(), [&, ts, blob] (auto& builder) {
        builder.set_cell(0, ts, int32_type->decompose(data_value(v1)));
        builder.set_cell(2, ts, blob);
        builder.set_cell(4, ts, long_type->decompose(data_value(v5)));
    });
    BOOST_CHECK(re.equal(re));

    BOOST_CHECK(!re.empty());
    BOOST_CHECK(re.is_live(*s));

    {
        auto cells = re.cells();
        auto it = cells.begin();

        BOOST_CHECK(it != cells.end());
        auto i_a_c = *it;
        BOOST_CHECK_EQUAL(i_a_c.first, 0);
        BOOST_CHECK(i_a_c.second.is_live());
        BOOST_CHECK_EQUAL(i_a_c.second.timestamp(), ts);
        BOOST_CHECK(int32_type->equal(i_a_c.second.value(), int32_type->decompose(data_value(v1))));

        ++it;
        BOOST_CHECK(it != cells.end());
        i_a_c = *it;
        BOOST_CHECK_EQUAL(i_a_c.first, 2);
        BOOST_CHECK(i_a_c.second.is_live());
        BOOST_CHECK_EQUAL(i_a_c.second.timestamp(), ts);
        //BOOST_CHECK_EQUAL(int32_type->equal(i_a_c.second.value(), int32_type->decompose(data_value(v1))));

        ++it;
        BOOST_CHECK(it != cells.end());
        i_a_c = *it;
        BOOST_CHECK_EQUAL(i_a_c.first, 4);
        BOOST_CHECK(i_a_c.second.is_live());
        BOOST_CHECK_EQUAL(i_a_c.second.timestamp(), ts);
        BOOST_CHECK(long_type->equal(i_a_c.second.value(), long_type->decompose(data_value(v5))));

        ++it;
        BOOST_CHECK(it == cells.end());
    }

    auto v1b = random_int<int32_t>();
    auto v5b = random_int<int64_t>();
    auto v6 = random_int<int32_t>();
    auto ts2 = api::new_timestamp();
    auto re2 = v2::rows_entry_ptr::make(*s, clustering_key::make_empty(), [&] (auto& builder) {
        builder.set_cell(0, ts2, int32_type->decompose(data_value(v1b)));
        builder.set_cell(4, ts - 100, int32_type->decompose(data_value(v5b)));
        builder.set_cell(5, ts2, int32_type->decompose(data_value(v6)));
    });
    auto re2b = re2.copy(*s);
    BOOST_CHECK(!re.equal(re2));

    auto re3 = re.copy(*s);
    BOOST_CHECK(re.equal(re3));

    re.apply(*s, re2);
    BOOST_CHECK(!re.equal(re3));
    auto re4 = re.copy(*s);
    BOOST_CHECK(re.equal(re4));

    BOOST_CHECK(!re.empty());
    BOOST_CHECK(re.is_live(*s));

    {
        auto cells = re.cells();
        auto it = cells.begin();

        BOOST_CHECK(it != cells.end());
        auto i_a_c = *it;
        BOOST_CHECK_EQUAL(i_a_c.first, 0);
        BOOST_CHECK(i_a_c.second.is_live());
        BOOST_CHECK_EQUAL(i_a_c.second.timestamp(), ts2);
        BOOST_CHECK(int32_type->equal(i_a_c.second.value(), int32_type->decompose(data_value(v1b))));

        ++it;
        BOOST_CHECK(it != cells.end());
        i_a_c = *it;
        BOOST_CHECK_EQUAL(i_a_c.first, 2);
        BOOST_CHECK(i_a_c.second.is_live());
        BOOST_CHECK_EQUAL(i_a_c.second.timestamp(), ts);
        //BOOST_CHECK_EQUAL(int32_type->equal(i_a_c.second.value(), int32_type->decompose(data_value(v1))));

        ++it;
        BOOST_CHECK(it != cells.end());
        i_a_c = *it;
        BOOST_CHECK_EQUAL(i_a_c.first, 4);
        BOOST_CHECK(i_a_c.second.is_live());
        BOOST_CHECK_EQUAL(i_a_c.second.timestamp(), ts);
        BOOST_CHECK(long_type->equal(i_a_c.second.value(), long_type->decompose(data_value(v5))));

        ++it;
        BOOST_CHECK(it != cells.end());
        i_a_c = *it;
        BOOST_CHECK_EQUAL(i_a_c.first, 5);
        BOOST_CHECK(i_a_c.second.is_live());
        BOOST_CHECK_EQUAL(i_a_c.second.timestamp(), ts2);
        BOOST_CHECK(long_type->equal(i_a_c.second.value(), int32_type->decompose(data_value(v6))));

        ++it;
        BOOST_CHECK(it == cells.end());
    }

    re.revert(re2);
    BOOST_CHECK(re.equal(re3));
    BOOST_CHECK(!re.equal(re4));

    re.apply(*s, re2);
    BOOST_CHECK(re.equal(re4));

    auto diff = re.difference(*s, re4);
    BOOST_CHECK(!diff);

    diff = re.difference(*s, re2b);
    BOOST_CHECK(diff);
    {
        auto cells = diff->cells();
        auto it = cells.begin();

        BOOST_CHECK(it != cells.end());
        auto i_a_c = *it;
        BOOST_CHECK_EQUAL(i_a_c.first, 2);
        BOOST_CHECK(i_a_c.second.is_live());
        BOOST_CHECK_EQUAL(i_a_c.second.timestamp(), ts);
        //BOOST_CHECK_EQUAL(int32_type->equal(i_a_c.second.value(), int32_type->decompose(data_value(v1))));

        ++it;
        BOOST_CHECK(it != cells.end());
        i_a_c = *it;
        BOOST_CHECK_EQUAL(i_a_c.first, 4);
        BOOST_CHECK(i_a_c.second.is_live());
        BOOST_CHECK_EQUAL(i_a_c.second.timestamp(), ts);
        BOOST_CHECK(long_type->equal(i_a_c.second.value(), long_type->decompose(data_value(v5))));

        ++it;
        BOOST_CHECK(it == cells.end());
    }
}

BOOST_AUTO_TEST_CASE(test_rows_entry_lsa) {
    // dedup schema
    auto s = schema_builder("ks", "cf")
        .with_column("pk", bytes_type, column_kind::partition_key)
        .with_column("v01", int32_type)
        .with_column("v02", bytes_type)
        .with_column("v03", bytes_type)
        .with_column("v04", int32_type)
        .with_column("v05", long_type)
        .with_column("v06", int32_type)
        .with_column("v07", bytes_type)
        .with_column("v08", bytes_type)
        .with_column("v09", bytes_type)
        .with_column("v10", bytes_type)
        .with_column("v11", bytes_type)
        .with_column("v12", bytes_type)
        .with_column("v13", bytes_type)
        .with_column("v14", bytes_type)
        .with_column("v15", bytes_type)
        .with_column("v16", bytes_type)
        .with_column("v17", bytes_type)
        .with_column("v18", bytes_type)
        .with_column("v19", bytes_type)
        .with_column("v20", bytes_type)
        .with_column("v21", bytes_type)
        .with_column("v22", bytes_type)
        .with_column("v23", bytes_type)
        .with_column("v24", bytes_type)
        .with_column("v25", bytes_type)
        .with_column("v26", bytes_type)
        .build();

    logalloc::region lsa;

    for (auto i : boost::irange(0, random_test_iteration_count)) {
        (void)i;

        with_allocator(lsa.allocator(), [&] {
            auto v2 = random_int<uint32_t>() % 10'000'000;
            auto v3 = random_int<uint8_t>();
            auto v5 = random_int<int64_t>();

            auto ts = api::new_timestamp();
            auto blob2 = random_bytes(v2);
            auto blob3 = random_bytes(v3);

            auto cn = random_int<uint8_t>() % 19 + 6;
            BOOST_TEST_MESSAGE("additional cell at column_id " << cn);
            auto vn = random_int<uint8_t>();
            auto blobn = random_bytes(vn);

            auto check_re = [&] (const v2::rows_entry_ptr& re) {
                size_t idx = 0;
                data::row::for_each(re.view(), [&] (auto& i_a_c) {
                    switch (idx) {
                    case 0:
                        BOOST_CHECK_EQUAL(i_a_c.first, 1);
                        BOOST_CHECK(i_a_c.second.is_live());
                        BOOST_CHECK_EQUAL(i_a_c.second.timestamp(), ts);
                        BOOST_CHECK(boost::equal(i_a_c.second.value(), blob2));
                        break;
                    case 1:
                        BOOST_CHECK_EQUAL(i_a_c.first, 2);
                        BOOST_CHECK(i_a_c.second.is_live());
                        BOOST_CHECK_EQUAL(i_a_c.second.timestamp(), ts);
                        BOOST_CHECK(boost::equal(i_a_c.second.value(), blob3));
                        break;
                    case 2:
                        BOOST_CHECK_EQUAL(i_a_c.first, 4);
                        BOOST_CHECK(i_a_c.second.is_live());
                        BOOST_CHECK_EQUAL(i_a_c.second.timestamp(), ts);
                        BOOST_CHECK(long_type->equal(i_a_c.second.value(), long_type->decompose(data_value(v5))));
                        break;
                    case 3:
                        BOOST_CHECK_EQUAL(i_a_c.first, cn);
                        BOOST_CHECK(i_a_c.second.is_live());
                        BOOST_CHECK_EQUAL(i_a_c.second.timestamp(), ts);
                        BOOST_CHECK(boost::equal(i_a_c.second.value(), blobn));
                        break;
                    default:
                        BOOST_FAIL("should not reach");
                        break;
                    }
                    ++idx;
                });
                BOOST_CHECK_EQUAL(idx, 4);
            };

            // TODO: disable relcaim here
            auto re = v2::rows_entry_ptr::make(*s, clustering_key::make_empty(), [&] (auto& builder) {
                builder.set_cell(1, ts, blob2);
                builder.set_cell(2, ts, blob3);
                builder.set_cell(4, ts, long_type->decompose(data_value(v5)));
                builder.set_cell(cn, ts, blobn);
            });
            check_re(re);

            intrusive_set_external_comparator<v2::rows_entry_header, &v2::rows_entry_header::_link> tree;
            tree.insert_before(tree.end(), *re.release());

            BOOST_TEST_MESSAGE("starting full compaction...");
            lsa.full_compaction();
            BOOST_TEST_MESSAGE("full compaction completed");

            re = v2::rows_entry_ptr(*s, *static_cast<v2::rows_entry*>(&*tree.begin()));
            tree.erase(tree.begin());
            BOOST_CHECK(tree.empty());
            check_re(re);
        });
    }
}
