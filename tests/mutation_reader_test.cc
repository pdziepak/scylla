/*
 * Copyright (C) 2015 ScyllaDB
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
#include <boost/range/irange.hpp>

#include "core/sleep.hh"
#include "core/do_with.hh"
#include "core/thread.hh"

#include "tests/test-utils.hh"
#include "tests/mutation_assertions.hh"
#include "tests/mutation_reader_assertions.hh"
#include "tests/flat_mutation_reader_assertions.hh"
#include "tests/tmpdir.hh"
#include "tests/sstable_utils.hh"
#include "tests/simple_schema.hh"
#include "tests/test_services.hh"
#include "tests/mutation_source_test.hh"

#include "mutation_reader.hh"
#include "schema_builder.hh"
#include "cell_locking.hh"
#include "sstables/sstables.hh"
#include "database.hh"
#include "partition_slice_builder.hh"

static schema_ptr make_schema() {
    return schema_builder("ks", "cf")
        .with_column("pk", bytes_type, column_kind::partition_key)
        .with_column("v", bytes_type, column_kind::regular_column)
        .build();
}

SEASTAR_TEST_CASE(test_combining_two_readers_with_the_same_row) {
    return seastar::async([] {
        auto s = make_schema();

        mutation m1(partition_key::from_single_value(*s, "key1"), s);
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        mutation m2(partition_key::from_single_value(*s, "key1"), s);
        m2.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v2")), 2);

        assert_that(make_combined_reader(s, flat_mutation_reader_from_mutations({m1}), flat_mutation_reader_from_mutations({m2})))
            .produces(m2)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_two_non_overlapping_readers) {
    return seastar::async([] {
        auto s = make_schema();

        mutation m1(partition_key::from_single_value(*s, "keyB"), s);
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        mutation m2(partition_key::from_single_value(*s, "keyA"), s);
        m2.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v2")), 2);

        auto cr = make_combined_reader(s, flat_mutation_reader_from_mutations({m1}), flat_mutation_reader_from_mutations({m2}));
        assert_that(std::move(cr))
            .produces(m2)
            .produces(m1)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_two_partially_overlapping_readers) {
    return seastar::async([] {
        auto s = make_schema();

        mutation m1(partition_key::from_single_value(*s, "keyA"), s);
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        mutation m2(partition_key::from_single_value(*s, "keyB"), s);
        m2.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v2")), 1);

        mutation m3(partition_key::from_single_value(*s, "keyC"), s);
        m3.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v3")), 1);

        assert_that(make_combined_reader(s, flat_mutation_reader_from_mutations({m1, m2}), flat_mutation_reader_from_mutations({m2, m3})))
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_one_reader_with_many_partitions) {
    return seastar::async([] {
        auto s = make_schema();

        mutation m1(partition_key::from_single_value(*s, "keyA"), s);
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        mutation m2(partition_key::from_single_value(*s, "keyB"), s);
        m2.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v2")), 1);

        mutation m3(partition_key::from_single_value(*s, "keyC"), s);
        m3.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v3")), 1);

        std::vector<flat_mutation_reader> v;
        v.push_back(flat_mutation_reader_from_mutations({m1, m2, m3}));
        assert_that(make_combined_reader(s, std::move(v), streamed_mutation::forwarding::no, mutation_reader::forwarding::no))
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces_end_of_stream();
    });
}

static mutation make_mutation_with_key(schema_ptr s, dht::decorated_key dk) {
    mutation m(std::move(dk), s);
    m.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);
    return m;
}

static mutation make_mutation_with_key(schema_ptr s, const char* key) {
    return make_mutation_with_key(s, dht::global_partitioner().decorate_key(*s, partition_key::from_single_value(*s, bytes(key))));
}

namespace mutation_readers {

template<typename FragmentConsumer, typename EOSConsumer>
auto make_output(FragmentConsumer&& frag_fn, EOSConsumer&& eos_fn)
{
    class output {
        FragmentConsumer _fragment;
        EOSConsumer _eos;
    public:
        output(FragmentConsumer frag, EOSConsumer eos)
            : _fragment(std::move(frag)), _eos(std::move(eos)) { }
        void emit(mutation_fragment&& mf) {
            _fragment(std::move(mf));
        }
        void emit_end_of_stream() {
            _eos();
        }
    };
    return output(std::forward<FragmentConsumer>(frag_fn),
                  std::forward<EOSConsumer>(eos_fn));
}

template<typename Input, typename Impl>
class intermediate {
    Input _input;
protected:
    void input_to_next_partition() {
        _input.next_partition();
    }

    template<typename Output>
    void process_one(mutation_fragment&& in, Output& out) {
        out.emit(std::move(in));
    }
    template<typename Output>
    void process_end_of_stream(Output& out) {
        out.emit_end_of_stream();
    }
private:
    Impl& impl() { return *static_cast<Impl*>(this); }
    const Impl& impl() const { return *static_cast<Impl*>(this); }
public:
    schema_ptr schema() {
        return _input.schema();
    }

    void suspend_consume() {
        _input.suspend_consume();
    }

    void next_partition() {
        _input.next_partition();
    }

    bool needs_refill() const {
        return _input.needs_refill();
    }
    future<> refill(db::timeout_clock::time_point timeout) {
        return _input.refill(timeout);
    }
public:
    explicit intermediate(Input&& in) : _input(std::move(in)) { }

    template<typename Output>
    void emit_fragments(Output out) {
        _input.emit_fragments(make_output([&] (mutation_fragment&& mf) {
            impl().process_one(std::move(mf), out);
        }, [&] {
            // TODO: allow the intermediate reader to emit more fragments after
            // the underlying reader has reached EOS
            impl().process_end_of_stream(out);
        }));
    }
};

class fmr_input_reader {
    flat_mutation_reader _reader;
    bool _suspend = false;
public:
    explicit fmr_input_reader(flat_mutation_reader rd) : _reader(std::move(rd)) { }

    schema_ptr schema() const { return _reader.schema(); }
    bool needs_refill() const {
        return _reader.is_buffer_empty() && !_reader.is_end_of_stream();
    }
    future<> refill(db::timeout_clock::time_point timeout) {
        return _reader.fill_buffer(timeout);
    }

    void suspend_consume() {
        _suspend = true;
    };

    void next_partition() {
        _reader.next_partition();
    }

    template<typename Emitter>
    void emit_fragments(Emitter e) {
        _suspend = false;
        while (!_reader.is_buffer_empty() && !_suspend) {
            e.emit(_reader.pop_mutation_fragment());
        }
        if (_reader.is_end_of_stream()) {
            e.emit_end_of_stream();
        }
    }
};

template<typename Reader>
Reader wrap_reader(Reader rd) {
    return std::move(rd);
}

fmr_input_reader wrap_reader(flat_mutation_reader rd) {
    return fmr_input_reader(std::move(rd));
}

template<typename Reader>
class fmr_output final : public flat_mutation_reader::impl {
    Reader _rd;
public:
    explicit fmr_output(Reader rd)
        : impl(rd.schema()), _rd(std::move(rd)) { }

    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
        return do_until([this] { return is_buffer_full() || is_end_of_stream(); }, [this, timeout] {
            if (_rd.needs_refill()) {
                return _rd.refill(timeout);
            }
            _rd.emit_fragments(make_output([&] (mutation_fragment&& mf) {
                push_mutation_fragment(std::move(mf));
                if (is_buffer_full()) {
                    _rd.suspend_consume();
                }
            }, [&] {
                _end_of_stream = true;
            }));
            return make_ready_future<>();
        });
    }
    virtual void next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty()) {
            _end_of_stream = false;
            _rd.next_partition();
        }
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
        abort(); // TODO
    }
    virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override {
        abort(); // TODO
    }
};

template<template<typename> typename Intermediate, typename... Args>
struct intermediate_op {
    std::tuple<Args...> _args;
public:
    explicit intermediate_op(Args... args) : _args(std::forward<Args>(args)...) { }

    template<typename Reader, size_t... Idx>
    friend auto apply(Reader rd, std::index_sequence<Idx...>, intermediate_op&& op) {
        // FIXME: properly forward
        return Intermediate(wrap_reader(std::move(rd)), std::move(std::get<Idx>(op._args))...);
    }

    template<typename Reader>
    friend auto operator|(Reader rd, intermediate_op&& op) {
        return apply(std::move(rd), std::index_sequence_for<Args...>(), std::move(op));
    }
};


struct erase_type { };

template<typename Reader>
flat_mutation_reader operator|(Reader reader, erase_type)
{
    return make_flat_mutation_reader<fmr_output<Reader>>(std::move(reader));
}

flat_mutation_reader operator|(flat_mutation_reader reader, erase_type)
{
    return std::move(reader);
}

////////////////////////////////////////////////////////////////////////////////

template<typename FilterFn>
struct filter_partitions {
    template<typename InputReader>
    struct reader : public intermediate<InputReader, reader<InputReader>> {
        FilterFn _filter;
    public:
        reader(InputReader rd, FilterFn&& fn)
            : intermediate<InputReader, reader>(std::move(rd))
            , _filter(std::move(fn))
        { }

        template<typename Output>
        void process_one(mutation_fragment&& mf, Output& out) {
            if (mf.is_partition_start() && !_filter(mf.as_partition_start().key())) {
                this->input_to_next_partition();
                return;
            }
            out.emit(std::move(mf));
        }
    };
};

template<typename FilterFn>
struct filter : intermediate_op<filter_partitions<FilterFn>::template reader, FilterFn> {
    explicit filter(FilterFn fn)
        : intermediate_op<filter_partitions<FilterFn>::template reader, FilterFn>(std::move(fn))
    { }
};

}

SEASTAR_TEST_CASE(test_filtering) {
    return seastar::async([] {
        auto s = make_schema();

        auto m1 = make_mutation_with_key(s, "key1");
        auto m2 = make_mutation_with_key(s, "key2");
        auto m3 = make_mutation_with_key(s, "key3");
        auto m4 = make_mutation_with_key(s, "key4");

        // All pass
        //assert_that(make_filtering_reader(flat_mutation_reader_from_mutations({m1, m2, m3, m4}),
        //         [] (const dht::decorated_key& dk) { return true; }))
        assert_that(flat_mutation_reader_from_mutations({m1, m2, m3, m4})
                    | mutation_readers::filter([] (const dht::decorated_key& dk) { return true; })
                    | mutation_readers::erase_type())
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces(m4)
            .produces_end_of_stream();

        // None pass
        //assert_that(make_filtering_reader(flat_mutation_reader_from_mutations({m1, m2, m3, m4}),
        //         [] (const dht::decorated_key& dk) { return false; }))
        assert_that(flat_mutation_reader_from_mutations({m1, m2, m3, m4})
                    | mutation_readers::filter([] (const dht::decorated_key& dk) { return false; })
                    | mutation_readers::erase_type())
            .produces_end_of_stream();

        // Trim front
        //assert_that(make_filtering_reader(flat_mutation_reader_from_mutations({m1, m2, m3, m4}),
        //        [&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m1.key()); }))
        assert_that(flat_mutation_reader_from_mutations({m1, m2, m3, m4})
                    | mutation_readers::filter([&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m1.key()); })
                    | mutation_readers::erase_type())
            .produces(m2)
            .produces(m3)
            .produces(m4)
            .produces_end_of_stream();

        //assert_that(make_filtering_reader(flat_mutation_reader_from_mutations({m1, m2, m3, m4}),
        //    [&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m1.key()) && !dk.key().equal(*s, m2.key()); }))
        assert_that(flat_mutation_reader_from_mutations({m1, m2, m3, m4})
                    | mutation_readers::filter([&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m1.key()) && !dk.key().equal(*s, m2.key()); })
                    | mutation_readers::erase_type())
            .produces(m3)
            .produces(m4)
            .produces_end_of_stream();

        // Trim back
        //assert_that(make_filtering_reader(flat_mutation_reader_from_mutations({m1, m2, m3, m4}),
        //         [&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m4.key()); }))
        assert_that(flat_mutation_reader_from_mutations({m1, m2, m3, m4})
                    | mutation_readers::filter([&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m4.key()); })
                    | mutation_readers::erase_type())
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces_end_of_stream();

        //assert_that(make_filtering_reader(flat_mutation_reader_from_mutations({m1, m2, m3, m4}),
        //         [&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m4.key()) && !dk.key().equal(*s, m3.key()); }))
        assert_that(flat_mutation_reader_from_mutations({m1, m2, m3, m4})
                    | mutation_readers::filter([&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m4.key()) && !dk.key().equal(*s, m3.key()); })
                    | mutation_readers::erase_type())
            .produces(m1)
            .produces(m2)
            .produces_end_of_stream();

        // Trim middle
        //assert_that(make_filtering_reader(flat_mutation_reader_from_mutations({m1, m2, m3, m4}),
        //         [&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m3.key()); }))
        assert_that(flat_mutation_reader_from_mutations({m1, m2, m3, m4})
                    | mutation_readers::filter([&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m3.key()); })
                    | mutation_readers::erase_type())
            .produces(m1)
            .produces(m2)
            .produces(m4)
            .produces_end_of_stream();

        //assert_that(make_filtering_reader(flat_mutation_reader_from_mutations({m1, m2, m3, m4}),
        //         [&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m2.key()) && !dk.key().equal(*s, m3.key()); }))
        assert_that(flat_mutation_reader_from_mutations({m1, m2, m3, m4})
                    | mutation_readers::filter([&] (const dht::decorated_key& dk) { return !dk.key().equal(*s, m2.key()) && !dk.key().equal(*s, m3.key()); })
                    | mutation_readers::erase_type())
            .produces(m1)
            .produces(m4)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_two_readers_with_one_reader_empty) {
    return seastar::async([] {
        auto s = make_schema();
        mutation m1(partition_key::from_single_value(*s, "key1"), s);
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        assert_that(make_combined_reader(s, flat_mutation_reader_from_mutations({m1}), make_empty_flat_reader(s)))
            .produces(m1)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_two_empty_readers) {
    return seastar::async([] {
        auto s = make_schema();
        assert_that(make_combined_reader(s, make_empty_flat_reader(s), make_empty_flat_reader(s)))
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_one_empty_reader) {
    return seastar::async([] {
        std::vector<flat_mutation_reader> v;
        auto s = make_schema();
        v.push_back(make_empty_flat_reader(s));
        assert_that(make_combined_reader(s, std::move(v), streamed_mutation::forwarding::no, mutation_reader::forwarding::no))
            .produces_end_of_stream();
    });
}

std::vector<dht::decorated_key> generate_keys(schema_ptr s, int count) {
    auto keys = boost::copy_range<std::vector<dht::decorated_key>>(
        boost::irange(0, count) | boost::adaptors::transformed([s] (int key) {
            auto pk = partition_key::from_single_value(*s, int32_type->decompose(data_value(key)));
            return dht::global_partitioner().decorate_key(*s, std::move(pk));
        }));
    return std::move(boost::range::sort(keys, dht::decorated_key::less_comparator(s)));
}

std::vector<dht::ring_position> to_ring_positions(const std::vector<dht::decorated_key>& keys) {
    return boost::copy_range<std::vector<dht::ring_position>>(keys | boost::adaptors::transformed([] (const dht::decorated_key& key) {
        return dht::ring_position(key);
    }));
}

SEASTAR_TEST_CASE(test_fast_forwarding_combining_reader) {
    return seastar::async([] {
        auto s = make_schema();

        auto keys = generate_keys(s, 7);
        auto ring = to_ring_positions(keys);

        std::vector<std::vector<mutation>> mutations {
            {
                make_mutation_with_key(s, keys[0]),
                make_mutation_with_key(s, keys[1]),
                make_mutation_with_key(s, keys[2]),
            },
            {
                make_mutation_with_key(s, keys[2]),
                make_mutation_with_key(s, keys[3]),
                make_mutation_with_key(s, keys[4]),
            },
            {
                make_mutation_with_key(s, keys[1]),
                make_mutation_with_key(s, keys[3]),
                make_mutation_with_key(s, keys[5]),
            },
            {
                make_mutation_with_key(s, keys[0]),
                make_mutation_with_key(s, keys[5]),
                make_mutation_with_key(s, keys[6]),
            },
        };

        auto make_reader = [&] (const dht::partition_range& pr) {
            std::vector<flat_mutation_reader> readers;
            boost::range::transform(mutations, std::back_inserter(readers), [&pr] (auto& ms) {
                return flat_mutation_reader_from_mutations({ms}, pr);
            });
            return make_combined_reader(s, std::move(readers));
        };

        auto pr = dht::partition_range::make_open_ended_both_sides();
        assert_that(make_reader(pr))
            .produces(keys[0])
            .produces(keys[1])
            .produces(keys[2])
            .produces(keys[3])
            .produces(keys[4])
            .produces(keys[5])
            .produces(keys[6])
            .produces_end_of_stream();

        pr = dht::partition_range::make(ring[0], ring[0]);
            assert_that(make_reader(pr))
                    .produces(keys[0])
                    .produces_end_of_stream()
                    .fast_forward_to(dht::partition_range::make(ring[1], ring[1]))
                    .produces(keys[1])
                    .produces_end_of_stream()
                    .fast_forward_to(dht::partition_range::make(ring[3], ring[4]))
                    .produces(keys[3])
            .fast_forward_to(dht::partition_range::make({ ring[4], false }, ring[5]))
                    .produces(keys[5])
                    .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make_starting_with(ring[6]))
                    .produces(keys[6])
                    .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_sm_fast_forwarding_combining_reader) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        simple_schema s;

        const auto pkeys = s.make_pkeys(4);
        const auto ckeys = s.make_ckeys(4);

        auto make_mutation = [&] (uint32_t n) {
            mutation m(pkeys[n], s.schema());

            int i{0};
            s.add_row(m, ckeys[i], sprint("val_%i", i));
            ++i;
            s.add_row(m, ckeys[i], sprint("val_%i", i));
            ++i;
            s.add_row(m, ckeys[i], sprint("val_%i", i));
            ++i;
            s.add_row(m, ckeys[i], sprint("val_%i", i));

            return m;
        };

        std::vector<std::vector<mutation>> readers_mutations{
            {make_mutation(0), make_mutation(1), make_mutation(2), make_mutation(3)},
            {make_mutation(0)},
            {make_mutation(2)},
        };

        std::vector<flat_mutation_reader> readers;
        for (auto& mutations : readers_mutations) {
            readers.emplace_back(flat_mutation_reader_from_mutations(mutations, streamed_mutation::forwarding::yes));
        }

        assert_that(make_combined_reader(s.schema(), std::move(readers), streamed_mutation::forwarding::yes, mutation_reader::forwarding::no))
                .produces_partition_start(pkeys[0])
                .produces_end_of_stream()
                .fast_forward_to(position_range::all_clustered_rows())
                .produces_row_with_key(ckeys[0])
                .next_partition()
                .produces_partition_start(pkeys[1])
                .produces_end_of_stream()
                .fast_forward_to(position_range(position_in_partition::before_key(ckeys[2]), position_in_partition::after_key(ckeys[2])))
                .produces_row_with_key(ckeys[2])
                .produces_end_of_stream()
                .fast_forward_to(position_range(position_in_partition::after_key(ckeys[2]), position_in_partition::after_all_clustered_rows()))
                .produces_row_with_key(ckeys[3])
                .produces_end_of_stream()
                .next_partition()
                .produces_partition_start(pkeys[2])
                .fast_forward_to(position_range::all_clustered_rows())
                .produces_row_with_key(ckeys[0])
                .produces_row_with_key(ckeys[1])
                .produces_row_with_key(ckeys[2])
                .produces_row_with_key(ckeys[3])
                .produces_end_of_stream();
    });
}

struct sst_factory {
    schema_ptr s;
    sstring path;
    unsigned gen;
    int level;

    sst_factory(schema_ptr s, const sstring& path, unsigned gen, int level)
        : s(s)
        , path(path)
        , gen(gen)
        , level(level)
    {}

    sstables::shared_sstable operator()() {
        auto sst = sstables::make_sstable(s, path, gen, sstables::sstable::version_types::la, sstables::sstable::format_types::big);
        sst->set_unshared();

        //TODO set sstable level, to make the test more interesting

        return sst;
    }
};

SEASTAR_TEST_CASE(combined_mutation_reader_test) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        //logging::logger_registry().set_logger_level("database", logging::log_level::trace);

        simple_schema s;

        const auto pkeys = s.make_pkeys(4);
        const auto ckeys = s.make_ckeys(4);

        std::vector<mutation> base_mutations = boost::copy_range<std::vector<mutation>>(
                pkeys | boost::adaptors::transformed([&s](const auto& k) { return mutation(k, s.schema()); }));

        // Data layout:
        //   d[xx]
        // b[xx][xx]c
        // a[x    x]

        int i{0};

        // sstable d
        std::vector<mutation> table_d_mutations;

        i = 1;
        table_d_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_d_mutations.back(), ckeys[i], sprint("val_d_%i", i));

        i = 2;
        table_d_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_d_mutations.back(), ckeys[i], sprint("val_d_%i", i));
        const auto t_static_row = s.add_static_row(table_d_mutations.back(), sprint("%i_static_val", i));

        // sstable b
        std::vector<mutation> table_b_mutations;

        i = 0;
        table_b_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_b_mutations.back(), ckeys[i], sprint("val_b_%i", i));

        i = 1;
        table_b_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_b_mutations.back(), ckeys[i], sprint("val_b_%i", i));

        // sstable c
        std::vector<mutation> table_c_mutations;

        i = 2;
        table_c_mutations.emplace_back(base_mutations[i]);
        const auto t_row = s.add_row(table_c_mutations.back(), ckeys[i], sprint("val_c_%i", i));

        i = 3;
        table_c_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_c_mutations.back(), ckeys[i], sprint("val_c_%i", i));

        // sstable a
        std::vector<mutation> table_a_mutations;

        i = 0;
        table_a_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_a_mutations.back(), ckeys[i], sprint("val_a_%i", i));

        i = 3;
        table_a_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_a_mutations.back(), ckeys[i], sprint("val_a_%i", i));

        auto tmp = make_lw_shared<tmpdir>();

        unsigned gen{0};

        std::vector<sstables::shared_sstable> tables = {
                make_sstable_containing(sst_factory(s.schema(), tmp->path, gen++, 0), table_a_mutations),
                make_sstable_containing(sst_factory(s.schema(), tmp->path, gen++, 1), table_b_mutations),
                make_sstable_containing(sst_factory(s.schema(), tmp->path, gen++, 1), table_c_mutations),
                make_sstable_containing(sst_factory(s.schema(), tmp->path, gen++, 2), table_d_mutations)
        };

        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, {});
        auto sstables = make_lw_shared<sstables::sstable_set>(cs.make_sstable_set(s.schema()));

        std::vector<flat_mutation_reader> sstable_mutation_readers;

        for (auto table : tables) {
            sstables->insert(table);

            sstable_mutation_readers.emplace_back(
                table->read_range_rows_flat(
                    s.schema(),
                    query::full_partition_range,
                    s.schema()->full_slice(),
                    seastar::default_priority_class(),
                    no_resource_tracking(),
                    streamed_mutation::forwarding::no,
                    mutation_reader::forwarding::yes));
        }

        auto list_reader = make_combined_reader(s.schema(),
                std::move(sstable_mutation_readers));

        auto incremental_reader = make_local_shard_sstable_reader(
                s.schema(),
                sstables,
                query::full_partition_range,
                s.schema()->full_slice(),
                seastar::default_priority_class(),
                no_resource_tracking(),
                nullptr,
                streamed_mutation::forwarding::no,
                mutation_reader::forwarding::yes);

        // merge c[0] with d[1]
        i = 2;
        auto c_d_merged = mutation(pkeys[i], s.schema());
        s.add_row(c_d_merged, ckeys[i], sprint("val_c_%i", i), t_row);
        s.add_static_row(c_d_merged, sprint("%i_static_val", i), t_static_row);

        assert_that(std::move(list_reader))
            .produces(table_a_mutations.front())
            .produces(table_b_mutations[1])
            .produces(c_d_merged)
            .produces(table_a_mutations.back());

        assert_that(std::move(incremental_reader))
            .produces(table_a_mutations.front())
            .produces(table_b_mutations[1])
            .produces(c_d_merged)
            .produces(table_a_mutations.back());
    });
}

static mutation make_mutation_with_key(simple_schema& s, dht::decorated_key dk) {
    static int i{0};

    mutation m(std::move(dk), s.schema());
    s.add_row(m, s.make_ckey(++i), sprint("val_%i", i));
    return m;
}

class dummy_incremental_selector : public reader_selector {
    std::vector<std::vector<mutation>> _readers_mutations;
    streamed_mutation::forwarding _fwd;
    dht::partition_range _pr;

    const dht::token& position() const {
        return _readers_mutations.back().front().token();
    }
    flat_mutation_reader pop_reader() {
        auto muts = std::move(_readers_mutations.back());
        _readers_mutations.pop_back();
        _selector_position = _readers_mutations.empty() ? dht::ring_position::max() : dht::ring_position::starting_at(position());
        return flat_mutation_reader_from_mutation_reader(_s, make_reader_returning_many(std::move(muts), _pr), _fwd);
    }
public:
    // readers_mutations is expected to be sorted on both levels.
    // 1) the inner vector is expected to be sorted by decorated_key.
    // 2) the outer vector is expected to be sorted by the decorated_key
    //  of its first mutation.
    dummy_incremental_selector(schema_ptr s,
            std::vector<std::vector<mutation>> reader_mutations,
            dht::partition_range pr = query::full_partition_range,
            streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no)
        : reader_selector(s, dht::ring_position::min())
        , _readers_mutations(std::move(reader_mutations))
        , _fwd(fwd)
        , _pr(std::move(pr)) {
        // So we can pop the next reader off the back
        boost::reverse(_readers_mutations);
        _selector_position = dht::ring_position::starting_at(position());
    }
    virtual std::vector<flat_mutation_reader> create_new_readers(const dht::token* const t) override {
        if (_readers_mutations.empty()) {
            return {};
        }

        std::vector<flat_mutation_reader> readers;

        if (!t) {
            readers.emplace_back(pop_reader());
            return readers;
        }

        while (!_readers_mutations.empty() && *t >= _selector_position.token()) {
            readers.emplace_back(pop_reader());
        }
        return readers;
    }
    virtual std::vector<flat_mutation_reader> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
        return create_new_readers(&pr.start()->value().token());
    }
};

SEASTAR_TEST_CASE(reader_selector_gap_between_readers_test) {
    return seastar::async([] {
        storage_service_for_tests ssft;

        simple_schema s;
        auto pkeys = s.make_pkeys(3);

        auto mut1 = make_mutation_with_key(s, pkeys[0]);
        auto mut2a = make_mutation_with_key(s, pkeys[1]);
        auto mut2b = make_mutation_with_key(s, pkeys[1]);
        auto mut3 = make_mutation_with_key(s, pkeys[2]);
        std::vector<std::vector<mutation>> readers_mutations{
            {mut1},
            {mut2a},
            {mut2b},
            {mut3}
        };

        auto reader = make_combined_reader(s.schema(),
                std::make_unique<dummy_incremental_selector>(s.schema(), std::move(readers_mutations)),
                streamed_mutation::forwarding::no,
                mutation_reader::forwarding::no);

        assert_that(std::move(reader))
            .produces_partition(mut1)
            .produces_partition(mut2a + mut2b)
            .produces_partition(mut3)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(reader_selector_overlapping_readers_test) {
    return seastar::async([] {
        storage_service_for_tests ssft;

        simple_schema s;
        auto pkeys = s.make_pkeys(3);

        auto mut1 = make_mutation_with_key(s, pkeys[0]);
        auto mut2a = make_mutation_with_key(s, pkeys[1]);
        auto mut2b = make_mutation_with_key(s, pkeys[1]);
        auto mut3a = make_mutation_with_key(s, pkeys[2]);
        auto mut3b = make_mutation_with_key(s, pkeys[2]);
        auto mut3c = make_mutation_with_key(s, pkeys[2]);

        tombstone tomb(100, {});
        mut2b.partition().apply(tomb);

        std::vector<std::vector<mutation>> readers_mutations{
            {mut1, mut2a, mut3a},
            {mut2b, mut3b},
            {mut3c}
        };

        auto reader = make_combined_reader(s.schema(),
                std::make_unique<dummy_incremental_selector>(s.schema(), std::move(readers_mutations)),
                streamed_mutation::forwarding::no,
                mutation_reader::forwarding::no);

        assert_that(std::move(reader))
            .produces_partition(mut1)
            .produces_partition(mut2a + mut2b)
            .produces_partition(mut3a + mut3b + mut3c)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(reader_selector_fast_forwarding_test) {
    return seastar::async([] {
        storage_service_for_tests ssft;

        simple_schema s;
        auto pkeys = s.make_pkeys(5);

        auto mut1a = make_mutation_with_key(s, pkeys[0]);
        auto mut1b = make_mutation_with_key(s, pkeys[0]);
        auto mut2a = make_mutation_with_key(s, pkeys[1]);
        auto mut2c = make_mutation_with_key(s, pkeys[1]);
        auto mut3a = make_mutation_with_key(s, pkeys[2]);
        auto mut3d = make_mutation_with_key(s, pkeys[2]);
        auto mut4b = make_mutation_with_key(s, pkeys[3]);
        auto mut5b = make_mutation_with_key(s, pkeys[4]);
        std::vector<std::vector<mutation>> readers_mutations{
            {mut1a, mut2a, mut3a},
            {mut1b, mut4b, mut5b},
            {mut2c},
            {mut3d},
        };

        auto reader = make_combined_reader(s.schema(),
                std::make_unique<dummy_incremental_selector>(s.schema(),
                        std::move(readers_mutations),
                        dht::partition_range::make_ending_with(dht::partition_range::bound(pkeys[1], false))),
                streamed_mutation::forwarding::no,
                mutation_reader::forwarding::yes);

        assert_that(std::move(reader))
            .produces_partition(mut1a + mut1b)
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make(dht::partition_range::bound(pkeys[2], true), dht::partition_range::bound(pkeys[3], true)))
            .produces_partition(mut3a + mut3d)
            .fast_forward_to(dht::partition_range::make_starting_with(dht::partition_range::bound(pkeys[4], true)))
            .produces_partition(mut5b)
            .produces_end_of_stream();
    });
}

static const std::size_t new_reader_base_cost{16 * 1024};

template<typename EventuallySucceedingFunction>
static bool eventually_true(EventuallySucceedingFunction&& f) {
    const unsigned max_attempts = 10;
    unsigned attempts = 0;
    while (true) {
        if (f()) {
            return true;
        }

        if (++attempts < max_attempts) {
            seastar::sleep(std::chrono::milliseconds(1 << attempts)).get0();
        } else {
            return false;
        }
    }

    return false;
}

#define REQUIRE_EVENTUALLY_EQUAL(a, b) BOOST_REQUIRE(eventually_true([&] { return a == b; }))


sstables::shared_sstable create_sstable(simple_schema& sschema, const sstring& path) {
    std::vector<mutation> mutations;
    mutations.reserve(1 << 14);

    for (std::size_t p = 0; p < (1 << 10); ++p) {
        mutation m(sschema.make_pkey(p), sschema.schema());
        sschema.add_static_row(m, sprint("%i_static_val", p));

        for (std::size_t c = 0; c < (1 << 4); ++c) {
            sschema.add_row(m, sschema.make_ckey(c), sprint("val_%i", c));
        }

        mutations.emplace_back(std::move(m));
        thread::yield();
    }

    return make_sstable_containing([&] {
            return make_lw_shared<sstables::sstable>(sschema.schema(), path, 0, sstables::sstable::version_types::la, sstables::sstable::format_types::big);
        }
        , mutations);
}

static
sstables::shared_sstable create_sstable(schema_ptr s, std::vector<mutation> mutations) {
    static auto tmp = make_lw_shared<tmpdir>();
    static int gen = 0;
    return make_sstable_containing([&] {
        return make_lw_shared<sstables::sstable>(s, tmp->path, gen++, sstables::sstable::version_types::la, sstables::sstable::format_types::big);
    }, mutations);
}

class tracking_reader : public flat_mutation_reader::impl {
    flat_mutation_reader _reader;
    std::size_t _call_count{0};
    std::size_t _ff_count{0};
public:
    tracking_reader(db::timeout_semaphore* resources_sem, schema_ptr schema, lw_shared_ptr<sstables::sstable> sst)
        : impl(schema)
        , _reader(sst->read_range_rows_flat(
                        schema,
                        query::full_partition_range,
                        schema->full_slice(),
                        default_priority_class(),
                        reader_resource_tracker(resources_sem),
                        streamed_mutation::forwarding::no,
                        mutation_reader::forwarding::yes)) {
    }


    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
        ++_call_count;
        return _reader.fill_buffer(timeout).then([this] {
            _end_of_stream = _reader.is_end_of_stream();
            while (!_reader.is_buffer_empty()) {
                push_mutation_fragment(_reader.pop_mutation_fragment());
            }
        });
    }

    virtual void next_partition() override {
        _end_of_stream = false;
        clear_buffer_to_next_partition();
        if (is_buffer_empty()) {
            _reader.next_partition();
        }
    }

    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
        ++_ff_count;
        // Don't forward this to the underlying reader, it will force us
        // to come up with meaningful partition-ranges which is hard and
        // unecessary for these tests.
        return make_ready_future<>();
    }

    virtual future<> fast_forward_to(position_range, db::timeout_clock::time_point timeout) override {
        throw std::bad_function_call();
    }

    std::size_t call_count() const {
        return _call_count;
    }

    std::size_t ff_count() const {
        return _ff_count;
    }
};

class reader_wrapper {
    flat_mutation_reader _reader;
    tracking_reader* _tracker{nullptr};
    db::timeout_clock::time_point _timeout;
public:
    reader_wrapper(
            const restricted_mutation_reader_config& config,
            schema_ptr schema,
            lw_shared_ptr<sstables::sstable> sst,
            db::timeout_clock::duration timeout_duration = {})
        : _reader(make_empty_flat_reader(schema))
        , _timeout(db::timeout_clock::now() + timeout_duration)
    {
        auto ms = mutation_source([this, &config, sst=std::move(sst)] (schema_ptr schema, const dht::partition_range&) {
            auto tracker_ptr = std::make_unique<tracking_reader>(config.resources_sem, std::move(schema), std::move(sst));
            _tracker = tracker_ptr.get();
            return flat_mutation_reader(std::move(tracker_ptr));
        });

        _reader = make_restricted_flat_reader(config, std::move(ms), schema);
    }

    future<> operator()() {
        while (!_reader.is_buffer_empty()) {
            _reader.pop_mutation_fragment();
        }
        return _reader.fill_buffer(_timeout);
    }

    future<> fast_forward_to(const dht::partition_range& pr) {
        return _reader.fast_forward_to(pr);
    }

    std::size_t call_count() const {
        return _tracker ? _tracker->call_count() : 0;
    }

    std::size_t ff_count() const {
        return _tracker ? _tracker->ff_count() : 0;
    }

    bool created() const {
        return bool(_tracker);
    }
};

struct restriction_data {
    std::unique_ptr<db::timeout_semaphore> reader_semaphore;
    restricted_mutation_reader_config config;

    restriction_data(std::size_t units,
            std::size_t max_queue_length = std::numeric_limits<std::size_t>::max())
        : reader_semaphore(std::make_unique<db::timeout_semaphore>(units)) {
        config.resources_sem = reader_semaphore.get();
        config.max_queue_length = max_queue_length;
    }
};


class dummy_file_impl : public file_impl {
    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<> flush(void) override {
        return make_ready_future<>();
    }

    virtual future<struct stat> stat(void) override {
        return make_ready_future<struct stat>();
    }

    virtual future<> truncate(uint64_t length) override {
        return make_ready_future<>();
    }

    virtual future<> discard(uint64_t offset, uint64_t length) override {
        return make_ready_future<>();
    }

    virtual future<> allocate(uint64_t position, uint64_t length) override {
        return make_ready_future<>();
    }

    virtual future<uint64_t> size(void) override {
        return make_ready_future<uint64_t>(0);
    }

    virtual future<> close() override {
        return make_ready_future<>();
    }

    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        throw std::bad_function_call();
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override {
        temporary_buffer<uint8_t> buf(1024);

        memset(buf.get_write(), 0xff, buf.size());

        return make_ready_future<temporary_buffer<uint8_t>>(std::move(buf));
    }
};

SEASTAR_TEST_CASE(reader_restriction_file_tracking) {
    return async([&] {
        restriction_data rd(4 * 1024);

        {
            reader_resource_tracker resource_tracker(rd.config.resources_sem);

            auto tracked_file = resource_tracker.track(
                    file(shared_ptr<file_impl>(make_shared<dummy_file_impl>())));

            BOOST_REQUIRE_EQUAL(4 * 1024, rd.reader_semaphore->available_units());

            auto buf1 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(3 * 1024, rd.reader_semaphore->available_units());

            auto buf2 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(2 * 1024, rd.reader_semaphore->available_units());

            auto buf3 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(1 * 1024, rd.reader_semaphore->available_units());

            auto buf4 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(0 * 1024, rd.reader_semaphore->available_units());

            auto buf5 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(-1 * 1024, rd.reader_semaphore->available_units());

            // Reassing buf1, should still have the same amount of units.
            buf1 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(-1 * 1024, rd.reader_semaphore->available_units());

            // Move buf1 to the heap, so that we can safely destroy it
            auto buf1_ptr = std::make_unique<temporary_buffer<char>>(std::move(buf1));
            BOOST_REQUIRE_EQUAL(-1 * 1024, rd.reader_semaphore->available_units());

            buf1_ptr.reset();
            BOOST_REQUIRE_EQUAL(0 * 1024, rd.reader_semaphore->available_units());

            // Move tracked_file to the heap, so that we can safely destroy it.
            auto tracked_file_ptr = std::make_unique<file>(std::move(tracked_file));
            tracked_file_ptr.reset();

            // Move buf4 to the heap, so that we can safely destroy it
            auto buf4_ptr = std::make_unique<temporary_buffer<char>>(std::move(buf4));
            BOOST_REQUIRE_EQUAL(0 * 1024, rd.reader_semaphore->available_units());

            // Releasing buffers that overlived the tracked-file they
            // originated from should succeed.
            buf4_ptr.reset();
            BOOST_REQUIRE_EQUAL(1 * 1024, rd.reader_semaphore->available_units());
        }

        // All units should have been deposited back.
        REQUIRE_EVENTUALLY_EQUAL(4 * 1024, rd.reader_semaphore->available_units());
    });
}

SEASTAR_TEST_CASE(restricted_reader_reading) {
    return async([&] {
        storage_service_for_tests ssft;
        restriction_data rd(new_reader_base_cost);

        {
            simple_schema s;
            auto tmp = make_lw_shared<tmpdir>();
            auto sst = create_sstable(s, tmp->path);

            auto reader1 = reader_wrapper(rd.config, s.schema(), sst);

            reader1().get();

            BOOST_REQUIRE_LE(rd.reader_semaphore->available_units(), 0);
            BOOST_REQUIRE_EQUAL(reader1.call_count(), 1);

            auto reader2 = reader_wrapper(rd.config, s.schema(), sst);
            auto read_fut = reader2();

            // reader2 shouldn't be allowed just yet.
            BOOST_REQUIRE_EQUAL(reader2.call_count(), 0);

            // Move reader1 to the heap, so that we can safely destroy it.
            auto reader1_ptr = std::make_unique<reader_wrapper>(std::move(reader1));
            reader1_ptr.reset();

            // reader1's destruction should've made some space for reader2 by now.
            REQUIRE_EVENTUALLY_EQUAL(reader2.call_count(), 1);
            read_fut.get();

            {
                // Consume all available units.
                const auto consume_guard = consume_units(*rd.reader_semaphore, rd.reader_semaphore->current());

                // Already allowed readers should not be blocked anymore even if
                // there are no more units available.
                read_fut = reader2();
                BOOST_REQUIRE_EQUAL(reader2.call_count(), 2);
                read_fut.get();
            }
        }

        // All units should have been deposited back.
        REQUIRE_EVENTUALLY_EQUAL(new_reader_base_cost, rd.reader_semaphore->available_units());
    });
}

SEASTAR_TEST_CASE(restricted_reader_timeout) {
    return async([&] {
        storage_service_for_tests ssft;
        restriction_data rd(new_reader_base_cost);
        {
            simple_schema s;
            auto tmp = make_lw_shared<tmpdir>();
            auto sst = create_sstable(s, tmp->path);

            auto timeout = std::chrono::duration_cast<db::timeout_clock::time_point::duration>(std::chrono::milliseconds{10});
            auto reader1 = reader_wrapper(rd.config, s.schema(), sst, timeout);

            reader1().get();

            auto reader2 = reader_wrapper(rd.config, s.schema(), sst, timeout);
            auto read_fut = reader2();

            seastar::sleep(std::chrono::milliseconds(20)).get();

            // The read should have timed out.
            BOOST_REQUIRE(read_fut.failed());
            BOOST_REQUIRE_THROW(std::rethrow_exception(read_fut.get_exception()), timed_out_error);
        }

        // All units should have been deposited back.
        REQUIRE_EVENTUALLY_EQUAL(new_reader_base_cost, rd.reader_semaphore->available_units());
    });
}

SEASTAR_TEST_CASE(restricted_reader_max_queue_length) {
    return async([&] {
        storage_service_for_tests ssft;
        restriction_data rd(new_reader_base_cost, 1);

        {
            simple_schema s;
            auto tmp = make_lw_shared<tmpdir>();
            auto sst = create_sstable(s, tmp->path);

            auto reader1_ptr = std::make_unique<reader_wrapper>(rd.config, s.schema(), sst);
            (*reader1_ptr)().get();

            auto reader2_ptr = std::make_unique<reader_wrapper>(rd.config, s.schema(), sst);
            auto read_fut = (*reader2_ptr)();

            // The queue should now be full.
            BOOST_REQUIRE_THROW(reader_wrapper(rd.config, s.schema(), sst), std::runtime_error);

            reader1_ptr.reset();
            read_fut.get();
        }

        REQUIRE_EVENTUALLY_EQUAL(new_reader_base_cost, rd.reader_semaphore->available_units());
    });
}

SEASTAR_TEST_CASE(restricted_reader_create_reader) {
    return async([&] {
        storage_service_for_tests ssft;
        restriction_data rd(new_reader_base_cost);

        {
            simple_schema s;
            auto tmp = make_lw_shared<tmpdir>();
            auto sst = create_sstable(s, tmp->path);

            {
                auto reader = reader_wrapper(rd.config, s.schema(), sst);
                // This fast-forward is stupid, I know but the
                // underlying dummy reader won't care, so it's fine.
                reader.fast_forward_to(query::full_partition_range).get();

                BOOST_REQUIRE(reader.created());
                BOOST_REQUIRE_EQUAL(reader.call_count(), 0);
                BOOST_REQUIRE_EQUAL(reader.ff_count(), 1);
            }

            {
                auto reader = reader_wrapper(rd.config, s.schema(), sst);
                reader().get();

                BOOST_REQUIRE(reader.created());
                BOOST_REQUIRE_EQUAL(reader.call_count(), 1);
                BOOST_REQUIRE_EQUAL(reader.ff_count(), 0);
            }
        }

        REQUIRE_EVENTUALLY_EQUAL(new_reader_base_cost, rd.reader_semaphore->available_units());
    });
}

static mutation compacted(const mutation& m) {
    auto result = m;
    result.partition().compact_for_compaction(*result.schema(), always_gc, gc_clock::now());
    return result;
}

SEASTAR_TEST_CASE(test_fast_forwarding_combined_reader_is_consistent_with_slicing) {
    return async([&] {
        storage_service_for_tests ssft;
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        auto s = gen.schema();

        const int n_readers = 10;
        auto keys = gen.make_partition_keys(3);
        std::vector<mutation> combined;
        std::vector<flat_mutation_reader> readers;
        for (int i = 0; i < n_readers; ++i) {
            std::vector<mutation> muts;
            for (auto&& key : keys) {
                mutation m = compacted(gen());
                muts.push_back(mutation(s, key, std::move(m.partition())));
            }
            if (combined.empty()) {
                combined = muts;
            } else {
                int j = 0;
                for (auto&& m : muts) {
                    combined[j++].apply(m);
                }
            }
            mutation_source ds = create_sstable(s, muts)->as_mutation_source();
            readers.push_back(ds.make_flat_mutation_reader(s,
                dht::partition_range::make({keys[0]}, {keys[0]}),
                s->full_slice(), default_priority_class(), nullptr,
                streamed_mutation::forwarding::yes,
                mutation_reader::forwarding::yes));
        }

        flat_mutation_reader rd = make_combined_reader(s, std::move(readers),
            streamed_mutation::forwarding::yes,
            mutation_reader::forwarding::yes);

        std::vector<query::clustering_range> ranges = gen.make_random_ranges(3);

        auto check_next_partition = [&] (const mutation& expected) {
            mutation result(expected.decorated_key(), expected.schema());

            rd.consume_pausable([&](mutation_fragment&& mf) {
                position_in_partition::less_compare less(*s);
                if (!less(mf.position(), position_in_partition_view::before_all_clustered_rows())) {
                    BOOST_FAIL(sprint("Received clustering fragment: %s", mf));
                }
                result.partition().apply(*s, std::move(mf));
                return stop_iteration::no;
            }).get();

            for (auto&& range : ranges) {
                auto prange = position_range(range);
                rd.fast_forward_to(prange).get();
                rd.consume_pausable([&](mutation_fragment&& mf) {
                    if (!mf.relevant_for_range(*s, prange.start())) {
                        BOOST_FAIL(sprint("Received fragment which is not relevant for range: %s, range: %s", mf, prange));
                    }
                    position_in_partition::less_compare less(*s);
                    if (!less(mf.position(), prange.end())) {
                        BOOST_FAIL(sprint("Received fragment is out of range: %s, range: %s", mf, prange));
                    }
                    result.partition().apply(*s, std::move(mf));
                    return stop_iteration::no;
                }).get();
            }

            assert_that(result).is_equal_to(expected, ranges);
        };

        check_next_partition(combined[0]);
        rd.fast_forward_to(dht::partition_range::make_singular(keys[2])).get();
        check_next_partition(combined[2]);
    });
}

SEASTAR_TEST_CASE(test_combined_reader_slicing_with_overlapping_range_tombstones) {
    return async([&] {
        storage_service_for_tests ssft;
        simple_schema ss;
        auto s = ss.schema();

        auto rt1 = ss.make_range_tombstone(ss.make_ckey_range(1, 10));
        auto rt2 = ss.make_range_tombstone(ss.make_ckey_range(1, 5)); // rt1 + rt2 = {[1, 5], (5, 10]}

        mutation m1 = ss.new_mutation(make_local_key(s));
        m1.partition().apply_delete(*s, rt1);
        mutation m2 = m1;
        m2.partition().apply_delete(*s, rt2);
        ss.add_row(m2, ss.make_ckey(4), "v2"); // position after rt2.position() but before rt2.end_position().

        std::vector<flat_mutation_reader> readers;

        mutation_source ds1 = create_sstable(s, {m1})->as_mutation_source();
        mutation_source ds2 = create_sstable(s, {m2})->as_mutation_source();

        // upper bound ends before the row in m2, so that the raw is fetched after next fast forward.
        auto range = ss.make_ckey_range(0, 3);

        {
            auto slice = partition_slice_builder(*s).with_range(range).build();
            readers.push_back(ds1.make_flat_mutation_reader(s, query::full_partition_range, slice));
            readers.push_back(ds2.make_flat_mutation_reader(s, query::full_partition_range, slice));

            auto rd = make_combined_reader(s, std::move(readers),
                streamed_mutation::forwarding::no, mutation_reader::forwarding::no);

            auto prange = position_range(range);
            mutation result(m1.decorated_key(), m1.schema());

            rd.consume_pausable([&] (mutation_fragment&& mf) {
                if (mf.position().has_clustering_key() && !mf.range().overlaps(*s, prange.start(), prange.end())) {
                    BOOST_FAIL(sprint("Received fragment which is not relevant for the slice: %s, slice: %s", mf, range));
                }
                result.partition().apply(*s, std::move(mf));
                return stop_iteration::no;
            }).get();

            assert_that(result).is_equal_to(m1 + m2, query::clustering_row_ranges({range}));
        }

        // Check fast_forward_to()
        {

            readers.push_back(ds1.make_flat_mutation_reader(s, query::full_partition_range, s->full_slice(), default_priority_class(),
                nullptr, streamed_mutation::forwarding::yes));
            readers.push_back(ds2.make_flat_mutation_reader(s, query::full_partition_range, s->full_slice(), default_priority_class(),
                nullptr, streamed_mutation::forwarding::yes));

            auto rd = make_combined_reader(s, std::move(readers),
                streamed_mutation::forwarding::yes, mutation_reader::forwarding::no);

            auto prange = position_range(range);
            mutation result(m1.decorated_key(), m1.schema());

            rd.consume_pausable([&](mutation_fragment&& mf) {
                BOOST_REQUIRE(!mf.position().has_clustering_key());
                result.partition().apply(*s, std::move(mf));
                return stop_iteration::no;
            }).get();

            rd.fast_forward_to(prange).get();

            position_in_partition last_pos = position_in_partition::before_all_clustered_rows();
            auto consume_clustered = [&] (mutation_fragment&& mf) {
                position_in_partition::less_compare less(*s);
                if (less(mf.position(), last_pos)) {
                    BOOST_FAIL(sprint("Out of order fragment: %s, last pos: %s", mf, last_pos));
                }
                last_pos = position_in_partition(mf.position());
                result.partition().apply(*s, std::move(mf));
                return stop_iteration::no;
            };

            rd.consume_pausable(consume_clustered).get();
            rd.fast_forward_to(position_range(prange.end(), position_in_partition::after_all_clustered_rows())).get();
            rd.consume_pausable(consume_clustered).get();

            assert_that(result).is_equal_to(m1 + m2);
        }
    });
}

SEASTAR_TEST_CASE(test_combined_mutation_source_is_a_mutation_source) {
    return seastar::async([] {
        // Creates a mutation source which combines N mutation sources with mutation fragments spread
        // among them in a round robin fashion.
        auto make_combined_populator = [] (int n_sources) {
            return [=] (schema_ptr s, const std::vector<mutation>& muts) {
                std::vector<lw_shared_ptr<memtable>> memtables;
                for (int i = 0; i < n_sources; ++i) {
                    memtables.push_back(make_lw_shared<memtable>(s));
                }

                int source_index = 0;
                for (auto&& m : muts) {
                    flat_mutation_reader_from_mutations({m}).consume_pausable([&] (mutation_fragment&& mf) {
                        mutation mf_m(m.decorated_key(), m.schema());
                        mf_m.partition().apply(*s, mf);
                        memtables[source_index++ % memtables.size()]->apply(mf_m);
                        return stop_iteration::no;
                    }).get();
                }

                std::vector<mutation_source> sources;
                for (auto&& mt : memtables) {
                    sources.push_back(mt->as_data_source());
                }
                return make_combined_mutation_source(std::move(sources));
            };
        };
        run_mutation_source_tests(make_combined_populator(1));
        run_mutation_source_tests(make_combined_populator(2));
        run_mutation_source_tests(make_combined_populator(3));
    });
}
