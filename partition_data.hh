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

#pragma once

#include <boost/range/algorithm/copy.hpp>

// something about data representation goes here
#include "in_memory_representation.hh"
#include "imr/utils.hh"

namespace data {

class type_info {
    uint32_t _fixed_size = 0;
public:
    struct fixed_size_tag { };
    struct variable_size_tag { };

    explicit type_info(fixed_size_tag, uint32_t size) noexcept
        : _fixed_size(size) { }
    explicit type_info(variable_size_tag) noexcept { }

    bool is_fixed_size() const noexcept {
        return !!_fixed_size;
    }

    size_t value_size() const noexcept {
        return _fixed_size;
    }
};

class schema_row_info {
    std::vector<type_info> _columns;
public:
    explicit schema_row_info(std::vector<type_info> tis) noexcept
        : _columns(std::move(tis)) { }

    const type_info& type_info_for(size_t id) const noexcept {
        assert(_columns.size() > id);
        return _columns[id];
    }
};

struct cell {
    static constexpr size_t maximum_internal_storage_length = 64;
    static constexpr size_t maximum_external_chunk_length = 8 * 1024;

    struct tags {
        class flags;
        class live;
        class exprigng;
        class counter_update;
        class ttl;
        class expiry;
        class empty;
        class timestamp;
        class value;
        class dead;
        class counter_update;
        class fixed_value;
        class variable_value;
        class value_size;
        class value_data;
        class pointer;
        class data;
        class external_data;

        class chunk_back_pointer;
        class chunk_next;
        class chunk_data;
        class last_chunk_size;
    };

    using flags = imr::flags<
        tags::live,
        tags::exprigng,
        tags::counter_update,
        tags::empty
    >;
    using value_data_variant = imr::variant<tags::value_data,
        imr::member<tags::pointer, imr::tagged_type<tags::pointer, imr::fixed_size_value<void*>>>,
        imr::member<tags::data, imr::fixed_buffer<tags::data>>
    >;
    using variable_value = imr::structure<
        imr::member<tags::value_size, imr::compressed_integer<uint32_t>>,
        imr::member<tags::value_data, value_data_variant>
    >;
    using fixed_value = imr::fixed_buffer<tags::fixed_value>;
    using value_variant = imr::variant<tags::value,
        imr::member<tags::dead, imr::compressed_integer<int32_t>>,
        imr::member<tags::counter_update, imr::compressed_integer<int64_t>>,
        imr::member<tags::fixed_value, fixed_value>,
        imr::member<tags::variable_value, variable_value>
    >;
    using structure = imr::structure<
        imr::member<tags::flags, flags>,
        imr::member<tags::timestamp, imr::compressed_integer<api::timestamp_type>>,
        imr::optional_member<tags::exprigng, imr::structure<
            imr::member<tags::ttl, imr::compressed_integer<int32_t>>,
            imr::member<tags::expiry, imr::compressed_integer<int32_t>>
        >>,
        imr::member<tags::value, value_variant>
    >;

    using external_last_chunk_size = imr::fixed_size_value<uint16_t>;
    using external_last_chunk = imr::structure<
        imr::member<tags::chunk_back_pointer, imr::tagged_type<tags::chunk_back_pointer, imr::fixed_size_value<void*>>>,
        imr::member<tags::last_chunk_size, external_last_chunk_size>,
        imr::member<tags::chunk_data, imr::fixed_buffer<tags::chunk_data>>
    >;

    using external_chunk = imr::structure<
        imr::member<tags::chunk_back_pointer, imr::tagged_type<tags::chunk_back_pointer, imr::fixed_size_value<void*>>>,
        imr::member<tags::chunk_next, imr::fixed_size_value<void*>>,
        imr::member<tags::chunk_data, imr::fixed_buffer<tags::chunk_data>>
    >;

    class context;
    class last_chunk_context;
    class chunk_context;

    class view;

    static thread_local imr::utils::lsa_migrate_fn<external_last_chunk,
            imr::utils::context_factory<last_chunk_context>> lsa_last_chunk_migrate_fn;
    static thread_local imr::utils::lsa_migrate_fn<external_chunk,
            imr::utils::context_factory<chunk_context>> lsa_chunk_migrate_fn;

    static auto make_live(const type_info& ti, api::timestamp_type ts, bytes_view value) noexcept {
        return [&ti, ts, value] (auto serializer, auto allocations) noexcept {
            auto ser = serializer
                .serialize(imr::set_flag<tags::live>(), imr::set_flag<tags::empty>(value.empty()))
                .serialize(ts)
                .skip();
            return [&] {
                if (ti.is_fixed_size() || value.empty()) {
                    return ser.template serialize_as<tags::fixed_value>(value);
                } else {
                    // TODO: consider flattening serializers of nested structs
                    return ser.template serialize_as<tags::variable_value>([&] (auto serializer) noexcept {
                        auto ser = serializer
                            .serialize(value.size());
                        return [&] {
                            if (value.size() <= maximum_internal_storage_length) {
                                return ser.template serialize_as<tags::data>(value);
                            } else {
                                auto pointer = ser.position();
                                // TODO: propagate compile-time info about the phase we are in
                                imr::placeholder<imr::fixed_size_value<void*>> placeholder;
                                auto s = ser.template serialize_as<tags::pointer>(placeholder);
                                auto offset = 0;
                                while (value.size() - offset > maximum_external_chunk_length) {
                                    uint8_t* ptr2 = nullptr;
                                    imr::placeholder<imr::fixed_size_value<void*>> phldr;
                                    auto ptr = allocations.template allocate2<external_chunk>(&lsa_chunk_migrate_fn, [&] (auto chunk_serializer, auto back_ptr) noexcept {
                                        auto s1 = chunk_serializer
                                                .serialize(back_ptr);
                                        ptr2 = s1.position();
                                         return s1.serialize(phldr)
                                                .serialize(bytes_view(value.begin() + offset, maximum_external_chunk_length))
                                                .done();
                                    }, pointer);
                                    placeholder.serialize(ptr);
                                    placeholder = phldr;
                                    offset += maximum_external_chunk_length;
                                    pointer = ptr2;
                                }
                                auto ptr = allocations.template allocate2<external_last_chunk>(&lsa_last_chunk_migrate_fn, [&] (auto chunk_serializer, auto back_ptr) noexcept {
                                    assert(value.size() - offset < std::numeric_limits<uint16_t>::max());
                                    return chunk_serializer
                                            .serialize(back_ptr)
                                            .serialize(value.size() - offset)
                                            .serialize(bytes_view(value.begin() + offset, value.size() - offset))
                                            .done();
                                }, pointer);
                                placeholder.serialize(ptr);
                                return s;
                            }
                        }().done();
                    });
                }
            }().done();
        };
    }

    template<typename Builder>
    static size_t size_of(Builder&& builder, imr::utils::external_object_allocator& allocator) noexcept {
        return structure::size_when_serialized(std::forward<Builder>(builder), allocator.get_sizer());
    }

    template<typename Builder>
    static size_t serialize(uint8_t* ptr, Builder&& builder, imr::utils::external_object_allocator& allocator) noexcept {
        return structure::serialize(ptr, std::forward<Builder>(builder), allocator.get_serializer());
    }

    static view make_view(const type_info& ti, uint8_t* ptr) noexcept;

    static void destroy(const type_info& ti, uint8_t* ptr) noexcept;
};

class cell::last_chunk_context {
    external_last_chunk_size::view _size;
public:
    explicit last_chunk_context(const uint8_t* ptr) noexcept
        : _size(external_last_chunk::get_member<1>(ptr))
    { }

    template<typename Tag>
    size_t size_of() const noexcept;

    template<typename Tag>
    auto context_for(...) const noexcept {
        return *this;
    }
};

template<>
inline size_t cell::last_chunk_context::size_of<cell::tags::chunk_data>() const noexcept {
    return _size.load();
}

struct cell::chunk_context {
    explicit constexpr chunk_context(const uint8_t*) noexcept { }

    template<typename Tag>
    static constexpr size_t size_of() noexcept { return cell::maximum_external_chunk_length; }

    template<typename Tag>
    auto context_for(...) const noexcept {
        return *this;
    }
};

class cell::context {
    cell::flags::view _flags;
    type_info _type;
public:
    class variable_value_context {
        size_t _value_size;
    public:
        explicit variable_value_context(size_t value_size) noexcept
            : _value_size(value_size) { }

        template<typename Tag>
        auto active_alternative_of() const noexcept {
            if (_value_size > maximum_internal_storage_length) {
                return value_data_variant::index_for<tags::pointer>();
            } else {
                return value_data_variant::index_for<tags::data>();
            }
        }

        template<typename Tag>
        size_t size_of() const noexcept {
            return _value_size;
        }

        template<typename Tag>
        auto context_for(...) const noexcept {
            return *this;
        }
    };
public:
    explicit context(cell::flags::view flags, const type_info& tinfo) noexcept
        : _flags(flags), _type(tinfo) { }

    template<typename Tag>
    bool is_present() const noexcept;

    template<typename Tag>
    auto active_alternative_of() const noexcept;

    template<typename Tag>
    size_t size_of() const noexcept;

    template<typename Tag>
    decltype(auto) context_for(const uint8_t*) const noexcept {
        return *this;
    }
};

template<>
inline decltype(auto) cell::context::context_for<cell::tags::variable_value>(const uint8_t* ptr) const noexcept {
    auto length = variable_value::get_first_member(ptr);
    return variable_value_context(length.load());
}

template<>
inline bool cell::context::is_present<cell::tags::exprigng>() const noexcept {
    return _flags.get<tags::exprigng>();
}

template<>
inline auto cell::context::active_alternative_of<cell::tags::value>() const noexcept {
    if (!_flags.get<tags::live>()) {
        return cell::value_variant::index_for<tags::dead>();
    } else if (_flags.get<tags::counter_update>()) {
        // TODO: have separate contexts for counter and regular schemas
        return cell::value_variant::index_for<tags::counter_update>();
    } else if (_type.is_fixed_size() || _flags.get<tags::empty>()) {
        return cell::value_variant::index_for<tags::fixed_value>();
    } else {
        return cell::value_variant::index_for<tags::variable_value>();
    }
}

template<>
inline size_t cell::context::size_of<cell::tags::fixed_value>() const noexcept {
    return _flags.get<tags::empty>() ? 0 : _type.value_size();
}

// stolen from imr_test.cc

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
inline auto build_visitor(Functions&&... fns) {
    return do_build_visitor<Functions...>(std::forward<Functions>(fns)...);
}

// </stolen>

class cell::view {
    context _context;
    structure::view _view;
public:
    view(context ctx, structure::view v)
        : _context(std::move(ctx)), _view(std::move(v)) { }

    view(const type_info& ti, const uint8_t* ptr)
        : _context(structure::get_first_member(ptr), ti)
        , _view(structure::make_view(ptr, _context))
    { }

    bool is_live() const noexcept {
        return _view.get<tags::flags>().get<tags::live>();
    }

    api::timestamp_type timestamp() const noexcept {
        return _view.get<tags::timestamp>().load();
    }

    int64_t counter_update() const noexcept {
        return _view.get<tags::value>().as<tags::counter_update>().load();
    }

    // TODO: do not linearise, provide apropriate wrapper
    bytes value() const noexcept {
        // TODO: let visit take Visitors... and call build_visitor internally
        return _view.get<tags::value>().visit(build_visitor(
            [] (fixed_value::view view) { return bytes(view.begin(), view.end()); },
            [&] (variable_value::view view) {
                return view.get<tags::value_data>().visit(build_visitor(
                    [&view] (imr::fixed_size_value<void*>::view ptr) {
                        auto size = view.get<tags::value_size>().load();
                        auto ex_ptr = static_cast<const uint8_t*>(ptr.load());

                        bytes data(bytes::initialized_later(), size);
                        auto out = data.begin();
                        while (size > maximum_external_chunk_length) {
                            auto ex_ctx = chunk_context(ex_ptr);
                            auto ex_view = external_chunk::make_view(ex_ptr, ex_ctx);
                            ex_ptr = static_cast<const uint8_t*>(ex_view.get<tags::chunk_next>().load());
                            out = boost::copy(ex_view.get<tags::chunk_data>(ex_ctx), out);
                            size -= maximum_external_chunk_length;
                        }

                        auto ex_ctx = last_chunk_context(ex_ptr);
                        auto ex_view = external_last_chunk::make_view(ex_ptr, ex_ctx);
                        assert(size == ex_view.get<tags::last_chunk_size>(ex_ctx).load());
                        boost::copy(ex_view.get<tags::chunk_data>(ex_ctx), out);
                        return data;
                    },
                    [] (imr::fixed_buffer<tags::data>::view data) {
                        return bytes(data.begin(), data.end());
                    }
                ), _context.context_for<tags::variable_value>(view.raw_pointer())); // TODO: hide this raw_pointer
            },
            [] (...) -> bytes { __builtin_unreachable(); }
        ), _context);
    }
};

inline cell::view cell::make_view(const type_info& ti, uint8_t* ptr) noexcept {
    return view(ti, ptr);
}

inline void cell::destroy(const type_info& ti, uint8_t* ptr) noexcept {
    context ctx(structure::get_first_member(ptr), ti);
    imr::methods::destroy<structure>(ptr, ctx);
}

struct row {
    struct tags {
        class cells;
        class next;
        class prev;
    };
    static constexpr size_t max_cell_count = 16;
    using cell_array = imr::containers::sparse_array<cell::structure, max_cell_count>;

    using external_chunk = imr::structure<
        imr::member<tags::next, imr::tagged_type<tags::next, imr::fixed_size_value<void*>>>,
        imr::member<tags::prev, imr::tagged_type<tags::prev, imr::fixed_size_value<void*>>>,
        imr::member<tags::cells, cell_array>
    >;

    using structure = imr::structure<
        imr::member<tags::next, imr::tagged_type<tags::next, imr::fixed_size_value<void*>>>,
        imr::member<tags::cells, cell_array>
    >;

    using serialization_state = cell_array::serialization_state;

    class builder_state {
        std::deque<serialization_state> _state;
    public:
        serialization_state& get(size_t idx) { // split by phase
            if (_state.size() <= idx) {
                _state.resize(idx + 1);
            }
            return _state[idx];
        }
    };

    template<typename Writer, typename Allocator, typename... Args>
    static size_t size_of(builder_state& state, const std::vector<std::unique_ptr<migrate_fn_type>>& lsa_migrators, Writer&& writer, Allocator& allocator, Args&&... args) {
        return structure::size_when_serialized([&] (auto serializer) {
            imr::placeholder<imr::fixed_size_value<void*>> next;
            auto ptr = serializer.position();
            auto array_sizer = serializer
                .serialize(next)
                .serialize_nested(state.get(0));
            return writer(row_builder<decltype(array_sizer), Allocator>(array_sizer, allocator, state, next, ptr, lsa_migrators), allocator, std::forward<Args>(args)...);
        });
    }

    template<typename Writer, typename Allocator, typename... Args>
    static size_t serialize(uint8_t* ptr, builder_state& state, const std::vector<std::unique_ptr<migrate_fn_type>>& lsa_migrators, Writer&& writer, Allocator& allocator, Args&&... args) {
        return structure::serialize(ptr, [&] (auto serializer) {
            imr::placeholder<imr::fixed_size_value<void*>> next;
            auto ptr = serializer.position();
            auto array_writer = serializer
                .serialize(next)
                .serialize_nested(state.get(0));
            return writer(row_builder<decltype(array_writer), Allocator>(array_writer, allocator, state, next, ptr, lsa_migrators), allocator, std::forward<Args>(args)...);
        });
    }


    // is there any reason to make Allocator a template parameter?
    template<typename Writer, typename Allocator>
    class row_builder { // just builder
        const std::vector<std::unique_ptr<migrate_fn_type>>* _lsa_migrators;
        void* _original_pointer;
        using array_writer_type = imr::rehook<Writer, imr::noop_done_hook>;

        Writer _original_writer;
        array_writer_type _writer;

        using external_writer_type = decltype(std::declval<Allocator>().template allocate_nested<external_chunk>(nullptr).serialize(nullptr).serialize(nullptr).serialize_nested(std::declval<serialization_state&>()));
        stdx::optional<external_writer_type> _fragment_writer;

        Allocator& _allocator;
        size_t _max_id = max_cell_count;
        builder_state& _state;
        imr::placeholder<imr::fixed_size_value<void*>> _next;
    private:
        void advance_to_fragment(size_t id) {
            imr::placeholder<imr::fixed_size_value<void*>> nxt;
            while (id >= _max_id) {
                void* ptr = nullptr;
                if (_fragment_writer) {
                    _fragment_writer->internal_state() = std::move(_writer.internal_state());
                    ptr = _fragment_writer->done().done();
                } else {
                    _original_writer.internal_state() = std::move(_writer.internal_state());
                    ptr = _original_pointer;
                }
                auto ser = _allocator.template allocate_nested<external_chunk>((*_lsa_migrators)[_max_id / max_cell_count].get());
                auto& state = _state.get(_max_id / max_cell_count);
                _next.serialize(ser.position());
                _fragment_writer = ser
                        .serialize(_next)
                        .serialize(ptr)
                        .serialize_nested(state);

                _writer = array_writer_type(std::move(_fragment_writer->internal_state()), imr::noop_done_hook());
                _max_id += max_cell_count;
            }
        }
    public:
        explicit row_builder(Writer wr, Allocator& allocator, builder_state& state, imr::placeholder<imr::fixed_size_value<void*>> nxt, void* orig_ptr,
                             const std::vector<std::unique_ptr<migrate_fn_type>>& lsa_migrators)
            : _lsa_migrators(&lsa_migrators)
            , _original_pointer(orig_ptr)
            , _original_writer(std::move(wr))
            , _writer(std::move(_original_writer.internal_state()), imr::noop_done_hook())
            , _allocator(allocator)
            , _state(state)
            , _next(nxt)
        { }

        template<typename... Args>
        row_builder& set_live_cell(size_t id, Args&&... args) {
            if (id >= _max_id) {
                advance_to_fragment(id);
            }
            assert(id - (_max_id - max_cell_count) < max_cell_count);
            _writer.emplace(id - (_max_id - max_cell_count), std::forward<Args>(args)...);
            return *this;
        }

        auto done() noexcept {
            _next.serialize(nullptr);
            if (_fragment_writer) {
                _fragment_writer->internal_state() = std::move(_writer.internal_state());
                _fragment_writer->done().done();
                return _original_writer.done().done();
            } else {
                _original_writer.internal_state() = std::move(_writer.internal_state());
                return _original_writer.done().done();
            }
        }
    };

    class context { // this is row_chunk context
        const schema_row_info* _sri;
    public:
        explicit context(const schema_row_info& sri) : _sri(&sri) { }
        context(const uint8_t*, const schema_row_info& sri) noexcept : _sri(&sri) { }

        auto context_for_element(size_t id, const uint8_t* ptr) const noexcept {
            return cell::context(cell::structure::get_first_member(ptr), _sri->type_info_for(id));
        }

        template<typename Tag>
        decltype(auto) context_for(...) const noexcept {
            return *this;
        }
    };

    // merge both contexts?
    struct destructor_context {
        std::vector<data::schema_row_info>::const_iterator _current;
    private:
        explicit destructor_context(std::vector<data::schema_row_info>::const_iterator it)
            : _current(it) { }
    public:
        explicit destructor_context(const std::vector<schema_row_info>& sri)
            : _current(sri.begin()) { }

        destructor_context next() const noexcept {
            return destructor_context(std::next(_current));
        }

        auto context_for_element(size_t id, const uint8_t* ptr) const noexcept {
            return cell::context(cell::structure::get_first_member(ptr), _current->type_info_for(id));
        }

        template<typename Tag>
        decltype(auto) context_for(...) const noexcept {
            return *this;
        }
    };

    static void destroy(const std::vector<schema_row_info>& sri, const uint8_t* ptr) {
        destructor_context ctx(sri);
        imr::methods::destroy<structure>(ptr, ctx);
    }


    struct chunk {
        size_t _id_offset;
        row::context _context;
        cell_array::view _cells;
    public:
        chunk(size_t id_offset, row::context ctx, cell_array::view v) noexcept
                : _id_offset(id_offset), _context(ctx), _cells(v) { }

        auto cells() const noexcept {
            return _cells.elements_range(_context) | boost::adaptors::transformed([this] (auto&& element) {
                // TODO: cell::view from cell::structure::view
                auto id = element.first;
                auto& view = element.second;
                return std::make_pair(id + _id_offset, cell::view(_context.context_for_element(id, view.raw_pointer()), view));
            });
        }
    };

    class iterator {
        static_assert(std::is_trivially_destructible<chunk>::value, "");
    private:
        const std::vector<schema_row_info>* _sri;
        row::context _context;
        size_t _index = 0;
        const uint8_t* _current;

        union data {
            data() { }
            ~data() { }

            chunk chk;
        } _data;
    public:
        iterator() = default;
        iterator(const std::vector<schema_row_info>& sri, const void* pointer)
            : _sri(&sri), _context(sri[0]), _current(static_cast<const uint8_t*>(pointer))
        {
            if (_current) {
                new (&_data.chk) chunk(0, _context, structure::get_member<1>(_current, _context));
            }
        }

        const chunk& operator*() const noexcept {
            return _data.chk;
        }
        const chunk* operator->() const noexcept {
            return &_data.chk;
        }

        iterator& operator++() noexcept {
            auto next_v = external_chunk::get_member<0>(_current);
            _current = static_cast<const uint8_t*>(next_v.load());
            _index++;
            if (_current) {
                assert(_sri->size() > _index);
                _context = row::context((*_sri)[_index]);
                new (&_data.chk) chunk(_index * max_cell_count, _context, external_chunk::get_member<2>(_current, _context));
            }
            return *this;
        }
        iterator operator++(int) noexcept {
            auto it = *this;
            operator++();
            return it;
        }

        bool operator==(const iterator& other) const {
            return _current == other._current;
        }
        bool operator!=(const iterator& other) const {
            return !(*this == other);
        }
    };

    struct view {
        const std::vector<schema_row_info>* _sri;
        row::context _context;
        structure::view _view;
    public:
        explicit view(const uint8_t* ptr, const std::vector<schema_row_info>& sri)
            : _sri(&sri), _context(row::context(sri[0])), _view(structure::make_view(ptr, _context)) { }

        explicit view(structure::view view, const std::vector<schema_row_info>& sri)
            : _sri(&sri), _context(sri[0]), _view(view) { }

        const row::context& context() const noexcept { return _context; }

        auto cells() const {
            return _view.get<tags::cells>().elements_range(_context) | boost::adaptors::transformed([this] (auto&& element) {

                auto id = element.first;
                auto& view = element.second;
                return std::make_pair(id, cell::view(_context.context_for_element(id, view.raw_pointer()), view));
            });
        }

        auto begin() const {
            return iterator(*_sri, _view.raw_pointer());
        }
        auto end() const {
            return iterator(*_sri, nullptr);
        }

        bool empty() const {
            return _view.get<tags::cells>().empty();
        }
    };

    static view make_view(const std::vector<schema_row_info>& sri, const uint8_t* ptr) {
        return view(ptr, sri);
    }


    // replace with a range of chunks?
    template<typename Function>
    static void for_each(const view& v, Function&& fn) {
        for (auto&& chk : v) {
            for (auto&& cell : chk.cells()) {
                fn(cell);
            }
        }
    }
};

class fragment_chain_destructor_context : public imr::no_context_t {
    size_t _total_length;
public:
    explicit fragment_chain_destructor_context(size_t total_length) noexcept
        : _total_length(total_length) { }

    void next_chunk() noexcept { _total_length -= data::cell::maximum_external_chunk_length; }
    bool is_last_chunk() const noexcept { return _total_length <= data::cell::maximum_external_chunk_length; }
};

}

namespace imr {
namespace methods {

template<>
struct destructor<data::cell::variable_value> {
    static void run(const uint8_t* ptr, ...) {
        // TODO: make_view() doesn't need context
        auto varval = data::cell::variable_value::make_view(ptr);
        auto total_length = varval.get<data::cell::tags::value_size>().load();
        if (total_length <= data::cell::maximum_internal_storage_length) {
            return;
        }
        auto ctx = data::fragment_chain_destructor_context(total_length);
        auto ptr_view = varval.get<data::cell::tags::value_data>().as<data::cell::tags::pointer>();
        auto chk = static_cast<const uint8_t*>(ptr_view.load());
        size_t len;
        if (ctx.is_last_chunk()) {
            len = data::cell::external_last_chunk::serialized_object_size(chk, data::cell::last_chunk_context(chk));
            imr::methods::destroy<data::cell::external_last_chunk>(static_cast<const uint8_t*>(ptr_view.load()));
        } else {
            len = data::cell::external_chunk::serialized_object_size(chk, data::cell::chunk_context(chk));
            imr::methods::destroy<data::cell::external_chunk>(static_cast<const uint8_t*>(ptr_view.load()), ctx);
        }
        current_allocator().free(ptr_view.load(), len + 7); // don't make me probvide this, ask the migrator
    }
};

template<>
struct mover<imr::tagged_type<data::cell::tags::pointer, imr::fixed_size_value<void*>>> {
    static void run(const uint8_t* ptr, ...) {
        auto ptr_view = imr::fixed_size_value<void*>::make_view(ptr);
        auto chk_ptr = static_cast<uint8_t*>(ptr_view.load());
        // FIXME: it is not necessairly a last chunk
        auto chk = data::cell::external_last_chunk::make_view(chk_ptr, data::cell::last_chunk_context(chk_ptr));
        chk.get<data::cell::tags::chunk_back_pointer>().store(const_cast<uint8_t*>(ptr));
    }
};

template<>
struct mover<imr::tagged_type<data::cell::tags::chunk_back_pointer, imr::fixed_size_value<void*>>> {
    static void run(const uint8_t* bptr, ...) {
        auto bptr_view = imr::fixed_size_value<void*>::make_view(bptr);
        auto ptr_ptr = static_cast<uint8_t*>(bptr_view.load());
        auto ptr = imr::fixed_size_value<void*>::make_view(ptr_ptr);
        ptr.store(const_cast<uint8_t*>(bptr));

    }
};

template<>
struct destructor<data::cell::external_chunk> {
    static void run(const uint8_t* ptr, data::fragment_chain_destructor_context ctx) {
        ctx.next_chunk();

        auto echk_view = data::cell::external_chunk::make_view(ptr);
        auto ptr_view = echk_view.get<data::cell::tags::chunk_next>();
        auto chk = static_cast<const uint8_t*>(ptr_view.load());
        size_t len;
        if (ctx.is_last_chunk()) {
            len = data::cell::external_last_chunk::serialized_object_size(chk, data::cell::last_chunk_context(chk));
            imr::methods::destroy<data::cell::external_last_chunk>(static_cast<const uint8_t*>(ptr_view.load()));
        } else {
            len = data::cell::external_chunk::serialized_object_size(chk, data::cell::chunk_context(chk));
            imr::methods::destroy<data::cell::external_chunk>(static_cast<const uint8_t*>(ptr_view.load()), ctx);
        }
        current_allocator().free(ptr_view.load(), len + 7); // don't make me probvide this, ask the migrator
    }
};

template<>
struct mover<data::cell::external_chunk> {
    static void run(const uint8_t* ptr, ...) {
        auto echk_view = data::cell::external_chunk::make_view(ptr, data::cell::chunk_context(ptr));
        auto next_ptr = static_cast<uint8_t*>(echk_view.get<data::cell::tags::chunk_next>().load());
        auto bptr = imr::fixed_size_value<void*>::make_view(next_ptr);
        bptr.store(const_cast<uint8_t*>(ptr + echk_view.offset_of<data::cell::tags::chunk_next>()));

        // TODO: remove this and specialise only for forward pointer
        auto back_ptr = static_cast<uint8_t*>(echk_view.get<data::cell::tags::chunk_back_pointer>().load());
        auto nptr = imr::fixed_size_value<void*>::make_view(back_ptr);
        nptr.store(const_cast<uint8_t*>(ptr));
    }
};

template<>
struct destructor<imr::tagged_type<data::row::tags::next, imr::fixed_size_value<void*>>> {
    static void run(const uint8_t* ptr, data::row::destructor_context ctx) {
        auto next_v = data::row::external_chunk::get_member<0>(ptr);
        auto next = static_cast<const uint8_t*>(next_v.load());
        if (next) {
            auto nctx = ctx.next();
            auto length = data::row::external_chunk::serialized_object_size(next, nctx) + 7;
            imr::methods::destroy<data::row::external_chunk>(next, nctx);
            current_allocator().free(const_cast<void*>(static_cast<const void*>(next)), length);
        }
    }
};

template<>
struct mover<imr::tagged_type<data::row::tags::next, imr::fixed_size_value<void*>>> {
    static void run(const uint8_t* ptr, ...) {
        auto next_v = imr::fixed_size_value<void*>::make_view(ptr);
        auto next = static_cast<uint8_t*>(next_v.load());
        if (next) {
            auto pointee = data::row::external_chunk::get_member<1>(next);
            pointee.store((void*)ptr);
        }
    }
};

template<>
struct mover<imr::tagged_type<data::row::tags::prev, imr::fixed_size_value<void*>>> {
    static void run(const uint8_t* ptr, ...) {
        auto prev_v = imr::fixed_size_value<void*>::make_view(ptr);
        auto prev = static_cast<uint8_t*>(prev_v.load());
        auto pointee = data::row::external_chunk::get_member<0>(prev);
        pointee.store((uint8_t*)ptr - 8 /* static offset_of member*/);
    }
};

}
}
