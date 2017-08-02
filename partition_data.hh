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
        return _columns[id];
    }
};

struct cell {
    static constexpr size_t maximum_internal_storage_length = 64;

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

    class context;
    class last_chunk_context;

    class view;

    static thread_local imr::utils::lsa_migrate_fn<external_last_chunk,
            imr::utils::context_factory<last_chunk_context>> lsa_last_chunk_migrate_fn;

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
                    return ser.template serialize_as<tags::variable_value>([&] (auto serializer) noexcept {
                        auto ser = serializer
                            .serialize(value.size());
                        return [&] {
                            if (value.size() <= maximum_internal_storage_length) {
                                return ser.template serialize_as<tags::data>(value);
                            } else {
                                auto pointer = ser.position();
                                return ser.template serialize_as<tags::pointer>(
                                    allocations.template allocate2<external_last_chunk>(&lsa_last_chunk_migrate_fn, [&serializer, value] (auto chunk_serializer, auto back_ptr) noexcept {
                                        // FIXME
                                        assert(value.size() < std::numeric_limits<uint16_t>::max());
                                        // FIXME: try to avoid recursion once support for fragmentation is added
                                        return chunk_serializer
                                                .serialize(back_ptr)
                                                .serialize(value.size())
                                                .serialize(value)
                                                .done();
                                    }, pointer)
                                );
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

    // TODO: support fragmented buffers
    bytes_view value() const noexcept {
        // TODO: let visit take Visitors... and call build_visitor internally
        return _view.get<tags::value>().visit(build_visitor(
            [] (fixed_value::view view) { return view; },
            [&] (variable_value::view view) {
                return view.get<tags::value_data>().visit(build_visitor(
                    [&view] (imr::fixed_size_value<void*>::view ptr) {
                        auto size = view.get<tags::value_size>().load();

                        auto ex_ptr = static_cast<const uint8_t*>(ptr.load());
                        auto ex_ctx = last_chunk_context(ex_ptr);
                        auto ex_view = external_last_chunk::make_view(ex_ptr, ex_ctx);
                        assert(size == ex_view.get<tags::last_chunk_size>(ex_ctx).load());
                        return ex_view.get<tags::chunk_data>(ex_ctx);
                    },
                    [] (imr::fixed_buffer<tags::data>::view data) {
                        return data;
                    }
                ), _context.context_for<tags::variable_value>(view.raw_pointer())); // TODO: hide this raw_pointer
            },
            [] (...) -> bytes_view { __builtin_unreachable(); }
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
    };
    static constexpr size_t max_cell_count = 16;
    using cell_array = imr::containers::sparse_array<cell::structure, max_cell_count>;
    using structure = imr::structure<
        imr::member<tags::cells, cell_array>
    >;

    template<typename Writer, typename... Args>
    static size_t size_of(Writer&& writer, Args&&... args) {
        return structure::size_when_serialized([&writer, &args...] (auto serializer) {
            return serializer
                .serialize([&writer, &args...] (auto array_sizer) {
                    return writer(row_builder<decltype(array_sizer)>(array_sizer), std::forward<Args>(args)...);
                }).done();
        });
    }

    template<typename Writer, typename... Args>
    static size_t serialize(uint8_t* ptr, Writer&& writer, Args&&... args) {
        return structure::serialize(ptr, [&writer, &args...] (auto serializer) {
            return serializer
                .serialize([&writer, &args...] (auto array_serializer) {
                    return writer(row_builder<decltype(array_serializer)>(array_serializer), std::forward<Args>(args)...);
                }).done();
        });
    }

    // TODO: how to deal with fragmented rows?
    template<typename Writer>
    class row_builder {
        Writer _writer;
    public:
        explicit row_builder(Writer wr) : _writer(wr) { }

        template<typename... Args>
        row_builder& set_live_cell(size_t id, Args&&... args) {
            _writer.emplace(id, std::forward<Args>(args)...);
            return *this;
        }
        row_builder& remove_cell(size_t id) noexcept {
            _writer.erase(id);
            return *this;
        }
        auto done() noexcept {
            return _writer.done();
        }
    };

    class context {
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

    static void destroy(const schema_row_info& sri, const uint8_t* ptr) {
        context ctx(sri);
        imr::methods::destroy<structure>(ptr, ctx);
    }

    struct view {
        row::context _context;
        structure::view _view;
    public:
        explicit view(const uint8_t* ptr, const schema_row_info& sri)
            : view(ptr, row::context(sri)) { }

        explicit view(const uint8_t* ptr, const row::context& ctx)
            : _context(ctx), _view(structure::make_view(ptr, _context)) { }

        explicit view(structure::view view, const schema_row_info& sri)
            : _context(sri), _view(view) { }

        explicit view(structure::view view, const row::context& ctx)
            : _context(ctx), _view(view) { }


        const row::context& context() const noexcept { return _context; }

        auto cells() const {
            return _view.get<tags::cells>().elements_range(_context) | boost::adaptors::transformed([this] (auto&& element) {
                // TODO: cell::view from cell::structure::view
                auto id = element.first;
                auto& view = element.second;
                return std::make_pair(id, cell::view(_context.context_for_element(id, view.raw_pointer()), view));
            });
        }

        bool empty() const {
            return _view.get<tags::cells>().empty();
        }
    };

    static view make_view(const schema_row_info& sri, const uint8_t* ptr) {
        return view(ptr, sri);
    }
};

}

namespace imr {
namespace methods {

template<>
struct destructor<imr::tagged_type<data::cell::tags::pointer, imr::fixed_size_value<void*>>> {
static void run(const uint8_t* ptr, ...) {
    auto ptr_view = imr::fixed_size_value<void*>::make_view(ptr);
    auto chk = static_cast<const uint8_t*>(ptr_view.load());
    auto len = data::cell::external_last_chunk::serialized_object_size(chk, data::cell::last_chunk_context(chk));
    current_allocator().free(ptr_view.load(), len);
}
};

template<>
struct mover<imr::tagged_type<data::cell::tags::pointer, imr::fixed_size_value<void*>>> {
    static void run(const uint8_t* ptr, ...) {
        auto ptr_view = imr::fixed_size_value<void*>::make_view(ptr);
        auto chk_ptr = static_cast<uint8_t*>(ptr_view.load());
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

}
}
