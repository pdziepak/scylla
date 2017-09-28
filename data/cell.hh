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

#include "imr/compound.hh"
#include "imr/fundamental.hh"
#include "imr/alloc.hh"

#include "data/schema_info.hh"

#include "gc_clock.hh"
#include "timestamp.hh"
#include "utils/make_visitor.hh"

namespace data {

enum class const_view {
    no,
    yes,
};

struct cell {
    enum : size_t {
        maximum_internal_storage_length = 8 * 1024,
        maximum_external_chunk_length = 8 * 1024,
    };

    struct tags {
        class cell;
        class atomic_cell;
        class collection;

        class flags;
        class live;
        class expiring;
        class counter_update;
        class revert;
        class counter_in_place_revert;
        class external_data;

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
        tags::collection,
        tags::live,
        tags::expiring,
        tags::counter_update,
        tags::empty,
        tags::revert,
        tags::counter_in_place_revert,
        tags::external_data
    >;
    using value_data_variant = imr::variant<tags::value_data,
        imr::member<tags::pointer, imr::tagged_type<tags::pointer, imr::pod<void*>>>,
        imr::member<tags::data, imr::buffer<tags::data>>
    >;
    using variable_value = imr::structure<
        imr::member<tags::value_size, imr::compressed_integer<uint32_t>>,
        imr::member<tags::value_data, value_data_variant>
    >;
    using fixed_value = imr::buffer<tags::fixed_value>;
    using value_variant = imr::variant<tags::value,
        imr::member<tags::dead, imr::compressed_integer<int32_t>>,
        imr::member<tags::counter_update, imr::compressed_integer<int64_t>>,
        imr::member<tags::fixed_value, fixed_value>,
        imr::member<tags::variable_value, variable_value>
    >;
    using atomic_cell = imr::structure<
        imr::member<tags::timestamp, imr::pod<api::timestamp_type>>,
        imr::optional_member<tags::expiring, imr::structure<
            imr::member<tags::ttl, imr::compressed_integer<int32_t>>,
            imr::member<tags::expiry, imr::compressed_integer<int32_t>>
        >>,
        imr::member<tags::value, value_variant>
    >;
    using atomic_cell_or_collection = imr::variant<tags::cell,
        imr::member<tags::atomic_cell, atomic_cell>,
        imr::member<tags::collection, variable_value>
    >;
    using structure = imr::structure<
        imr::member<tags::flags, flags>,
        imr::member<tags::cell, atomic_cell_or_collection>
    >;

    using external_chunk = imr::structure<
        imr::member<tags::chunk_back_pointer, imr::tagged_type<tags::chunk_back_pointer, imr::pod<void*>>>,
        imr::member<tags::chunk_next, imr::pod<void*>>,
        imr::member<tags::chunk_data, imr::buffer<tags::chunk_data>>
    >;

    using external_last_chunk_size = imr::pod<uint16_t>;
    using external_last_chunk = imr::structure<
        imr::member<tags::chunk_back_pointer, imr::tagged_type<tags::chunk_back_pointer, imr::pod<void*>>>,
        imr::member<tags::last_chunk_size, external_last_chunk_size>,
        imr::member<tags::chunk_data, imr::buffer<tags::chunk_data>>
    >;

    class context;
    class minimal_context;

    struct chunk_context {
        explicit constexpr chunk_context(const uint8_t*) noexcept { }
        template<typename Tag>
        static constexpr size_t size_of() noexcept {
            return cell::maximum_external_chunk_length;
        }
        template<typename Tag, typename... Args>
        auto context_for(Args&&...) const noexcept {
            return *this;
        }
    };
    class last_chunk_context {
        uint16_t _size;
    public:
        explicit last_chunk_context(const uint8_t* ptr) noexcept
                : _size(external_last_chunk::get_member<tags::last_chunk_size>(ptr).load())
        { }

        template<typename Tag>
        size_t size_of() const noexcept {
            return _size;
        }

        template<typename Tag, typename... Args>
        auto context_for(Args&&...) const noexcept {
            return *this;
        }
    };

    template<const_view is_const>
    class basic_atomic_cell_view;

    using atomic_cell_view = basic_atomic_cell_view<const_view::yes>;
    using mutable_atomic_cell_view = basic_atomic_cell_view<const_view::no>;
private:
    static thread_local imr::alloc::lsa_migrate_fn<external_last_chunk,
        imr::alloc::context_factory<last_chunk_context>> lsa_last_chunk_migrate_fn;
    static thread_local imr::alloc::lsa_migrate_fn<external_chunk,
        imr::alloc::context_factory<chunk_context>> lsa_chunk_migrate_fn;
private:
    template<typename Serializer, typename Allocations>
    static auto serialize_variable_value(Serializer&& serializer, Allocations&& allocations, bytes_view value) noexcept {
        auto after_size = serializer.serialize(value.size());
        if (value.size() <= maximum_internal_storage_length) {
            return after_size.template serialize_as<tags::data>(value).done();
        } else {
            imr::placeholder<imr::pod<void*>> next_pointer_phldr;
            auto next_pointer_position = after_size.position();
            auto cell_ser = after_size.template serialize_as<tags::pointer>(next_pointer_phldr);

            auto offset = 0;
            while (value.size() - offset > maximum_external_chunk_length) {
                imr::placeholder<imr::pod<void*>> phldr;
                auto chunk_ser = allocations.template allocate_nested<external_chunk>(&lsa_chunk_migrate_fn)
                        .serialize(next_pointer_position);
                next_pointer_position = chunk_ser.position();
                next_pointer_phldr.serialize(
                        chunk_ser.serialize(phldr)
                                .serialize(bytes_view(value.begin() + offset, maximum_external_chunk_length))
                                .done()
                );
                next_pointer_phldr = phldr;
                offset += maximum_external_chunk_length;
            }

            auto ptr = allocations.template allocate_nested<external_last_chunk>(&lsa_last_chunk_migrate_fn)
                    .serialize(next_pointer_position)
                    .serialize(value.size() - offset)
                    .serialize(bytes_view(value.begin() + offset, value.size() - offset))
                    .done();
            next_pointer_phldr.serialize(ptr);
            return cell_ser.done();
        }
    }
public:
    static auto copy_fn(const type_info& ti, const uint8_t* ptr);
    static auto make_collection(bytes_view data) noexcept {
        return [data] (auto&& serializer, auto&& allocations) noexcept {
            return serialize_variable_value(serializer
                .serialize(imr::set_flag<tags::collection>(),
                           imr::set_flag<tags::external_data>(data.size() > maximum_internal_storage_length))
                .template serialize_as_nested<tags::collection>(), allocations, data)
                .done();
        };
    }
    static auto make_dead(api::timestamp_type ts, gc_clock::time_point deletion_time) noexcept {
        return [ts, deletion_time] (auto&& serializer, auto&&...) noexcept {
            return serializer
                .serialize()
                .template serialize_as_nested<tags::atomic_cell>()
                    .serialize(ts)
                    .skip()
                    .template serialize_as<tags::dead>(deletion_time.time_since_epoch().count())
                    .done()
                .done();
        };
    }
    static auto make_live_counter_update(api::timestamp_type ts, int64_t delta) noexcept {
        return [ts, delta] (auto&& serializer, auto&&...) noexcept {
            return serializer
                .serialize()
                .template serialize_as_nested<tags::atomic_cell>()
                    .serialize(ts)
                    .skip()
                    .template serialize_as<tags::counter_update>(delta)
                    .done()
                .done();
        };
    }

    static auto make_live(const type_info& ti, api::timestamp_type ts, bytes_view value) noexcept {
        return [&ti, ts, value] (auto&& serializer, auto&& allocations) noexcept {
            auto after_expiring = serializer
                .serialize(imr::set_flag<tags::live>(),
                           imr::set_flag<tags::empty>(value.empty()),
                           imr::set_flag<tags::external_data>(!ti.is_fixed_size() && value.size() > maximum_internal_storage_length))
                .template serialize_as_nested<tags::atomic_cell>()
                    .serialize(ts)
                    .skip();
            return [&] {
                if (ti.is_fixed_size() || value.empty()) {
                    return after_expiring.template serialize_as<tags::fixed_value>(value);
                } else {
                    return serialize_variable_value(after_expiring.template serialize_as_nested<tags::variable_value>(),
                                                    allocations, value);

                }
            }().done().done();
        };
    }

    template<typename Builder>
    static size_t size_of(Builder&& builder, imr::alloc::object_allocator& allocator) noexcept {
        return structure::size_when_serialized(std::forward<Builder>(builder), allocator.get_sizer());
    }

    template<typename Builder>
    static size_t serialize(uint8_t* ptr, Builder&& builder, imr::alloc::object_allocator& allocator) noexcept {
        return structure::serialize(ptr, std::forward<Builder>(builder), allocator.get_serializer());
    }

    static atomic_cell_view make_atomic_cell_view(const type_info& ti, const uint8_t* ptr) noexcept;
    static mutable_atomic_cell_view make_atomic_cell_view(const type_info& ti, uint8_t* ptr) noexcept;

    static void destroy(uint8_t* ptr) noexcept;
};

class cell::minimal_context {
protected:
    cell::flags::view _flags;
public:
    explicit minimal_context(cell::flags::view flags) noexcept
        : _flags(flags) { }

    template<typename Tag>
    bool is_present() const noexcept;

    template<typename Tag>
    auto active_alternative_of() const noexcept;

    template<typename Tag>
    size_t size_of() const noexcept;

    template<typename Tag>
    auto context_for(const uint8_t*) const noexcept {
        return *this;
    }
};

template<>
inline bool cell::minimal_context::is_present<cell::tags::expiring>() const noexcept {
    return _flags.get<tags::expiring>();
}

template<>
inline auto cell::minimal_context::active_alternative_of<cell::tags::cell>() const noexcept {
    if (_flags.get<tags::collection>()) {
        return cell::atomic_cell_or_collection::index_for<tags::collection>();
    } else {
        return cell::atomic_cell_or_collection::index_for<tags::atomic_cell>();
    }
}

class cell::context : public cell::minimal_context {
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

        template<typename Tag, typename... Args>
        auto context_for(Args&&...) const noexcept {
            return *this;
        }
    };
public:
    explicit context(const uint8_t* ptr, const type_info& tinfo) noexcept
        : context(structure::get_member<tags::flags>(ptr), tinfo) { }

    explicit context(cell::flags::view flags, const type_info& tinfo) noexcept
        : minimal_context(flags), _type(tinfo) { }

    template<typename Tag>
    bool is_present() const noexcept {
        return minimal_context::is_present<Tag>();
    }

    template<typename Tag>
    auto active_alternative_of() const noexcept {
        return minimal_context::active_alternative_of<Tag>();
    }

    template<typename Tag>
    size_t size_of() const noexcept;

    template<typename Tag>
    auto context_for(const uint8_t*) const noexcept {
        return *this;
    }
};

template<>
inline auto cell::context::context_for<cell::tags::variable_value>(const uint8_t* ptr) const noexcept {
    auto length = variable_value::get_member<tags::value_size>(ptr);
    return variable_value_context(length.load());
}

template<>
inline auto cell::context::context_for<cell::tags::collection>(const uint8_t* ptr) const noexcept {
    auto length = variable_value::get_member<tags::value_size>(ptr);
    return variable_value_context(length.load());
}


template<>
inline auto cell::context::active_alternative_of<cell::tags::value>() const noexcept {
    if (!_flags.get<tags::live>()) {
        return cell::value_variant::index_for<tags::dead>();
    } else if (_flags.get<tags::counter_update>()) {
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

class value_view {
    size_t _remaining_size;
    bytes_view _first_chunk;
    const uint8_t* _next;
public:
    value_view(bytes_view first, size_t remaining_size, const uint8_t* next)
        : _remaining_size(remaining_size), _first_chunk(first), _next(next)
    { }

    class iterator {
        bytes_view _view;
        const uint8_t* _next;
        size_t _left;
    public:
        // FIXME: make this a real iterator
        iterator(bytes_view bv, size_t total, const uint8_t* next) noexcept
            : _view(bv), _next(next), _left(total) { }

        const bytes_view& operator*() const {
            return _view;
        }
        const bytes_view* operator->() const {
            return &_view;
        }

        // FIXME: iterators destroy the information about the last chunk
        // Use range-based approach exclusively.
        iterator& operator++() {
            if (!_next) {
                _view = bytes_view();
            } else if (_left > cell::maximum_external_chunk_length) {
                cell::chunk_context ctx(_next);
                auto v = cell::external_chunk::make_view(_next, ctx);
                _next = static_cast<const uint8_t*>(v.get<cell::tags::chunk_next>(ctx).load());
                _view = v.get<cell::tags::chunk_data>(ctx);
                _left -= cell::maximum_external_chunk_length;
            } else {
                cell::last_chunk_context ctx(_next);
                auto v = cell::external_last_chunk::make_view(_next, ctx);
                _view = v.get<cell::tags::chunk_data>(ctx);
                _next = nullptr;
            }
            return *this;
        }
        iterator operator++(int) {
            auto it = *this;
            operator++();
            return it;
        }

        bool operator==(const iterator& other) const {
            return _view.data() == other._view.data();
        }
        bool operator!=(const iterator& other) const {
            return !(*this == other);
        }
    };

    auto begin() const {
        return iterator(_first_chunk, _remaining_size, _next);
    }
    auto end() const {
        return iterator(bytes_view(), 0, nullptr);
    }

    bool operator==(const value_view& other) const noexcept {
        // We can assume that all values are fragmented exactly in the same way.
        auto it1 = begin();
        auto it2 = other.begin();
        while (it1 != end() && it2 != other.end()) {
            if (*it1 != *it2) {
                return false;
            }
            ++it1;
            ++it2;
        }
        return it1 == end() && it2 == other.end();
    }

    size_t size() const noexcept {
        return _first_chunk.size() + _remaining_size;
    }

    bool is_fragmented() const noexcept {
        return bool(_next);
    }

    bytes_view first_chunk() const noexcept {
        return _first_chunk;
    }

    template<typename Function>
    void for_each(Function&& fn) const {
        for (auto&& chunk : *this) {
            fn(chunk);
        }
    }

    // [[deprecated]]
    bytes linearize() const {
        bytes b(bytes::initialized_later(), size());
        auto it = b.begin();
        for (auto chunk : *this) {
            it = boost::copy(chunk, it);
        }
        return b;
    }

    template<typename Function>
    // [[deprecated]]
    auto with_linearized(Function&& fn) const {
        bytes b;
        bytes_view bv;
        if (is_fragmented()) {
            b = linearize();
            bv = b;
        } else {
            bv = _first_chunk;
        }
        fn(bv);
    }
};


template<const_view is_const>
class cell::basic_atomic_cell_view {
public:
    using flags_type = std::conditional_t<is_const == const_view::yes,
                                          flags::view,
                                          flags::mutable_view>;
    using view_type = std::conditional_t<is_const == const_view::yes,
                                         atomic_cell::view,
                                         atomic_cell::mutable_view>;
private:
    context _context;
    flags_type _flags;
    view_type _view;
public:
    basic_atomic_cell_view(context ctx, flags_type f, view_type v)
        : _context(std::move(ctx)), _flags(std::move(f)), _view(std::move(v)) { }

    const uint8_t* raw_pointer() const { return _view.raw_pointer() - flags::size_when_serialized(); }

    bool is_live() const noexcept {
        return _flags.template get<tags::live>();
    }
    bool is_expiring() const noexcept {
        return _flags.template get<tags::expiring>();
    }
    bool is_counter_update() const noexcept {
        return _flags.template get<tags::counter_update>();
    }
    bool is_revert_set() const noexcept {
        return _flags.template get<tags::revert>();
    }
    bool is_counter_in_place_revert_set() const noexcept {
        return _flags.template get<tags::counter_in_place_revert>();
    }

    void set_revert(bool flag) noexcept {
        return _flags.template set<tags::revert>(flag);
    }
    void set_counter_in_place_revert(bool flag) noexcept {
        return _flags.template set<tags::counter_in_place_revert>(flag);
    }

    api::timestamp_type timestamp() const noexcept {
        return _view.template get<tags::timestamp>().load();
    }
    void set_timestamp(api::timestamp_type ts) noexcept {
        _view.template get<tags::timestamp>().store(ts);
    }

    gc_clock::time_point expiry() const noexcept {
        auto v = _view.template get<tags::expiring>().get().template get<tags::expiry>().load();
        return gc_clock::time_point(gc_clock::duration(v));
    }
    gc_clock::duration ttl() const noexcept {
        auto v = _view.template get<tags::expiring>().get().template get<tags::ttl>().load();
        return gc_clock::duration(v);
    }

    gc_clock::time_point deletion_time() const noexcept {
        auto v = _view.template get<tags::value>().template as<tags::dead>().load();
        return gc_clock::time_point(gc_clock::duration(v));
    }

    int64_t counter_update_value() const noexcept {
        return _view.template get<tags::value>().template as<tags::counter_update>().load();
    }

    value_view value() const noexcept {
        return _view.template get<tags::value>().visit(utils::make_visitor(
                [] (fixed_value::view view) { return value_view(view, 0, nullptr); },
                [&] (variable_value::view view) {
                    return view.get<tags::value_data>().visit(utils::make_visitor(
                            [&view] (imr::pod<void*>::view ptr) {
                                auto size = view.get<tags::value_size>().load();
                                auto ex_ptr = static_cast<const uint8_t*>(ptr.load());
                                if (size > maximum_external_chunk_length) {
                                    auto ex_ctx = chunk_context(ex_ptr);
                                    auto ex_view = external_chunk::make_view(ex_ptr, ex_ctx);
                                    auto next = static_cast<const uint8_t*>(ex_view.get<tags::chunk_next>().load());
                                    return value_view(ex_view.get<tags::chunk_data>(ex_ctx), size - maximum_external_chunk_length, next);
                                } else {
                                    auto ex_ctx = last_chunk_context(ex_ptr);
                                    auto ex_view = external_last_chunk::make_view(ex_ptr, ex_ctx);
                                    assert(ex_view.get<tags::chunk_data>(ex_ctx).size() == size);
                                    return value_view(ex_view.get<tags::chunk_data>(ex_ctx), 0, nullptr);
                                }
                            },
                            [] (imr::buffer<tags::data>::view data) {
                                return value_view(data, 0, nullptr);
                            }
                    ), _context.context_for<tags::variable_value>(view.raw_pointer()));
                },
                [] (...) -> value_view { __builtin_unreachable(); }
        ), _context);
    }
};

inline auto cell::copy_fn(const type_info& ti, const uint8_t* ptr)
{
    // FIXME: This is far from optimal. We could just compute the size
    // of the buffer, memcpy() it and then deal with whatever objects the
    // original IMR object may own.
    // Not to mention that now we deserialize the original cell twice.
    return [&ti, ptr] (auto&& serializer, auto&& allocations) noexcept {
        auto f = structure::get_member<tags::flags>(ptr);
        context ctx(f, ti);
        if (f.get<tags::collection>()) {
            //auto data = structure::get_member<tags::cell>(ptr).as<tags::atomic_cell>(ctx);
            abort();
        } else {
            auto ac = structure::get_member<tags::cell>(ptr).as<tags::atomic_cell>(ctx);
            auto acv = atomic_cell_view(ctx, std::move(f), std::move(ac));
            // FIXME: linearize()
            return make_live(ti, acv.timestamp(), acv.value().linearize())(serializer, allocations);
        }
    };
}

inline cell::atomic_cell_view cell::make_atomic_cell_view(const type_info& ti, const uint8_t* ptr) noexcept {
    auto f = structure::get_member<tags::flags>(ptr);
    context ctx(f, ti);
    auto ac = structure::get_member<tags::cell>(ptr).as<tags::atomic_cell>(ctx);
    return atomic_cell_view(ctx, std::move(f), std::move(ac));
}

inline cell::mutable_atomic_cell_view cell::make_atomic_cell_view(const type_info& ti, uint8_t* ptr) noexcept {
    auto f = structure::get_member<tags::flags>(ptr);
    context ctx(f, ti);
    auto ac = structure::get_member<tags::cell>(ptr).as<tags::atomic_cell>(ctx);
    return mutable_atomic_cell_view(ctx, std::move(f), std::move(ac));
}

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
struct destructor<data::cell::structure> {
    static void run(uint8_t* ptr, ...) {
        auto flags = data::cell::structure::get_member<data::cell::tags::flags>(ptr);
        if (flags.get<data::cell::tags::external_data>()) {
            auto cell_offset = data::cell::structure::offset_of<data::cell::tags::cell>(ptr);
            auto variable_value_ptr = [&] {
                if (flags.get<data::cell::tags::collection>()) {
                    return ptr + cell_offset;
                } else {
                    auto ctx = data::cell::minimal_context(flags);
                    auto offset = data::cell::atomic_cell::offset_of<data::cell::tags::value>(ptr + cell_offset, ctx);
                    return ptr + cell_offset + offset;
                }
            }();
            imr::methods::destroy<data::cell::variable_value>(variable_value_ptr);
        }
    }
};

template<>
struct mover<data::cell::structure> {
    static void run(uint8_t* ptr, ...) {
        auto flags = data::cell::structure::get_member<data::cell::tags::flags>(ptr);
        if (flags.get<data::cell::tags::external_data>()) {
            auto cell_offset = data::cell::structure::offset_of<data::cell::tags::cell>(ptr);
            auto variable_value_ptr = [&] {
                if (flags.get<data::cell::tags::collection>()) {
                    return ptr + cell_offset;
                } else {
                    auto ctx = data::cell::minimal_context(flags);
                    auto offset = data::cell::atomic_cell::offset_of<data::cell::tags::value>(ptr + cell_offset, ctx);
                    return ptr + cell_offset + offset;
                }
            }();
            variable_value_ptr += data::cell::variable_value::offset_of<data::cell::tags::value_data>(variable_value_ptr);
            imr::methods::move<imr::tagged_type<data::cell::tags::pointer, imr::pod<void*>>>(variable_value_ptr);
        }
    }
};

template<>
struct destructor<data::cell::variable_value> {
    static void run(uint8_t* ptr, ...) {
        // TODO: make_view() doesn't need context
        auto varval = data::cell::variable_value::make_view(ptr);
        auto total_length = varval.get<data::cell::tags::value_size>().load();
        if (total_length <= data::cell::maximum_internal_storage_length) {
            return;
        }
        auto ctx = data::fragment_chain_destructor_context(total_length);
        auto ptr_view = varval.get<data::cell::tags::value_data>().as<data::cell::tags::pointer>();
        if (ctx.is_last_chunk()) {
            imr::methods::destroy<data::cell::external_last_chunk>(static_cast<uint8_t*>(ptr_view.load()));
        } else {
            imr::methods::destroy<data::cell::external_chunk>(static_cast<uint8_t*>(ptr_view.load()), ctx);
        }
        current_allocator().free(ptr_view.load());
    }
};

template<>
struct mover<imr::tagged_type<data::cell::tags::pointer, imr::pod<void*>>> {
    static void run(uint8_t* ptr, ...) {
        auto ptr_view = imr::pod<void*>::make_view(ptr);
        auto chk_ptr = static_cast<uint8_t*>(ptr_view.load());
        // FIXME: it is not necessairly a last chunk
        auto chk = data::cell::external_last_chunk::make_view(chk_ptr, data::cell::last_chunk_context(chk_ptr));
        chk.get<data::cell::tags::chunk_back_pointer>().store(const_cast<uint8_t*>(ptr));
    }
};

template<>
struct mover<imr::tagged_type<data::cell::tags::chunk_back_pointer, imr::pod<void*>>> {
    static void run(uint8_t* bptr, ...) {
        auto bptr_view = imr::pod<void*>::make_view(bptr);
        auto ptr_ptr = static_cast<uint8_t*>(bptr_view.load());
        auto ptr = imr::pod<void*>::make_view(ptr_ptr);
        ptr.store(const_cast<uint8_t*>(bptr));

    }
};

template<>
struct destructor<data::cell::external_chunk> {
    static void run(uint8_t* ptr, data::fragment_chain_destructor_context ctx) {
        ctx.next_chunk();

        auto echk_view = data::cell::external_chunk::make_view(ptr);
        auto ptr_view = echk_view.get<data::cell::tags::chunk_next>();
        if (ctx.is_last_chunk()) {
            imr::methods::destroy<data::cell::external_last_chunk>(static_cast<uint8_t*>(ptr_view.load()));
        } else {
            imr::methods::destroy<data::cell::external_chunk>(static_cast<uint8_t*>(ptr_view.load()), ctx);
        }
        current_allocator().free(ptr_view.load());
    }
};

template<>
struct mover<data::cell::external_chunk> {
    static void run(uint8_t* ptr, ...) {
        auto echk_view = data::cell::external_chunk::make_view(ptr, data::cell::chunk_context(ptr));
        auto next_ptr = static_cast<uint8_t*>(echk_view.get<data::cell::tags::chunk_next>().load());
        auto bptr = imr::pod<void*>::make_view(next_ptr);
        bptr.store(const_cast<uint8_t*>(ptr + echk_view.offset_of<data::cell::tags::chunk_next>()));

        // TODO: remove this and specialise only for forward pointer
        auto back_ptr = static_cast<uint8_t*>(echk_view.get<data::cell::tags::chunk_back_pointer>().load());
        auto nptr = imr::pod<void*>::make_view(back_ptr);
        nptr.store(const_cast<uint8_t*>(ptr));
    }
};

}
}

template<>
struct appending_hash<data::value_view> {
    template<typename Hasher> // FIXME: add tests verifying that fragmentation does not change the hash
    void operator()(Hasher& h, data::value_view v) const {
        feed_hash(h, v.size());
        v.for_each([&h] (auto&& chk) {
            h.update(reinterpret_cast<const char*>(chk.data()), chk.size());
        });
    }
};


// FIXME: This function doesn't return an int, it returns std::strong_ordering.
inline int compare_unsigned(data::value_view lhs, data::value_view rhs) noexcept {
    abort();
}

namespace data {

struct type_imr_state {
    using context_factory = imr::alloc::context_factory<cell::context, data::type_info>;
    using lsa_migrate_fn = imr::alloc::lsa_migrate_fn<cell::structure, context_factory>;
private:
    data::type_info _type_info;
    lsa_migrate_fn _lsa_migrator;
public:
    explicit type_imr_state(data::type_info ti)
        : _type_info(ti)
        , _lsa_migrator(context_factory(ti))
    { }

    const data::type_info& type_info() const { return _type_info; }
    const lsa_migrate_fn& lsa_migrator() const { return _lsa_migrator; }
};

}
