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

#include <type_traits>

#include "imr/core.hh"
#include "imr/alloc.hh"

namespace imr {
namespace utils {


class basic_object {
public:
    struct tags {
        class back_pointer { };
        class object { };
    };
protected:
    uint8_t* _data = nullptr; // FIXME: unique_ptr

    friend struct methods::mover<imr::tagged_type<tags::back_pointer, imr::pod<basic_object*>>>;
protected:
    explicit basic_object(uint8_t* ptr) noexcept : _data(ptr) { }

    void set_data(uint8_t* ptr) noexcept { _data = ptr; }
public:
    basic_object() = default;
    basic_object(basic_object&& other) noexcept : _data(std::exchange(other._data, nullptr)) { }
    basic_object(const basic_object&) = delete;
};

template<typename Context, typename... State>
class object_context {
    std::tuple<State...> _state;
private:
    template<size_t... Index>
    Context create(const uint8_t* ptr, std::index_sequence<Index...>) const noexcept {
        return Context(ptr, std::get<Index>(_state)...);
    }
public:
    object_context(const uint8_t*, State... state) : _state { state... } { }
    template<typename Tag, typename... Args>
    Context context_for(const uint8_t* ptr, Args&&... args) const noexcept {
        return create(ptr, std::index_sequence_for<State...>());
    }
};

}

namespace methods {

template<>
struct mover<imr::tagged_type<utils::basic_object::tags::back_pointer, imr::pod<utils::basic_object*>>> {
    static void run(uint8_t* ptr, ...) {
        auto bptr = imr::tagged_type<utils::basic_object::tags::back_pointer, imr::pod<utils::basic_object*>>::make_view(ptr).load();
        bptr->_data = ptr;
    }
};

}

namespace utils {

template<typename Structure>
class object : public basic_object {
public:
    using structure = imr::structure<
                        imr::member<tags::back_pointer, imr::tagged_type<tags::back_pointer, imr::pod<basic_object*>>>,
                        imr::member<tags::object, Structure>
                      >;
public:
    object() = default;
    object(object&& other) noexcept : basic_object(std::move(other)) {
        if (_data) {
            structure::template get_member<tags::back_pointer>(_data).store(this);
        }
    }

    object& operator=(object&& other) noexcept {
        this->~object();
        new (this) object(std::move(other));
        return *this;
    }

    ~object() {
        if (_data) {
            imr::methods::destroy<structure>(_data);
            current_allocator().free(_data);
        }
    }

    explicit operator bool() const noexcept { return bool(_data); }

    uint8_t* get() noexcept { return _data ? _data + structure::template offset_of<tags::object>(_data) : nullptr; }
    const uint8_t* get() const noexcept { return _data ? _data + structure::template offset_of<tags::object>(_data) : nullptr; }

    template<typename Serializer>
    static object make(Serializer&& object_serializer,
                       allocation_strategy::migrate_fn migrate = &imr::alloc::default_lsa_migrate_fn<structure>::migrate_fn) {
        object obj;
        auto serializer = [&obj, &object_serializer] (auto&& ser, auto&& alloc) {
            return object_serializer(ser.serialize(&obj).serialize_nested(), alloc).done();
        };

        alloc::object_allocator allocator;
        auto obj_size = structure::size_when_serialized(serializer, allocator.get_sizer());
        auto ptr = static_cast<uint8_t*>(current_allocator().alloc(migrate, obj_size + 7, 1));
        try {
            // FIXME: RAII to protect ptr
            allocator.allocate_all();
        } catch (...) {
            current_allocator().free(ptr, obj_size + 7);
            throw;
        }
        structure::serialize(ptr, serializer, allocator.get_serializer());
        obj.set_data(ptr);
        return obj;
    }
};

}

}
