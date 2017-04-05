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

template<typename Structure>
class object {
    uint8_t* _data = nullptr;
private:
    explicit object(uint8_t* ptr) noexcept : _data(ptr) { }
public:
    object() = default;
    object(object&& other) noexcept : _data(std::exchange(other._data, nullptr)) { }
    object(const object&) = delete;

    object& operator=(object&& other) noexcept {
        this->~object();
        new (this) object(std::move(other));
        return *this;
    }

    ~object() {
        if (_data) {
            imr::methods::destroy<Structure>(_data);
            current_allocator().free(_data);
        }
    }

    explicit operator bool() const noexcept { return bool(_data); }

    uint8_t* get() noexcept { return _data; }
    const uint8_t* get() const noexcept { return _data; }

    template<typename Serializer>
    static object make(Serializer&& serializer,
                       allocation_strategy::migrate_fn migrate = &imr::alloc::default_lsa_migrate_fn<Structure>::migrate_fn) {
        alloc::object_allocator allocator;
        auto obj_size = Structure::size_when_serialized(serializer, allocator.get_sizer());
        auto ptr = static_cast<uint8_t*>(current_allocator().alloc(migrate, obj_size + 7, 1));
        try {
            // FIXME: RAII to protect ptr
            allocator.allocate_all();
        } catch (...) {
            current_allocator().free(ptr, obj_size + 7);
            throw;
        }
        Structure::serialize(ptr, serializer, allocator.get_serializer());
        return object { ptr };
    }
};

}
}
