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

#include <limits>

namespace data {

class type_info {
    int32_t _fixed_size;
private:
    explicit type_info(int32_t size) noexcept : _fixed_size(size) { }
public:
    static type_info make_fixed_size(size_t size) noexcept {
        assert(size <= std::numeric_limits<int32_t>::max());
        return type_info { int32_t(size) };
    }
    static type_info make_variable_size() noexcept {
        return type_info { 0 };
    }
    static type_info make_collection() noexcept {
        return type_info { -1 };
    }

    bool is_collection() const noexcept {
        return _fixed_size < 0;
    }
    bool is_fixed_size() const noexcept {
        return _fixed_size > 0;
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

}
