/*
 * Copyright (C) 2018 ScyllaDB
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

#include "bytes.hh"
#include "concept-utils.hh"

enum class const_view { no, yes, };

GCC6_CONCEPT(

/// Fragmented buffer
///
/// Concept `FragmentedBuffer` is satisfied by any class that is a range of
/// fragments and provides a method `size()` which returns the total
/// size of the buffer. The interfaces accepting `FragmentedBuffer` will attempt
/// to avoid unnecessary linearisation.
template<typename T>
concept bool FragmentRange = requires (T range) {
    { *range.begin() } -> result_type::any_of<bytes_view, bytes_mutable_view>;
    { *range.end() } -> result_type::any_of<bytes_view, bytes_mutable_view>;
    { range.size() } -> size_t; // FIXME: rename, clashes with the number of elements in a range
};

)

template<typename T, typename = void>
struct is_fragment_range : std::false_type { };

template<typename T>
struct is_fragment_range<T, std::void_t<typename T::fragment_type>> : std::true_type { };

template<typename T>
static constexpr bool is_fragment_range_v = is_fragment_range<T>::value;

/// Single-element fragment range
///
/// This is a helper that allows converting a bytes_view into a FragmentRange.
template<const_view is_const>
class single_fragment_range {
public:
    using fragment_type = std::conditional_t<is_const == const_view::yes,
                                             bytes_view, bytes_mutable_view>;
private:
    using array_type = std::array<fragment_type, 1>;
    array_type _view;
public:
    using iterator = typename array_type::const_iterator;
    using const_iterator = typename array_type::const_iterator;

    explicit single_fragment_range(fragment_type f) : _view { f } { }

    const_iterator begin() const { return _view.begin(); }
    const_iterator end() const { return _view.end(); }

    size_t size() const { return _view[0].size(); }
};

single_fragment_range(bytes_view) -> single_fragment_range<const_view::yes>;
single_fragment_range(bytes_mutable_view) -> single_fragment_range<const_view::no>;

/// Empty fragment range.
struct empty_fragment_range {
    using fragment_type = bytes_view;
    using iterator = bytes_view*;
    using const_iterator = bytes_view*;

    iterator begin() const { return nullptr; }
    iterator end() const { return nullptr; }

    size_t size() const { return 0; }
};
