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

namespace result_type {

template<typename... Args>
struct any_of { };

/// A type constructible from any type in `Args`
///
/// This class helps specifying in a concise manner a concept constraint
/// which requires the expression to return any type from the provided list
/// of types. For example, the following requirement is satisfied if the
/// result of `obj.fn()` is implicitly convertible to A, B or C.
///
/// \code
/// requires (T obj) {
///     { obj.fn() } -> result_type::any_of<A, B, C>;
/// }
/// \endcode
template<typename Arg, typename... Args>
struct any_of<Arg, Args...> : any_of<Args...> {
    using any_of<Args...>::any_of;
    any_of(Arg);
};

}