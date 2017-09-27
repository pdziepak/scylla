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

namespace utils {

namespace internal {

template<typename... Functions>
struct do_make_visitor {
    void operator()();
};

template<typename Function, typename... Functions>
struct do_make_visitor<Function, Functions...> : Function, do_make_visitor<Functions...> {
    do_make_visitor(Function&& fn, Functions&&... fns)
            : Function(std::move(fn))
              , do_make_visitor<Functions...>(std::move(fns)...)
    { }

    using Function::operator();
    using do_make_visitor<Functions...>::operator();
};

}

template<typename... Functions>
auto make_visitor(Functions&&... fns) {
    return internal::do_make_visitor<Functions...>(std::forward<Functions>(fns)...);
}

}
