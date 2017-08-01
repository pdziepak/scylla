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

#pragma once

// combine two sorted uniqued sequences into a single sorted sequence
// unique elements are copied, duplicate elements are merged with a
// binary function.
template <typename InputIterator1,
          typename InputIterator2,
          typename OutputIterator,
          typename Compare,
          typename Merge>
OutputIterator
combine(InputIterator1 begin1, InputIterator1 end1,
        InputIterator2 begin2, InputIterator2 end2,
        OutputIterator out,
        Compare compare,
        Merge merge) {
    while (begin1 != end1 && begin2 != end2) {
        auto& e1 = *begin1;
        auto& e2 = *begin2;
        if (compare(e1, e2)) {
            *out++ = e1;
            ++begin1;
        } else if (compare(e2, e1)) {
            *out++ = e2;
            ++begin2;
        } else {
            *out++ = merge(e1, e2);
            ++begin1;
            ++begin2;
        }
    }
    out = std::copy(begin1, end1, out);
    out = std::copy(begin2, end2, out);
    return out;
}

// TODO: generalise to multiple ranges
template<typename InputRange1, typename InputRange2, typename Emit, typename LessCompare>
void combine2(InputRange1&& range1, InputRange2&& range2, Emit&& emit, LessCompare&& less)
{
    auto begin1 = range1.begin();
    auto end1 = range1.end();
    auto begin2 = range2.begin();
    auto end2 = range2.end();
    while (begin1 != end1 && begin2 != end2) {
        auto&& e1 = *begin1;
        auto&& e2 = *begin2;
        if (less(e1, e2)) {
            emit(e1);
            ++begin1;
        } else if (less(e2, e1)) {
            emit(e2);
            ++begin2;
        } else {
            emit(e1, e2);
            ++begin1;
            ++begin2;
        }
    }
    std::for_each(begin1, end1, emit);
    std::for_each(begin2, end2, emit);
}

static struct none_t { } none;

template<typename InputRange1, typename InputRange2, typename Emit, typename LessCompare>
void combine3(InputRange1&& range1, InputRange2&& range2, Emit&& emit, LessCompare&& less)
{
    auto begin1 = range1.begin();
    auto end1 = range1.end();
    auto begin2 = range2.begin();
    auto end2 = range2.end();
    while (begin1 != end1 && begin2 != end2) {
        auto&& e1 = *begin1;
        auto&& e2 = *begin2;
        if (less(e1, e2)) {
            emit(e1, none);
            ++begin1;
        } else if (less(e2, e1)) {
            emit(none, e2);
            ++begin2;
        } else {
            emit(e1, e2);
            ++begin1;
            ++begin2;
        }
    }
    std::for_each(begin1, end1, [&emit] (auto&& e) { emit(e, none); });
    std::for_each(begin2, end2, [&emit] (auto&& e) { emit(none, e); });
}

template<typename... Functions>
struct do_make_visitor {
    void operator()();
};

template<typename Function, typename... Functions>
struct do_make_visitor<Function, Functions...> : Function, do_make_visitor<Functions...> {
    do_make_visitor(Function fn, Functions... fns)
        : Function(std::move(fn))
        , do_make_visitor<Functions...>(std::move(fns)...)
    { }

    using Function::operator();
    using do_make_visitor<Functions...>::operator();
};

template<typename... Functions>
auto make_visitor(Functions&&... fns) {
    return do_make_visitor<Functions...>(std::forward<Functions>(fns)...);
}
