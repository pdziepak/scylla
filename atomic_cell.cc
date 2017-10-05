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

#include "atomic_cell.hh"
#include "atomic_cell_or_collection.hh"
#include "types.hh"

static thread_local data::type_imr_state no_type_imr_state(data::type_info::make_variable_size());

atomic_cell atomic_cell::make_dead(api::timestamp_type timestamp, gc_clock::time_point deletion_time) {
    auto& imr_data = no_type_imr_state;
    return atomic_cell(
            imr_data,
            imr_object_type::make(data::cell::make_dead(timestamp, deletion_time), &imr_data.lsa_migrator())
    );
}

atomic_cell atomic_cell::make_live(const abstract_type& type, api::timestamp_type timestamp, bytes_view value) {
    auto& imr_data = type.imr_state();
    return atomic_cell(
        imr_data,
        imr_object_type::make(data::cell::make_live(imr_data.type_info(), timestamp, value), &imr_data.lsa_migrator())
    );
}

atomic_cell atomic_cell::make_live(const abstract_type& type, api::timestamp_type timestamp, bytes_view value,
                             gc_clock::time_point expiry, gc_clock::duration ttl) {
    return make_live(type, timestamp, value);
}

atomic_cell atomic_cell::make_live_counter_update(api::timestamp_type timestamp, int64_t value) {
    auto& imr_data = no_type_imr_state;
    return atomic_cell(
            imr_data,
            imr_object_type::make(data::cell::make_live_counter_update(timestamp, value), &imr_data.lsa_migrator())
    );
}

atomic_cell::atomic_cell(atomic_cell_view other)
    : atomic_cell(other.type_imr_state(),
                  imr_object_type::make(data::cell::copy_fn(other.type_imr_state().type_info(), other._view.raw_pointer()),
                                        &other.type_imr_state().lsa_migrator()))
{ }

atomic_cell_or_collection::atomic_cell_or_collection(collection_mutation cm)
    : _data(imr_object_type::make(data::cell::make_collection(cm.data), &no_type_imr_state.lsa_migrator()))
{
}

atomic_cell_or_collection atomic_cell_or_collection::copy(const abstract_type& type) const {
    if (!_data.get()) {
        return atomic_cell_or_collection();
    }
    auto& imr_data = type.imr_state();
    return atomic_cell_or_collection(
        imr_object_type::make(data::cell::copy_fn(imr_data.type_info(), _data.get()), &imr_data.lsa_migrator())
    );
}
