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

#include "data/cell.hh"

thread_local imr::alloc::context_factory<data::cell::last_chunk_context> lcc;
thread_local imr::alloc::lsa_migrate_fn<data::cell::external_last_chunk,
        imr::alloc::context_factory<data::cell::last_chunk_context>> data::cell::lsa_last_chunk_migrate_fn(lcc);
thread_local imr::alloc::context_factory<data::cell::chunk_context> ecc;
thread_local imr::alloc::lsa_migrate_fn<data::cell::external_chunk,
        imr::alloc::context_factory<data::cell::chunk_context>> data::cell::lsa_chunk_migrate_fn(ecc);

static thread_local data::type_imr_state no_type_imr_state(data::type_info::make_variable_size());
