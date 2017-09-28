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

#include "utils/chunked_vector.hh"
#include "utils/logalloc.hh"

#include "imr/core.hh"
#include "imr/methods.hh"

namespace imr {
namespace alloc {

static const struct no_context_factory_t {
    static no_context_t create(const void*) noexcept { return no_context; }
} no_context_factory;

template<typename Context, typename... State>
class context_factory {
    std::tuple<State...> _state;
private:
    template<size_t... Index>
    Context create(const uint8_t* ptr, std::index_sequence<Index...>) const noexcept {
        return Context(ptr, std::get<Index>(_state)...);
    }
public:
    template<typename... Args>
    context_factory(Args&&... args) : _state(std::forward<Args>(args)...) { }

    context_factory(context_factory&) = default;
    context_factory(const context_factory&) = default;
    context_factory(context_factory&&) = default;

    Context create(const uint8_t* ptr) const noexcept {
        return create(ptr, std::index_sequence_for<State...>());
    }
};

GCC6_CONCEPT(
template<typename T>
concept bool ContextFactory = requires(const T factory, const uint8_t* ptr) {
    { factory.create(ptr) } noexcept;
};

static_assert(ContextFactory<no_context_factory_t>,
              "no_context_factory_t has to meet ContextFactory constraints");
)

template<typename Structure, typename CtxFactory>
GCC6_CONCEPT(requires ContextFactory<CtxFactory>)
class lsa_migrate_fn final : public migrate_fn_type, CtxFactory {
public:
    explicit lsa_migrate_fn(CtxFactory context_factory)
        : migrate_fn_type(1)
        , CtxFactory(std::move(context_factory))
    { }

    lsa_migrate_fn(lsa_migrate_fn&&) = delete;
    lsa_migrate_fn(const lsa_migrate_fn&) = delete;

    lsa_migrate_fn& operator=(lsa_migrate_fn&&) = delete;
    lsa_migrate_fn& operator=(const lsa_migrate_fn&) = delete;

    virtual void migrate(void* src_ptr, void* dst_ptr, size_t size) const noexcept override {
        std::memcpy(dst_ptr, src_ptr, size);
        auto dst = static_cast<uint8_t*>(dst_ptr);
        methods::move<Structure>(dst, CtxFactory::create(dst));
    }

    virtual size_t size(const void* obj_ptr) const noexcept override {
        auto ptr = static_cast<const uint8_t*>(obj_ptr);
        return Structure::serialized_object_size(ptr, CtxFactory::create(ptr)) + 7;
    }
};

template<typename Structure>
struct default_lsa_migrate_fn {
    static lsa_migrate_fn<Structure, no_context_factory_t> migrate_fn;
};

template<typename Structure>
lsa_migrate_fn<Structure, no_context_factory_t> default_lsa_migrate_fn<Structure>::migrate_fn(no_context_factory);

}
}
