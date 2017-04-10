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

namespace imr {
namespace utils {

static struct { } no_context;

template<typename Structure, typename Context>
class lsa_migrate_fn final : public migrate_fn_type {
    const Context& _context;
public:
    explicit lsa_migrate_fn(const Context& context) noexcept : _context(context) { }

    lsa_migrate_fn(lsa_migrate_fn&&) = delete;
    lsa_migrate_fn(const lsa_migrate_fn&) = delete;

    lsa_migrate_fn& operator=(lsa_migrate_fn&&) = delete;
    lsa_migrate_fn& operator=(const lsa_migrate_fn&) = delete;

    virtual void migrate(void* src_ptr, void* dst_ptr, size_t size) const noexcept override {
        std::memcpy(dst_ptr, src_ptr, size);
        methods::move<Structure>(static_cast<uint8_t*>(dst_ptr), _context);
    }
};

template<typename Structure>
struct default_lsa_migrate_fn {
    static lsa_migrate_fn<Structure, decltype(no_context)> migrate_fn;
};

template<typename Structure>
lsa_migrate_fn<Structure, decltype(no_context)> default_lsa_migrate_fn<Structure>::migrate_fn(no_context);


// LSA-aware helper for creating hybrids of C++ and IMR objects. Particularly
// useful in the intermediate before we are fully converted to IMR so that we
// can easily have e.g. IMR objects with boost::intrusive containers hooks.
template<typename Header, typename Structure>
GCC6_CONCEPT(requires std::is_nothrow_move_constructible<Header>::value
                   && std::is_nothrow_destructible<Header>::value)
class object_with_header : public Header {
    using Header::Header;
    ~object_with_header() = default;
    object_with_header(object_with_header&&) = default;
private:
    const uint8_t* imr_data() const noexcept {
        return reinterpret_cast<const uint8_t*>(this + 1);
    }
    uint8_t* imr_data() noexcept {
        return reinterpret_cast<uint8_t*>(this + 1);
    }
public:
    object_with_header(const object_with_header&) = delete;
    object_with_header& operator=(const object_with_header&) = delete;
    object_with_header& operator=(object_with_header&&) = delete;

    template<typename Context = decltype(no_context)>
    typename Structure::view imr_object(const Context& context = no_context) const noexcept {
        return Structure::make_view(imr_data(), context);
    }

    template<typename Context = decltype(no_context)>
    typename Structure::mutable_view imr_object(const Context& context = no_context) const noexcept {
        return Structure::make_view(imr_data(), context);
    }
public:
    template<typename Context>
    class lsa_migrator final : migrate_fn_type {
        const Context& _context;
    public:
        explicit lsa_migrator(const Context& context) noexcept : _context(context) { }

        lsa_migrator(lsa_migrator&&) = delete;
        lsa_migrator(const lsa_migrator&) = delete;

        lsa_migrator& operator=(lsa_migrator&&) = delete;
        lsa_migrator& operator=(const lsa_migrator&) = delete;

        virtual void migrate(void* src_ptr, void* dst_ptr, size_t size) const noexcept override {
            auto src = static_cast<object_with_header*>(src_ptr);
            auto dst = new (dst_ptr) object_with_header(std::move(*src));
            std::copy_n(src->imr_data(), size - sizeof(object_with_header), dst->imr_data());
            methods::move<Structure>(dst->imr_data(), _context);
        }
    };
private:
    static lsa_migrator<decltype(no_context)> _lsa_migrator;
public:
    template<typename HeaderArg, typename ObjectArg>
    GCC6_CONCEPT(requires std::is_nothrow_constructible<Header, HeaderArg>::value)
    static object_with_header* create(HeaderArg&& hdr, ObjectArg&& obj,
                                      allocation_strategy::migrate_fn* migrate = &_lsa_migrator) {
        auto obj_size = Structure::size_when_serialized(obj);
        auto ptr = current_allocator().alloc(migrate, sizeof(Header) + obj_size, alignof(Header));
        auto owh = new (ptr) object_with_header(std::forward<HeaderArg>(hdr));
        Structure::serialize(owh->imr_data(), std::forward<ObjectArg>(obj));
        return owh;
    }

    template<typename Context = decltype(no_context)>
    static void destroy(object_with_header* object, const Context& context = no_context) noexcept {
        methods::destroy<Structure>(object->imr_data(), context);
        current_allocator().destroy(object);
    }
};

class external_object_allocator {
    union allocation {
    private:
        void* _pointer;
        std::pair<size_t, allocation_strategy::migrate_fn> _request; // ensure that pair is a trivially destructible
    public:
        explicit allocation(size_t n, allocation_strategy::migrate_fn fn) noexcept
            : _request(std::make_pair(n, fn)) { }

        void allocate() {
            auto ptr = current_allocator().alloc(_request.second, _request.first, 1);
            _pointer = ptr;
        }

        void free() noexcept {
            current_allocator().free(_pointer);
        }

        void* pointer() const noexcept { return _pointer; }
    };
    std::deque<allocation> _allocations; // clustered list?
    size_t _position = 0;
private:
    void reserve(size_t n, allocation_strategy::migrate_fn migrate) {
        _allocations.emplace_back(n, migrate);
    }
    uint8_t* get_next() {
        return static_cast<uint8_t*>(_allocations[_position++].pointer());
    }
public:
    // TODO: split phases into two types to enforce correctness

    size_t requested_allocations_count() const { return _allocations.size(); }

    void allocate_all() {
        auto it = _allocations.begin();
        try {
            // TODO: send batch of allocations to the allocation strategy and
            // let it worry about it
            while (it != _allocations.end()) {
                it->allocate();
                ++it;
            }
        } catch (...) {
            while (it != _allocations.begin()) {
                --it;
                it->free();
            }
            throw;
        }
    }

    class sizer {
        external_object_allocator& _parent;
    public:
        explicit sizer(external_object_allocator& parent) noexcept
            : _parent(parent) { }

        // FIXME: allow users to specify their own migrate_fn (for context-dependent
        // movers).
        template<typename T, typename... Args>
        uint8_t* serialize(Args&& ... args) {
            auto size = T::size_when_serialized(std::forward<Args>(args)...);
            _parent.reserve(size, &default_lsa_migrate_fn<T>::migrate_fn);
            return nullptr;
        }
    };

    class serializer {
        external_object_allocator& _parent;
    public:
        explicit serializer(external_object_allocator& parent) noexcept
            : _parent(parent) { }

        template<typename T, typename... Args>
        uint8_t* serialize(Args&& ... args) noexcept {
            auto ptr = _parent.get_next();
            T::serialize(ptr, std::forward<Args>(args)...);
            return ptr;
        }
    };

    sizer get_sizer() noexcept { return sizer(*this); }
    serializer get_serializer() noexcept { return serializer(*this); }
};

}
}
