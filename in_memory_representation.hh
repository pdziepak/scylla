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

#include <array>

#include <core/align.hh>
#include <core/bitops.hh>
#include <util/gcc6-concepts.hh>

#include "bytes.hh"

// In-Memory Representation
//
// ...high-level overview goes here...
// ...explain deserialisation context...

// If you are reviewing RFC I've sent to the mailing list make sure that you
// have read the cover letter before continuing with this patch.

// IMR types:
// - flags
// - fixed_size_value
// - fixed_buffer
// - compressed_integer
// - optional
// - structure

// IMR type concept:
//
// struct imr_type {
//     static auto make_view(bytes_view) noexcept;
//     static auto make_view(bytes_mutable_view) noexcept;
//
//     template<typename Context>
//     static size_t serialized_object_size(const uint8_t*, const Context&) noexcept;
//
//     static size_t size_when_serialized(...) noexcept;
//     static size_t serialize(uint8_t* out, ...) noexcept;
// };

// Some hints regarding IMR usage:
// 1. Fixed-size objects can be updated in-place, variable-size cannot.
// 2. Creating view may be expensive (e.g. structure computes offsets of its
//    member).
// 3. When view is created structure doesn't compute the size of last member,
//    IOW try to make the most complex member the last one.
// 4. structure allows reading its first member without creating view properly,
//    useful for storing context-related information in the first member.
// 5. compressed_integer touches (but doesn't destroy) up to 7 bytes after its
//    end, make sure it is legal to access these memory locations.
// 6. Where applicable use provide (size, serializer) pair to object
//    serializers so that unnecessary copies can be avoided.

namespace imr {

namespace meta {

template<typename U, size_t N, typename... Ts>
struct do_find { };

template<typename U, size_t N, typename T, typename... Ts>
struct do_find<U, N, T, Ts...> : do_find<U, N + 1, Ts...> { };

template<typename U, size_t N, typename... Ts>
struct do_find<U, N, U, Ts...> : std::integral_constant<size_t, N> { };

// Returns (via member 'value') index of the first occurrence of type Tag in the
// list of types Tags. If Tag is not found in Tags the member 'value' is not
// present.
template<typename T, typename... Ts>
struct find : do_find<T, 0, Ts...> { };

// Returns (via member 'type') the first type in the provided list of types.
// If the list of types is empty the member 'type' does not exist.
template<typename... Elements>
struct head { };

template<typename Element, typename... Elements>
struct head<Element, Elements...> {
    using type = Element;
};

}

namespace internal {

template<typename T, typename CharT>
GCC6_CONCEPT(requires std::is_pod<T>::value && sizeof(CharT) == 1)
inline T read_pod(const CharT* in) noexcept {
    T obj;
    std::copy_n(in, sizeof(T), reinterpret_cast<CharT*>(&obj));
    return obj;
}

template<typename T, typename CharT>
GCC6_CONCEPT(requires std::is_pod<T>::value && sizeof(CharT) == 1)
inline void write_pod(T obj, CharT* out) noexcept {
    std::copy_n(reinterpret_cast<const CharT*>(&obj), sizeof(T), out);
}

}

enum class const_view { no, yes, };

static struct { } no_context;

template<typename Tag>
class set_flag {
    bool _value = true;
public:
    set_flag() = default;
    explicit set_flag(bool v) noexcept : _value(v) { }
    bool value() const noexcept { return _value; }
};

// Represents a fixed-size set of tagged flags.
template<typename... Tags>
class flags {
    static constexpr auto object_size = align_up<size_t>(sizeof...(Tags), 8) / 8;
private:
    template<typename Tag>
    static void do_set(uint8_t* ptr, bool set) noexcept {
        const auto idx = meta::find<Tag, Tags...>::value;
        const auto byte_idx = idx / 8;
        const auto bit_idx = idx % 8;

        auto value = ptr[byte_idx];
        value &= ~uint8_t(1 << bit_idx);
        value |= uint8_t(set) << bit_idx;
        ptr[byte_idx] = value;
    }

    template<typename Tag>
    static bool do_get(const uint8_t* ptr) noexcept {
        const auto idx = meta::find<Tag, Tags...>::value;
        const auto byte_idx = idx / 8;
        const auto bit_idx = idx % 8;

        return ptr[byte_idx] & (1 << bit_idx);
    }
public:
    template<const_view is_const>
    class basic_view {
        using pointer_type = std::conditional_t<is_const == const_view::yes,
                                                const uint8_t*, uint8_t*>;
        pointer_type _ptr;
    public:
        explicit basic_view(pointer_type ptr) noexcept : _ptr(ptr) { }

        operator basic_view<const_view::yes>() const noexcept {
            return basic_view<const_view::yes>(_ptr);
        }

        template<typename Tag>
        bool get() const noexcept {
            return do_get<Tag>(_ptr);
        }

        template<typename Tag>
        void set(bool value = true) noexcept {
            do_set<Tag>(_ptr, value);
        }
    };

    using view = basic_view<const_view::yes>;
    using mutable_view = basic_view<const_view::no>;

public:
    template<typename Context = decltype(no_context)>
    static view make_view(const uint8_t* in, const Context& = no_context) noexcept {
        return view(in);
    }
    template<typename Context = decltype(no_context)>
    static mutable_view make_view(uint8_t* in, const Context& = no_context) noexcept {
        return mutable_view(in);
    }

public:
    template<typename Context = decltype(no_context)>
    static size_t serialized_object_size(const uint8_t*, const Context& = no_context) noexcept {
        return object_size;
    }

    template<typename... Tags1>
    static size_t size_when_serialized(set_flag<Tags1>...) noexcept {
        return object_size;
    }

    template<typename... Tags1>
    static size_t serialize(uint8_t* out, set_flag<Tags1>... sfs) noexcept {
        std::fill_n(out, object_size, 0);
        auto ignore_me = { 0, (do_set<Tags1>(out, sfs.value()), 0)... };
        (void)ignore_me;
        return object_size;
    }
};

// Represents a fixed-size POD value.
template<typename Type>
GCC6_CONCEPT(requires std::is_pod<Type>::value)
struct fixed_size_value {
    using underlying = Type;
    enum : size_t {
        size = sizeof(Type),
    };

    template<const_view is_const>
    class basic_view {
        using pointer_type = std::conditional_t<is_const == const_view::yes,
                const uint8_t*, uint8_t*>;
        pointer_type _ptr;
    public:
        explicit basic_view(pointer_type ptr) noexcept : _ptr(ptr) { }

        operator basic_view<const_view::yes>() const noexcept {
            return basic_view<const_view::yes>(_ptr);
        }

        Type load() const noexcept {
            return internal::read_pod<Type>(_ptr);
        }

        void store(const Type& object) noexcept {
            internal::write_pod(object, _ptr);
        }
    };

    using view = basic_view<const_view::yes>;
    using mutable_view = basic_view<const_view::no>;

public:
    template<typename Context = decltype(no_context)>
    static view make_view(const uint8_t* in, const Context& = no_context) noexcept {
        return view(in);
    }
    template<typename Context = decltype(no_context)>
    static mutable_view make_view(uint8_t* in, const Context& = no_context) noexcept {
        return mutable_view(in);
    }

public:
    template<typename Context = decltype(no_context)>
    static size_t serialized_object_size(const uint8_t*, const Context& = no_context) noexcept {
        return sizeof(Type);
    }

    static size_t size_when_serialized(const Type&) noexcept {
        return sizeof(Type);
    }

    static size_t serialize(uint8_t* out, const Type& value) noexcept {
        internal::write_pod(value, out);
        return sizeof(Type);
    }
};

template<typename Type>
using pointer = fixed_size_value<Type*>;

// Represents a fixed-size buffer. The size of the buffer is not stored and
// must be provided by external context.
// Fixed-buffer can be created from either a bytes_view or a (size, serializer)
// pair.
template<typename Tag>
struct fixed_buffer {
    template<typename Context>
    GCC6_CONCEPT(requires requires(const Context& ctx) {
        { ctx.template size_of<Tag>() } noexcept -> size_t;
    })
    static bytes_view make_view(const uint8_t* in, const Context& context) noexcept {
        auto ptr = reinterpret_cast<bytes_view::pointer>(in);
        return bytes_view(ptr, context.template size_of<Tag>());
    }

    template<typename Context>
    GCC6_CONCEPT(requires requires(const Context& ctx) {
        { ctx.template size_of<Tag>() } noexcept -> size_t;
    })
    static bytes_mutable_view make_view(uint8_t* in, const Context& context) noexcept {
        auto ptr = reinterpret_cast<bytes_mutable_view::pointer>(in);
        return bytes_mutable_view(ptr, context.template size_of<Tag>());
    }

public:
    template<typename Context>
    GCC6_CONCEPT(requires requires(const Context& ctx) {
        { ctx.template size_of<Tag>() } noexcept -> size_t;
    })
    static size_t serialized_object_size(const uint8_t*, const Context& context) noexcept {
        return context.template size_of<Tag>();
    }

    static size_t size_when_serialized(bytes_view src) noexcept {
        return src.size();
    }

    template<typename Serializer>
    GCC6_CONCEPT(requires requires (Serializer ser, uint8_t* ptr) {
        { ser(ptr) } noexcept;
    })
    static size_t size_when_serialized(size_t size, Serializer&&) noexcept {
        return size;
    }

    static size_t serialize(uint8_t* out, bytes_view src) {
        std::copy_n(src.begin(), src.size(),
                    reinterpret_cast<bytes_view::value_type*>(out));
        return src.size();
    }

    template<typename Serializer>
    GCC6_CONCEPT(requires requires (Serializer ser, uint8_t* ptr) {
        { ser(ptr) } noexcept;
    })
    static size_t serialize(uint8_t* out, size_t size, Serializer&& serializer) noexcept {
        std::forward<Serializer>(serializer)(out);
        return size;
    }
};

// Represents a compressed unsigned integer.
// Warning: this type can touch up to 7 bytes past the actual object it is
// accessing. Their values won't be altered, but the user needs to make sure
// that such memory accesses are legal.
template<typename T>
class compressed_unsigned_integer {
    // FIXME: assumes little-endian ordering
    static_assert(std::is_unsigned<T>::value);
    static_assert(sizeof(T) <= 8);

    static constexpr size_t size_field_size = log2ceil<size_t>(std::numeric_limits<T>::digits / 8 + 1) + 1;
    static constexpr size_t size_field_mask = (1 << size_field_size) - 1;
public:
    using underlying = T;
    enum : size_t {
        maximum_size
            = align_up<size_t>(std::numeric_limits<T>::digits + size_field_size, 8) / 8,
    };

    class view {
        const uint8_t* _ptr;
    public:
        explicit view(const uint8_t* ptr) noexcept : _ptr(ptr) { }

        T load() noexcept {
            auto value = internal::read_pod<uint64_t>(_ptr);
            auto size = value & size_field_mask;
            if (sizeof(T) >= 8 && size > 8) {
                value >>= size_field_size;
                value |= uint64_t(_ptr[8]) << (64 - size_field_size);
            } else {
                if (sizeof(T) < 8 || size < 8) {
                    value &= ~(std::numeric_limits<uint64_t>::max() << (size * 8));
                }
                value >>= size_field_size;
            }
            return value;
        }
    };
private:
    // precondition: value > 0
    // returns number of bytes required to represent value
    static size_t size_of_positive_value(uint64_t value) noexcept {
        return align_up<size_t>(64 - count_leading_zeros(uint64_t(value)) + size_field_size, 8) / 8;
    }
public:
    template<typename Context = decltype(no_context)>
    static view make_view(const uint8_t* in, const Context& = no_context) noexcept {
        return view(in);
    }

    template<typename Context = decltype(no_context)>
    static size_t serialized_object_size(const uint8_t* in, const Context& = no_context) noexcept {
        return *in & size_field_mask;
    }

    static size_t size_when_serialized(T value) noexcept {
        if (!value) {
            return 1;
        }
        return size_of_positive_value(value);
    }

    static size_t serialize(uint8_t* out, T value) noexcept {
        if (!value) {
            *out = 1;
            return 1;
        }
        auto size = size_of_positive_value(value);
        if (sizeof(T) >= 8 && size > 8) {
            internal::write_pod((value << size_field_size) | size, out);
            out[8] = uint64_t(value) >> (64 - size_field_size);
        } else {
            auto new_value = (uint64_t(value) << size_field_size) | size;
            if (sizeof(T) < 8 || size < 8) {
                auto old_value = internal::read_pod<uint64_t>(out);
                old_value &= std::numeric_limits<uint64_t>::max() << (size * 8);
                new_value = old_value | new_value;
            }
            internal::write_pod(new_value, out);
        }
        return size;
    };
};

template<typename T>
struct compressed_signed_integer {
    // TODO: proper implementation
    static_assert(std::is_signed<T>::value);
    static_assert(sizeof(T) <= 8);
private:
    using unsigned_type = std::make_unsigned_t<T>;
public:
    using underlying = T;
    enum : size_t {
        maximum_size = compressed_unsigned_integer<unsigned_type>::maximum_size,
    };

    class view {
        const uint8_t* _ptr;
    public:
        explicit view(const uint8_t* ptr) noexcept : _ptr(ptr) { }

        T load() noexcept {
            using uview = typename compressed_unsigned_integer<unsigned_type>::view;
            return uview(_ptr).load();
        }
    };
public:
    template<typename Context = decltype(no_context)>
    static view make_view(const uint8_t* in, const Context& = no_context) noexcept {
        return view(in);
    }

    template<typename Context = decltype(no_context)>
    static size_t serialized_object_size(const uint8_t* in, const Context& = no_context) noexcept {
        return compressed_unsigned_integer<unsigned_type>::serialized_object_size(in);
    }

    static size_t size_when_serialized(T value) noexcept {
        return compressed_unsigned_integer<unsigned_type>::size_when_serialized(value);
    }

    static size_t serialize(uint8_t* out, T value) noexcept {
        return compressed_unsigned_integer<unsigned_type>::serialize(out, value);
    };
};

template<typename T>
GCC6_CONCEPT(requires std::is_integral<T>::value)
using compressed_integer = std::conditional_t<std::is_unsigned<T>::value,
                                              compressed_unsigned_integer<T>,
                                              compressed_signed_integer<T>>;

// Represents a value that may be not present. Information whether or not
// the optional is engaged is not stored and must be provided by external
// context.
template<typename Tag, typename Type>
struct optional {
    using underlying = Type;

public:
    template<typename Context = decltype(no_context)>
    static auto make_view(const uint8_t* in, const Context& ctx = no_context) noexcept {
        return Type::make_view(in, ctx);
    }
    template<typename Context = decltype(no_context)>
    static auto make_view(uint8_t* in, const Context& ctx = no_context) noexcept {
        return Type::make_view(in, ctx);
    }

public:
    template<typename Context>
    GCC6_CONCEPT(requires requires(const Context& ctx) {
        { ctx.template is_present<Tag>() } noexcept -> bool;
    })
    static size_t serialized_object_size(const uint8_t* in, const Context& context) noexcept {
        return context.template is_present<Tag>()
               ? Type::serialized_object_size(in, context)
               : 0;
    }

    template<typename... Args>
    static size_t size_when_serialized(Args&&... args) noexcept {
        return Type::size_when_serialized(std::forward<Args>(args)...);
    }

    template<typename... Args>
    static size_t serialize(uint8_t* out, Args&&... args) noexcept {
        return Type::serialize(out, std::forward<Args>(args)...);
    }
};

template<typename Tag, typename Type>
struct member {
    using tag = Tag;
    using type = Type;
};

template<typename Tag, typename Type>
using optional_member = member<Tag, optional<Tag, Type>>;

template<typename Tag, size_t N, typename... Members>
struct structure_do_get_member { };

template<typename Tag, typename Member, size_t N, typename... Members>
struct structure_do_get_member<Tag, N, Member, Members...>
        : structure_do_get_member<Tag, N + 1, Members...> { };

template<typename Tag, typename Type, size_t N, typename... Members>
struct structure_do_get_member<Tag, N, member<Tag, Type>, Members...>
        : std::integral_constant<size_t, N> {
    using type = Type;
};

template<typename Tag, typename... Members>
struct structure_get_member : structure_do_get_member<Tag, 0, Members...> { };

template<typename... Members>
class structure_sizer {
    size_t _size;
public:
    explicit structure_sizer(size_t size) noexcept : _size(size) { }

    size_t done() noexcept { return _size; }
};

template<typename Member, typename... Members>
class structure_sizer<Member, Members...> {
    size_t _size;
public:
    explicit structure_sizer(size_t size) noexcept : _size(size) { }

    template<typename... Args>
    structure_sizer<Members...> serialize(Args&&... args) noexcept {
        auto size = Member::type::size_when_serialized(std::forward<Args>(args)...);
        return structure_sizer<Members...>(size + _size);
    }
};

template<typename Tag, typename Type, typename... Members>
class structure_sizer<optional_member<Tag, Type>, Members...> {
    size_t _size;
public:
    explicit structure_sizer(size_t size) noexcept : _size(size) { }

    template<typename... Args>
    structure_sizer<Members...> serialize(Args&&... args) noexcept {
        auto size = Type::size_when_serialized(std::forward<Args>(args)...);
        return structure_sizer<Members...>(size + _size);
    }

    structure_sizer<Members...> skip() noexcept {
        return structure_sizer<Members...>(_size);
    }
};

template<typename... Members>
struct structure_serializer {
    uint8_t* _out;
public:
    explicit structure_serializer(uint8_t* out) noexcept : _out(out) { }
    uint8_t* done() noexcept { return _out; }
};

template<typename Member, typename... Members>
class structure_serializer<Member, Members...> {
    uint8_t* _out;
public:
    explicit structure_serializer(uint8_t* out) noexcept : _out(out) { }

    template<typename... Args>
    structure_serializer<Members...> serialize(Args&&... args) noexcept {
        auto size = Member::type::serialize(_out, std::forward<Args>(args)...);
        return structure_serializer<Members...>(_out + size);
    }
};

template<typename Tag, typename Type, typename... Members>
class structure_serializer<optional_member<Tag, Type>, Members...> {
    uint8_t* _out;
public:
    explicit structure_serializer(uint8_t* out) noexcept : _out(out) { }

    template<typename... Args>
    structure_serializer<Members...> serialize(Args&&... args) noexcept {
        auto size = Type::serialize(_out, std::forward<Args>(args)...);
        return structure_serializer<Members...>(_out + size);
    }

    structure_serializer<Members...> skip() noexcept {
        return structure_serializer<Members...>(_out);
    }
};

// Represents a compound type.
template<typename... Members>
struct structure {
    template<const_view is_const>
    class basic_view {
        using pointer_type = std::conditional_t<is_const == const_view::yes,
                                                const uint8_t*, uint8_t*>;
        pointer_type _ptr;
        std::array<uint32_t, sizeof...(Members)> _offsets;
    private:
        basic_view(pointer_type ptr, const std::array<uint32_t, sizeof...(Members)>& offsets) noexcept
            : _ptr(ptr), _offsets(offsets) { }
        friend class basic_view<const_view::no>;
    public:
        template<typename Context>
        explicit basic_view(pointer_type ptr, const Context& context) noexcept : _ptr(ptr) {
            size_t idx = 0;
            _offsets[0] = 0;
            auto visit_member = [&] (auto ptr) noexcept {
                // FIXME: This won't prevent us from expecting context to be able
                // to provide information about the last element.
                if (idx + 1 >= sizeof...(Members)) {
                    return;
                }
                using member_type = typename std::remove_pointer_t<decltype(ptr)>::type;
                auto total_size = _offsets[idx];
                auto this_size = member_type::serialized_object_size(_ptr + total_size, context);
                total_size += this_size;
                _offsets[++idx] = total_size;
            };
            auto ignore_me = { (visit_member((Members*)0), 0)... };
            (void)ignore_me;
        }

        operator basic_view<const_view::yes>() const noexcept {
            return basic_view<const_view::yes>(_ptr, _offsets);
        }

        template<typename Tag>
        auto offset_of() const noexcept {
            using member = structure_get_member<Tag, Members...>;
            return _offsets[member::value];
        }

        template<typename Tag, typename Context = decltype(no_context)>
        auto get(const Context& context = no_context) const noexcept {
            using member = structure_get_member<Tag, Members...>;
            return member::type::make_view(_ptr + _offsets[member::value], context);
        }
    };

    using view = basic_view<const_view::yes>;
    using mutable_view = basic_view<const_view::no>;
public:
    template<typename Context = decltype(no_context)>
    static view make_view(const uint8_t* in, const Context& context = no_context) noexcept {
        return view(in, context);
    }
    template<typename Context = decltype(no_context)>
    static mutable_view make_view(uint8_t* in, const Context& context = no_context) noexcept {
        return mutable_view(in, context);
    }

public:
    template<typename Context = decltype(no_context)>
    static size_t serialized_object_size(const uint8_t* in, const Context& context = no_context) noexcept {
        size_t total_size = 0;
        auto visit_member = [&] (auto ptr) noexcept {
            using member_type = typename std::remove_pointer_t<decltype(ptr)>::type;
            auto this_size = member_type::serialized_object_size(in + total_size, context);
            total_size += this_size;
        };
        auto ignore_me = { (visit_member((Members*)0), 0)... };
        (void)ignore_me;
        return total_size;
    }

    template<typename Writer>
    GCC6_CONCEPT(requires requires(Writer wr, structure_sizer<Members...> ser) {
        { wr(ser) } noexcept -> size_t;
    })
    static size_t size_when_serialized(Writer&& writer) noexcept {
        return std::forward<Writer>(writer)(structure_sizer<Members...>(0));
    }

    template<typename Writer>
    GCC6_CONCEPT(requires requires(Writer wr, structure_serializer<Members...> ser) {
        { wr(ser) } noexcept -> uint8_t*;
    })
    static size_t serialize(uint8_t* out, Writer&& writer) noexcept {
        auto ptr = std::forward<Writer>(writer)(structure_serializer<Members...>(out));
        return ptr - out;
    }

    template<typename Context = decltype(no_context)>
    static auto get_first_member(uint8_t* in, const Context& ctx = no_context) noexcept {
        return meta::head<Members...>::type::type::make_view(in, ctx);
    }
    template<typename Context = decltype(no_context)>
    static auto get_first_member(const uint8_t* in, const Context& ctx = no_context) noexcept {
        return meta::head<Members...>::type::type::make_view(in, ctx);
    }
};

}
