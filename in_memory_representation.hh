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

// TODO: using variable templates could significantly decrease the number of
// created types making these TMP algorithms faster.

namespace internal {

template<bool... Vs>
constexpr ssize_t do_find() {
    ssize_t i = -1;
    ssize_t j = 0;
    auto ignore_me = { ssize_t(0), ((Vs && i == -1) ? i = j : j++)... };
    (void)ignore_me;
    return i;
}

template<ssize_t N>
struct negative_to_empty : std::integral_constant<size_t, N> { };

template<>
struct negative_to_empty<-1> { };

template<typename T>
struct is_same_as {
    template<typename U>
    using type = std::is_same<T, U>;
};

}

// Returns (via member 'value') index of the first type in the list of types
// list of types Ts for which Predicate<T::value is true. If no such type is
// found the member 'value' is not present.
template<template <class> typename Predicate, typename... Ts>
using find_if = internal::negative_to_empty<internal::do_find<Predicate<Ts>::value...>()>;

// Returns (via member 'value') index of the first occurrence of type T in the
// list of types Ts. If T is not found in Ts the member 'value' is not present.
template<typename T, typename... Ts>
using find = find_if<internal::is_same_as<T>::template type, Ts...>;

template<size_t N, typename... Ts>
struct get { };

template<size_t N, typename T, typename... Ts>
struct get<N, T, Ts...> : get<N - 1, Ts...> { };

template<typename T, typename... Ts>
struct get<0, T, Ts...> {
    using type = T;
};

// Returns (via member 'type') the first type in the provided list of types.
// If the list of types is empty the member 'type' does not exist.
template<typename... Ts>
using head = get<0, Ts...>;

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
    using view = bytes_view;
    using mutable_view = bytes_mutable_view;

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

namespace meta {

// Perhaps not needed and the compiler is clever enough?
template<size_t Offset, size_t N, template<size_t> typename Function>
struct do_generate_branch_tree {
    template<typename... Args>
    static decltype(auto) run(size_t n, Args&&... args) {
        if (n < Offset + N / 2) {
            return do_generate_branch_tree<Offset, N / 2, Function>::run(n, std::forward<Args>(args)...);
        } else {
            return do_generate_branch_tree<Offset + N / 2, N - N / 2, Function>::run(n, std::forward<Args>(args)...);
        }
    }
};

template<size_t Offset, template<size_t> typename Function>
struct do_generate_branch_tree<Offset, 1, Function> {
    template<typename... Args>
    static decltype(auto) run(size_t, Args&&... args) {
        return Function<Offset>::run(std::forward<Args>(args)...);
    }
};

template<size_t N, template<size_t> typename Function>
struct generate_branch_tree : do_generate_branch_tree<0, N, Function> { };

}

template<size_t N, typename... Types>
struct variant_alternative_visitor {
    template<typename Visitor>
    static decltype(auto) run(Visitor&& visitor) {
        using type = typename meta::get<N, Types...>::type;
        return visitor(static_cast<type*>(nullptr));
    }
};

template<typename Tag, typename... Types> // Members/Alternatives
struct variant {
    class alternative_index {
        size_t _index;
    public:
        constexpr explicit alternative_index(size_t idx) noexcept
            : _index(idx) { }
        constexpr size_t index() const noexcept { return _index; }
    };

    template<typename AlternativeTag>
    constexpr static alternative_index index_for() noexcept {
        using tmember = structure_get_member<AlternativeTag, Types...>;
        return alternative_index(meta::find<member<AlternativeTag, typename tmember::type>, Types...>::value);
    }
private:
    template<size_t N>
    using alternative_visitor = variant_alternative_visitor<N, Types...>;

    template<typename Visitor>
    static decltype(auto) choose_alternative(alternative_index index, Visitor&& visitor) noexcept {
        return meta::generate_branch_tree<sizeof...(Types), alternative_visitor>::run(index.index(), std::forward<Visitor>(visitor));
    }
public:
    template<const_view is_const>
    class basic_view {
        using pointer_type = std::conditional_t<is_const == const_view::yes,
                                                const uint8_t*, uint8_t*>;
        pointer_type _ptr;
        alternative_index _alternative;
    private:
        basic_view(pointer_type ptr, alternative_index idx) noexcept
            : _ptr(ptr)
            , _alternative(idx)
        { }
        friend class basic_view<const_view::no>;
    public:
        template<typename Context>
        explicit basic_view(pointer_type ptr, const Context& context) noexcept
            : _ptr(ptr)
            , _alternative(context.template get_alternative<Tag>())
        { }

        operator basic_view<const_view::yes>() const noexcept {
            return basic_view<const_view::yes>(_ptr, _alternative);
        }

        template<typename AlternativeTag>
        auto as() noexcept {
            using member = structure_get_member<AlternativeTag, Types...>;
            return member::type::make_view(_ptr);
        }

        template<typename Visitor, typename Context>
        decltype(auto) visit(Visitor&& visitor, const Context& context) {
            return choose_alternative(_alternative, [&] (auto object) {
                using type = std::remove_pointer_t<decltype(object)>;
                return visitor(type::type::make_view(_ptr, context));
            });
        }

        template<typename Visitor, typename Context>
        decltype(auto) visit_type(Visitor&& visitor, const Context& context) {
            return choose_alternative(_alternative, [&] (auto object) {
                using type = std::remove_pointer_t<decltype(object)>;
                return visitor(static_cast<typename type::type*>(nullptr));
            });
        }
    };

    using view = basic_view<const_view::yes>;
    using mutable_view = basic_view<const_view::no>;

public:
    template<typename Context>
    GCC6_CONCEPT(requires requires(const Context& ctx) {
        { ctx.template get_alternative<Tag>() } noexcept -> alternative_index;
    })
    static view make_view(const uint8_t* in, const Context& context) noexcept {
        return view(in, context);
    }

    template<typename Context>
    GCC6_CONCEPT(requires requires(const Context& ctx) {
        { ctx.template get_alternative<Tag>() } noexcept -> alternative_index;
    })
    static mutable_view make_view(uint8_t* in, const Context& context) noexcept {
        return mutable_view(in, context);
    }

public:
    template<typename Context>
    GCC6_CONCEPT(requires requires(const Context& ctx) {
        { ctx.template get_alternative<Tag>() } noexcept -> alternative_index;
    })
    static size_t serialized_object_size(const uint8_t* in, const Context& context) noexcept {
        return choose_alternative(context.template get_alternative<Tag>(), [&] (auto object) noexcept {
            using type = std::remove_pointer_t<decltype(object)>;
            return type::type::serialized_object_size(in, context);
        });
    }

    template<typename AlternativeTag, typename... Args>
    static size_t size_when_serialized(Args&&... args) noexcept {
        using member = structure_get_member<AlternativeTag, Types...>;
        return member::type::size_when_serialized(std::forward<Args>(args)...);
    }

    template<typename AlternativeTag, typename... Args>
    static size_t serialize(uint8_t* out, Args&&... args) noexcept {
        using member = structure_get_member<AlternativeTag, Types...>;
        return member::type::serialize(out, std::forward<Args>(args)...);
    }
};

template<typename Tag, typename... Types>
using variant_member = member<Tag, variant<Tag, Types...>>;

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

namespace methods {

struct trivial_destructor {
    static void run(...) noexcept { }
};

template<typename T>
struct destructor : trivial_destructor { };

template<typename T>
using is_trivially_destructible = std::is_base_of<trivial_destructor, destructor<T>>;

namespace internal {

template<bool... Vs>
constexpr bool conjunction() noexcept {
    bool result = true;
    auto ignore_me = { true, (result = Vs && result)... };
    (void)ignore_me;
    return result;
}

}

template<typename T, typename... Members>
struct generate_destructor : trivial_destructor { };

template<typename Structure, typename... Tags, typename... Types>
struct generate_destructor<Structure, member<Tags, Types>...> {
    template<typename Context>
    static void run(const uint8_t* ptr, const Context& context) noexcept {
        auto view = Structure::make_view(ptr);
        auto ignore_me = { 0, (destructor<Types>::run(ptr + view.template offset_of<Tags>(), context), 0)... };
        (void)ignore_me;
    }
};

template<typename Tag, typename Type>
struct generate_destructor<optional<Tag, Type>> {
    template<typename Context>
    static void run(const uint8_t* ptr, const Context& context) noexcept {
        if (context.template is_present<Tag>()) {
            destructor<Type>::run(ptr, context);
        }
    }
};

template<typename Tag, typename... Members>
struct generate_destructor<variant<Tag, Members...>> {
    template<typename Context>
    static void run(const uint8_t* ptr, const Context& context) noexcept {
        auto view = variant<Tag, Members...>::make_view(ptr, context);
        view.visit_type([&] (auto alternative_type) {
            using type = std::remove_pointer_t<decltype(alternative_type)>;
            destructor<type>::run(ptr, context);
        }, context);
    }
};

template<typename T>
struct member_is_trivially_destructible;

template<typename Tag, typename Type>
struct member_is_trivially_destructible<member<Tag, Type>> : is_trivially_destructible<Type> { };

template<template <class> typename Predicate, typename... Ts>
using all_of = std::integral_constant<bool, internal::conjunction<Predicate<Ts>::value...>()>;

template<typename... Members>
struct destructor<structure<Members...>>
    : std::conditional_t<all_of<member_is_trivially_destructible, Members...>::value,
                         trivial_destructor,
                         generate_destructor<structure<Members...>, Members...>>
{ };

template<typename Tag, typename Type>
struct destructor<optional<Tag, Type>>
    : std::conditional_t<is_trivially_destructible<Type>::value,
                         trivial_destructor,
                         generate_destructor<optional<Tag, Type>>>
{ };

template<typename Tag, typename... Members>
struct destructor<variant<Tag, Members...>>
    : std::conditional_t<all_of<member_is_trivially_destructible, Members...>::value,
                         trivial_destructor,
                         generate_destructor<variant<Tag, Members...>>>
{ };


template<typename T, typename Context = decltype(no_context)>
void destroy(const uint8_t* ptr, const Context& context = no_context) {
    destructor<T>::run(ptr, context);
}

}

namespace containers {

// If all containers are able to tell the size of the contained objects,
// perhaps there is no need to have imr_type::size_when_serialized().

template<typename T, size_t MaxElementCount>
class sparse_array {
    // make compatible with imr types so that it can be nested or stored
    // inside other containers or structure

    using offset_type = uint16_t; // choose using MaxElementCount
    enum : size_t { header_size = (MaxElementCount + 1) * sizeof(offset_type) };

    using pointer_type = uint8_t*;
    pointer_type _ptr;
private:
    uint8_t* header_entry_for(size_t idx) const noexcept {
        return _ptr + idx * sizeof(offset_type);
    }

    size_t end_position() const noexcept {
        return internal::read_pod<offset_type>(header_entry_for(MaxElementCount));
    }
public:
    template<typename Context>
    class iterator : public std::iterator<std::forward_iterator_tag, T> {
        const Context& _ctx;
        pointer_type _base;
        pointer_type _position;
    private:
        void skip_absent() noexcept {
            auto header_end = _base + MaxElementCount * sizeof(offset_type);
            while (_position != header_end) {
                auto begin_pos = internal::read_pod<offset_type>(_position);
                auto end_pos = internal::read_pod<offset_type>(_position + sizeof(offset_type));
                if (begin_pos != end_pos) {
                    break;
                }
                _position += sizeof(offset_type);
            }
        }
    public:
        iterator(const Context& ctx, pointer_type base, pointer_type pos) noexcept
            : _ctx(ctx), _base(base), _position(pos) {
            skip_absent();
        }

        std::pair<size_t, typename T::view> operator*() const noexcept {
            auto idx = (_position - _base) / sizeof(offset_type);
            auto ptr = _base + internal::read_pod<offset_type>(_position);
            auto ctx = _ctx.context_for_element(idx); // do not require this member to be present
            return std::make_pair(idx, T::make_view(ptr, ctx));
        }

        iterator& operator++() noexcept {
            _position += sizeof(offset_type);
            skip_absent();
            return *this;
        }
        iterator operator++(int) noexcept {
            auto it = *this;
            operator++();
            return it;
        }

        bool operator==(const iterator& other) noexcept {
            return _position == other._position;
        }
        bool operator!=(const iterator& other) noexcept {
            return !(*this == other);
        }
    };
public:
    explicit sparse_array(pointer_type ptr) noexcept : _ptr(ptr) {
        for (auto i = 0u; i <= MaxElementCount; i++) {
            internal::write_pod(header_size, header_entry_for(i));
        }
    }

    template<typename Context>
    struct range {
        pointer_type _ptr;
        const Context& _ctx;
    public:
        range(pointer_type ptr, const Context& ctx) noexcept : _ptr(ptr), _ctx(ctx) { }
        auto begin() const noexcept {
            return iterator<Context>(_ctx, _ptr, _ptr);
        }
        auto end() const noexcept {
            return iterator<Context>(_ctx, _ptr, _ptr + MaxElementCount * sizeof(offset_type));
        }

        stdx::optional<typename T::view> operator[](size_t idx) noexcept {
            auto entry = _ptr + idx * sizeof(offset_type);
            auto begin_pos = internal::read_pod<offset_type>(entry);
            auto end_pos = internal::read_pod<offset_type>(entry + sizeof(uint16_t));
            if (begin_pos == end_pos) {
                return stdx::nullopt;
            }
            return T::make_view(_ptr + begin_pos, _ctx.context_for_element(idx));
        }
    };

    template<typename Context = decltype(no_context)>
    auto elements_range(const Context& ctx = no_context) {
        return range<Context>(_ptr, ctx);
    }

    template<typename... Args>
    void insert(size_t idx, size_t size, Args&&... args) noexcept {
        auto begin_pos = internal::read_pod<offset_type>(header_entry_for(idx));
        auto end_pos = internal::read_pod<offset_type>(header_entry_for(idx + 1));
        auto space = size_t(end_pos - begin_pos);
        if (size != space) {
            // assume there is room, just a matter for moving things around
            auto array_end = _ptr + end_position();
            if (size > space) {
                auto delta = size - space;
                std::copy_backward(_ptr + end_pos, array_end, array_end + delta);
                for (auto i = idx + 1; i <= MaxElementCount; i++) {
                    // vectorise with SSE?
                    auto entry_pos = header_entry_for(i);
                    auto off = internal::read_pod<offset_type>(entry_pos);
                    off += delta;
                    internal::write_pod(off, entry_pos);
                }
            } else {
                auto delta = space - size;
                std::copy(_ptr + end_pos, array_end, _ptr + end_pos - delta);
                for (auto i = idx + 1; i <= MaxElementCount; i++) {
                    // vectorise with SSE?
                    auto entry_pos = header_entry_for(i);
                    auto off = internal::read_pod<offset_type>(entry_pos);
                    off -= delta;
                    internal::write_pod(off, entry_pos);
                }
            }
        }
        T::serialize(_ptr + begin_pos, std::forward<Args>(args)...);
    }

    void erase(size_t idx) noexcept {
        auto begin_pos = internal::read_pod<offset_type>(header_entry_for(idx));
        auto end_pos = internal::read_pod<offset_type>(header_entry_for(idx + 1));
        auto size = end_pos - begin_pos;
        if (!size) {
            return;
        }
        auto array_end = _ptr + end_position();
        std::copy(_ptr + end_pos, array_end, _ptr + begin_pos);
        for (auto i = idx + 1; i <= MaxElementCount; i++) {
            // duplicated with insert's element shrinking
            // vectorise with SSE?
            auto entry_pos = header_entry_for(i);
            auto off = internal::read_pod<offset_type>(entry_pos);
            off -= size;
            internal::write_pod(off, entry_pos);
        }
    }
};


}

}
