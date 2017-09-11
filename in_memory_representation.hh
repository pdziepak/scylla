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
#include <bitset>

#include <core/align.hh>
#include <core/bitops.hh>
#include <util/gcc6-concepts.hh>

#include <boost/range/numeric.hpp>

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

template<typename... Ts>
struct list { };

template<size_t N, typename Result, typename... Us>
struct do_take { };

template<typename... Ts>
struct do_take<0, list<Ts...>> {
    using type = list<Ts...>;
};

template<typename... Ts, typename U, typename... Us>
struct do_take<0, list<Ts...>, U, Us...> {
    using type = list<Ts...>;
};

template<size_t N, typename... Ts, typename U, typename... Us>
struct do_take<N, list<Ts...>, U, Us...> {
    using type = typename do_take<N - 1, list<Ts..., U>, Us...>::type;
};

template<size_t N, typename... Ts>
using take = typename do_take<N, list<>, Ts...>::type;

template<typename... Ts, typename Function>
void for_each(list<Ts...>, Function&& fn) {
    auto ignore_me = { 0, (fn(static_cast<Ts*>(nullptr)), 0)... };
    (void)ignore_me;
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

static struct no_context_t {
    template<typename Tag>
    auto context_for(...) const noexcept { return *this; }
} no_context;

template<typename T>
class placeholder {
    uint8_t* _pointer = nullptr;
public:
    placeholder() = default;
    explicit placeholder(uint8_t* ptr) noexcept : _pointer(ptr) { }

    void set_pointer(uint8_t* ptr) noexcept { _pointer = ptr; }

    template<typename... Args>
    void serialize(Args&&... args) noexcept {
        if (!_pointer) {
            // Please, Mx Compiler, be able to optimise this 'if' away.
            return;
        }
        T::serialize(_pointer, std::forward<Args>(args)...);
    }
};

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

    static size_t size_when_serialized(placeholder<fixed_size_value<Type>>&) noexcept {
        return sizeof(Type);
    }

    static size_t serialize(uint8_t* out, const Type& value) noexcept {
        internal::write_pod(value, out);
        return sizeof(Type);
    }

    static size_t serialize(uint8_t* out, placeholder<fixed_size_value<Type>>& phldr) noexcept {
        phldr.set_pointer(out);
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

    template<typename... Args>
    static size_t size_when_serialized(T value, Args&&...) noexcept {
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

    // TODO: context_for for nested type
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
    public:
        explicit basic_view(pointer_type ptr) noexcept
            : _ptr(ptr)
        { }

        pointer_type raw_pointer() const noexcept { return _ptr; }

        operator basic_view<const_view::yes>() const noexcept {
            return basic_view<const_view::yes>(_ptr);
        }

        template<typename AlternativeTag, typename Context = decltype(no_context)>
        auto as(const Context& context = no_context) noexcept {
            using member = structure_get_member<AlternativeTag, Types...>;
            return member::type::make_view(_ptr, context.template context_for<AlternativeTag>(_ptr));
        }

        template<typename Visitor, typename Context>
        decltype(auto) visit(Visitor&& visitor, const Context& context) {
            auto alt_idx = context.template active_alternative_of<Tag>();
            return choose_alternative(alt_idx, [&] (auto object) {
                using type = std::remove_pointer_t<decltype(object)>;
                return visitor(type::type::make_view(_ptr, context.template context_for<typename type::tag>(_ptr)));
            });
        }

        template<typename Visitor, typename Context>
        decltype(auto) visit_type(Visitor&& visitor, const Context& context) {
            auto alt_idx = context.template active_alternative_of<Tag>();
            return choose_alternative(alt_idx, [&] (auto object) {
                using type = std::remove_pointer_t<decltype(object)>;
                return visitor(static_cast<type*>(nullptr));
            });
        }
    };

    using view = basic_view<const_view::yes>;
    using mutable_view = basic_view<const_view::no>;

public:
    template<typename Context>
    static view make_view(const uint8_t* in, const Context& context) noexcept {
        return view(in);
    }

    template<typename Context>
    static mutable_view make_view(uint8_t* in, const Context& context) noexcept {
        return mutable_view(in);
    }

public:
    template<typename Context>
    GCC6_CONCEPT(requires requires(const Context& ctx) {
        { ctx.template active_alternative_of<Tag>() } noexcept -> alternative_index;
    })
    static size_t serialized_object_size(const uint8_t* in, const Context& context) noexcept {
        return choose_alternative(context.template active_alternative_of<Tag>(), [&] (auto object) noexcept {
            using alternative = std::remove_pointer_t<decltype(object)>;
            return alternative::type::serialized_object_size(in, context.template context_for<typename alternative::tag>(in));
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

struct noop_done_hook {
    template<typename T>
    static T done(T value) noexcept {
        return value;
    }
};

template<typename Hook, typename... Members>
class structure_sizer : Hook {
    size_t _size;
public:
    explicit structure_sizer(size_t size, Hook&& hook) noexcept
        : Hook(std::move(hook)), _size(size) { }

    uint8_t* position() const noexcept { return nullptr; }

    auto done() noexcept { return Hook::done(_size); }
};

template<typename Hook, typename Member, typename... Members>
class structure_sizer<Hook, Member, Members...> : Hook {
    class nested_hook : Hook {
        size_t _size;
    public:
        explicit nested_hook(size_t size, Hook&& hook) noexcept
            : Hook(std::move(hook)), _size(size) { }

        structure_sizer<Hook, Members...> done(size_t size) noexcept {
            return structure_sizer<Hook, Members...>(size + _size, std::move(*static_cast<Hook*>(this)));
        }
    };
private:
    size_t _size;
public:
    explicit structure_sizer(size_t size, Hook&& hook) noexcept
        : Hook(std::move(hook)), _size(size) { }

    uint8_t* position() const noexcept { return nullptr; }

    template<typename... Args>
    structure_sizer<Hook, Members...> serialize(Args&&... args) noexcept {
        auto size = Member::type::size_when_serialized(std::forward<Args>(args)...);
        return structure_sizer<Hook, Members...>(size + _size, std::move(*static_cast<Hook*>(this)));
    }

    template<typename... Args>
    auto serialize_nested(Args&&... args) noexcept {
        return Member::type::get_sizer(nested_hook(_size, std::move(*static_cast<Hook*>(this))), std::forward<Args>(args)...);
    }
};

template<typename Hook, typename Tag, typename Type, typename... Members>
class structure_sizer<Hook, optional_member<Tag, Type>, Members...> : Hook {
    size_t _size;
public:
    explicit structure_sizer(size_t size, Hook&& hook) noexcept
        : Hook(std::move(hook)), _size(size) { }

    uint8_t* position() const noexcept { return nullptr; }

    template<typename... Args>
    structure_sizer<Hook, Members...> serialize(Args&&... args) noexcept {
        auto size = Type::size_when_serialized(std::forward<Args>(args)...);
        return structure_sizer<Hook, Members...>(size + _size, std::move(*static_cast<Hook*>(this)));
    }

    structure_sizer<Hook, Members...> skip() noexcept {
        return structure_sizer<Hook, Members...>(_size, std::move(*static_cast<Hook*>(this)));
    }
};

template<typename Hook, typename Tag, typename... Types, typename... Members>
class structure_sizer<Hook, variant_member<Tag, Types...>, Members...> : Hook {
    size_t _size;
public:
    explicit structure_sizer(size_t size, Hook&& hook) noexcept
        : Hook(std::move(hook)), _size(size) { }

    uint8_t* position() const noexcept { return nullptr; }

    template<typename AlternativeTag, typename... Args>
    structure_sizer<Hook, Members...> serialize_as(Args&&... args) noexcept {
        using type = variant<Tag, Types...>;
        auto size = type::template size_when_serialized<AlternativeTag>(std::forward<Args>(args)...);
        return structure_sizer<Hook, Members...>(size + _size, std::move(*static_cast<Hook*>(this)));
    }
};

template<typename Hook, typename... Members>
struct structure_serializer : Hook {
    uint8_t* _out;
public:
    explicit structure_serializer(uint8_t* out, Hook&& hook) noexcept
        : Hook(std::move(hook)), _out(out) { }
    uint8_t* position() const noexcept { return _out; }
    auto done() noexcept { return Hook::done(_out); }
};

template<typename Hook, typename Member, typename... Members>
class structure_serializer<Hook, Member, Members...> : Hook {
    class nested_hook : Hook {
    public:
        explicit nested_hook(Hook&& hook) noexcept
            : Hook(std::move(hook)) { }

        structure_serializer<Hook, Members...> done(uint8_t* out) noexcept {
            return structure_serializer<Hook, Members...>(out, std::move(*static_cast<Hook*>(this)));
        }
    };
private:
    uint8_t* _out;
public:
    explicit structure_serializer(uint8_t* out, Hook&& hook) noexcept
        : Hook(std::move(hook)), _out(out) { }

    uint8_t* position() const noexcept { return _out; }

    template<typename... Args>
    structure_serializer<Hook, Members...> serialize(Args&&... args) noexcept {
        auto size = Member::type::serialize(_out, std::forward<Args>(args)...);
        return structure_serializer<Hook, Members...>(_out + size, std::move(*static_cast<Hook*>(this)));
    }

    template<typename... Args>
    auto serialize_nested(Args&&... args) noexcept {
        return Member::type::get_writer(nested_hook(std::move(*static_cast<Hook*>(this))), _out, std::forward<Args>(args)...);
    }
};

template<typename Hook, typename Tag, typename Type, typename... Members>
class structure_serializer<Hook, optional_member<Tag, Type>, Members...> : Hook {
    uint8_t* _out;
public:
    explicit structure_serializer(uint8_t* out, Hook&& hook) noexcept
        : Hook(std::move(hook)), _out(out) { }

    uint8_t* position() const noexcept { return _out; }

    template<typename... Args>
    structure_serializer<Hook, Members...> serialize(Args&&... args) noexcept {
        auto size = Type::serialize(_out, std::forward<Args>(args)...);
        return structure_serializer<Hook, Members...>(_out + size, std::move(*static_cast<Hook*>(this)));
    }

    structure_serializer<Hook, Members...> skip() noexcept {
        return structure_serializer<Hook, Members...>(_out, std::move(*static_cast<Hook*>(this)));
    }
};

template<typename Hook, typename Tag, typename... Types, typename... Members>
class structure_serializer<Hook, variant_member<Tag, Types...>, Members...> : Hook {
    uint8_t* _out;
public:
    explicit structure_serializer(uint8_t* out, Hook&& hook) noexcept
        : Hook(std::move(hook)), _out(out) { }

    uint8_t* position() const noexcept { return _out; }

    template<typename AlternativeTag, typename... Args>
    structure_serializer<Hook, Members...> serialize_as(Args&&... args) noexcept {
        using type = variant<Tag, Types...>;
        auto size = type::template serialize<AlternativeTag>(_out, std::forward<Args>(args)...);
        return structure_serializer<Hook, Members...>(_out + size, std::move(*static_cast<Hook*>(this)));
    }
};

// Represents a compound type.
// TODO: if you are reading this in a distant future and reflection TS is now
// part of the C++ standard consider using it for generating imr structures.
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
                using member = std::remove_pointer_t<decltype(ptr)>;
                using mtype = typename member::type;
                auto total_size = _offsets[idx];
                auto offset = _ptr + total_size;
                auto this_size = mtype::serialized_object_size(offset, context.template context_for<typename member::tag>(offset));
                total_size += this_size;
                _offsets[++idx] = total_size;
            };
            meta::for_each(meta::take<sizeof...(Members) - 1, Members...>(), visit_member);
        }

        pointer_type raw_pointer() const noexcept { return _ptr; }

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
            auto offset = _ptr + _offsets[member::value];
            return member::type::make_view(offset, context.template context_for<Tag>(offset));
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
            using member = std::remove_pointer_t<decltype(ptr)>;
            auto offset = in + total_size;
            auto this_size = member::type::serialized_object_size(offset, context.template context_for<typename member::tag>(offset));
            total_size += this_size;
        };
        auto ignore_me = { (visit_member((Members*)0), 0)... };
        (void)ignore_me;
        return total_size;
    }

    template<typename Writer, typename... Args>
    //GCC6_CONCEPT(requires requires(Writer wr, structure_sizer<Members...> ser, Args... args) {
    //    { wr(ser, args...) } -> size_t;
    //})
    static size_t size_when_serialized(Writer&& writer, Args&&... args) noexcept {
        return std::forward<Writer>(writer)(structure_sizer<noop_done_hook, Members...>(0, noop_done_hook()), std::forward<Args>(args)...);
    }

    template<typename Hook>
    static auto size_when_serialized_nested(Hook&& hook) noexcept {
        return structure_sizer<Hook, Members...>(0, std::forward<Hook>(hook));
    }

    template<typename Writer, typename... Args>
    //GCC6_CONCEPT(requires requires(Writer wr, structure_serializer<Members...> ser, Args... args) {
    //    { wr(ser, args...) } -> uint8_t*;
    //})
    static size_t serialize(uint8_t* out, Writer&& writer, Args&&... args) noexcept {
        auto ptr = std::forward<Writer>(writer)(structure_serializer<noop_done_hook, Members...>(out, noop_done_hook()), std::forward<Args>(args)...);
        return ptr - out;
    }

    template<typename Hook>
    static auto serialize_nested(Hook&& hook, uint8_t* out) noexcept {
        return structure_serializer<Hook, Members...>(out, std::forward<Hook>(hook));
    }

    template<typename Context = decltype(no_context)>
    static auto get_first_member(uint8_t* in, const Context& ctx = no_context) noexcept {
        return meta::head<Members...>::type::type::make_view(in, ctx);
    }
    template<typename Context = decltype(no_context)>
    static auto get_first_member(const uint8_t* in, const Context& ctx = no_context) noexcept {
        return meta::head<Members...>::type::type::make_view(in, ctx);
    }

    // FIXME: use tags instead of index
    template<size_t N, typename Context = decltype(no_context)> // why N and not tag?
    static auto get_member(const uint8_t* in, const Context& context = no_context) noexcept {
        // TODO: dedup with serialized_object_size and basic_view
        size_t total_size = 0;
        meta::for_each(meta::take<N, Members...>(), [&] (auto ptr) {
            using member = std::remove_pointer_t<decltype(ptr)>;
            auto offset = in + total_size;
            auto this_size = member::type::serialized_object_size(offset, context.template context_for<typename member::tag>(offset));
            total_size += this_size;
        });
        return meta::get<N, Members...>::type::type::make_view(in + total_size, context);
    }

    template<size_t N, typename Context = decltype(no_context)> // why N and not tag?
    static auto get_member(uint8_t* in, const Context& context = no_context) noexcept {
        // TODO: dedup with serialized_object_size and basic_view
        size_t total_size = 0;
        meta::for_each(meta::take<N, Members...>(), [&] (auto ptr) {
            using member = std::remove_pointer_t<decltype(ptr)>;
            auto offset = in + total_size;
            auto this_size = member::type::serialized_object_size(offset, context.template context_for<typename member::tag>(offset));
            total_size += this_size;
        });
        return meta::get<N, Members...>::type::type::make_view(in + total_size, context);
    }
};

template<typename Tag, typename T>
struct tagged_type : T { };

template<typename Tag, typename T>
struct placeholder<tagged_type<Tag, T>> : placeholder<T> { };

namespace methods {

template<template<class> typename Method>
struct trivial_method {
    static void run(...) noexcept { }
};

template<typename T>
struct destructor : trivial_method<destructor> { };

using trivial_destructor = trivial_method<destructor>;

template<template<class> typename Method, typename T>
using has_trivial_method = std::is_base_of<trivial_method<Method>, Method<T>>;

template<typename T>
using is_trivially_destructible = has_trivial_method<destructor, T>;

namespace internal {

template<bool... Vs>
constexpr bool conjunction() noexcept {
    bool result = true;
    auto ignore_me = { true, (result = Vs && result)... };
    (void)ignore_me;
    return result;
}

}

template<template<class> typename Method, typename...>
struct generate_method : trivial_method<Method> { };

template<template<class> typename Method, typename Structure, typename... Tags, typename... Types>
struct generate_method<Method, Structure, member<Tags, Types>...> {
    template<typename Context>
    static void run(const uint8_t* ptr, const Context& context) noexcept {
        auto view = Structure::make_view(ptr, context);
        // (potential) FIXME: make sure offset_of() is not computed twice
        auto ignore_me = { 0, (Method<Types>::run(ptr + view.template offset_of<Tags>(), context.template context_for<Tags>(ptr + view.template offset_of<Tags>())), 0)... };
        (void)ignore_me;
    }
};

template<template<class> typename Method, typename Tag, typename Type>
struct generate_method<Method, optional<Tag, Type>> {
    template<typename Context>
    static void run(const uint8_t* ptr, const Context& context) noexcept {
        if (context.template is_present<Tag>()) {
            Method<Type>::run(ptr, context.template context_for<Tag>(ptr));
        }
    }
};

template<template<class> typename Method, typename Tag, typename... Members>
struct generate_method<Method, variant<Tag, Members...>> {
    template<typename Context>
    static void run(const uint8_t* ptr, const Context& context) noexcept {
        auto view = variant<Tag, Members...>::make_view(ptr, context);
        view.visit_type([&] (auto alternative_type) {
            using type = std::remove_pointer_t<decltype(alternative_type)>;
            Method<typename type::type>::run(ptr, context.template context_for<typename type::tag>(ptr));
        }, context);
    }
};

template<template<class> typename Method>
struct member_has_trivial_method {
    template<typename T>
    struct type;
};

template<template<class> typename Method>
template<typename Tag, typename Type>
struct member_has_trivial_method<Method>::type<member<Tag, Type>> : has_trivial_method<Method, Type> { };

template<template <class> typename Predicate, typename... Ts>
using all_of = std::integral_constant<bool, internal::conjunction<Predicate<Ts>::value...>()>;

template<template<class> typename Method, typename T>
struct get_method;

template<template<class> typename Method, typename... Members>
struct get_method<Method, structure<Members...>>
    : std::conditional_t<all_of<member_has_trivial_method<Method>::template type, Members...>::value,
                         trivial_method<Method>,
                         generate_method<Method, structure<Members...>, Members...>>
{ };

template<template<class> typename Method, typename Tag, typename Type>
struct get_method<Method, optional<Tag, Type>>
    : std::conditional_t<has_trivial_method<Method, Type>::value,
                         trivial_method<Method>,
                         generate_method<Method, optional<Tag, Type>>>
{ };

template<template<class> typename Method, typename Tag, typename... Members>
struct get_method<Method, variant<Tag, Members...>>
    : std::conditional_t<all_of<member_has_trivial_method<Method>::template type, Members...>::value,
                         trivial_method<Method>,
                         generate_method<Method, variant<Tag, Members...>>>
{ };

template<template<class> typename Method, typename Tag, typename Type>
struct get_method<Method, tagged_type<Tag,Type>>
    : std::conditional_t<has_trivial_method<Method, Type>::value,
                         trivial_method<Method>,
                         Method<Type>>
{ };

template<typename... Members>
struct destructor<structure<Members...>> : get_method<destructor, structure<Members...>> { };

template<typename Tag, typename Type>
struct destructor<optional<Tag, Type>> : get_method<destructor, optional<Tag, Type>> { };

template<typename Tag, typename... Members>
struct destructor<variant<Tag, Members...>> : get_method<destructor, variant<Tag, Members...>> { };

template<typename T, typename Context = decltype(no_context)>
void destroy(const uint8_t* ptr, const Context& context = no_context) {
    destructor<T>::run(ptr, context);
}

template<typename T>
struct mover : trivial_method<mover> { };

using trivial_mover = trivial_method<mover>;

template<typename T>
using is_trivially_movable = has_trivial_method<mover, T>;


template<typename... Members>
struct mover<structure<Members...>> : get_method<mover, structure<Members...>> { };

template<typename Tag, typename Type>
struct mover<optional<Tag, Type>> : get_method<mover, optional<Tag, Type>> { };

template<typename Tag, typename... Members>
struct mover<variant<Tag, Members...>> : get_method<mover, variant<Tag, Members...>> { };

template<typename T, typename Context = decltype(no_context)>
void move(const uint8_t* ptr, const Context& context = no_context) {
    mover<T>::run(ptr, context);
}

}

namespace containers {

struct sparse_array_serialization_state {
    uint32_t element_count = 0;
};

struct sparse_array_sizer_state {
    sparse_array_serialization_state* _state;
    uint32_t _total_size = 0;

    template<typename Hook, typename T, size_t MaxElementCount>
    friend class sparse_array_sizer;
private:
    explicit sparse_array_sizer_state(sparse_array_serialization_state& st)
        : _state(&st) { }
};

template<typename Hook, typename T, size_t MaxElementCount>
class sparse_array_sizer : Hook {
    sparse_array_sizer_state _state;
public:
    explicit sparse_array_sizer(sparse_array_serialization_state& state, Hook&& hook) noexcept
        : Hook(std::move(hook)), _state(state) { }

    explicit sparse_array_sizer(sparse_array_sizer_state state, Hook&& hook) noexcept
        : Hook(std::move(hook)), _state(std::move(state)) { }

    sparse_array_sizer_state& internal_state() {
        return _state;
    }

    template<typename... Args>
    sparse_array_sizer& emplace(size_t idx, Args&&... args) noexcept {
        _state._state->element_count = idx + 1;
        _state._total_size += T::size_when_serialized(std::forward<Args>(args)...);
        return *this;
    }

    auto done() noexcept {
        auto total_size = _state._total_size + (_state._state->element_count + 1) * sizeof(uint16_t) + sizeof(uint8_t);
        return Hook::done(total_size);
    }
};

// internal state
class sparse_array_writer_state {
    uint8_t* _ptr;
    uint16_t _element_count;
    uint16_t _offset;
    uint16_t _index = 0;

    template<typename Hook, typename T, size_t MaxElementCount>
    friend class sparse_array_writer;
private:
    sparse_array_writer_state(uint8_t* ptr, const sparse_array_serialization_state& state)
        : _ptr(ptr)
        , _element_count(state.element_count)
        , _offset((_element_count + 1) * sizeof(uint16_t) + sizeof(uint8_t))
    { }
};

template<typename Hook, typename T, size_t MaxElementCount>
class sparse_array_writer : Hook {
public:

private:
    sparse_array_writer_state _state;
public:
    sparse_array_writer(uint8_t* ptr, const sparse_array_serialization_state& state, Hook&& hook) noexcept
        : Hook(std::move(hook))
        , _state(ptr, state)
    { }

    sparse_array_writer(sparse_array_writer_state st, Hook&& hook) noexcept
        : Hook(std::move(hook))
        , _state(std::move(st))
    { }

    sparse_array_writer_state& internal_state() {
        return _state;
    }

    template<typename... Args>
    sparse_array_writer& emplace(size_t idx, Args&&... args) noexcept {
        assert(idx < _state._element_count);
        assert(_state._index <= idx);
        while (_state._index <= idx) {
            internal::write_pod(_state._offset, _state._ptr + sizeof(uint8_t) + _state._index++ * sizeof(uint16_t));
        }
        _state._offset += T::serialize(_state._ptr + _state._offset, std::forward<Args>(args)...);
        return *this;
    }

    auto done() noexcept {
        internal::write_pod<uint8_t>(_state._element_count, _state._ptr);
        internal::write_pod(_state._offset, _state._ptr + _state._element_count * sizeof(uint16_t) + sizeof(uint8_t));
        return Hook::done(_state._ptr + _state._offset);
    }
};

// If all containers are able to tell the size of the contained objects,
// perhaps there is no need to have imr_type::size_when_serialized().

template<typename T, size_t MaxElementCount>
class sparse_array {
    static_assert(MaxElementCount < std::numeric_limits<uint8_t>::max(),
                  "MaxElementCount is too large");
    // also, require that the whole sparse_array is smaller than 64 kB.

    using offset_type = uint16_t;

    using pointer_type = uint8_t*;
    pointer_type _ptr;
private:
    uint8_t* header_entry_for(size_t idx) const noexcept {
        return _ptr + idx * sizeof(offset_type) + sizeof(uint8_t);
    }

    size_t end_position() const noexcept {
        return internal::read_pod<offset_type>(header_entry_for(MaxElementCount));
    }
public:
    using serialization_state = sparse_array_serialization_state;

    template<const_view is_const>
    class basic_view {
        using pointer_type = std::conditional_t<is_const == const_view::yes,
                                                const uint8_t*, uint8_t*>;
        pointer_type _ptr;
    public:
        explicit basic_view(pointer_type ptr) noexcept : _ptr(ptr) { }

        template<typename Context>
        struct range {
            pointer_type _ptr;
            uint32_t _element_count;
            const Context& _ctx;
        public:
            class iterator : public std::iterator<std::input_iterator_tag, const std::pair<size_t, typename T::view>> {
                const Context* _ctx;
                pointer_type _base;
                uint32_t _index;
                uint32_t _end;
            private:
                void skip_absent() noexcept {
                    while (_index != _end) {
                        auto begin_pos = internal::read_pod<offset_type>(_base + _index * sizeof(offset_type));
                        auto end_pos = internal::read_pod<offset_type>(_base + (_index + 1) * sizeof(offset_type));
                        if (begin_pos != end_pos) {
                            break;
                        }
                        _index++;
                    }
                }
            public:
                iterator() = default;
                iterator(const Context& ctx, pointer_type base, uint32_t element_count, uint32_t position = 0) noexcept
                        : _ctx(&ctx), _base(base), _index(position), _end(element_count) {
                    skip_absent();
                }

                std::pair<size_t, typename T::view> operator*() const noexcept {
                    auto ptr = _base + internal::read_pod<offset_type>(_base + _index * sizeof(offset_type)) - sizeof(uint8_t);
                    auto ctx = _ctx->context_for_element(_index, ptr); // do not require this member to be present
                    return std::make_pair(_index, T::make_view(ptr, ctx));
                }

                iterator& operator++() noexcept {
                    _index++;
                    skip_absent();
                    return *this;
                }
                iterator operator++(int) noexcept {
                    auto it = *this;
                    operator++();
                    return it;
                }

                bool operator==(const iterator& other) const noexcept {
                    return _index == other._index;
                }
                bool operator!=(const iterator& other) const noexcept {
                    return !(*this == other);
                }
            };
        public:
            range(pointer_type ptr, const Context& ctx) noexcept
                : _ptr(ptr)
                , _element_count(internal::read_pod<uint8_t>(_ptr))
                , _ctx(ctx)
            { }

            using const_iterator = iterator;

            auto begin() const noexcept {
                return iterator(_ctx, _ptr + sizeof(uint8_t), _element_count);
            }
            auto end() const noexcept {
                return iterator(_ctx, _ptr + sizeof(uint8_t), _element_count, _element_count);
            }

            stdx::optional<typename T::view> operator[](size_t idx) noexcept {
                auto entry = _ptr + sizeof(uint8_t) + idx * sizeof(offset_type);
                auto begin_pos = internal::read_pod<offset_type>(entry);
                auto end_pos = internal::read_pod<offset_type>(entry + sizeof(uint8_t));
                if (begin_pos == end_pos) {
                    return stdx::nullopt;
                }
                return T::make_view(_ptr + begin_pos, _ctx.context_for_element(idx, _ptr + begin_pos));
            }
        };

        size_t size() const {
            return internal::read_pod<uint8_t>(_ptr);
        }

        bool empty() const {
            return !size();
        }

        template<typename Context = decltype(no_context)>
        auto elements_range(const Context& ctx = no_context) const {
            return range<Context>(_ptr, ctx);
        }
    };

    using view = basic_view<const_view::yes>;
public:
    static view make_view(const uint8_t* ptr, ...) {
        return view(ptr);
    }

    template<typename Context = decltype(no_context)>
    static size_t serialized_object_size(const uint8_t* ptr, const Context& = no_context) {
        auto element_count = internal::read_pod<uint8_t>(ptr);
        return internal::read_pod<uint16_t>(ptr + element_count * sizeof(uint16_t) + sizeof(uint8_t));
    }

    template<typename Writer>
    static size_t size_when_serialized(serialization_state& state, Writer&& writer) noexcept {
        return std::forward<Writer>(writer)(sparse_array_sizer<noop_done_hook, T, MaxElementCount>(state, noop_done_hook()));
    }

    template<typename Serializer>
    static size_t serialize(uint8_t* out, serialization_state& state, Serializer&& serializer) noexcept {
        return std::forward<Serializer>(serializer)(sparse_array_writer<noop_done_hook, T, MaxElementCount>(out, state, noop_done_hook()));
    }

    template<typename Hook>
    static auto get_sizer(Hook&& hook, serialization_state& state) {
        return sparse_array_sizer<Hook, T, MaxElementCount>(state, std::forward<Hook>(hook));
    }

    template<typename Hook>
    static auto get_writer(Hook&& hook, uint8_t* out, serialization_state& state) {
        return sparse_array_writer<Hook, T, MaxElementCount>(out, state, std::forward<Hook>(hook));
    }
};

}

namespace methods {

template<template<class> typename Method, typename Type, size_t MaxCount>
struct generate_method<Method, containers::sparse_array<Type, MaxCount>> {
    template<typename Context>
    static void run(const uint8_t* ptr, const Context& context) noexcept {
        auto element_count = imr::internal::read_pod<uint8_t>(ptr);
        if (!element_count) {
            return;
        }
        auto current_offset = imr::internal::read_pod<uint16_t>(ptr + sizeof(uint8_t));
        for (auto i = 0u; i < element_count; i++) {
            auto next_offset = imr::internal::read_pod<uint16_t>(ptr + sizeof(uint16_t) * (i + 1) + sizeof(uint8_t));
            if (current_offset != next_offset) {
                auto element_ptr = ptr + current_offset;
                Method<Type>::run(element_ptr, context.template context_for_element(i, element_ptr));
            }
            current_offset = next_offset;
        }
    }
};

template<template<class> typename Method, typename Type, size_t MaxCount>
struct get_method<Method, containers::sparse_array<Type, MaxCount>>
    : std::conditional_t<has_trivial_method<Method, Type>::value,
                         trivial_method<Method>,
                         generate_method<Method, containers::sparse_array<Type, MaxCount>>>
{ };

template<typename Type, size_t MaxCount>
struct destructor<containers::sparse_array<Type, MaxCount>> : get_method<destructor, containers::sparse_array<Type, MaxCount>> { };

template<typename Type, size_t MaxCount>
struct mover<containers::sparse_array<Type, MaxCount>> : get_method<mover, containers::sparse_array<Type, MaxCount>> { };

}

template<typename Serializer, typename NewHook>
struct do_rehook { };

template<template<typename, typename...> typename Writer, typename OldHook, typename... Args, typename NewHook>
struct do_rehook<Writer<OldHook, Args...>, NewHook> {
    using type = Writer<NewHook, Args...>;
};

template<template<typename, typename, size_t> typename Writer, typename OldHook, typename T, size_t N, typename NewHook>
struct do_rehook<Writer<OldHook, T, N>, NewHook> {
    using type = Writer<NewHook, T, N>;
};

template<typename Serializer, typename NewHook>
using rehook = typename do_rehook<Serializer, NewHook>::type;

}
