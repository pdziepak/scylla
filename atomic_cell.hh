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

#include "bytes.hh"
#include "timestamp.hh"
#include "tombstone.hh"
#include "gc_clock.hh"
#include "utils/managed_bytes.hh"
#include "net/byteorder.hh"
#include <cstdint>
#include <iosfwd>
#include <seastar/util/gcc6-concepts.hh>
#include "data/cell.hh"
#include "utils/compare_unsigned.hh"

class abstract_type;
class collection_type_impl;

template<typename T, typename Input>
static inline
void set_field(Input& v, unsigned offset, T val) {
    reinterpret_cast<net::packed<T>*>(v.begin() + offset)->raw = net::hton(val);
}

template<typename T>
static inline
T get_field(const bytes_view& v, unsigned offset) {
    return net::ntoh(*reinterpret_cast<const net::packed<T>*>(v.begin() + offset));
}

class atomic_cell_or_collection;

template<data::const_view is_const>
class basic_atomic_cell_value_view {
public:
    using fragment_view = std::conditional_t<is_const == data::const_view::yes,
                                             bytes_view, bytes_mutable_view>;
private:
    fragment_view _value;
public:
    explicit basic_atomic_cell_value_view(fragment_view value) : _value(value) { }

    class iterator {
        fragment_view _view;
    public:
        using iterator_category	= std::forward_iterator_tag;
        using value_type = fragment_view;
        using pointer = const fragment_view*;
        using reference = const fragment_view&;
        using difference_type = std::ptrdiff_t;

        explicit iterator(fragment_view fv) noexcept
            : _view(fv) { }

        const fragment_view& operator*() const {
            return _view;
        }
        const fragment_view* operator->() const {
            return &_view;
        }
        iterator& operator++() {
            _view = { };
            return *this;
        }
        iterator operator++(int) {
            auto it = *this;
            operator++();
            return it;
        }

        bool operator==(const iterator& other) const {
            return _view.data() == other._view.data();
        }
        bool operator!=(const iterator& other) const {
            return !(*this == other);
        }
    };

    using const_iterator = iterator;

    auto begin() const {
        return iterator(_value);
    }
    auto end() const {
        return iterator(fragment_view());
    }

    bool operator==(const basic_atomic_cell_value_view& other) const noexcept {
        return _value == other._value;
    }
    bool operator==(bytes_view bv) const noexcept {
        return _value == bv;
    }

    size_t size() const noexcept {
        return _value.size();
    }

    bool empty() const noexcept {
        return _value.empty();
    }

    bool is_fragmented() const noexcept {
        return false;
    }

    fragment_view first_fragment() const noexcept {
        return _value;
    }

    bytes linearize() const {
        return bytes(_value.begin(), _value.end());
    }

    template<typename Function>
    decltype(auto) with_linearized(Function&& fn) const {
        return fn(_value);
    }

    friend std::ostream& operator<<(std::ostream& os, const basic_atomic_cell_value_view& vv) {
        return os << vv.first_fragment();
    }

};

using atomic_cell_value_view = basic_atomic_cell_value_view<data::const_view::yes>;
using atomic_cell_value_mutable_view = basic_atomic_cell_value_view<data::const_view::no>;

inline int compare_unsigned(atomic_cell_value_view a, atomic_cell_value_view b)
{
    assert(!a.is_fragmented() && !b.is_fragmented());
    return compare_unsigned(a.first_fragment(), b.first_fragment());
}

/*
 * Represents atomic cell layout. Works on serialized form.
 *
 * Layout:
 *
 *  <live>  := <int8_t:flags><int64_t:timestamp>(<int32_t:expiry><int32_t:ttl>)?<value>
 *  <dead>  := <int8_t:    0><int64_t:timestamp><int32_t:deletion_time>
 */
class atomic_cell_type final {
private:
    static constexpr int8_t LIVE_FLAG = 0x01;
    static constexpr int8_t EXPIRY_FLAG = 0x02; // When present, expiry field is present. Set only for live cells
    static constexpr int8_t COUNTER_UPDATE_FLAG = 0x08; // Cell is a counter update.
    static constexpr unsigned flags_size = 1;
    static constexpr unsigned timestamp_offset = flags_size;
    static constexpr unsigned timestamp_size = 8;
    static constexpr unsigned expiry_offset = timestamp_offset + timestamp_size;
    static constexpr unsigned expiry_size = 4;
    static constexpr unsigned deletion_time_offset = timestamp_offset + timestamp_size;
    static constexpr unsigned deletion_time_size = 4;
    static constexpr unsigned ttl_offset = expiry_offset + expiry_size;
    static constexpr unsigned ttl_size = 4;
    friend class counter_cell_builder;
private:
    static bool is_counter_update(bytes_view cell) {
        return cell[0] & COUNTER_UPDATE_FLAG;
    }
    static bool is_live(const bytes_view& cell) {
        return cell[0] & LIVE_FLAG;
    }
    static bool is_live_and_has_ttl(const bytes_view& cell) {
        return cell[0] & EXPIRY_FLAG;
    }
    static bool is_dead(const bytes_view& cell) {
        return !is_live(cell);
    }
    // Can be called on live and dead cells
    static api::timestamp_type timestamp(const bytes_view& cell) {
        return get_field<api::timestamp_type>(cell, timestamp_offset);
    }
    template<typename BytesContainer>
    static void set_timestamp(BytesContainer& cell, api::timestamp_type ts) {
        set_field(cell, timestamp_offset, ts);
    }
    // Can be called on live cells only
private:
    template<typename BytesView>
    static BytesView do_get_value(BytesView cell) {
        auto expiry_field_size = bool(cell[0] & EXPIRY_FLAG) * (expiry_size + ttl_size);
        auto value_offset = flags_size + timestamp_size + expiry_field_size;
        cell.remove_prefix(value_offset);
        return cell;
    }
public:
    static atomic_cell_value_view value(bytes_view cell) {
        return atomic_cell_value_view(do_get_value(cell));
    }
    static atomic_cell_value_mutable_view value(bytes_mutable_view cell) {
        return atomic_cell_value_mutable_view(do_get_value(cell));
    }
    // Can be called on live counter update cells only
    static int64_t counter_update_value(bytes_view cell) {
        return get_field<int64_t>(cell, flags_size + timestamp_size);
    }
    // Can be called only when is_dead() is true.
    static gc_clock::time_point deletion_time(const bytes_view& cell) {
        assert(is_dead(cell));
        return gc_clock::time_point(gc_clock::duration(
            get_field<int32_t>(cell, deletion_time_offset)));
    }
    // Can be called only when is_live_and_has_ttl() is true.
    static gc_clock::time_point expiry(const bytes_view& cell) {
        assert(is_live_and_has_ttl(cell));
        auto expiry = get_field<int32_t>(cell, expiry_offset);
        return gc_clock::time_point(gc_clock::duration(expiry));
    }
    // Can be called only when is_live_and_has_ttl() is true.
    static gc_clock::duration ttl(const bytes_view& cell) {
        assert(is_live_and_has_ttl(cell));
        return gc_clock::duration(get_field<int32_t>(cell, ttl_offset));
    }
    static managed_bytes make_dead(api::timestamp_type timestamp, gc_clock::time_point deletion_time) {
        managed_bytes b(managed_bytes::initialized_later(), flags_size + timestamp_size + deletion_time_size);
        b[0] = 0;
        set_field(b, timestamp_offset, timestamp);
        set_field(b, deletion_time_offset, deletion_time.time_since_epoch().count());
        return b;
    }
    static managed_bytes make_live(api::timestamp_type timestamp, bytes_view value) {
        auto value_offset = flags_size + timestamp_size;
        managed_bytes b(managed_bytes::initialized_later(), value_offset + value.size());
        b[0] = LIVE_FLAG;
        set_field(b, timestamp_offset, timestamp);
        std::copy_n(value.begin(), value.size(), b.begin() + value_offset);
        return b;
    }
    static managed_bytes make_live_counter_update(api::timestamp_type timestamp, int64_t value) {
        auto value_offset = flags_size + timestamp_size;
        managed_bytes b(managed_bytes::initialized_later(), value_offset + sizeof(value));
        b[0] = LIVE_FLAG | COUNTER_UPDATE_FLAG;
        set_field(b, timestamp_offset, timestamp);
        set_field(b, value_offset, value);
        return b;
    }
    static managed_bytes make_live(api::timestamp_type timestamp, bytes_view value, gc_clock::time_point expiry, gc_clock::duration ttl) {
        auto value_offset = flags_size + timestamp_size + expiry_size + ttl_size;
        managed_bytes b(managed_bytes::initialized_later(), value_offset + value.size());
        b[0] = EXPIRY_FLAG | LIVE_FLAG;
        set_field(b, timestamp_offset, timestamp);
        set_field(b, expiry_offset, expiry.time_since_epoch().count());
        set_field(b, ttl_offset, ttl.count());
        std::copy_n(value.begin(), value.size(), b.begin() + value_offset);
        return b;
    }
    // make_live_from_serializer() is intended for users that need to serialise
    // some object or objects to the format used in atomic_cell::value().
    // With just make_live() the patter would look like follows:
    // 1. allocate a buffer and write to it serialised objects
    // 2. pass that buffer to make_live()
    // 3. make_live() needs to prepend some metadata to the cell value so it
    //    allocates a new buffer and copies the content of the original one
    //
    // The allocation and copy of a buffer can be avoided.
    // make_live_from_serializer() allows the user code to specify the timestamp
    // and size of the cell value as well as provide the serialiser function
    // object, which would write the serialised value of the cell to the buffer
    // given to it by make_live_from_serializer().
    template<typename Serializer>
    GCC6_CONCEPT(requires requires(Serializer serializer, bytes::iterator it) {
        serializer(it);
    })
    static managed_bytes make_live_from_serializer(api::timestamp_type timestamp, size_t size, Serializer&& serializer) {
        auto value_offset = flags_size + timestamp_size;
        managed_bytes b(managed_bytes::initialized_later(), value_offset + size);
        b[0] = LIVE_FLAG;
        set_field(b, timestamp_offset, timestamp);
        serializer(b.begin() + value_offset);
        return b;
    }
    template<typename ByteContainer>
    friend class atomic_cell_base;
    friend class atomic_cell;
};

template<typename ByteContainer>
class atomic_cell_base {
protected:
    ByteContainer _data;
protected:
    atomic_cell_base(ByteContainer&& data) : _data(std::forward<ByteContainer>(data)) { }
    friend class atomic_cell_or_collection;
public:
    bool is_counter_update() const {
        return atomic_cell_type::is_counter_update(_data);
    }
    bool is_live() const {
        return atomic_cell_type::is_live(_data);
    }
    bool is_live(tombstone t, bool is_counter) const {
        return is_live() && !is_covered_by(t, is_counter);
    }
    bool is_live(tombstone t, gc_clock::time_point now, bool is_counter) const {
        return is_live() && !is_covered_by(t, is_counter) && !has_expired(now);
    }
    bool is_live_and_has_ttl() const {
        return atomic_cell_type::is_live_and_has_ttl(_data);
    }
    bool is_dead(gc_clock::time_point now) const {
        return atomic_cell_type::is_dead(_data) || has_expired(now);
    }
    bool is_covered_by(tombstone t, bool is_counter) const {
        return timestamp() <= t.timestamp || (is_counter && t.timestamp != api::missing_timestamp);
    }
    // Can be called on live and dead cells
    api::timestamp_type timestamp() const {
        return atomic_cell_type::timestamp(_data);
    }
    void set_timestamp(api::timestamp_type ts) {
        atomic_cell_type::set_timestamp(_data, ts);
    }
    // Can be called on live cells only
    auto value() const {
        return atomic_cell_type::value(_data);
    }
    bool is_value_fragmented() const {
        return false;
    }
    // Can be called on live counter update cells only
    int64_t counter_update_value() const {
        return atomic_cell_type::counter_update_value(_data);
    }
    // Can be called only when is_dead(gc_clock::time_point)
    gc_clock::time_point deletion_time() const {
        return !is_live() ? atomic_cell_type::deletion_time(_data) : expiry() - ttl();
    }
    // Can be called only when is_live_and_has_ttl()
    gc_clock::time_point expiry() const {
        return atomic_cell_type::expiry(_data);
    }
    // Can be called only when is_live_and_has_ttl()
    gc_clock::duration ttl() const {
        return atomic_cell_type::ttl(_data);
    }
    // Can be called on live and dead cells
    bool has_expired(gc_clock::time_point now) const {
        return is_live_and_has_ttl() && expiry() <= now;
    }
    bytes_view serialize() const {
        return _data;
    }
};

class atomic_cell_view final : public atomic_cell_base<bytes_view> {
    atomic_cell_view(bytes_view data) : atomic_cell_base(std::move(data)) {}
public:
    static atomic_cell_view from_bytes(const data::type_info&, bytes_view data) { return atomic_cell_view(data); }

    friend class atomic_cell;
    friend std::ostream& operator<<(std::ostream& os, const atomic_cell_view& acv);
};

class atomic_cell_mutable_view final : public atomic_cell_base<bytes_mutable_view> {
    atomic_cell_mutable_view(bytes_mutable_view data) : atomic_cell_base(std::move(data)) {}
public:
    static atomic_cell_mutable_view from_bytes(const data::type_info&, bytes_mutable_view data) { return atomic_cell_mutable_view(data); }

    friend class atomic_cell;
};

template<data::const_view is_const>
using basic_atomic_cell_view = std::conditional_t<is_const == data::const_view::yes,
                                                  atomic_cell_view, atomic_cell_mutable_view>;

class atomic_cell_ref final : public atomic_cell_base<managed_bytes&> {
public:
    atomic_cell_ref(managed_bytes& buf) : atomic_cell_base(buf) {}
};

class atomic_cell final : public atomic_cell_base<managed_bytes> {
    atomic_cell(managed_bytes b) : atomic_cell_base(std::move(b)) {}
public:
    atomic_cell(const abstract_type&, const atomic_cell& ac) : atomic_cell_base(managed_bytes(ac._data)) { }
    atomic_cell(atomic_cell&&) = default;
    atomic_cell& operator=(const atomic_cell&) = delete;
    atomic_cell& operator=(atomic_cell&&) = default;
    atomic_cell(const abstract_type&, atomic_cell_view other) : atomic_cell_base(managed_bytes{other._data}) {}
    operator atomic_cell_view() const {
        return atomic_cell_view(_data);
    }
    static atomic_cell make_dead(api::timestamp_type timestamp, gc_clock::time_point deletion_time) {
        return atomic_cell_type::make_dead(timestamp, deletion_time);
    }
    static atomic_cell make_live(const abstract_type&, api::timestamp_type timestamp, bytes_view value) {
        return atomic_cell_type::make_live(timestamp, value);
    }
    static atomic_cell make_live(const abstract_type& type, api::timestamp_type timestamp, const bytes& value) {
        return make_live(type, timestamp, bytes_view(value));
    }
    static atomic_cell make_live_counter_update(api::timestamp_type timestamp, int64_t value) {
        return atomic_cell_type::make_live_counter_update(timestamp, value);
    }
    static atomic_cell make_live(const abstract_type&, api::timestamp_type timestamp, bytes_view value,
        gc_clock::time_point expiry, gc_clock::duration ttl)
    {
        return atomic_cell_type::make_live(timestamp, value, expiry, ttl);
    }
    static atomic_cell make_live(const abstract_type& type, api::timestamp_type timestamp, const bytes& value,
                                 gc_clock::time_point expiry, gc_clock::duration ttl)
    {
        return make_live(type, timestamp, bytes_view(value), expiry, ttl);
    }
    static atomic_cell make_live(const abstract_type&, api::timestamp_type timestamp, bytes_view value, ttl_opt ttl) {
        if (!ttl) {
            return atomic_cell_type::make_live(timestamp, value);
        } else {
            return atomic_cell_type::make_live(timestamp, value, gc_clock::now() + *ttl, *ttl);
        }
    }
    template<typename Serializer>
    static atomic_cell make_live_from_serializer(const abstract_type&, api::timestamp_type timestamp, size_t size, Serializer&& serializer) {
        return atomic_cell_type::make_live_from_serializer(timestamp, size, std::forward<Serializer>(serializer));
    }
    friend class atomic_cell_or_collection;
    friend std::ostream& operator<<(std::ostream& os, const atomic_cell& ac);
};

class collection_mutation_view;

// Represents a mutation of a collection.  Actual format is determined by collection type,
// and is:
//   set:  list of atomic_cell
//   map:  list of pair<atomic_cell, bytes> (for key/value)
//   list: tbd, probably ugly
class collection_mutation {
public:
    managed_bytes data;
    collection_mutation() {}
    collection_mutation(const collection_type_impl&, collection_mutation_view v);
    collection_mutation(const collection_type_impl&, bytes_view bv);
    operator collection_mutation_view() const;
};

class collection_mutation_view {
public:
    // FIXME: encapsulate properly
    atomic_cell_value_view data;
};

inline
collection_mutation::collection_mutation(const collection_type_impl&, collection_mutation_view v)
        : data(v.data.linearize()) {
}

inline
collection_mutation::collection_mutation(const collection_type_impl&, bytes_view bv)
        : data(bv) {
}

inline
collection_mutation::operator collection_mutation_view() const {
    return { atomic_cell_value_view(bytes_view(data)) };
}

class column_definition;

int compare_atomic_cell_for_merge(atomic_cell_view left, atomic_cell_view right);
void merge_column(const abstract_type& def,
        atomic_cell_or_collection& old,
        const atomic_cell_or_collection& neww);
