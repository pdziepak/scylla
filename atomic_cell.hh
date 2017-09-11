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
#include "in_memory_representation.hh"

class atomic_cell_or_collection;

struct tags { // not in the global namespace
    class flags;
    class live;
    class expiring;
    class revert;
    class counter_update;
    class counter_in_place_revert;

    class timestamp;
    class dead;
    class expiry;
    class ttl;
};

using ac_imr_flags = imr::flags<
        tags::live, tags::expiring, tags::revert,
        tags::counter_update, tags::counter_in_place_revert
    >;

struct ac_cell_context {
    ac_imr_flags::view _flags;

    template<typename Tag>
    auto context_for(...) const noexcept { return *this; }

    template<typename Tag>
    inline bool is_present() const noexcept;

    template<typename Tag>
    size_t size_of() const noexcept {
        abort(); // imr requires but not uses
    }
};

template<>
inline bool ac_cell_context::is_present<tags::expiring>() const noexcept {
    return _flags.get<tags::expiring>();
}

template<>
inline bool ac_cell_context::is_present<tags::dead>() const noexcept {
    return !_flags.get<tags::live>();
}

template<>
inline bool ac_cell_context::is_present<tags::live>() const noexcept {
    return _flags.get<tags::live>();
}

template<>
inline bool ac_cell_context::is_present<tags::counter_update>() const noexcept {
    return _flags.get<tags::counter_update>();
}

using ac_imr_schema = imr::structure<
        imr::member<tags::flags, ac_imr_flags>,
        imr::member<tags::timestamp, imr::compressed_integer<int64_t>>,
        imr::optional_member<tags::expiring, imr::structure<
                imr::member<tags::expiry, imr::compressed_integer<int32_t>>,
                imr::member<tags::ttl, imr::compressed_integer<int32_t>>
        >>,
        imr::optional_member<tags::dead, imr::compressed_integer<int32_t>>,
        imr::optional_member<tags::counter_update, imr::compressed_integer<int64_t>>,
        imr::optional_member<tags::live, imr::fixed_buffer<tags::live>>
    >;


struct ac_value_context {
    size_t _size;
    template<typename Tag>
    size_t size_of() const noexcept {
        return _size;
    }

    template<typename Tag>
    auto context_for(...) const noexcept { return *this; }
};

class atomic_cell_type final {
private:
    template<typename BytesContainer>
    static auto get_view(const BytesContainer& bc) {
        using pointer_type = std::make_unsigned_t<typename BytesContainer::value_type>*;
        return ac_imr_schema::make_view(reinterpret_cast<pointer_type>(bc.begin()));
    }
private:
    static bool is_counter_update(ac_imr_schema::view cell) {
        return cell.get<tags::flags>().get<tags::counter_update>();
    }
    static bool is_revert_set(ac_imr_schema::view cell) {
        return cell.get<tags::flags>().get<tags::revert>();
    }
    static bool is_counter_in_place_revert_set(ac_imr_schema::view cell) {
        return cell.get<tags::flags>().get<tags::counter_in_place_revert>();
    }
    static void set_revert(ac_imr_schema::mutable_view cell, bool revert) {
        return cell.get<tags::flags>().set<tags::revert>(revert);
    }
    static void set_counter_in_place_revert(ac_imr_schema::mutable_view cell, bool flag) {
        return cell.get<tags::flags>().set<tags::counter_in_place_revert>(flag);
    }
    static bool is_live(ac_imr_schema::view cell) {
        return cell.get<tags::flags>().get<tags::live>();
    }
    static bool is_live_and_has_ttl(ac_imr_schema::view cell) {
        return cell.get<tags::flags>().get<tags::expiring>();
    }
    static bool is_dead(ac_imr_schema::view cell) {
        return !is_live(cell);
    }
    // Can be called on live and dead cells
    static api::timestamp_type timestamp(ac_imr_schema::view cell) {
        return cell.get<tags::timestamp>().load();
    }
    template<typename BytesContainer>
    static void set_timestamp(BytesContainer& cell, api::timestamp_type ts) {
        //set_field(cell, timestamp_offset, ts);
        // oops! perhaps allow updating compressed ints in-place if that won't
        // change the size
    }
    // Can be called on live cells only
private:
    template<typename BytesView>
    static auto do_get_value(BytesView cell, size_t total_size) {
        auto offset = cell.template offset_of<tags::live>();
        return cell.template get<tags::live>(ac_value_context { total_size - offset });
    }
public:
    static bytes_view value(ac_imr_schema::view cell, size_t total_size) {
        return do_get_value(cell, total_size);
    }
    static bytes_mutable_view value(ac_imr_schema::mutable_view cell, size_t total_size) {
        return do_get_value(cell, total_size);
    }
    // Can be called on live counter update cells only
    static int64_t counter_update_value(ac_imr_schema::view cell) {
        return cell.get<tags::counter_update>().load();
    }
    // Can be called only when is_dead() is true.
    static gc_clock::time_point deletion_time(ac_imr_schema::view cell) {
        assert(is_dead(cell));
        auto dt = cell.get<tags::dead>().load();
        return gc_clock::time_point(gc_clock::duration(dt));
    }
    // Can be called only when is_live_and_has_ttl() is true.
    static gc_clock::time_point expiry(ac_imr_schema::view cell) {
        assert(is_live_and_has_ttl(cell));
        auto expiry = cell.get<tags::expiring>().get<tags::expiry>().load();
        return gc_clock::time_point(gc_clock::duration(expiry));
    }
    // Can be called only when is_live_and_has_ttl() is true.
    static gc_clock::duration ttl(ac_imr_schema::view cell) {
        assert(is_live_and_has_ttl(cell));
        auto ttl = cell.get<tags::expiring>().get<tags::ttl>().load();
        return gc_clock::duration(ttl);
    }
    static managed_bytes make_dead(api::timestamp_type timestamp, gc_clock::time_point deletion_time) {
        auto writer = [&] (auto serializer) noexcept {
            return serializer
                .serialize()
                .serialize(timestamp)
                .skip()
                .serialize(deletion_time.time_since_epoch().count())
                .skip()
                .skip()
                .done();
        };
        auto total_size = ac_imr_schema::size_when_serialized(writer);
        managed_bytes b(managed_bytes::initialized_later(), total_size);
        ac_imr_schema::serialize((uint8_t*)b.begin(), writer);
        return b;
    }
    static managed_bytes make_live(api::timestamp_type timestamp, bytes_view value) {
        auto writer = [&] (auto serializer) noexcept {
            return serializer
                    .serialize(imr::set_flag<tags::live>())
                    .serialize(timestamp)
                    .skip()
                    .skip()
                    .skip()
                    .serialize(value)
                    .done();
        };
        auto total_size = ac_imr_schema::size_when_serialized(writer);
        managed_bytes b(managed_bytes::initialized_later(), total_size);
        ac_imr_schema::serialize((uint8_t*)b.begin(), writer);
        return b;
    }
    static managed_bytes make_live_counter_update(api::timestamp_type timestamp, int64_t value) {
        auto writer = [&] (auto serializer) noexcept {
            return serializer
                    .serialize(imr::set_flag<tags::live>(), imr::set_flag<tags::counter_update>())
                    .serialize(timestamp)
                    .skip()
                    .skip()
                    .serialize(value)
                    .skip()
                    .done();
        };
        auto total_size = ac_imr_schema::size_when_serialized(writer);
        managed_bytes b(managed_bytes::initialized_later(), total_size);
        ac_imr_schema::serialize((uint8_t*)b.begin(), writer);
        return b;
    }
    static managed_bytes make_live(api::timestamp_type timestamp, bytes_view value, gc_clock::time_point expiry, gc_clock::duration ttl) {
        auto writer = [&] (auto serializer) noexcept {
            return serializer
                    .serialize(imr::set_flag<tags::live>(), imr::set_flag<tags::expiring>())
                    .serialize(timestamp)
                    .serialize([&] (auto serializer) noexcept {
                        return serializer
                            .serialize(expiry.time_since_epoch().count())
                            .serialize(ttl.count())
                            .done();
                    })
                    .skip()
                    .skip()
                    .serialize(value)
                    .done();
        };
        auto total_size = ac_imr_schema::size_when_serialized(writer);
        managed_bytes b(managed_bytes::initialized_later(), total_size);
        ac_imr_schema::serialize((uint8_t*)b.begin(), writer);
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
    //GCC6_CONCEPT(requires requires(Serializer serializer, bytes::iterator it) {
    //    serializer(it);
    //})
    static managed_bytes make_live_from_serializer(api::timestamp_type timestamp, size_t size, Serializer&& serializer) {
        auto writer = [&] (auto serializer0) noexcept {
            return serializer0
                    .serialize(imr::set_flag<tags::live>())
                    .serialize(timestamp)
                    .skip()
                    .skip()
                    .skip()
                    .serialize(size, serializer)
                    .done();
        };
        auto total_size = ac_imr_schema::size_when_serialized(writer);
        managed_bytes b(managed_bytes::initialized_later(), total_size);
        ac_imr_schema::serialize((uint8_t*)b.begin(), writer);
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
    bytes_view _view;
protected:
    atomic_cell_base(ByteContainer data, bytes_view view)
        : _data(data)
        , _view(view) { }
    friend class atomic_cell_or_collection;
public:
    bool is_counter_update() const {
        return atomic_cell_type::is_counter_update(_data);
    }
    bool is_revert_set() const {
        return atomic_cell_type::is_revert_set(_data);
    }
    bool is_counter_in_place_revert_set() const {
        return atomic_cell_type::is_counter_in_place_revert_set(_data);
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
        return atomic_cell_type::value(_data, _view.size());
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
        return _view;
    }
    void set_revert(bool revert) {
        atomic_cell_type::set_revert(_data, revert);
    }
    void set_counter_in_place_revert(bool flag) {
        atomic_cell_type::set_counter_in_place_revert(_data, flag);
    }
};

class atomic_cell_view final : public atomic_cell_base<ac_imr_schema::view> {
    atomic_cell_view(ac_imr_schema::view data, bytes_view view) : atomic_cell_base(data, view) {}
public:
    static atomic_cell_view from_bytes(bytes_view data) {
        auto in = (const uint8_t*)data.begin();
        ac_cell_context ctx { ac_imr_schema::get_first_member(in) };
        return atomic_cell_view(ac_imr_schema::make_view(in, ctx), data);
    }

    friend class atomic_cell;
    friend std::ostream& operator<<(std::ostream& os, const atomic_cell_view& acv);
};

class atomic_cell_mutable_view final : public atomic_cell_base<ac_imr_schema::mutable_view> {
    atomic_cell_mutable_view(ac_imr_schema::mutable_view data, bytes_view view) : atomic_cell_base(data, view) {}
public:
    static atomic_cell_mutable_view from_bytes(bytes_mutable_view data) {
        auto in = (uint8_t*)data.begin();
        ac_cell_context ctx { ac_imr_schema::get_first_member(in) };
        return atomic_cell_mutable_view(ac_imr_schema::make_view(in, ctx), data);
    }

    friend class atomic_cell;
};

using atomic_cell_ref = atomic_cell_mutable_view;
/*
class atomic_cell_ref final : public atomic_cell_base<managed_bytes&> {
public:
    atomic_cell_ref(managed_bytes& buf) : atomic_cell_base(buf) {}
};*/

class atomic_cell final {
    managed_bytes _bytes;
    ac_imr_schema::mutable_view _data;
    static auto vfrom_bytes(managed_bytes& data) {
        auto in = (uint8_t*)data.begin();
        ac_cell_context ctx { ac_imr_schema::get_first_member(in) };
        return ac_imr_schema::make_view(in, ctx);
    }
    atomic_cell(managed_bytes b) : _bytes(std::move(b)), _data(vfrom_bytes(_bytes)) {}
public:
    atomic_cell(const atomic_cell& other) : _bytes(other._bytes), _data(vfrom_bytes(_bytes)) { }
    atomic_cell(atomic_cell&& other) : _bytes(std::move(other._bytes)), _data(vfrom_bytes(_bytes)) { }
    atomic_cell& operator=(const atomic_cell& other) {
        auto copy = other;
        this->~atomic_cell();
        new (this) atomic_cell(std::move(copy));
        return *this;
    }
    atomic_cell& operator=(atomic_cell&& other) noexcept {
        this->~atomic_cell();
        new (this) atomic_cell(std::move(other));
        return *this;
    }
    static atomic_cell from_bytes(managed_bytes b) {
        return atomic_cell(std::move(b));
    }
    atomic_cell(atomic_cell_view other) : _bytes(other.serialize()), _data(vfrom_bytes(_bytes)) {}

    // the most appropriate comment for the following piece of code is: 'ugh'
    bool is_counter_update() const {
        return atomic_cell_type::is_counter_update(_data);
    }
    bool is_revert_set() const {
        return atomic_cell_type::is_revert_set(_data);
    }
    bool is_counter_in_place_revert_set() const {
        return atomic_cell_type::is_counter_in_place_revert_set(_data);
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
        return atomic_cell_type::value(_data, _bytes.size());
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
        return is_live_and_has_ttl() && expiry() < now;
    }
    bytes_view serialize() const {
        return _bytes;
    }
    // </ugh>


    operator atomic_cell_view() const {
        return atomic_cell_view::from_bytes(_bytes);
    }
    static atomic_cell make_dead(api::timestamp_type timestamp, gc_clock::time_point deletion_time) {
        return atomic_cell_type::make_dead(timestamp, deletion_time);
    }
    static atomic_cell make_live(api::timestamp_type timestamp, bytes_view value) {
        return atomic_cell_type::make_live(timestamp, value);
    }
    static atomic_cell make_live(api::timestamp_type timestamp, const bytes& value) {
        return make_live(timestamp, bytes_view(value));
    }
    static atomic_cell make_live_counter_update(api::timestamp_type timestamp, int64_t value) {
        return atomic_cell_type::make_live_counter_update(timestamp, value);
    }
    static atomic_cell make_live(api::timestamp_type timestamp, bytes_view value,
        gc_clock::time_point expiry, gc_clock::duration ttl)
    {
        return atomic_cell_type::make_live(timestamp, value, expiry, ttl);
    }
    static atomic_cell make_live(api::timestamp_type timestamp, const bytes& value,
                                 gc_clock::time_point expiry, gc_clock::duration ttl)
    {
        return make_live(timestamp, bytes_view(value), expiry, ttl);
    }
    static atomic_cell make_live(api::timestamp_type timestamp, bytes_view value, ttl_opt ttl) {
        if (!ttl) {
            return atomic_cell_type::make_live(timestamp, value);
        } else {
            return atomic_cell_type::make_live(timestamp, value, gc_clock::now() + *ttl, *ttl);
        }
    }
    template<typename Serializer>
    static atomic_cell make_live_from_serializer(api::timestamp_type timestamp, size_t size, Serializer&& serializer) {
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
    collection_mutation(managed_bytes b) : data(std::move(b)) {}
    collection_mutation(collection_mutation_view v);
    operator collection_mutation_view() const;
};

class collection_mutation_view {
public:
    bytes_view data;
    bytes_view serialize() const { return data; }
    static collection_mutation_view from_bytes(bytes_view v) { return { v }; }
};

inline
collection_mutation::collection_mutation(collection_mutation_view v)
        : data(v.data) {
}

inline
collection_mutation::operator collection_mutation_view() const {
    return { data };
}

class column_definition;

int compare_atomic_cell_for_merge(atomic_cell_view left, atomic_cell_view right);
void merge_column(const column_definition& def,
        atomic_cell_or_collection& old,
        const atomic_cell_or_collection& neww);
