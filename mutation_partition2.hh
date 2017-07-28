// Temporary file for development of IMR. Should be erased from history before
// sending any patches for review.

#pragma once

#include "mutation_partition.hh"

namespace v2 {

class rows_entry_header {
    intrusive_set_external_comparator_member_hook _link;
    clustering_key _key;

    // TODO: move tombstone and row_marker to IMR (ck is going to be more
    // complicated).
    tombstone _deleted_at;
    row_marker _marker;

    friend class mutation_partition;
public:
    explicit rows_entry_header(clustering_key&& key)
        : _key(std::move(key))
    { }
    explicit rows_entry_header(const clustering_key& key)
        : _key(key)
    { }

    clustering_key& key() { return _key; }
    const clustering_key& key() const { return _key; }
    tombstone deleted_at() const { return _deleted_at; }
    api::timestamp_type created_at() const { return _marker.timestamp(); }
    row_marker& marker() { return _marker; }
    const row_marker& marker() const { return _marker; }


    void apply(tombstone deleted_at) {
        _deleted_at.apply(deleted_at);
    }

    void apply(const row_marker& rm) {
        _marker.apply(rm);
    }

    void remove_tombstone() {
        _deleted_at = tombstone();
    }

    struct compare {
        clustering_key::less_compare _c;
        compare(const schema& s) : _c(s) {}
        bool operator()(const rows_entry_header& e1, const rows_entry_header& e2) const {
            return _c(e1._key, e2._key);
        }
        bool operator()(const clustering_key& key, const rows_entry_header& e) const {
            return _c(key, e._key);
        }
        bool operator()(const rows_entry_header& e, const clustering_key& key) const {
            return _c(e._key, key);
        }
        bool operator()(const clustering_key_view& key, const rows_entry_header& e) const {
            return _c(key, e._key);
        }
        bool operator()(const rows_entry_header& e, const clustering_key_view& key) const {
            return _c(e._key, key);
        }
    };
    /*
    template <typename Comparator>
    struct delegating_compare {
        Comparator _c;
        delegating_compare(Comparator&& c) : _c(std::move(c)) {}
        template <typename Comparable>
        bool operator()(const Comparable& v, const rows_entry& e) const {
            return _c(v, e._key);
        }
        template <typename Comparable>
        bool operator()(const rows_entry& e, const Comparable& v) const {
            return _c(e._key, v);
        }
    };
    template <typename Comparator>
    static auto key_comparator(Comparator&& c) {
        return delegating_compare<Comparator>(std::move(c));
    }
     */
};

using rows_entry = imr::utils::object_with_header<rows_entry_header, data::row::structure>;

// This duplicates some of data::row stuff
// This is a reference
class rows_entry_ptr {
    rows_entry* _entry;
    data::row::view _view;
public:
    rows_entry_ptr(const data::schema_row_info& sri, rows_entry* re) noexcept
        : _entry(re)
        , _view(re->imr_object(data::row::context(sri)), sri)
    { }

    rows_entry_ptr(const rows_entry_ptr&) = delete;
    rows_entry_ptr(rows_entry_ptr&& other) noexcept
        : _entry(std::exchange(other._entry, nullptr))
        , _view(std::move(other._view))
    { }

    rows_entry_ptr& operator=(const rows_entry_ptr&) = delete;
    rows_entry_ptr& operator=(rows_entry_ptr&& other) noexcept {
        this->~rows_entry_ptr();
        new (this) rows_entry_ptr(std::move(other));
        return *this;
    }

    ~rows_entry_ptr() {
        if (_entry) {
            rows_entry::destroy(_entry, _view.context());
        }
    }

    auto cells() const {
        return _view.cells();
    }

    bool empty() const {
        return _view.empty();
    }

    bool is_live(const schema& s, tombstone base_tombstone = tombstone(), gc_clock::time_point query_time = gc_clock::time_point::min()) const {
        for (auto&& id_and_cell : cells()) {
            auto id = id_and_cell.first;
            auto& cell = id_and_cell.second;

            const column_definition& def = s.column_at(column_kind::regular_column, id);
            if (def.is_atomic()) {
                if (cell.is_live()) { // tomb, now
                    return true;
                }
            } else {
                /*auto&& cell = cell_or_collection.as_collection_mutation();
                auto&& ctype = static_pointer_cast<const collection_type_impl>(def.type);
                if (ctype->is_any_live(cell, tomb, now)) {
                    any_live = true;
                    return stop_iteration::yes;
                }*/
                abort();
            }
        }
        return false;
    }

    template<typename Serializer, typename Allocator>
    class row_builder {
        const data::schema_row_info& _row_info;
        Serializer _serializer;
        Allocator _allocator;
    public:
        row_builder(const data::schema_row_info& sri, Serializer ser, Allocator alloc) noexcept
            : _row_info(sri), _serializer(std::move(ser)), _allocator(std::move(alloc)) { }

        row_builder& set_cell(column_id id, api::timestamp_type ts, bytes_view data) {
            _serializer.set_live_cell(id, data::cell::make_live(_row_info.type_info_for(id), ts, data), _allocator);
            return *this;
        }

        auto done() {
            return _serializer.done();
        }
    };

    // TODO: pass current_allocator() as an argument
    template<typename Writer>
    static rows_entry_ptr make(const schema& s, const clustering_key& ck, Writer&& writer) {
        return rows_entry_ptr(s.regular_row_imr_info(),
                              rows_entry::create(rows_entry_header(ck), [&] (auto serializer, auto allocator) {
                                  return serializer.serialize([&] (auto array_serializer) {
                                      data::row::row_builder<decltype(array_serializer)> rb(array_serializer);
                                      row_builder<decltype(rb), decltype(allocator)> builder(s.regular_row_imr_info(), std::move(rb), std::move(allocator));
                                      std::forward<Writer>(writer)(builder);
                                      return builder.done();
                                  }).done();
                              }, s.lsa_regular_row_migrator()));
    }
};
    // These have to be static:
    // See reversibly_mergeable.hh
    //void apply_reversibly(const schema& s, rows_entry& e) {
    //    _row.apply_reversibly(s, e._row);
    //}
    // See reversibly_mergeable.hh
    //void revert(const schema& s, rows_entry& e) noexcept {
    //    _row.revert(s, e._row);
    //}
/*
    // TODO: pass allocator as an argument to avoid accessing thread_local variable
    template<typename Writer>
    static rows_entry* create(const schema&, Writer&& writer) {
        rows_entry::create_derived<rows_entry>
    }

    // Static?
    rows_entry* difference(const schema&, column_kind, const rows_entry& other) const;

    bool equal(const schema& s, const rows_entry& other) const;
    bool equal(const schema& s, const rows_entry& other, const schema& other_schema) const;

    friend std::ostream& operator<<(std::ostream& os, const rows_entry& re);
*/

}
