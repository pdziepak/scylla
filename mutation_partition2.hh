// Temporary file for development of IMR. Should be erased from history before
// sending any patches for review.

#pragma once

#include "mutation_partition.hh"
#include "combine.hh"

namespace v2 {

class rows_entry_header {
public:
    intrusive_set_external_comparator_member_hook _link;
private:
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
    const std::vector<data::schema_row_info>* _sri;
public:
    /*rows_entry_ptr(const data::row::context& ctx, rows_entry* re) noexcept
        : _entry(re)
        , _view(re->imr_object(ctx), ctx)
    { }*/

    rows_entry_ptr(const std::vector<data::schema_row_info>& sri, rows_entry* re) noexcept
        : _entry(re)
        , _view(re->imr_object(data::row::context(sri[0])), sri)
        , _sri(&sri)
    { }

    rows_entry_ptr(const schema& s, rows_entry& re) noexcept
        : rows_entry_ptr(s.regular_row_imr_info(), &re)
    { }

    rows_entry_ptr(const rows_entry_ptr&) = delete;
    rows_entry_ptr(rows_entry_ptr&& other) noexcept
        : _entry(std::exchange(other._entry, nullptr))
        , _view(std::move(other._view))
        , _sri(other._sri)
    { }

    rows_entry_ptr& operator=(const rows_entry_ptr&) = delete;
    rows_entry_ptr& operator=(rows_entry_ptr&& other) noexcept {
        this->~rows_entry_ptr();
        new (this) rows_entry_ptr(std::move(other));
        return *this;
    }

    ~rows_entry_ptr() {
        if (_entry) {
            rows_entry::destroy(_entry, data::row::destructor_context(*_sri));
        }
    }

    auto view() const { return _view; }

    rows_entry* release() noexcept {
        return std::exchange(_entry, nullptr);
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
        const std::vector<data::schema_row_info>& _sri;
        Serializer _serializer;
        Allocator _allocator;
    public:
        row_builder(const std::vector<data::schema_row_info>& sri, Serializer ser, Allocator alloc) noexcept
            : _sri(sri), _serializer(std::move(ser)), _allocator(std::move(alloc)) { }

        row_builder(const row_builder&) = delete;
        row_builder(row_builder&&) = delete;

        row_builder& set_cell(column_id id, api::timestamp_type ts, bytes_view data) {
            _serializer.set_live_cell(id, data::cell::make_live(_sri[id / data::row::max_cell_count].type_info_for(id % data::row::max_cell_count), ts, data), _allocator);
            return *this;
        }

        auto done() {
            return _serializer.done();
        }
    };

    // TODO: pass current_allocator() as an argument
    template<typename Writer>
    static rows_entry_ptr make(const schema& s, const clustering_key& ck, Writer&& writer) {
        data::row::builder_state state;
        return rows_entry_ptr(s.regular_row_imr_info(),
                              rows_entry::create(rows_entry_header(ck), [&] (auto serializer, auto allocator) {
                                  imr::placeholder<imr::fixed_size_value<void*>> next;
                                  auto ptr = serializer.position();
                                  auto array_serializer = serializer.serialize(next).serialize_nested(state.get(0));
                                  data::row::row_builder<decltype(array_serializer), decltype(allocator)> rb(array_serializer, allocator, state, next, ptr, s.lsa_regular_row_migrators());
                                  row_builder<decltype(rb), decltype(allocator)> builder(s.regular_row_imr_info(), std::move(rb), std::move(allocator));
                                  std::forward<Writer>(writer)(builder);
                                  return builder.done();
                              }, s.lsa_regular_row_migrators()[0].get()));
    }

    void apply(const schema& s, rows_entry_ptr& other) {
        data::row::builder_state state;
        auto re = rows_entry::create(rows_entry_header(_entry->key()), [&] (auto serializer, auto allocator) {
            imr::placeholder<imr::fixed_size_value<void*>> next;
            auto ptr = serializer.position();
            auto array_serializer = serializer.serialize(next).serialize_nested(state.get(0));
                data::row::row_builder<decltype(array_serializer), decltype(allocator)> rb(array_serializer, allocator, state, next, ptr, s.lsa_regular_row_migrators());
                combine2(cells(), other.cells(), make_visitor(
                    [&] (auto&& id_a_c) {
                        data::cell::view& cell = id_a_c.second;
                        column_id id = id_a_c.first;
                        // TODO: this is not reasonable at all
                        rb.set_live_cell(id_a_c.first, data::cell::make_live(s.regular_row_imr_info()[id / data::row::max_cell_count].type_info_for(id % data::row::max_cell_count), cell.timestamp(), cell.value().linearize()), allocator);
                    },
                    [&] (auto&& a, auto&& b) {
                        auto cell = &a.second;
                        // TODO: this is not correct
                        if (a.second.timestamp() < b.second.timestamp()) {
                            cell = &b.second;
                        }
                        column_id id = a.first;
                        rb.set_live_cell(a.first, data::cell::make_live(s.regular_row_imr_info()[id / data::row::max_cell_count].type_info_for(id % data::row::max_cell_count), cell->timestamp(), cell->value().linearize()), allocator);
                    }
                ), [] (auto& x, auto& y) { return x.first < y.first; });
                return rb.done();
            //array_serializer.done();
        }, s.lsa_regular_row_migrators()[0].get());
        // Rehook rows_entry_header in the tree.
        rows_entry::destroy(other._entry, data::row::destructor_context(s.regular_row_imr_info()));
        other._entry = std::exchange(_entry, re);
        other._view = std::exchange(_view, data::row::view(_entry->imr_object(_view.context()), s.regular_row_imr_info()));
    }

    rows_entry_ptr copy(const schema& s) {
        data::row::builder_state state;
        return rows_entry_ptr(*_sri,
                              rows_entry::create(rows_entry_header(_entry->key()), [&] (auto serializer, auto allocator) {
                                  imr::placeholder<imr::fixed_size_value<void*>> next;
                                  auto ptr = serializer.position();
                                  auto array_serializer = serializer.serialize(next).serialize_nested(state.get(0));
                                      data::row::row_builder<decltype(array_serializer), decltype(allocator)> rb(array_serializer, allocator, state, next, ptr, s.lsa_regular_row_migrators());
                                      for (auto&& id_a_c : cells()) {
                                          data::cell::view& cell = id_a_c.second;
                                          // TODO: this is not reasonable at all
                                          column_id id = id_a_c.first;
                                          rb.set_live_cell(id_a_c.first, data::cell::make_live(s.regular_row_imr_info()[id / data::row::max_cell_count].type_info_for(id % data::row::max_cell_count), cell.timestamp(), cell.value().linearize()), allocator);
                                      }
                                      return rb.done();
                              }, s.lsa_regular_row_migrators()[0].get()));
    }

    stdx::optional<rows_entry_ptr> difference(const schema& s, const rows_entry_ptr& other) {
        data::row::builder_state state;
        std::array<stdx::optional<data::cell::view>, data::row::max_cell_count> diff_cells;
        size_t cell_count = 0;
        combine3(cells(), other.cells(), make_visitor(
            [&] (auto&& id_a_c, none_t) { diff_cells[id_a_c.first] = id_a_c.second; cell_count = id_a_c.first + 1; },
            [] (none_t, auto&& id_a_c) { },
            [&] (auto&& a, auto&& b) {
                // TODO: this is not correct
                if (a.second.timestamp() > b.second.timestamp()) {
                    diff_cells[a.first] = a.second; cell_count = a.first + 1;
                }
            }), [] (auto& x, auto& y) { return x.first < y.first; });
        if (!cell_count) {
            return {};
        }
        return rows_entry_ptr(*_sri,
                              rows_entry::create(rows_entry_header(_entry->key()), [&] (auto serializer, auto allocator) {
                                  imr::placeholder<imr::fixed_size_value<void*>> next;
                                  auto ptr = serializer.position();
                                  auto array_serializer = serializer.serialize(next).serialize_nested(state.get(0));
                                      data::row::row_builder<decltype(array_serializer), decltype(allocator)> rb(array_serializer, allocator, state, next, ptr, s.lsa_regular_row_migrators());
                                      for (auto i = 0u; i < cell_count; i++) {
                                          if (!diff_cells[i]) {
                                              continue;
                                          }
                                          data::cell::view& cell = *diff_cells[i];
                                          // TODO: this is not reasonable at all
                                          rb.set_live_cell(i, data::cell::make_live(s.regular_row_imr_info()[0].type_info_for(i), cell.timestamp(), cell.value().linearize()), allocator);
                                      }
                                      return rb.done();
                              }, s.lsa_regular_row_migrators()[0].get()));
    }

    bool equal(const rows_entry_ptr& other) const noexcept {
        auto it1 = _view.begin();
        auto it2 = other._view.begin();
        while (it1 != _view.end() && it2 != other._view.end()) {
            // TODO: use IMR generated comparator so that we could optimise it
            // to memcmp over whole row in best case?
            auto equal = boost::equal(cells(), other.cells(), [] (auto&& x, auto&& y) {
                return x.first == y.first
                       && x.second.timestamp() == y.second.timestamp()
                       && x.second.value() == y.second.value();
                // FIXME: incomplete
            });
            if (!equal) {
                return false;
            }
            ++it1;
            ++it2;
        }
        return it1 == _view.end() && it2 == other._view.end();
    }

    void revert(rows_entry_ptr& other) noexcept {
        std::swap(_entry, other._entry);
        std::swap(_view, other._view);
    }
};
/*
    // TODO: pass allocator as an argument to avoid accessing thread_local variable
    template<typename Writer>
    static rows_entry* create(const schema&, Writer&& writer) {
        rows_entry::create_derived<rows_entry>
    }

    bool equal(const schema& s, const rows_entry& other, const schema& other_schema) const;

    friend std::ostream& operator<<(std::ostream& os, const rows_entry& re);
*/

}
