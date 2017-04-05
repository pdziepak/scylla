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

// something about data representation goes here
#include "schema.hh"
#include "in_memory_representation.hh"

namespace data {

class type_info {
    uint16_t _fixed_size = 0;
public:
    bool is_fixed_size() const noexcept {
        return !!_fixed_size;
    }

    size_t value_size() const noexcept {
        return _fixed_size;
    }
};

class schema_info {
    std::vector<type_info> _columns;
public:
    const type_info& type_info_for(column_id id) const noexcept {
        return _columns[id];
    }
};

struct cell {
    struct tags {
        class flags;
        class live;
        class exprigng;
        class counter_update;
        class ttl;
        class expiry;
        class empty;
        class timestamp;
        class value;
        class dead;
        class counter_update;
        class fixed_value;
        class variable_value;
        class value_size;
        class value_data;
        class pointer;
        class data;
    };

    using flags = imr::flags<
        tags::live,
        tags::exprigng,
        tags::counter_update,
        tags::empty
    >;
    using value_data_variant = imr::variant<tags::value_data,
            imr::member<tags::pointer, imr::fixed_size_value<void*>>,
        imr::member<tags::data, imr::fixed_buffer<tags::data>>
    >;
    using value_variant = imr::variant<tags::value,
        imr::member<tags::dead, imr::compressed_integer<int32_t>>,
        imr::member<tags::counter_update, imr::compressed_integer<int64_t>>,
        imr::member<tags::fixed_value, imr::fixed_buffer<tags::fixed_value>>,
        imr::member<tags::variable_value, imr::structure<
            imr::member<tags::value_size, imr::compressed_integer<uint32_t>>,
            imr::member<tags::value_data, value_data_variant>
        >>
    >;
    using structure = imr::structure<
        imr::member<tags::flags, flags>,
        imr::member<tags::timestamp, imr::compressed_integer<api::timestamp_type>>,
        imr::optional_member<tags::exprigng, imr::structure<
            imr::member<tags::ttl, imr::compressed_integer<int32_t>>,
            imr::member<tags::expiry, imr::compressed_integer<int32_t>>
        >>,
        imr::member<tags::value, value_variant>
    >;

    class context;
    class view;

    static auto make_live(const type_info& ti, api::timestamp_type ts, bytes_view value) noexcept {
        return [&ti, ts, value] (auto serializer) noexcept {
            auto ser = serializer
                .serialize(imr::set_flag<tags::live>(), imr::set_flag<tags::empty>(value.empty()))
                .serialize(ts)
                .skip();
            return [&] {
                if (ti.is_fixed_size() || value.empty()) {
                    return ser.template serialize_as<tags::fixed_value>(value);
                } else {
                    return ser.template serialize_as<tags::variable_value>([&] (auto serializer) noexcept {
                        return serializer
                            .serialize(value.size())
                            .template serialize_as<tags::data>(value) // TODO: external storage
                            .done();
                    });
                }
            }().done();
        };
    }

    template<typename Builder>
    static size_t size_of(Builder&& builder) noexcept {
        return structure::size_when_serialized(std::forward<Builder>(builder));
    }

    template<typename Builder>
    static size_t serialize(uint8_t* ptr, Builder&& builder) noexcept {
        return structure::serialize(ptr, std::forward<Builder>(builder));
    }

    static view make_view(const type_info& ti, uint8_t* ptr) noexcept;
};


class cell::context {
    cell::flags::view _flags;
    type_info _type;
public:
    explicit context(cell::flags::view flags, const type_info& tinfo) noexcept
        : _flags(flags), _type(tinfo) { }

    template<typename Tag>
    bool is_present() const noexcept;

    template<typename Tag>
    auto active_alternative_of() const noexcept;

    template<typename Tag>
    size_t size_of() const noexcept;
};

template<>
bool cell::context::is_present<cell::tags::exprigng>() const noexcept {
    return _flags.get<tags::exprigng>();
}

template<>
auto cell::context::active_alternative_of<cell::tags::value>() const noexcept {
    if (!_flags.get<tags::live>()) {
        return cell::value_variant::index_for<tags::dead>();
    } else if (_flags.get<tags::counter_update>()) {
        // TODO: have separate contexts for counter and regular schemas
        return cell::value_variant::index_for<tags::counter_update>();
    } else if (_type.is_fixed_size() || _flags.get<tags::empty>()) {
        return cell::value_variant::index_for<tags::fixed_value>();
    } else {
        return cell::value_variant::index_for<tags::variable_value>();
    }
}

template<>
auto cell::context::active_alternative_of<cell::tags::value_data>() const noexcept {
    assert(0 && "no support for subcontexts yet");
    return value_data_variant::index_for<tags::data>();
}

template<>
size_t cell::context::size_of<cell::tags::fixed_value>() const noexcept {
    return _flags.get<tags::empty>() ? 0 : _type.value_size();
}

template<>
size_t cell::context::size_of<cell::tags::data>() const noexcept {
    assert(0 && "no support for subcontexts yet");
    return 0;
}

class cell::view {
    context _context;
    structure::view _view;
public:
    view(const type_info& ti, const uint8_t* ptr)
        : _context(structure::get_first_member(ptr), ti)
        , _view(structure::make_view(ptr, _context))
    { }

    bool is_live() const noexcept {
        return _view.get<tags::flags>().get<tags::live>();
    }

    api::timestamp_type timestamp() const noexcept {
        return _view.get<tags::timestamp>().load();
    }

    int64_t counter_update() const noexcept {
        return _view.get<tags::value>().as<tags::counter_update>().load();
    }

    // TODO: support fragmented buffers
    bytes_view value() const noexcept {
        abort();
    }
};

inline cell::view cell::make_view(const type_info& ti, uint8_t* ptr) noexcept {
    return view(ti, ptr);
}

}
