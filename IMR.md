# In-Memory Representation

## Introduction

## General overview

### Context

### Views

### Serialisation

## Primitive types

### `flags`
context requirements

### `fixed_size_value`

### `compressed_integer`

### `fixed_buffer`
size_of<Tag>

### `optional`
is_present<Tag>

### `alternative`
get_alternative<Tag> 
active_alternative_of<Tag>

### `structure`

### Type concept

```c++
struct imr_type {
    static auto make_view(bytes_view) noexcept;
    static auto make_view(bytes_mutable_view) noexcept;

    template<typename Context>
    static size_t serialized_object_size(const uint8_t*, const Context&) noexcept;

    static size_t size_when_serialized(...) noexcept;
    static size_t serialize(uint8_t* out, ...) noexcept;
};
```

## Methods
IMR objects can define destructor and mover methods. Their goal is to provide
similar functionality to C++ destructors and move constructors, but they operate
in a slightly different way.

methods inside structure are called in an unspecified order

movers and destructors cannot throw

### Mover
unlike c++ moved object is the same one (and not create new, destory old one)
destructor is not called for the old one, objects is memcpy() to the new
place then mover is called

### Destructor


## Containers

### `sparse_array`
context: context_for(size_t n)

## Hints & guidelines

### General
1. Fixed-size objects can be updated in-place, variable-size cannot.
2. Creating view may be expensive (e.g. `structure` computes offsets of its
   members).
3. When view is created structure doesn't compute the size of last member, IOW
   try to make the most complex member the last one.
4. `structure` allows reading its first member without properly creating a view,
   this is useful for storing context-related information in the first member.
5. `compressed_integer` touches (but doesn't destroy) up to 7 bytes after its
   end – make sure it is legal to access these memory locations.
6. Where applicable provide (size, serializer) pair to object serializers so
   that unnecessary copies can be avoided.

### Methods
1. The C++ 'Rule of All or Nothing' still applies. If an IMR object defines mover
   or destructor it probably should also define the other one.
   
### Containers
*guidelines pending invention*
