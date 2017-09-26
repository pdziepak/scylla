# In-Memory Representation

## Introduction

## Basic concepts
C++ types vs. IMR types

### Contexts

provide information necessary to deserialize imr objects

context, context_factory

### Views & mutable views

### Serialisers

3 phases: determining size (cannot fail), allocations (can fail), serialisation (cannot fail)
serializer has to execute the same operations in both phases

1. Computing the object sizes and recording necessary memory allocations.
2. Allocating memory. This is the only phase that can fail.
3. Writing serialised data.

#### Placeholders

#### Continuation hooks

```c++
return serializer
    .serialize(...)
    .serialize_nested()
        .serialize(...)
        .done()
    .serialize(...)
    .done();
```

### IMR types

#### Tags

#### Type concept

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

## Fundamental types

TODO: context requirements of each type

 Type                 | Fixed size | Needs context | In-place update            | Placeholders |
----------------------|:----------:|:-------------:|:--------------------------:|:------------:|
 `flags`              | Yes        | No            | Yes                        | Yes          |
 `pod`                | Yes        | No            | Yes                        | Yes          |
 `compressed_integer` | No         | No            | No                         | No           |
 `buffer`             | No         | Yes           | Yes (length cannot change) | No           |

### `flags`

```c++
template<typename Tag>
struct imr::set_flag {
    set_flag(bool v = true) noexcept;
};
    
template<typename Tags...>
struct imr::flags {
    struct view {
        template<typename Tag>
        bool get() const noexcept;
        
        template<typename Tag>
        bool set(bool value = true) noexcept;
    };
    
    template<typename... Tags1>
    void serialize(imr::set_flag<Tags1>...) noexcept;
};
```

### `pod`

```c++
template<typename PODType>
struct imr::pod {
    struct view {
        PODType load() const noexcept;
        void store(PODType object) noexcept;
    };
    
    void serialize(PODType object) noexcept;
};
```

`pod` is a POD object using the same representation as that defined by the C++
ABI including all the internal padding in case of compound types. It is
therefore recommended that `pod` is used exclusively to represent C++
fundamental types.

`pod` is a fixed-size type which can be updated in place.
It does not require an external context for deserialisation.

### `compressed_integer`

```c++
template<typename IntegerType>
struct imr::compressed_integer {
    struct view {
        IntegerType load() const noexcept;
    };
    
    void serialize(IntegerType value) noexcept;
};
```

`compressed_integer` is an integer type which internally uses variable-length
encoding inspired by LEB128. The template parameter `IntegerType` is used to
limit the maximum range of the value and provide information whether negative
values are allowed which may simplify the encoding and decoding logic as well
as reduce the number of required non-value bits.

`compressed_integer` is an variable-size type which cannot be updated in place.
It does not require an external context for deserialisation.

*`compressed_integer` accessors may touch up to 7 bytes following the IMR
object. Under no circumstances the value of these bytes will be changed but it
is the higher layer's responsibility to ensure that such memory access is
legal.*

### `buffer`

```c++
template<typename Tag>
struct imr::buffer {
    using view = bytes_view;
    
    void serialize(bytes_view) noexcept;
};
```

`buffer` is a type representing an array of bytes of an arbitrary length.

`buffer` is a variable-size type which can be updated in-place as long as the
length of the buffer remains unchanged.

#### Context requirements

Deserialisation of `buffer` objects requires an external context providing the
following member:

```c++
size_t size_of<Tag>() const noexcept;
```

Where `Tag` is the same type as the one used as a template parameter for
`buffer`. `size_of()` is must return the length of the buffer.


## Compound types

### `tagged_type`

```c++
template<typename Tag, typename IMRType>
struct imr::tagged_type : IMRType { };
```

`tagged_type` is a thin wrapper around an IMR type that allows annotating it
with a tag without changing its views or serialisers in any way.

The main purpose of adding tags to the type name is to allow defining custom
methods for only some usage of a particular IMR type (see section
[Methods](#methods)). 

### `optional`

FIXME: change optional to provide thin abstraction that imposes no performance
cost but allows checking whether the optional is engaged? (consistent with
`imr::view` as well)

```c++
template<typename Tag, typename IMRType>
struct imr::optional {
    using view = IMRType::view;
    
    template<typename... Args>
    void serialize(Args&&...) noexcept;
};
```

#### Context requirements

```c++
bool is_present<Tag>() const noexcept;
```

### `variant`

```c++
template<typename Tag, typename IMRType>
struct imr::alternative;

template<typename Tag, typename Alternatives>
struct imr::variant {
    struct view {
        template<typename AlternativeTag>
        auto as() const noexcept;
        
        template<typename Visitor>
        decltype(auto) accept(Visitor&&) const;
    };
    
    template<typename AlternativeTag, typename... Args>
    void serialize(Args&&...) noexcept;
};
```

#### Context requirements

```c++
imr::variant::alternative_index active_alternative_of<Tag>() const noexcept;

template<typename AlternativeTag>
auto context_for() const noexcept;
```

### `structure`

#### Context requirements

```c++
template<typename Tag, typename IMRType>
struct imr::member;

template<typename Tag, typename Members>
struct imr::structure {
    struct view {
        template<typename MemberTag>
        auto get() noexcept;
        
        template<typename MemberTag>
        size_t offset_of() const noexcept;
    };
    
    template<typename Writer, typename... Args>
    void serialize(Writer&&, Args&&...) noexcept;
    
    template<typename Tag, typename Context>
    auto get_member(uint8_t*, const Context&) const noexcept;
};

struct imr::structure_writer {
    // For imr::optional:
    template<typename... Args>
    auto serialize(Args&&...);
    
    auto skip();
    
    // For imr::variant:
    template<typename AlternativeTag, typename... Args>
    auto serialize_as(Args&&...);
    
    // For other types:
    template<typename... Args>
    auto serialize(Args&&...);
    
    // After all members:
    auto done() noexcept;
};
```

```c++
template<typename AlternativeTag>
auto context_for() const noexcept;
```

## Methods

IMR objects can define destructor and mover methods. Their goal is to provide
similar functionality to C++ destructors and move constructors and thus enable
owning references inside IMR objects. Movers are also essential in ensuring that
all references are properly updated when LSA moves objects around.

The default mover and destructor for fundamental types does nothing. The
default methods for compound types and containers invokes that method for
all present elements in an unspecified order.

User can define mover and destructor methods for any IMR type which would
override any default methods that this type might have.

Neither movers nor destructors are allowed to fail.

```c++
template<>
struct imr::mover<IMRType> {
    static void run(uint8_t* ptr, const Context& ctx) noexcept {
        // user-defined mover action
    }
};

template<>
struct imr::destructor<IMRType> {
    static void run(const uint8_t* ptr, const Context& ctx) noexcept {
        // user-defined destructor action
    }
};

```

### Mover

When an IMR object is being moved its serialised form is copied to the new
place in memory and then a mover method is called with the address of the new
location passed as an argument.

*Note that, unlike C++, IMR does not call the destructor of the object moved
from. In other words, while moving in C++ involves creating a new object that
steals internal state from another, IMR just copies the object to the new
location and invokes a mover to give it a chance to do necessary updates, it is
however still considered to be the same object as the original one.*

### Destructor

The destructor of an IMR object is called before the storage it occupies is
freed. The address of the object passed to the destructor is either a location
at which the object was originally created or the address that was given to the
latest invocation of the object mover method.

## Memory allocations

