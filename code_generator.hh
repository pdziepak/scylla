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

#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"

#include "bytes.hh"

class abstract_type;

namespace codegen {

struct context {
    llvm::LLVMContext _context;
    llvm::IRBuilder<> _builder;
    std::unique_ptr<llvm::Module> _module;

    llvm::Function* _function;
    llvm::AllocaInst* _return_value;
    llvm::BasicBlock* _return_block;
    llvm::BasicBlock* _continue_block;
    bool _block_has_ended;

    llvm::Value* _a_ptr;
    llvm::Value* _a_len;
    llvm::Value* _b_ptr;
    llvm::Value* _b_len;

    llvm::Function* _bswap16;
    llvm::Function* _bswap32;
    llvm::Function* _bswap64;

    explicit context(const std::string& name)
        : _builder(_context)
        , _module(std::make_unique<llvm::Module>(name, _context))
    { }
};

namespace cgen {

extern thread_local context* current_context;

template<typename T>
std::enable_if_t<std::is_integral<T>::value, llvm::Type*>
type_of() {
    auto n = std::is_same<T, bool>::value ? 1 : sizeof(T) * 8;
    return llvm::Type::getIntNTy(current_context->_context, n);
}

template<typename T>
std::enable_if_t<std::is_pointer<T>::value, llvm::Type*>
type_of() {
    return type_of<std::decay_t<decltype(*std::declval<T>())>>()->getPointerTo();
}

template<typename T>
class value {
    llvm::Value* _value;
public:
    explicit value(llvm::Value* v) : _value(v) { }

    operator llvm::Value*() const noexcept { return _value; }

    value<T> bswap() {
        static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8, "no! no! no! no!");
        auto& ctx = *current_context;
        if (sizeof(T) == 1) {
            return *this;
        }
        llvm::Function* bswap_fn = nullptr;
        switch (sizeof(T)) {
        case 2: bswap_fn = ctx._bswap16; break;
        case 4: bswap_fn = ctx._bswap32; break;
        case 8: bswap_fn = ctx._bswap64; break;
        }
        return value<T>(ctx._builder.CreateCall(bswap_fn, { _value }, "bswap"));
    }
};

template<typename T>
class value<T*> {
    llvm::Value* _value;
public:
    explicit value(llvm::Value* v) : _value(v) { }

    operator llvm::Value*() const noexcept { return _value; }

    template<typename U>
    value<U> cast_to() const noexcept {
        return value<U>(current_context->_builder.CreateBitCast(_value, type_of<U>(), "cast_to"));
    }

    value<T> load() const noexcept {
        return value<T>(current_context->_builder.CreateLoad(_value, "load"));
    }

    value<T> unaligned_load() const noexcept {
        return value<T>(current_context->_builder.CreateAlignedLoad(_value, 1, "unaligned_load"));
    }
};

template<typename Function>
void if_(value<bool> condition, Function&& func) {
    auto& ctx = *current_context;

    auto true_block = llvm::BasicBlock::Create(ctx._context, "true_block", ctx._function);
    auto false_block = llvm::BasicBlock::Create(ctx._context, "false_block");
    ctx._builder.CreateCondBr(condition, true_block, false_block);

    ctx._builder.SetInsertPoint(true_block);
    std::forward<Function>(func)();

    if (!ctx._block_has_ended) {
        ctx._builder.CreateBr(false_block);
    }
    ctx._block_has_ended = false;

    ctx._function->getBasicBlockList().push_back(false_block);
    ctx._builder.SetInsertPoint(false_block);
}

inline void if_(value<bool> condition, llvm::BasicBlock* true_block) {
    auto& ctx = *current_context;
    auto false_block = llvm::BasicBlock::Create(ctx._context, "false_block", ctx._function);
    ctx._builder.CreateCondBr(condition, true_block, false_block);
    ctx._builder.SetInsertPoint(false_block);
}


template<typename T>
value<T> select(value<bool> condition, value<T> a, value<T> b) {
    return value<T>(current_context->_builder.CreateSelect(condition, a, b, "select"));
}

template<typename T>
void return_(value<T> val) {
    auto& ctx = *current_context;
    ctx._builder.CreateStore(val, ctx._return_value);
    ctx._builder.CreateBr(ctx._return_block);
    ctx._block_has_ended = true;
}

template<typename T>
std::enable_if_t<std::is_integral<T>::value, value<T>>
operator+(value<T> a, value<T> b) {
    return value<T>(current_context->_builder.CreateAdd(a, b, "add"));
}

template<typename T, typename U>
std::enable_if_t<std::is_pointer<T>::value && std::is_integral<U>::value, value<T>>
operator+(value<T> a, value<U> b) {
    return value<T>(current_context->_builder.CreateGEP(a, b, "add_ptr"));
}

template<typename T>
std::enable_if_t<std::is_integral<T>::value, value<bool>>
operator==(value<T> a, value<T> b) {
    return value<bool>(current_context->_builder.CreateICmpEQ(a, b, "icmp_eq"));
}

template<typename T>
std::enable_if_t<std::is_unsigned<T>::value, value<bool>>
operator<(value<T> a, value<T> b) {
    return value<bool>(current_context->_builder.CreateICmpULT(a, b, "icmp_ult"));
}

template<typename T>
std::enable_if_t<std::is_signed<T>::value, value<bool>>
operator<(value<T> a, value<T> b) {
    return value<bool>(current_context->_builder.CreateICmpSLT(a, b, "icmp_slt"));
}

template<typename T>
std::enable_if_t<std::is_integral<T>::value, value<T>>
const_(T v) {
    auto n = std::is_same<T, bool>::value ? 1 : sizeof(T) * 8;
    return value<T>(llvm::ConstantInt::get(current_context->_context,
                                           llvm::APInt(n, v, std::is_signed<T>::value)));
}

}

class module {
    class impl;
    std::unique_ptr<impl> _impl;

    int32_t (*_tri_compare)(const void*, uint32_t, const void*, uint32_t) = nullptr;
    int32_t (*_prefix_equality_tri_compare)(const void*, const void*, uint32_t, const void*, uint32_t) = nullptr;
public:
    static std::unique_ptr<module> create(abstract_type&);
    static std::unique_ptr<module> create_for_compound(std::vector<abstract_type*>);

    module();
    ~module();

    int tri_compare(bytes_view a, bytes_view b) noexcept {
        return _tri_compare(a.data(), a.size(), b.data(), b.size());
    }

    int prefix_equality_tri_compare(bytes_view a, bytes_view b) noexcept {
        return _prefix_equality_tri_compare(nullptr, a.data(), a.size(), b.data(), b.size());
    }

    auto get_prefix_equality_tri_compare_fn() const { return _prefix_equality_tri_compare; }
};

class code_generator {

public:
    static void initialize();
};

}

