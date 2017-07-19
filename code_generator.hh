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

