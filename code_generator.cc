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

#include "code_generator.hh"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/ExecutionEngine/RuntimeDyld.h"
#include "llvm/ExecutionEngine/SectionMemoryManager.h"
#include "llvm/ExecutionEngine/JITSymbol.h"
#include "llvm/ExecutionEngine/Orc/CompileUtils.h"
#include "llvm/ExecutionEngine/Orc/IRCompileLayer.h"
#include "llvm/ExecutionEngine/Orc/IRTransformLayer.h"
#include "llvm/ExecutionEngine/Orc/LambdaResolver.h"
#include "llvm/ExecutionEngine/Orc/ObjectLinkingLayer.h"
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include "llvm/IR/DataLayout.h"
#include "llvm/IR/Mangler.h"
#include "llvm/Support/DynamicLibrary.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Target/TargetMachine.h"

#include "types.hh"

namespace codegen {

static logging::logger logger("codegen");

namespace cgen {

thread_local context* current_context;

}

class module::impl {
    std::unique_ptr<llvm::TargetMachine> _target;
    llvm::DataLayout _data_layout;

    llvm::orc::ObjectLinkingLayer<> _object_layer;
    llvm::orc::IRCompileLayer<decltype(_object_layer)> _compile_layer;
    using optimize_fn = std::function<std::unique_ptr<llvm::Module>(std::unique_ptr<llvm::Module>)> ;
    llvm::orc::IRTransformLayer<decltype(_compile_layer), optimize_fn> _optimize_layer;
public:
    impl(std::unique_ptr<llvm::Module> m)
        : _target(llvm::EngineBuilder().selectTarget())
        , _data_layout(_target->createDataLayout())
        , _compile_layer(_object_layer, llvm::orc::SimpleCompiler(*_target))
        , _optimize_layer(_compile_layer, [this] (auto m) { return optimize(std::move(m)); })
    {
        auto resolver = llvm::orc::createLambdaResolver(
            [&] (const std::string& name) {
                auto sym = _optimize_layer.findSymbol(name, false);
                if (sym) {
                    return  llvm::JITSymbol(sym.getAddress(), sym.getFlags());
                }
                return  llvm::JITSymbol(nullptr);
            },
            [] (const std::string& name) {
                return  llvm::JITSymbol(nullptr);
            }
        );

        std::vector<std::unique_ptr<llvm::Module>> modules;
        modules.emplace_back(std::move(m));
        _optimize_layer.addModuleSet(std::move(modules),
                                     std::make_unique<llvm::SectionMemoryManager>(),
                                     std::move(resolver));
    }

    template<typename FunctionPointer>
    FunctionPointer  find_symbol(const std::string& name) {
        std::string mangled_name;
        llvm::raw_string_ostream mangled_name_stream(mangled_name);
        llvm::Mangler::getNameWithPrefix(mangled_name_stream, name, _data_layout);
        auto sym = _optimize_layer.findSymbol(mangled_name_stream.str(), true);
        auto ptr = reinterpret_cast<void*>(sym.getAddress());
        logger.info("module@{}: {} is at {}", this, name, ptr);
        return reinterpret_cast<FunctionPointer>(ptr);
    }

    std::unique_ptr<llvm::Module> optimize(std::unique_ptr<llvm::Module> m) {
        logger.info("module@{} before:", this);
        m->dump();

        auto fpm = std::make_unique<llvm::legacy::FunctionPassManager>(m.get());
        auto pm = std::make_unique<llvm::legacy::PassManager>();

        auto fpmb = llvm::PassManagerBuilder();
        fpmb.OptLevel = 3;
        fpmb.populateFunctionPassManager(*fpm);

        fpm->doInitialization();

        for (auto&& fn : *m) {
            fpm->run(fn);
        }

        auto lpmb = llvm::PassManagerBuilder();
        lpmb.OptLevel = 3;
        lpmb.populateLTOPassManager(*pm);

        pm->run(*m);

        logger.info("module@{} after:", this);
        m->dump();
        return m;
    }
};

std::unique_ptr<module> module::create(abstract_type& t)
{
    logger.info("compiling tri_compare for type \"{}\"", t.name());

    context ctx(t.name());

    {
        std::vector<llvm::Type*> args(1);

        args[0] = llvm::Type::getInt16Ty(ctx._context);
        auto fn_type = llvm::FunctionType::get(llvm::Type::getInt16Ty(ctx._context), args, false);
        ctx._bswap16 = llvm::Function::Create(fn_type, llvm::Function::ExternalLinkage, "llvm.bswap.i16", ctx._module.get());

        args[0] = llvm::Type::getInt32Ty(ctx._context);
        fn_type = llvm::FunctionType::get(llvm::Type::getInt32Ty(ctx._context), args, false);
        ctx._bswap32 = llvm::Function::Create(fn_type, llvm::Function::ExternalLinkage, "llvm.bswap.i32", ctx._module.get());

        args[0] = llvm::Type::getInt64Ty(ctx._context);
        fn_type = llvm::FunctionType::get(llvm::Type::getInt64Ty(ctx._context), args, false);
        ctx._bswap64 = llvm::Function::Create(fn_type, llvm::Function::ExternalLinkage, "llvm.bswap.i64", ctx._module.get());
    }

    std::vector<llvm::Type*> arg_types = {
        llvm::Type::getVoidTy(ctx._context)->getPointerTo(),
        llvm::Type::getInt32Ty(ctx._context),
        llvm::Type::getVoidTy(ctx._context)->getPointerTo(),
        llvm::Type::getInt32Ty(ctx._context),
    };
    auto func_type = llvm::FunctionType::get(llvm::Type::getInt32Ty(ctx._context), arg_types, false);

    ctx._function = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, "tri_compare", ctx._module.get());

    auto block = llvm::BasicBlock::Create(ctx._context, "entry", ctx._function);
    ctx._builder.SetInsertPoint(block);

    auto arg_it = ctx._function->args().begin();
    arg_it->setName("a_ptr");
    ctx._a_ptr = &*arg_it++;
    arg_it->setName("a_len");
    ctx._a_len = &*arg_it++;
    arg_it->setName("b_ptr");
    ctx._b_ptr = &*arg_it++;
    arg_it->setName("b_len");
    ctx._b_len = &*arg_it;

    ctx._return_value = ctx._builder.CreateAlloca(llvm::Type::getInt32Ty(ctx._context), nullptr, "return_value");

    ctx._return_block = llvm::BasicBlock::Create(ctx._context, "return_block");
    ctx._builder.SetInsertPoint(ctx._return_block);
    ctx._builder.CreateRet(ctx._builder.CreateLoad(ctx._return_value, "return_value"));

    ctx._builder.SetInsertPoint(block);

    ctx._continue_block = llvm::BasicBlock::Create(ctx._context, "continue_block");
    t.generate_comparator(ctx);
    ctx._function->getBasicBlockList().push_back(ctx._continue_block);
    ctx._builder.SetInsertPoint(ctx._continue_block);

    auto result_zero = llvm::ConstantInt::get(ctx._context, llvm::APInt(32, 0, true));
    ctx._builder.CreateStore(result_zero, ctx._return_value);
    ctx._builder.CreateBr(ctx._return_block);

    ctx._function->getBasicBlockList().push_back(ctx._return_block);

    auto m = std::make_unique<module>();
    m->_impl = std::make_unique<module::impl>(std::move(ctx._module));
    m->_tri_compare = m->_impl->find_symbol<decltype(m->_tri_compare)>("tri_compare");
    return m;
}

std::unique_ptr<module> module::create_for_compound(std::vector<abstract_type*> ts)
{
    // A lot of code duplication here.

    auto name = ::join(",", ts | boost::adaptors::transformed(std::mem_fn(&abstract_type::name)));
    logger.info("compiling prefix_equality_tri_compare for compound type \"({})\"", name);

    context ctx(name);

    {
        std::vector<llvm::Type*> args(1);

        args[0] = llvm::Type::getInt16Ty(ctx._context);
        auto fn_type = llvm::FunctionType::get(llvm::Type::getInt16Ty(ctx._context), args, false);
        ctx._bswap16 = llvm::Function::Create(fn_type, llvm::Function::ExternalLinkage, "llvm.bswap.i16", ctx._module.get());

        args[0] = llvm::Type::getInt32Ty(ctx._context);
        fn_type = llvm::FunctionType::get(llvm::Type::getInt32Ty(ctx._context), args, false);
        ctx._bswap32 = llvm::Function::Create(fn_type, llvm::Function::ExternalLinkage, "llvm.bswap.i32", ctx._module.get());

        args[0] = llvm::Type::getInt64Ty(ctx._context);
        fn_type = llvm::FunctionType::get(llvm::Type::getInt64Ty(ctx._context), args, false);
        ctx._bswap64 = llvm::Function::Create(fn_type, llvm::Function::ExternalLinkage, "llvm.bswap.i64", ctx._module.get());
    }

    std::vector<llvm::Type*> arg_types = {
            llvm::Type::getVoidTy(ctx._context)->getPointerTo(),
            llvm::Type::getInt8PtrTy(ctx._context),
            llvm::Type::getInt32Ty(ctx._context),
            llvm::Type::getInt8PtrTy(ctx._context),
            llvm::Type::getInt32Ty(ctx._context),
    };
    auto func_type = llvm::FunctionType::get(llvm::Type::getInt32Ty(ctx._context), arg_types, false);

    ctx._function = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, "prefix_equality_tri_compare", ctx._module.get());

    auto block = llvm::BasicBlock::Create(ctx._context, "entry", ctx._function);
    ctx._builder.SetInsertPoint(block);

    auto arg_it = ctx._function->args().begin();
    arg_it++; // first argument is type for fallback comparators
    arg_it->setName("a_ptr");
    llvm::Value* a_ptr = &*arg_it++;
    arg_it->setName("a_len");
    auto a_len = &*arg_it++;
    arg_it->setName("b_ptr");
    llvm::Value* b_ptr = &*arg_it++;
    arg_it->setName("b_len");
    auto b_len = &*arg_it;

    ctx._return_value = ctx._builder.CreateAlloca(llvm::Type::getInt32Ty(ctx._context), nullptr, "return_value");

    ctx._return_block = llvm::BasicBlock::Create(ctx._context, "return_block");
    ctx._builder.SetInsertPoint(ctx._return_block);
    ctx._builder.CreateRet(ctx._builder.CreateLoad(ctx._return_value, "return_value"));

    ctx._builder.SetInsertPoint(block);

    auto a_ptr_end = ctx._builder.CreateGEP(a_ptr, a_len, "a_ptr_end");
    auto b_ptr_end = ctx._builder.CreateGEP(b_ptr, b_len, "b_ptr_end");

    auto const2 = llvm::ConstantInt::get(ctx._context, llvm::APInt(32, 2));

    auto end_equal_block = llvm::BasicBlock::Create(ctx._context, "end_equal_block");

    for (auto&& t : ts) {
        auto a_end = ctx._builder.CreateICmpEQ(a_ptr, a_ptr_end, "a_end");
        auto a_not_end = llvm::BasicBlock::Create(ctx._context, "a_not_end", ctx._function);
        ctx._builder.CreateCondBr(a_end, end_equal_block, a_not_end);
        ctx._builder.SetInsertPoint(a_not_end);

        auto b_end = ctx._builder.CreateICmpEQ(b_ptr, b_ptr_end, "b_end");
        auto b_not_end = llvm::BasicBlock::Create(ctx._context, "b_not_end", ctx._function);
        ctx._builder.CreateCondBr(b_end, end_equal_block, b_not_end);
        ctx._builder.SetInsertPoint(b_not_end);

        auto a_ptr_16 = ctx._builder.CreateBitCast(a_ptr, llvm::Type::getInt16PtrTy(ctx._context), "a_ptr16");
        auto a1_len_be = ctx._builder.CreateAlignedLoad(a_ptr_16, 1, "a1_len16_be");

        std::vector<llvm::Value*> values(1);
        values[0] = a1_len_be;
        auto a1_len = ctx._builder.CreateSExt(ctx._builder.CreateCall(ctx._bswap16, values, "a1_len16"), llvm::Type::getInt32Ty(ctx._context), "a1_len");
        a_ptr = ctx._builder.CreateGEP(a_ptr, const2, "a1_ptr");

        auto b_ptr_16 = ctx._builder.CreateBitCast(b_ptr, llvm::Type::getInt16PtrTy(ctx._context), "b_ptr16");
        auto b1_len_be = ctx._builder.CreateAlignedLoad(b_ptr_16, 1, "b1_len16_be");

        values[0] = b1_len_be;
        auto b1_len = ctx._builder.CreateSExt(ctx._builder.CreateCall(ctx._bswap16, values, "b1_len16"), llvm::Type::getInt32Ty(ctx._context), "b1_len");
        b_ptr = ctx._builder.CreateGEP(b_ptr, const2, "b1_ptr");

        ctx._a_len = a1_len;
        ctx._a_ptr = a_ptr;
        ctx._b_len = b1_len;
        ctx._b_ptr = b_ptr;

        ctx._continue_block = llvm::BasicBlock::Create(ctx._context, "continue_block");
        t->generate_comparator(ctx);
        ctx._function->getBasicBlockList().push_back(ctx._continue_block);
        ctx._builder.SetInsertPoint(ctx._continue_block);

        a_ptr = ctx._builder.CreateGEP(a_ptr, a1_len, "a1_ptr");
        b_ptr = ctx._builder.CreateGEP(b_ptr, b1_len, "a1_ptr");
    }

    ctx._builder.CreateBr(end_equal_block);

    ctx._function->getBasicBlockList().push_back(end_equal_block);
    ctx._builder.SetInsertPoint(end_equal_block);
    auto result_zero = llvm::ConstantInt::get(ctx._context, llvm::APInt(32, 0, true));
    ctx._builder.CreateStore(result_zero, ctx._return_value);
    ctx._builder.CreateBr(ctx._return_block);

    ctx._function->getBasicBlockList().push_back(ctx._return_block);

    auto m = std::make_unique<module>();
    m->_impl = std::make_unique<module::impl>(std::move(ctx._module));
    m->_prefix_equality_tri_compare = m->_impl->find_symbol<decltype(m->_prefix_equality_tri_compare)>("prefix_equality_tri_compare");
    return m;
}


module::module() { }
module::~module() { }

void code_generator::initialize()
{
    LLVMInitializeNativeTarget();
    LLVMInitializeNativeAsmPrinter();
}

}

