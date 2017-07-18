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

    t.generate_comparator(ctx);
    ctx._function->getBasicBlockList().push_back(ctx._return_block);

    auto m = std::make_unique<module>();
    m->_impl = std::make_unique<module::impl>(std::move(ctx._module));
    m->_tri_compare = m->_impl->find_symbol<decltype(m->_tri_compare)>("tri_compare");
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

