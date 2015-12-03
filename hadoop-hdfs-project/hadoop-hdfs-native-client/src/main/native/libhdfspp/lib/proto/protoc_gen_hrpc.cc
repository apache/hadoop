/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "protobuf/cpp_helpers.h"

#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/stubs/common.h>

#include <memory>

using ::google::protobuf::FileDescriptor;
using ::google::protobuf::MethodDescriptor;
using ::google::protobuf::ServiceDescriptor;
using ::google::protobuf::compiler::CodeGenerator;
using ::google::protobuf::compiler::GeneratorContext;
using ::google::protobuf::io::Printer;
using ::google::protobuf::io::ZeroCopyOutputStream;

class StubGenerator : public CodeGenerator {
public:
  virtual bool Generate(const FileDescriptor *file, const std::string &,
                        GeneratorContext *ctx,
                        std::string *error) const override;

private:
  void EmitService(const ServiceDescriptor *service, Printer *out) const;
  void EmitMethod(const MethodDescriptor *method, Printer *out) const;
};

bool StubGenerator::Generate(const FileDescriptor *file, const std::string &,
                             GeneratorContext *ctx, std::string *) const {
  namespace pb = ::google::protobuf;
  std::unique_ptr<ZeroCopyOutputStream> os(
      ctx->Open(StripProto(file->name()) + ".hrpc.inl"));
  Printer out(os.get(), '$');
  for (int i = 0; i < file->service_count(); ++i) {
    const ServiceDescriptor *service = file->service(i);
    EmitService(service, &out);
  }
  return true;
}

void StubGenerator::EmitService(const ServiceDescriptor *service,
                                Printer *out) const {
  out->Print("\n// GENERATED AUTOMATICALLY. DO NOT MODIFY.\n"
             "class $service$ {\n"
             "private:\n"
             "  ::hdfs::RpcEngine *const engine_;\n"
             "public:\n"
             "  typedef std::function<void(const ::hdfs::Status &)> Callback;\n"
             "  typedef ::google::protobuf::MessageLite Message;\n"
             "  inline $service$(::hdfs::RpcEngine *engine)\n"
             "    : engine_(engine) {}\n",
             "service", service->name());
  for (int i = 0; i < service->method_count(); ++i) {
    const MethodDescriptor *method = service->method(i);
    EmitMethod(method, out);
  }
  out->Print("};\n");
}

void StubGenerator::EmitMethod(const MethodDescriptor *method,
                               Printer *out) const {
  out->Print(
      "\n  inline void $camel_method$(const Message *req, "
      "const std::shared_ptr<Message> &resp, "
      "const Callback &handler) {\n"
      "    engine_->AsyncRpc(\"$method$\", req, resp, handler);\n"
      "  }\n",
      "camel_method", ToCamelCase(method->name()), "method", method->name());
}

int main(int argc, char *argv[]) {
  StubGenerator generator;
  return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
