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

#include <ctype.h>
#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/stubs/common.h>

#include <cstddef>
#ifdef _LIBCPP_VERSION
#include <memory>
#else
#include <tr1/memory>
#endif

#include <iostream>
#include <map>
#include <stdio.h>
#include <string>

#define PROTO_EXTENSION ".proto"

#define APACHE_HEADER \
"/**\n" \
" * Licensed to the Apache Software Foundation (ASF) under one\n" \
" * or more contributor license agreements.  See the NOTICE file\n" \
" * distributed with this work for additional information\n" \
" * regarding copyright ownership.  The ASF licenses this file\n" \
" * to you under the Apache License, Version 2.0 (the\n" \
" * \"License\"); you may not use this file except in compliance\n" \
" * with the License.  You may obtain a copy of the License at\n" \
" *\n" \
" *     http://www.apache.org/licenses/LICENSE-2.0\n" \
" *\n" \
" * Unless required by applicable law or agreed to in writing, software\n" \
" * distributed under the License is distributed on an \"AS IS\" BASIS,\n" \
" * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" \
" * See the License for the specific language governing permissions and\n" \
" * limitations under the License.\n" \
" */\n"

using google::protobuf::FileDescriptor;
using google::protobuf::MethodDescriptor;
using google::protobuf::ServiceDescriptor;
using google::protobuf::compiler::GeneratorContext;
using google::protobuf::io::Printer;
using std::map;
using std::string;
#ifdef _LIBCPP_VERSION
using std::shared_ptr;
#else
using std::tr1::shared_ptr;
#endif
typedef map<string, string> string_map_t;

static string camel_case_to_uscore(const string &in)
{
    string out;
    bool prev_lower = false;

    for (size_t i = 0; i < in.size(); i++) {
        char c = in[i];
        if (isupper(c)) {
            if (prev_lower) {
                out += "_";
            }
            prev_lower = false;
        } else if (islower(c) || isdigit(c)) {
            prev_lower = true;
        } else {
            prev_lower = false;
        }
        out += tolower(c);
    }
    return out;
}

static bool try_strip_suffix(const string &str, const char *suffix,
                                 string *out)
{
    size_t suffix_len = strlen(suffix);

    if (str.size() < suffix_len) {
        return false;
    }
    *out = str.substr(0, str.size() - suffix_len);
    return true;
}

static void get_base_name(const string &path, string *base)
{
    size_t last_slash = path.find_last_of("/");
    if (last_slash != string::npos) {
        *base = path.substr(last_slash + 1);
    } else {
        *base = path;
    }
}

static string set_path_substitutions(const FileDescriptor *file,
                                     string_map_t *map)
{
    string path = file->name();
    (*map)["path"] = path;
    // Initialize path_
    // If path = /foo/bar/baz_stuff.proto, path_ = /foo/bar/baz_stuff
    string path_without_extension;
    if (!try_strip_suffix(path, PROTO_EXTENSION, &path_without_extension)) {
        return string("file name " + path + " did not end in " +
                      PROTO_EXTENSION);
    }
    (*map)["path_without_extension"] = path_without_extension;

    // If path = /foo/bar/baz_stuff.proto, base_ = baz_stuff
    string base;
    get_base_name(path_without_extension, &base);
    (*map)["path_base"] = base;
    (*map)["function_prefix"] = base;
    return "";
}

static string shorten_service_prefix(const string &prefix)
{
    if (prefix == "ClientNamenodeProtocol") {
        return "cnn";
    } else if (prefix == "ClientDatanodeProtocolService") {
        return "cdn";
    } else if (prefix == "NamenodeProtocolService") {
        return "nnp";
    } else if (prefix == "DatanodeProtocolService") {
        return "dn";
    } else {
        return prefix;
    }
}

static void set_service_substitutions(const ServiceDescriptor *service,
                                     string_map_t *map)
{
    // Service prefix.
    // example: cnn
    (*map)["service_prefix"] = shorten_service_prefix(service->name());
}
 
/**
 * Process a dot-separated type name into a protobuf-c type name.
 *
 * @param input             The input type name.
 *
 * @return                  The protobuf-c type name.
 */
static string get_pbc_type_name(string input)
{
    char *word, *ptr = NULL;
    string output, prefix;
    char line[input.size() + 1];
    strcpy(line, input.c_str());

    for (word = strtok_r(line, ".", &ptr); word; 
             word = strtok_r(NULL, ".", &ptr)) {
        //fprintf(stderr, "word = %s\n", word);
        if (!isupper(word[0])) {
            word[0] = toupper(word[0]);
        }
        output += prefix;
        prefix = "__";
        output += word;
    }
    return output;
}

static string replace(string input, char target,
                      const char *replacement)
{
    string output;

    for (size_t i = 0; i < input.size(); i++) {
        if (input[i] == target) {
            output += replacement;
        } else {
            output += input[i];
        }
    }
    return output;
}

static void set_method_substitutions(const MethodDescriptor *method,
                                     string_map_t *map)
{
    // Request type, in camelcase.
    // example: Hadoop__Hdfs__SetReplicationRequestProto 
    (*map)["req_ty_camel"] =
        get_pbc_type_name(method->input_type()->full_name());

    // Request type, in underscore-separated lowercase.
    // example: hadoop__hdfs__set_replication_request_proto
    (*map)["req_ty_uscore"] = camel_case_to_uscore((*map)["req_ty_camel"]);

    // Response type, in camelcase.
    // example: Hadoop__Hdfs__SetReplicationResponseProto 
    (*map)["resp_ty_camel"] =
        get_pbc_type_name(method->output_type()->full_name());

    // Response type, in underscore-separated lowercase.
    // example: hadoop__hdfs__set_replication_response_proto
    (*map)["resp_ty_uscore"] = camel_case_to_uscore((*map)["resp_ty_camel"]);

    // RPC name, in camelcase.
    // example: setReplication
    (*map)["rpc_camel"] = method->name();

    // RPC name, in underscore-separated lowercase.
    // example: setReplication
    (*map)["rpc_uscore"] = camel_case_to_uscore((*map)["rpc_camel"]);

    // sync stub function name.
    // example: cnn_set_replication
    (*map)["sync_call"] =
        (*map)["service_prefix"] + "_" + (*map)["rpc_uscore"];

    // async stub function name.
    // example: cnn_async_set_replication
    (*map)["async_call"] =
        (*map)["service_prefix"] + "_async_" + (*map)["rpc_uscore"];

    // async callback adaptor function name.
    // example: cnn_async_adaptor_set_replication
    (*map)["async_adaptor"] =
        (*map)["service_prefix"] + "_async_adaptor_" + (*map)["rpc_uscore"];
}

class HrpcCodeGenerator
    : public ::google::protobuf::compiler::CodeGenerator
{
public:
    HrpcCodeGenerator()
    {
    }

    ~HrpcCodeGenerator()
    {
    }

    bool Generate(const google::protobuf::FileDescriptor *file,
        const string &, GeneratorContext *gen_context,
        string *error) const
    {
        string_map_t path_map;
        string ret = set_path_substitutions(file, &path_map);
        if (!ret.empty()) {
            *error = ret;
            return false;
        }
        generate_call_header(gen_context, &path_map, file);
        generate_call_body(gen_context, &path_map, file);
        return true;
    }

private:
    void generate_call_header(GeneratorContext *gen_context,
                              string_map_t *path_map,
                              const FileDescriptor *file) const
    {
        shared_ptr<google::protobuf::io::ZeroCopyOutputStream> output(
            gen_context->Open((*path_map)["path_without_extension"] +
                              ".call.h"));
        Printer printer(output.get(), '$');
        printer.Print(APACHE_HEADER);
        printer.Print(*path_map,
"\n"
"// This header file was auto-generated from $path$\n"
"\n"
"#ifndef HADOOP_NATIVE_CORE_$path_base$_CALL_H\n"
"#define HADOOP_NATIVE_CORE_$path_base$_CALL_H\n"
"\n"
"#include \"protobuf/$path_base$.pb-c.h\"\n"
"#include \"protobuf/$path_base$.pb-c.h.s\"\n"
"\n"
"struct hadoop_err;\n"
"struct hrpc_proxy;\n"
"\n");
        for (int service_idx = 0; service_idx < file->service_count();
                    ++service_idx) {
            string_map_t service_map = *path_map;
            const ServiceDescriptor *service = file->service(service_idx);
            set_service_substitutions(service, &service_map);
            for (int method_idx = 0; method_idx < service->method_count();
                    ++method_idx) {
                const MethodDescriptor *method = service->method(method_idx);
                string_map_t map = service_map;
                set_method_substitutions(method, &map);
                printer.Print(map,
"struct hadoop_err *$sync_call$(struct hrpc_proxy *proxy,\n"
"               const $req_ty_camel$ *req,\n"
"               $resp_ty_camel$ **resp);\n"
"\n"
"void $async_call$(struct hrpc_proxy *proxy,\n"
"    const $req_ty_camel$ *req,\n"
"    void (*cb)($resp_ty_camel$ *,\n"
"        struct hadoop_err *, void *cb_data),\n"
"    void *cb_data);\n"
"\n");
            }
        }
        printer.Print("#endif\n");
    }

    void generate_call_body(GeneratorContext *gen_context,
                            string_map_t *path_map,
                            const FileDescriptor *file) const
    {
        shared_ptr<google::protobuf::io::ZeroCopyOutputStream> output(
            gen_context->Open((*path_map)["path_without_extension"] +
                              ".call.c"));
        Printer printer(output.get(), '$');
        printer.Print(APACHE_HEADER);
        printer.Print(*path_map, "\n"
"#include \"common/hadoop_err.h\"\n"
"#include \"protobuf/$path_base$.call.h\"\n"
"#include \"rpc/messenger.h\"\n"
"#include \"rpc/proxy.h\"\n");
        printer.Print("\n"
"#include <errno.h>\n"
"#include <netinet/in.h>\n"
"#include <stdio.h>\n"
"#include <stdlib.h>\n"
"#include <string.h>\n"
"#include <uv.h>\n"
"\n");
        for (int service_idx = 0; service_idx < file->service_count();
                    ++service_idx) {
            string_map_t service_map = *path_map;
            const ServiceDescriptor *service = file->service(service_idx);
            set_service_substitutions(service, &service_map);
            for (int method_idx = 0; method_idx < service->method_count();
                    ++method_idx) {
                const MethodDescriptor *method = service->method(method_idx);
                string_map_t map = service_map;
                set_method_substitutions(method, &map);
                printer.Print(map,
"struct hadoop_err *$sync_call$(struct hrpc_proxy *proxy,\n"
"    const $req_ty_camel$ *req,\n"
"    $resp_ty_camel$ **out)\n"
"{\n"
"    struct hadoop_err *err;\n"
"    struct hrpc_sync_ctx *ctx;\n"
"    $resp_ty_camel$ *resp;\n"
"\n"
"    err = hrpc_proxy_activate(proxy);\n"
"    if (err) {\n"
"        return err;\n"
"    }\n"
"    ctx = hrpc_proxy_alloc_sync_ctx(proxy);\n"
"    if (!ctx) {\n"
"        hrpc_proxy_deactivate(proxy);\n"
"        return hadoop_lerr_alloc(ENOMEM, \"$sync_call$: \"\n"
"            \"failed to allocate sync_ctx\");\n"
"    }\n"
"    hrpc_proxy_start(proxy, \"$rpc_camel$\", req,\n"
"        $req_ty_uscore$__get_packed_size(req),\n"
"        (hrpc_pack_cb_t)$req_ty_uscore$__pack,\n"
"        hrpc_proxy_sync_cb, ctx);\n"
"    uv_sem_wait(&ctx->sem);\n"
"    if (ctx->err) {\n"
"        err = ctx->err;\n"
"        hrpc_release_sync_ctx(ctx);\n"
"        return err;\n"
"    }\n"
"    resp = $resp_ty_uscore$__unpack(NULL, ctx->resp.pb_len,\n"
"                                                  ctx->resp.pb_base);\n"
"    hrpc_release_sync_ctx(ctx);\n"
"    if (!resp) {\n"
"        return hadoop_lerr_alloc(EINVAL,\n"
"           \"$sync_call$: failed to parse response from server\");\n"
"    }\n"
"    *out = resp;\n"
"    return NULL;\n"
"}\n");
                printer.Print(map,
"struct $async_call$_cb_data {\n"
"    void (*cb)($resp_ty_camel$ *,\n"
"        struct hadoop_err *, void *);\n"
"    void *cb_data;\n"
"};\n"
"\n"
"void $async_adaptor$(struct hrpc_response *resp,\n"
"                        struct hadoop_err *err, void *cb_data)\n"
"{\n"
"    struct $async_call$_cb_data *wrapped = cb_data;\n"
"    $resp_ty_camel$ *msg;\n"
"\n"
"    if (err) {\n"
"        wrapped->cb(NULL, err, wrapped->cb_data);\n"
"        return;\n"
"    }\n"
"    msg = $resp_ty_uscore$__unpack(NULL, resp->pb_len,\n"
"                                                  resp->pb_base);\n"
"    free(resp->base);\n"
"    if (!msg) {\n"
"        wrapped->cb(NULL, hadoop_lerr_alloc(EIO,\n"
"            \"$async_adaptor$: \"\n"
"            \"failed to parse response from server.\"), wrapped->cb_data);\n"
"        return;\n"
"    }\n"
"    wrapped->cb(msg, NULL, wrapped->cb_data);\n"
"}\n");
                printer.Print(map,
"void $async_call$(struct hrpc_proxy *proxy,\n"
"    const $req_ty_camel$ *req,\n"
"    void (*cb)($resp_ty_camel$ *,\n"
"        struct hadoop_err *, void *),\n"
"    void *cb_data)\n"
"{\n"
"    struct $async_call$_cb_data *wrapped;\n"
"    struct hadoop_err *err;\n"
"\n"
"    err = hrpc_proxy_activate(proxy);\n"
"    if (err) {\n"
"        cb(NULL, err, cb_data);\n"
"        return;\n"
"    }\n"
"    wrapped = hrpc_proxy_alloc_userdata(proxy, sizeof(*wrapped));\n"
"    if (!wrapped) {\n"
"        hrpc_proxy_deactivate(proxy);\n"
"        cb(NULL, hadoop_lerr_alloc(ENOMEM, \"$async_call$: failed \"\n"
"                                 \"to allocate sync_ctx\"), cb_data);\n"
"        return;\n"
"    }\n"
"    wrapped->cb = cb;\n"
"    wrapped->cb_data = cb_data;\n"
"    hrpc_proxy_start(proxy, \"$rpc_camel$\", req, \n"
"        $req_ty_uscore$__get_packed_size(req),\n"
"        (hrpc_pack_cb_t)$req_ty_uscore$__pack,\n"
"        $async_adaptor$, wrapped);\n"
"}\n"
"\n");
            }
        }
        printer.Print("// vim: ts=4:sw=4:tw=79:et\n");
    }
};

int main(int argc, char *argv[])
{
  HrpcCodeGenerator generator;
  return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}

// vim: ts=4:sw=4:tw=79:et
