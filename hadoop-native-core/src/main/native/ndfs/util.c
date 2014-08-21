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

#include "common/hadoop_err.h"
#include "common/string.h"
#include "common/uri.h"
#include "fs/common.h"
#include "fs/fs.h"
#include "ndfs/meta.h"
#include "protobuf/ClientNamenodeProtocol.call.h"
#include "protobuf/hdfs.pb-c.h.s"
#include "rpc/messenger.h"
#include "rpc/proxy.h"

#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <uv.h>

#define CLIENT_NN_PROTOCOL "org.apache.hadoop.hdfs.protocol.ClientProtocol"

void ndfs_nn_proxy_init(struct native_fs *fs, struct hrpc_proxy *proxy)
{
    hrpc_proxy_init(proxy, fs->msgr, &fs->nn_addr, CLIENT_NN_PROTOCOL,
                    fs->conn_uri->user_info);
}

struct hadoop_err *build_path(struct native_fs *fs, const char *uri_str,
                                     char **out)
{
    struct hadoop_err *err = NULL;
    struct hadoop_uri *uri = NULL;

    uv_mutex_lock(&fs->working_uri_lock);
    err = hadoop_uri_parse(uri_str, fs->working_uri, &uri, H_URI_PARSE_PATH);
    if (err)
        goto done;
    // TODO: check URI scheme and user against saved values?
    if (uri->path[0]) {
        *out = strdup(uri->path);
    } else {
        // As a special case, when the URI given has an empty path, we assume that
        // we want the current working directory.  This is to allow things like
        // hdfs://mynamenode to map to the current working directory, as they do in
        // Hadoop.  Note that this is different than hdfs://mynamenode/ (note the
        // trailing slash) which maps to the root directory.
        *out = strdup(fs->working_uri->path);
    }
    if (!*out) {
        err = hadoop_lerr_alloc(ENOMEM, "build_path: out of memory.");
        goto done;
    }
    err = NULL;

done:
    uv_mutex_unlock(&fs->working_uri_lock);
    hadoop_uri_free(uri);
    return err;
}

// vim: ts=4:sw=4:tw=79:et
