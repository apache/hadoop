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

#ifndef HADOOP_CORE_NDFS_META_H
#define HADOOP_CORE_NDFS_META_H

#include <netinet/in.h>
#include <uv.h>

struct hadoop_vfs_stats;

struct native_fs {
    /** Fields common to all filesystems. */
    struct hadoop_fs_base base;

    /**
     * Address of the namenode.
     * TODO: implement HA failover
     * TODO: implement IPv6
     */
    struct sockaddr_in nn_addr;

    /** The messenger used to perform RPCs. */
    struct hrpc_messenger *msgr;

    /** The default block size obtained from getServerDefaults. */
    int64_t default_block_size;

    /** Prefix to use when building URLs. */
    char *url_prefix;

    /** URI that was used when connecting. */
    struct hadoop_uri *conn_uri;

    /**
     * A dynamically allocated working directory which will be prepended to
     * all relative paths.
     */
    struct hadoop_uri *working_uri;

    /** Lock which protects the working_uri. */
    uv_mutex_t working_uri_lock;

    /** Umask to use when creating files */
    uint32_t umask;

    /** How long to wait, in nanoseconds, before re-trying a dead datanode. */
    uint64_t dead_dn_timeout_ns;
};

struct hadoop_err *ndfs_connect(struct hdfsBuilder *hdfs_bld,
                                struct hdfs_internal **out);
int ndfs_disconnect(hdfsFS bfs);
int ndfs_file_exists(hdfsFS bfs, const char *uri);
int ndfs_unlink(struct hdfs_internal *bfs,
                const char *uri, int recursive);
int ndfs_rename(hdfsFS bfs, const char *src_uri, const char *dst_uri);
char* ndfs_get_working_directory(hdfsFS bfs, char *buffer, size_t bufferSize);
int ndfs_set_working_directory(hdfsFS bfs, const char* uri_str);
int ndfs_mkdir(hdfsFS bfs, const char* uri);
int ndfs_set_replication(hdfsFS bfs, const char* uri, int16_t replication);
hdfsFileInfo* ndfs_list_directory(hdfsFS bfs, const char* uri, int *numEntries);
hdfsFileInfo *ndfs_get_path_info(hdfsFS bfs, const char* uri);
tOffset ndfs_get_default_block_size(hdfsFS bfs);
tOffset ndfs_get_default_block_size_at_path(hdfsFS bfs, const char *uri);
struct hadoop_err *ndfs_statvfs(struct hadoop_fs_base *hfs,
        struct hadoop_vfs_stats *stats);
tOffset ndfs_get_capacity(hdfsFS bfs);
tOffset ndfs_get_used(hdfsFS bfs);
int ndfs_chown(hdfsFS bfs, const char* uri, const char *user,
               const char *group);
int ndfs_chmod(hdfsFS bfs, const char* uri, short mode);
int ndfs_utime(hdfsFS bfs, const char* uri, int64_t mtime, int64_t atime);

#endif

// vim: ts=4:sw=4:tw=79:et
