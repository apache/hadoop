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
#include "common/hconf.h"
#include "common/net.h"
#include "common/string.h"
#include "common/uri.h"
#include "fs/common.h"
#include "fs/fs.h"
#include "protobuf/ClientNamenodeProtocol.call.h"
#include "protobuf/hdfs.pb-c.h.s"
#include "rpc/messenger.h"
#include "rpc/proxy.h"

#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <uriparser/Uri.h>
#include <uv.h>

#define DEFAULT_NN_PORT 8020

#define CLIENT_NN_PROTOCOL "org.apache.hadoop.hdfs.protocol.ClientProtocol"

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

    /** User name to use for RPCs.  Immutable. */
    char *user_name;

    /**
     * A dynamically allocated working directory which will be prepended to
     * all relative paths.
     */
    UriUriA *working_uri; 

    /** Lock which protects the working_uri. */
    uv_mutex_t working_uri_lock;
};

/**
 * Set if the file is read-only... otherwise, the file is assumed to be
 * write-only.
 */
#define NDFS_FILE_FLAG_RO                       0x1

/** This flag is for compatibility with some old test harnesses. */
#define NDFS_FILE_FLAG_DISABLE_DIRECT_READ      0x2

/** Base class for both read-only and write-only files. */
struct native_file_base {
    /** Fields common to all filesystems. */
    struct hadoop_file_base base;

    /** NDFS file flags. */
    int flags;
};

/** A read-only file. */
struct native_ro_file {
    struct native_file_base base;
    uint64_t bytes_read;
};

/** A write-only file. */
struct native_wo_file {
    struct native_file_base base;
};

/** Whole-filesystem stats sent back from the NameNode. */
struct hadoop_vfs_stats {
    int64_t capacity;
    int64_t used;
    int64_t remaining;
    int64_t under_replicated;
    int64_t corrupt_blocks;
    int64_t missing_blocks;
};

/** Server defaults sent back from the NameNode. */
struct ndfs_server_defaults {
    uint64_t blocksize;
};

static hdfsFileInfo *ndfs_get_path_info(hdfsFS bfs, const char* uri);

static void ndfs_nn_proxy_init(struct native_fs *fs, struct hrpc_proxy *proxy)
{
    hrpc_proxy_init(proxy, fs->msgr, &fs->nn_addr, CLIENT_NN_PROTOCOL,
                    fs->user_name);
}

/**
 * Construct a canonical path from a URI.
 *
 * @param fs        The filesystem.
 * @param uri       The URI.
 * @param out       (out param) the canonical path.
 *
 * @return          NULL on success; the error otherwise.
 */
static struct hadoop_err *build_path(struct native_fs *fs, const char *uri_str,
                                     char **out)
{
    char *path = NULL;
    struct hadoop_err *err = NULL;
    UriParserStateA uri_state;
    UriUriA uri;

    memset(&uri_state, 0, sizeof(uri_state));

    uv_mutex_lock(&fs->working_uri_lock);
    err = uri_parse(uri_str, &uri_state, &uri, fs->working_uri);
    if (err)
        goto done;
    // TODO: check URI scheme and user against saved values?
    err = uri_get_path(&uri, &path);
    if (err)
        goto done;
    // As a special case, when the URI given has an empty path, we assume that
    // we want the current working directory.  This is to allow things like
    // hdfs://mynamenode to map to the current working directory, as they do in
    // Hadoop.  Note that this is different than hdfs://mynamenode/ (note the
    // trailing slash) which maps to the root directory.
    if (!path[0]) {
        free(path);
        path = NULL;
        err = uri_get_path(fs->working_uri, &path);
        if (err) {
            goto done;
        }
    }
    err = NULL;

done:
    uv_mutex_unlock(&fs->working_uri_lock);
    if (uri_state.uri) {
        uriFreeUriMembersA(&uri);
    }
    if (err) {
        free(path);
        return err;
    }
    *out = path;
    return NULL;
}

static int ndfs_file_is_open_for_read(hdfsFile bfile)
{
    struct native_file_base *file = (struct native_file_base *)bfile;
    return !!(file->flags & NDFS_FILE_FLAG_RO);
}

static int ndfs_file_is_open_for_write(hdfsFile bfile)
{
    struct native_file_base *file = (struct native_file_base *)bfile;
    return !(file->flags & NDFS_FILE_FLAG_RO);
}

static int ndfs_file_get_read_statistics(hdfsFile bfile,
            struct hdfsReadStatistics **out)
{
    struct hdfsReadStatistics *stats;
    struct native_ro_file *file = (struct native_ro_file *)bfile;

    if (!(file->base.flags & NDFS_FILE_FLAG_RO)) {
        errno = EINVAL;
        return -1;
    }
    stats = calloc(1, sizeof(*stats));
    if (!stats) {
        errno = ENOMEM; 
        return -1;
    }
    stats->totalBytesRead = file->bytes_read;
    *out = stats;
    return 0;
}

static struct hadoop_err *ndfs_get_server_defaults(struct native_fs *fs,
            struct ndfs_server_defaults *defaults)
{
    struct hadoop_err *err = NULL;
    GetServerDefaultsRequestProto req =
        GET_SERVER_DEFAULTS_REQUEST_PROTO__INIT;
    GetServerDefaultsResponseProto *resp = NULL;
    struct hrpc_proxy proxy;

    ndfs_nn_proxy_init(fs, &proxy);
    err = cnn_get_server_defaults(&proxy, &req, &resp);
    if (err) {
        goto done;
    }
    defaults->blocksize = resp->serverdefaults->blocksize;

done:
    if (resp) {
        get_server_defaults_response_proto__free_unpacked(resp, NULL);
    }
    return err;
}

/**
 * Parse an address in the form <hostname> or <hostname>:<port>.
 *
 * @param host          The hostname
 * @param addr          (out param) The sockaddr.
 * @param default_port  The default port to use, if one is not found in the
 *                          string.
 *
 * @return              NULL on success; the error otherwise.
 */
static struct hadoop_err *parse_rpc_addr(const char *input,
            struct sockaddr_in *out, int default_port)
{
    struct hadoop_err *err = NULL;
    char *host, *colon;
    uint32_t addr;
    int port;

    fprintf(stderr, "parse_rpc_addr(input=%s, default_port=%d)\n",
            input, default_port);

    // If the URI doesn't contain a port, we use a default.
    // This may come either from the hdfsBuilder, or from the
    // 'default default' for HDFS.
    // It's kind of silly that hdfsBuilder even includes this field, since this
    // information should just be included in the URI, but this is here for
    // compatibility.
    port = (default_port <= 0) ? DEFAULT_NN_PORT : default_port;
    host = strdup(input);
    if (!host) {
        err = hadoop_lerr_alloc(ENOMEM, "parse_rpc_addr: OOM");
        goto done;
    }
    colon = index(host, ':');
    if (colon) {
        // If the URI has a colon, we parse the next part as a port.
        char *port_str = colon + 1;
        *colon = '\0';
        port = atoi(colon);
        if ((port <= 0) || (port >= 65536)) {
            err = hadoop_lerr_alloc(EINVAL, "parse_rpc_addr: invalid port "
                                    "string %s", port_str);
            goto done;
        }
    }
    err = get_first_ipv4_addr(host, &addr);
    if (err)
        goto done;
    out->sin_family = AF_INET;
    out->sin_port = htons(port);
    out->sin_addr.s_addr = htonl(addr);
done:
    free(host);
    return err;
}

static struct hadoop_err *get_namenode_addr(const struct hdfsBuilder *hdfs_bld,
            struct sockaddr_in *nn_addr)
{
    const char *nameservice_id;
    const char *rpc_addr;

    nameservice_id = hconf_get(hdfs_bld->hconf, "dfs.nameservice.id");
    if (nameservice_id) {
        return hadoop_lerr_alloc(ENOTSUP, "get_namenode_addr: we "
                "don't yet support HA or federated configurations.");
    }
    rpc_addr = hconf_get(hdfs_bld->hconf, "dfs.namenode.rpc-address");
    if (rpc_addr) {
        return parse_rpc_addr(rpc_addr, nn_addr, hdfs_bld->port);
    }
    return parse_rpc_addr(hdfs_bld->uri_authority, nn_addr, hdfs_bld->port);
}

struct hadoop_err *ndfs_connect(struct hdfsBuilder *hdfs_bld,
                                struct hdfs_internal **out)
{
    struct hadoop_err *err = NULL;
    struct native_fs *fs = NULL;
    struct hrpc_messenger_builder *msgr_bld;
    struct ndfs_server_defaults defaults;
    int working_dir_lock_created = 0;
    char *working_dir = NULL;
    UriParserStateA uri_state;

    fs = calloc(1, sizeof(*fs));
    if (!fs) {
        err = hadoop_lerr_alloc(ENOMEM, "failed to allocate space "
                                "for a native_fs structure.");
        goto done;
    }
    fs->base.ty = HADOOP_FS_TY_NDFS;
    fs->user_name = strdup(hdfs_bld->uri_user_info); 
    if (!fs->user_name) {
        err = hadoop_lerr_alloc(ENOMEM, "failed to allocate space "
                                "for the user name.");
        goto done;
    }
    msgr_bld = hrpc_messenger_builder_alloc();
    if (!msgr_bld) {
        err = hadoop_lerr_alloc(ENOMEM, "failed to allocate space "
                                "for a messenger builder.");
        goto done;
    }
    err = get_namenode_addr(hdfs_bld, &fs->nn_addr);
    if (err)
        goto done;
    err = hrpc_messenger_create(msgr_bld, &fs->msgr);
    if (err)
        goto done;
    // Get the default working directory
    if (asprintf(&working_dir, "%s:///user/%s/",
                 hdfs_bld->uri_scheme, hdfs_bld->uri_user_info) < 0) {
        working_dir = NULL;
        err = hadoop_lerr_alloc(ENOMEM, "ndfs_connect: OOM allocating "
                                "working_dir");
        goto done;
    }
    fs->working_uri = calloc(1, sizeof(*(fs->working_uri)));
    if (!fs->working_uri) {
        err = hadoop_lerr_alloc(ENOMEM, "ndfs_connect: OOM allocating "
                                "fs->working_uri");
        goto done;
    }
    err = uri_parse_abs(working_dir, &uri_state, fs->working_uri,
                        hdfs_bld->uri_scheme);
    if (err) {
        free(fs->working_uri);
        fs->working_uri = NULL;
        goto done;
    }
    if (uv_mutex_init(&fs->working_uri_lock) < 0) {
        err = hadoop_lerr_alloc(ENOMEM, "failed to create a mutex.");
        goto done;
    }
    working_dir_lock_created = 1;

    // Ask the NameNode about our server defaults.  We'll use this information
    // later in ndfs_get_default_block_size, and when writing new files.  Just
    // as important, tghis validates that we can talk to the NameNode with our
    // current configuration.
    memset(&defaults, 0, sizeof(defaults));
    err = ndfs_get_server_defaults(fs, &defaults);
    if (err)
        goto done;
    fs->default_block_size = defaults.blocksize;
    err = NULL;

done:
    free(working_dir);
    if (err) {
        if (fs) {
            free(fs->user_name);
            if (fs->working_uri) {
                uriFreeUriMembersA(fs->working_uri);
                free(fs->working_uri);
            }
            if (working_dir_lock_created) {
                uv_mutex_destroy(&fs->working_uri_lock);
            }
            free(fs);
        }
        return err; 
    }
    *out = (struct hdfs_internal *)fs;
    return NULL; 
}

static int ndfs_disconnect(hdfsFS bfs)
{
    struct native_fs *fs = (struct native_fs*)bfs;

    hrpc_messenger_shutdown(fs->msgr);
    hrpc_messenger_free(fs->msgr);
    free(fs->user_name);
    uriFreeUriMembersA(fs->working_uri);
    free(fs->working_uri);
    uv_mutex_destroy(&fs->working_uri_lock);
    free(fs);
    return 0;
}

static struct hadoop_err *ndfs_open_file_for_read(
        struct native_ro_file **out __attribute__((unused)),
        struct native_fs *fs __attribute__((unused)),
        const char *uri __attribute__((unused)))
{
    errno = ENOTSUP;
    return NULL;
}

static struct hadoop_err *ndfs_open_file_for_write(
        struct native_ro_file **out __attribute__((unused)),
        struct native_fs *fs __attribute__((unused)),
        const char *uri __attribute__((unused)),
        int buffer_size __attribute__((unused)),
        short replication __attribute__((unused)),
        tSize block_size __attribute__((unused)))
{
    errno = ENOTSUP;
    return NULL;
}

static hdfsFile ndfs_open_file(hdfsFS bfs, const char* uri, int flags, 
                      int buffer_size, short replication, tSize block_size)
{
    struct native_fs *fs = (struct native_fs *)bfs;
    struct native_ro_file *file = NULL;
    struct hadoop_err *err;
    int accmode;
    char *path = NULL;

    err = build_path(fs, uri, &path);
    if (err) {
        goto done;
    }
    accmode = flags & O_ACCMODE;
    if (accmode == O_RDONLY) {
        err = ndfs_open_file_for_read(&file, fs, path);
    } else if (accmode == O_WRONLY) {
        err = ndfs_open_file_for_write(&file, fs, path,
                        buffer_size, replication, block_size);
    } else {
        err = hadoop_lerr_alloc(EINVAL, "cannot open a hadoop file in "
                "mode 0x%x\n", accmode);
    }
done:
    free(path);
    return hadoopfs_errno_and_retptr(err, file);
}

static int ndfs_close_file(hdfsFS fs __attribute__((unused)),
                           hdfsFile bfile __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

static int ndfs_file_exists(hdfsFS bfs, const char *uri)
{
    static hdfsFileInfo *info;

    info = ndfs_get_path_info(bfs, uri);
    if (!info) {
        // errno will be set
        return -1;
    }
    hdfsFreeFileInfo(info, 1);
    return 0;
}

static int ndfs_seek(hdfsFS bfs __attribute__((unused)),
                     hdfsFile bfile __attribute__((unused)),
                     tOffset desiredPos __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

static tOffset ndfs_tell(hdfsFS bfs __attribute__((unused)),
                         hdfsFile bfile __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

static tSize ndfs_read(hdfsFS bfs __attribute__((unused)),
                       hdfsFile bfile __attribute__((unused)),
                       void *buffer __attribute__((unused)),
                       tSize length __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

static tSize ndfs_pread(hdfsFS bfs __attribute__((unused)),
            hdfsFile bfile __attribute__((unused)),
            tOffset position __attribute__((unused)),
            void* buffer __attribute__((unused)),
            tSize length __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

static tSize ndfs_write(hdfsFS bfs __attribute__((unused)),
            hdfsFile bfile __attribute__((unused)),
            const void* buffer __attribute__((unused)),
            tSize length __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

static int ndfs_flush(hdfsFS bfs __attribute__((unused)),
                      hdfsFile bfile __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

static int ndfs_hflush(hdfsFS bfs __attribute__((unused)),
                      hdfsFile bfile __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

static int ndfs_hsync(hdfsFS bfs __attribute__((unused)),
                      hdfsFile bfile __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

static int ndfs_available(hdfsFS bfs __attribute__((unused)),
                          hdfsFile bfile __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

static int ndfs_copy(hdfsFS srcFS __attribute__((unused)),
                     const char* src __attribute__((unused)),
                     hdfsFS dstFS __attribute__((unused)),
                     const char* dst __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

static int ndfs_move(hdfsFS srcFS __attribute__((unused)),
                     const char* src __attribute__((unused)),
                     hdfsFS dstFS __attribute__((unused)),
                     const char* dst __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

static int ndfs_unlink(struct hdfs_internal *bfs,
                const char *uri, int recursive)
{
    struct native_fs *fs = (struct native_fs*)bfs;
    struct hadoop_err *err = NULL;
    DeleteRequestProto req = DELETE_REQUEST_PROTO__INIT;
    struct hrpc_proxy proxy;
    DeleteResponseProto *resp = NULL;
    char *path = NULL;

    ndfs_nn_proxy_init(fs, &proxy);
    err = build_path(fs, uri, &path);
    if (err) {
        goto done;
    }
    req.src = path;
    req.recursive = !!recursive;
    err = cnn_delete(&proxy, &req, &resp);
    if (err) {
        goto done;
    }

done:
    free(path);
    if (resp) {
        delete_response_proto__free_unpacked(resp, NULL);
    }
    return hadoopfs_errno_and_retcode(err);
}

static int ndfs_rename(hdfsFS bfs, const char *src_uri, const char *dst_uri)
{
    struct native_fs *fs = (struct native_fs*)bfs;
    struct hadoop_err *err = NULL;
    Rename2RequestProto req = RENAME2_REQUEST_PROTO__INIT;
    Rename2ResponseProto *resp = NULL;
    struct hrpc_proxy proxy;
    char *src_path = NULL, *dst_path = NULL;

    ndfs_nn_proxy_init(fs, &proxy);
    err = build_path(fs, src_uri, &src_path);
    if (err) {
        goto done;
    }
    err = build_path(fs, dst_uri, &dst_path);
    if (err) {
        goto done;
    }
    req.src = src_path;
    req.dst = dst_path;
    req.overwritedest = 0; // TODO: support overwrite
    err = cnn_rename2(&proxy, &req, &resp);
    if (err) {
        goto done;
    }

done:
    free(src_path);
    free(dst_path);
    if (resp) {
        rename2_response_proto__free_unpacked(resp, NULL);
    }
    return hadoopfs_errno_and_retcode(err);
}

static char* ndfs_get_working_directory(hdfsFS bfs, char *buffer,
                                       size_t bufferSize)
{
    size_t len;
    struct native_fs *fs = (struct native_fs *)bfs;
    struct hadoop_err *err = NULL;
    char *working_path = NULL;

    uv_mutex_lock(&fs->working_uri_lock);
    err = uri_get_path(fs->working_uri, &working_path);
    if (err) {
        err = hadoop_err_prepend(err, 0, "ndfs_get_working_directory: failed "
                                 "to get the path of the working_uri.");
        goto done;
    }
    len = strlen(working_path);
    if (len + 1 > bufferSize) {
        err = hadoop_lerr_alloc(ENAMETOOLONG, "ndfs_get_working_directory: "
                "the buffer supplied was only %zd bytes, but we would need "
                "%zd bytes to hold the working directory.",
                bufferSize, len + 1);
        goto done;
    }
    strcpy(buffer, working_path);
done:
    uv_mutex_unlock(&fs->working_uri_lock);
    free(working_path);
    return hadoopfs_errno_and_retptr(err, buffer);
}

static int ndfs_set_working_directory(hdfsFS bfs, const char* uri_str)
{
    struct native_fs *fs = (struct native_fs *)bfs;
    char *path = NULL;
    char *scheme = NULL;
    struct hadoop_err *err = NULL;
    UriParserStateA uri_state;
    UriUriA *uri = NULL;

    uv_mutex_lock(&fs->working_uri_lock);
    uri = calloc(1, sizeof(*uri));
    if (!uri) {
        err = hadoop_lerr_alloc(ENOMEM, "ndfs_set_working_directory: OOM");
        goto done;
    }
    err = uri_get_scheme(fs->working_uri, &scheme);
    if (err) {
        err = hadoop_err_prepend(err, ENOMEM, "ndfs_set_working_directory: "
                            "failed to get scheme of current working_uri");
        goto done;
    }
    err = build_path(fs, uri_str, &path);
    if (err)
        goto done;
    err = uri_parse_abs(path, &uri_state, uri, scheme);
    if (err)
        goto done;
    uriFreeUriMembersA(fs->working_uri);
    free(fs->working_uri);
    fs->working_uri = uri;
    err = NULL;

done:
    if (err) {
        free(uri);
    }
    uv_mutex_unlock(&fs->working_uri_lock);
    free(scheme);
    free(path);
    return hadoopfs_errno_and_retcode(err);
}

static int ndfs_mkdir(hdfsFS bfs, const char* uri)
{
    struct native_fs *fs = (struct native_fs *)bfs;
    struct hadoop_err *err = NULL;
    MkdirsRequestProto req = MKDIRS_REQUEST_PROTO__INIT;
    MkdirsResponseProto *resp = NULL;
    struct hrpc_proxy proxy;
    char *path = NULL;

    ndfs_nn_proxy_init(fs, &proxy);
    err = build_path(fs, uri, &path);
    if (err) {
        goto done;
    }
    req.src = path;
    req.createparent = 1; // TODO: add libhdfs API for non-recursive mkdir
    err = cnn_mkdirs(&proxy, &req, &resp);
    if (err) {
        goto done;
    }
    if (!resp->result) {
        err = hadoop_lerr_alloc(EEXIST, "ndfs_mkdir(%s): a path "
                "component already exists as a non-directory.", path);
        goto done;
    }
    err = NULL;

done:
    free(path);
    if (resp) {
        mkdirs_response_proto__free_unpacked(resp, NULL);
    }
    return hadoopfs_errno_and_retcode(err);
}

static int ndfs_set_replication(hdfsFS bfs, const char* uri,
                               int16_t replication)
{
    struct native_fs *fs = (struct native_fs *)bfs;
    struct hadoop_err *err = NULL;
    SetReplicationRequestProto req = SET_REPLICATION_REQUEST_PROTO__INIT;
    SetReplicationResponseProto *resp = NULL;
    struct hrpc_proxy proxy;
    char *path = NULL;

    ndfs_nn_proxy_init(fs, &proxy);
    err = build_path(fs, uri, &path);
    if (err) {
        goto done;
    }
    req.src = path;
    req.replication = replication;
    err = cnn_set_replication(&proxy, &req, &resp);
    if (err) {
        goto done;
    }
    if (!resp->result) {
        err = hadoop_lerr_alloc(EINVAL, "ndfs_set_replication(%s): path "
                "does not exist or is not a regular file.", path);
        goto done;
    }

done:
    free(path);
    if (resp) {
        set_replication_response_proto__free_unpacked(resp, NULL);
    }
    return hadoopfs_errno_and_retcode(err);
}

static hdfsFileInfo* ndfs_list_directory(hdfsFS bfs __attribute__((unused)),
                                         const char* uri __attribute__((unused)),
                                        int *numEntries __attribute__((unused)))
{
    errno = ENOTSUP;
    return NULL;
}

static hdfsFileInfo *ndfs_get_path_info(hdfsFS bfs __attribute__((unused)),
                                        const char* uri __attribute__((unused)))
{
    errno = ENOTSUP;
    return NULL;
}

char***
ndfs_get_hosts(hdfsFS bfs __attribute__((unused)),
               const char* path __attribute__((unused)),
               tOffset start __attribute__((unused)),
               tOffset length __attribute__((unused)))
{
    errno = ENOTSUP;
    return NULL;
}

static tOffset ndfs_get_default_block_size(hdfsFS bfs)
{
    struct native_fs *fs = (struct native_fs *)bfs;
    return fs->default_block_size;
}

static tOffset ndfs_get_default_block_size_at_path(hdfsFS bfs,
                    const char *uri)
{
    struct native_fs *fs = (struct native_fs *)bfs;
    struct hadoop_err *err = NULL;
    GetPreferredBlockSizeRequestProto req =
        GET_PREFERRED_BLOCK_SIZE_REQUEST_PROTO__INIT;
    GetPreferredBlockSizeResponseProto *resp = NULL;
    struct hrpc_proxy proxy;
    tOffset ret = 0;
    char *path = NULL;

    ndfs_nn_proxy_init(fs, &proxy);
    err = build_path(fs, uri, &path);
    if (err) {
        goto done;
    }
    req.filename = path;
    err = cnn_get_preferred_block_size(&proxy, &req, &resp);
    if (err) {
        goto done;
    }
    ret = resp->bsize;
    err = NULL;

done:
    free(path);
    if (resp) {
        get_preferred_block_size_response_proto__free_unpacked(resp, NULL);
    }
    if (err)
        return hadoopfs_errno_and_retcode(err);
    return ret;
}

static struct hadoop_err *ndfs_statvfs(struct hadoop_fs_base *hfs,
        struct hadoop_vfs_stats *stats)
{
    struct native_fs *fs = (struct native_fs*)hfs;

    GetFsStatusRequestProto req = GET_FS_STATUS_REQUEST_PROTO__INIT;
    GetFsStatsResponseProto *resp = NULL;
    struct hadoop_err *err = NULL;
    struct hrpc_proxy proxy;

    ndfs_nn_proxy_init(fs, &proxy);
    err = cnn_get_fs_stats(&proxy, &req, &resp);
    if (err) {
        goto done;
    }
    stats->capacity = resp->capacity;
    stats->used = resp->used;
    stats->remaining = resp->remaining;
    stats->under_replicated = resp->under_replicated;
    stats->corrupt_blocks = resp->corrupt_blocks;
    stats->missing_blocks = resp->missing_blocks;

done:
    if (resp) {
        get_fs_stats_response_proto__free_unpacked(resp, NULL);
    }
    return err;
}

static tOffset ndfs_get_capacity(hdfsFS bfs)
{
    struct hadoop_err *err;
    struct hadoop_vfs_stats stats;

    err = ndfs_statvfs((struct hadoop_fs_base *)bfs, &stats);
    if (err)
        return hadoopfs_errno_and_retcode(err);
    return stats.capacity;
}

static tOffset ndfs_get_used(hdfsFS bfs)
{
    struct hadoop_err *err;
    struct hadoop_vfs_stats stats;

    err = ndfs_statvfs((struct hadoop_fs_base *)bfs, &stats);
    if (err)
        return hadoopfs_errno_and_retcode(err);
    return stats.used;
}

static int ndfs_chown(hdfsFS bfs, const char* uri,
                      const char *user, const char *group)
{
    struct native_fs *fs = (struct native_fs *)bfs;
    struct hadoop_err *err = NULL;
    SetOwnerRequestProto req = SET_OWNER_REQUEST_PROTO__INIT;
    SetOwnerResponseProto *resp = NULL;
    struct hrpc_proxy proxy;
    char *path = NULL;

    ndfs_nn_proxy_init(fs, &proxy);
    err = build_path(fs, uri, &path);
    if (err) {
        goto done;
    }
    req.src = path;
    req.username = (char*)user;
    req.groupname = (char*)group;
    err = cnn_set_owner(&proxy, &req, &resp);
    if (err) {
        goto done;
    }

done:
    free(path);
    if (resp) {
        set_owner_response_proto__free_unpacked(resp, NULL);
    }
    return hadoopfs_errno_and_retcode(err);
}

static int ndfs_chmod(hdfsFS bfs, const char* uri, short mode)
{
    struct native_fs *fs = (struct native_fs *)bfs;
    FsPermissionProto perm = FS_PERMISSION_PROTO__INIT;
    SetPermissionRequestProto req = SET_PERMISSION_REQUEST_PROTO__INIT;
    SetPermissionResponseProto *resp = NULL;
    struct hadoop_err *err = NULL;
    struct hrpc_proxy proxy;
    char *path = NULL;

    ndfs_nn_proxy_init(fs, &proxy);
    err = build_path(fs, uri, &path);
    if (err) {
        goto done;
    }
    req.src = path;
    req.permission = &perm;
    perm.perm = mode;
    err = cnn_set_permission(&proxy, &req, &resp);
    if (err) {
        goto done;
    }

done:
    free(path);
    if (resp) {
        set_permission_response_proto__free_unpacked(resp, NULL);
    }
    return hadoopfs_errno_and_retcode(err);
}

static int ndfs_utime(hdfsFS bfs, const char* uri,
                      int64_t mtime, int64_t atime)
{
    struct native_fs *fs = (struct native_fs *)bfs;
    SetTimesRequestProto req = SET_TIMES_REQUEST_PROTO__INIT ;
    SetTimesResponseProto *resp = NULL;
    struct hadoop_err *err = NULL;
    struct hrpc_proxy proxy;
    char *path = NULL;

    ndfs_nn_proxy_init(fs, &proxy);
    err = build_path(fs, uri, &path);
    if (err) {
        goto done;
    }
    req.src = path;
    // If mtime or atime are -1, that means "no change."
    // Otherwise, we need to multiply by 1000, to take into account the fact
    // that libhdfs times are in seconds, and HDFS times are in milliseconds.
    // It's unfortunate that libhdfs doesn't support the full millisecond
    // precision.  We need to redo the API at some point.
    if (mtime < 0) {
        req.mtime = -1;
    } else {
        req.mtime = mtime;
        req.mtime *= 1000;
    }
    if (atime < 0) {
        req.atime = -1;
    } else {
        req.atime = atime;
        req.atime *= 1000;
    }
    err = cnn_set_times(&proxy, &req, &resp);
    if (err) {
        goto done;
    }

done:
    free(path);
    if (resp) {
        set_times_response_proto__free_unpacked(resp, NULL);
    }
    return hadoopfs_errno_and_retcode(err);
}

static struct hadoopRzBuffer* ndfs_read_zero(
            hdfsFile bfile __attribute__((unused)),
            struct hadoopRzOptions *opts __attribute__((unused)),
            int32_t maxLength __attribute__((unused)))
{
    errno = ENOTSUP;
    return NULL;
}

static void ndfs_rz_buffer_free(hdfsFile bfile __attribute__((unused)),
                struct hadoopRzBuffer *buffer __attribute__((unused)))
{
}

int ndfs_file_uses_direct_read(hdfsFile bfile)
{
    // Set the 'disable direct reads' flag so that old test harnesses designed
    // to test jniFS will run against NDFS.  The flag doesn't do anything,
    // since all reads are always direct in NDFS.
    struct native_file_base *file = (struct native_file_base *)bfile;
    return (!(file->flags & NDFS_FILE_FLAG_DISABLE_DIRECT_READ));
}

void ndfs_file_disable_direct_read(hdfsFile bfile __attribute__((unused)))
{
    struct native_file_base *file = (struct native_file_base *)bfile;
    file->flags |= NDFS_FILE_FLAG_DISABLE_DIRECT_READ;
}

const struct hadoop_fs_ops g_ndfs_ops = {
    .name = "ndfs",
    .file_is_open_for_read = ndfs_file_is_open_for_read,
    .file_is_open_for_write = ndfs_file_is_open_for_write,
    .get_read_statistics = ndfs_file_get_read_statistics,
    .connect = ndfs_connect,
    .disconnect = ndfs_disconnect,
    .open = ndfs_open_file,
    .close = ndfs_close_file,
    .exists = ndfs_file_exists,
    .seek = ndfs_seek,
    .tell = ndfs_tell,
    .read = ndfs_read,
    .pread = ndfs_pread,
    .write = ndfs_write,
    .flush = ndfs_flush,
    .hflush = ndfs_hflush,
    .hsync = ndfs_hsync,
    .available = ndfs_available,
    .copy = ndfs_copy,
    .move = ndfs_move,
    .unlink = ndfs_unlink,
    .rename = ndfs_rename,
    .get_working_directory = ndfs_get_working_directory,
    .set_working_directory = ndfs_set_working_directory,
    .mkdir = ndfs_mkdir,
    .set_replication = ndfs_set_replication,
    .list_directory = ndfs_list_directory,
    .get_path_info = ndfs_get_path_info,
    .get_hosts = ndfs_get_hosts,
    .get_default_block_size = ndfs_get_default_block_size,
    .get_default_block_size_at_path = ndfs_get_default_block_size_at_path,
    .get_capacity = ndfs_get_capacity,
    .get_used = ndfs_get_used,
    .chown = ndfs_chown,
    .chmod = ndfs_chmod,
    .utime = ndfs_utime,
    .read_zero = ndfs_read_zero,
    .rz_buffer_free = ndfs_rz_buffer_free,

    // test
    .file_uses_direct_read = ndfs_file_uses_direct_read,
    .file_disable_direct_read = ndfs_file_disable_direct_read,
};

// vim: ts=4:sw=4:tw=79:et
