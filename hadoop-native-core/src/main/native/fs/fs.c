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
#include "common/string.h"
#include "common/uri.h"
#include "common/user.h"
#include "fs/common.h"
#include "fs/fs.h"
#include "fs/hdfs.h"

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <uriparser/Uri.h>

#define DEFAULT_SCHEME "hdfs"

#define DEFAULT_NATIVE_HANDLERS "ndfs,jnifs"

const char* const HDFS_XML_NAMES[] = {
    "core-default.xml",
    "core-site.xml",
    "hdfs-default.xml",
    "hdfs-site.xml",
    NULL
};

const struct hadoop_fs_ops g_jni_ops;
const struct hadoop_fs_ops g_ndfs_ops;

static const struct hadoop_fs_ops *g_ops[] = {
    &g_jni_ops,
    &g_ndfs_ops,
    NULL
};

int hdfsFileIsOpenForRead(hdfsFile file)
{
    struct hadoop_file_base *base = (struct hadoop_file_base*)file;
    return g_ops[base->ty]->file_is_open_for_read(file);
}

int hdfsFileIsOpenForWrite(hdfsFile file)
{
    struct hadoop_file_base *base = (struct hadoop_file_base*)file;
    return g_ops[base->ty]->file_is_open_for_write(file);
}

int hdfsFileGetReadStatistics(hdfsFile file, struct hdfsReadStatistics **stats)
{
    struct hadoop_file_base *base = (struct hadoop_file_base*)file;
    return g_ops[base->ty]->get_read_statistics(file, stats);
}

int64_t hdfsReadStatisticsGetRemoteBytesRead(
                            const struct hdfsReadStatistics *stats)
{
    return stats->totalBytesRead - stats->totalLocalBytesRead;
}

void hdfsFileFreeReadStatistics(struct hdfsReadStatistics *stats)
{
    free(stats);
}

hdfsFS hdfsConnect(const char* host, tPort port)
{
    struct hdfsBuilder *bld = hdfsNewBuilder();
    if (!bld)
        return NULL;
    hdfsBuilderSetNameNode(bld, host);
    hdfsBuilderSetNameNodePort(bld, port);
    return hdfsBuilderConnect(bld);
}

hdfsFS hdfsConnectNewInstance(const char* host, tPort port)
{
    struct hdfsBuilder *bld = hdfsNewBuilder();
    if (!bld)
        return NULL;
    hdfsBuilderSetNameNode(bld, host);
    hdfsBuilderSetNameNodePort(bld, port);
    hdfsBuilderSetForceNewInstance(bld);
    return hdfsBuilderConnect(bld);
}

hdfsFS hdfsConnectAsUser(const char* host, tPort port, const char *user)
{
    struct hdfsBuilder *bld = hdfsNewBuilder();
    if (!bld)
        return NULL;
    hdfsBuilderSetNameNode(bld, host);
    hdfsBuilderSetNameNodePort(bld, port);
    hdfsBuilderSetUserName(bld, user);
    return hdfsBuilderConnect(bld);
}

hdfsFS hdfsConnectAsUserNewInstance(const char* host, tPort port,
        const char *user)
{
    struct hdfsBuilder *bld = hdfsNewBuilder();
    if (!bld)
        return NULL;
    hdfsBuilderSetNameNode(bld, host);
    hdfsBuilderSetNameNodePort(bld, port);
    hdfsBuilderSetForceNewInstance(bld);
    hdfsBuilderSetUserName(bld, user);
    return hdfsBuilderConnect(bld);
}

static const struct hadoop_fs_ops *find_fs_impl_by_name(const char *name)
{
    const struct hadoop_fs_ops *ops;
    int i = 0;

    while (1) {
        ops = g_ops[i++];
        if (!ops) {
            fprintf(stderr, "hdfsBuilderConnect: we don't support the '%s' "
                    "native fs implementation.\n", name);
            return NULL;
        }
        if (strcmp(ops->name, name) == 0) {
            return ops;
        }
    }
}

static struct hadoop_err *hdfs_builder_load_conf(struct hdfsBuilder *hdfs_bld)
{
    struct hadoop_err *err;
    struct hconf_builder *conf_bld = NULL;
    const char *classpath;
    struct hdfsBuilderConfOpt *opt;

    err = hconf_builder_alloc(&conf_bld);
    if (err) {
        goto done;
    }

    // Load the XML files.
    classpath = getenv("CLASSPATH");
    if (!classpath) {
        classpath = ".";
    }
    err = hconf_builder_load_xmls(conf_bld, HDFS_XML_NAMES, classpath);
    if (err) {
        goto done;
    }

    // Add the options that were specified by hdfsBuilderConfSetStr.
    for (opt = hdfs_bld->opts; opt; opt = opt->next) {
        hconf_builder_set(conf_bld, opt->key, opt->val);
    }

    // Create the conf object.
    err = hconf_build(conf_bld, &hdfs_bld->hconf);
    conf_bld = NULL;
    err = NULL;
done:
    if (conf_bld) {
        hconf_builder_free(conf_bld);
    }
    if (err)
        return err;
    return NULL;
}

static struct hadoop_err *hdfs_builder_parse_conn_uri(
                                    struct hdfsBuilder *hdfs_bld)
{
    int ret;
    char *uri_str = NULL, *uri_dbg = NULL;
    struct hadoop_err *err = NULL;

    if (hdfs_bld->nn) {
        if ((!index(hdfs_bld->nn, '/')) && (index(hdfs_bld->nn, ':'))) {
            // If the connection URI was set via hdfsBuilderSetNameNode, it may
            // not be a real URI, but just a <hostname>:<port> pair.  This won't
            // parse correctly unless we add a hdfs:// scheme in front of it.
            err = dynprintf(&uri_str, "hdfs://%s", hdfs_bld->nn);
            if (err)
                goto done;
        } else {
            uri_str = strdup(hdfs_bld->nn);
            if (!uri_str) {
                err = hadoop_lerr_alloc(ENOMEM, "hdfs_builder_parse_conn_uri: OOM");
                goto done;
            }
        }
    } else {
        const char *default_fs = hconf_get(hdfs_bld->hconf, "fs.defaultFS");
        if (!default_fs) {
            default_fs = "file:///";
        }
        uri_str = strdup(default_fs);
        if (!uri_str) {
            err = hadoop_lerr_alloc(ENOMEM, "hdfs_builder_parse_conn_uri: OOM");
            goto done;
        }
    }
    err = hadoop_uri_parse(uri_str, NULL, &hdfs_bld->uri,
                     H_URI_APPEND_SLASH | H_URI_PARSE_ALL);
    if (err)
        goto done;
    if (hdfs_bld->uri->user_info[0] == '\0') {
        // If we still don't have an authority, fill in the authority from the
        // current user name.
        free(hdfs_bld->uri->user_info);
        hdfs_bld->uri->user_info = NULL;
        ret = geteuid_string(&hdfs_bld->uri->user_info);
        if (ret) {
            err = hadoop_lerr_alloc(ret, "geteuid_string failed: error "
                                    "%d", ret);
            goto done;
        }
    }
    err = hadoop_uri_to_str(hdfs_bld->uri, &uri_dbg);
    if (err)
        goto done;
    fprintf(stderr, "hdfs_builder_parse_conn_uri: %s\n", uri_dbg);
    if (hdfs_bld->port == 0) {
        hdfs_bld->port = hdfs_bld->uri->port;
    } else {
        if (hdfs_bld->port != hdfs_bld->uri->port) {
            err = hadoop_lerr_alloc(EINVAL, "The connection URI specified "
                    "port %d, but hdfsBuilderSetNameNodePort specified port "
                    "%d.  Please only specify the port once, preferrably in "
                    "the URI.", hdfs_bld->uri->port, hdfs_bld->port);
            goto done;
        }
        hdfs_bld->uri->port = hdfs_bld->port;
    }
    err = NULL;

done:
    free(uri_dbg);
    free(uri_str);
    return err;
}

hdfsFS hdfsBuilderConnect(struct hdfsBuilder *bld)
{
    struct hadoop_err *err;
    hdfsFS fs = NULL;
    const char *fs_list_val;
    char *fs_list_key = NULL, *fs_list_val_copy = NULL, *ptr;
    char *fs_impl_name;

    //
    // Load the configuration from XML.
    //
    // The hconf object will be available to all native FS implementations to
    // use in their connect methods.
    //
    err = hdfs_builder_load_conf(bld);
    if (err)
        goto done;

    //
    // Determine the URI we should connect to.  It gets a bit complicated
    // because of all the defaults.
    //
    err = hdfs_builder_parse_conn_uri(bld);
    if (err)
        goto done;

    // Find out the native filesystems we should use for this URI.
    if (asprintf(&fs_list_key, "%s.native.handler.", bld->uri->scheme) < 0) {
        fs_list_key = NULL;
        err = hadoop_lerr_alloc(ENOMEM, "hdfsBuilderConnect: OOM");
        goto done;
    }
    fs_list_val = hconf_get(bld->hconf, fs_list_key);
    if (!fs_list_val) {
        fs_list_val = hconf_get(bld->hconf, "default.native.handler");
        if (!fs_list_val) {
            fs_list_val = DEFAULT_NATIVE_HANDLERS;
        }
    }
    fs_list_val_copy = strdup(fs_list_val);
    if (!fs_list_val_copy) {
        err = hadoop_lerr_alloc(ENOMEM, "hdfsBuilderConnect: OOM");
        goto done;
    }
    // Give each native filesystem implementation a shot at connecting.
    for (fs_impl_name = strtok_r(fs_list_val_copy, ",", &ptr); fs_impl_name;
                fs_impl_name = strtok_r(NULL, ",", &ptr)) {
        const struct hadoop_fs_ops *ops = find_fs_impl_by_name(fs_impl_name);
        if (!ops)
            continue;
        if (err)
            hadoop_err_free(err);
        err = ops->connect(bld, &fs);
        if (!err) {
            break;
        }
        fprintf(stderr, "hdfsBuilderConnect: %s failed to connect: "
                "%s (error %d)\n", fs_impl_name, hadoop_err_msg(err),
                hadoop_err_code(err));
    }

done:
    hdfsFreeBuilder(bld);
    free(fs_list_key);
    free(fs_list_val_copy);
    return hadoopfs_errno_and_retptr(err, fs);
}

struct hdfsBuilder *hdfsNewBuilder(void)
{
    struct hdfsBuilder *bld = calloc(1, sizeof(struct hdfsBuilder));
    if (!bld) {
        errno = ENOMEM;
        return NULL;
    }
    return bld;
}

void hdfsBuilderSetForceNewInstance(struct hdfsBuilder *bld __attribute__((unused)))
{
    // Does nothing-- present only for compatibility
}

void hdfsBuilderSetNameNode(struct hdfsBuilder *bld, const char *nn)
{
    bld->nn = nn;
}

void hdfsBuilderSetNameNodePort(struct hdfsBuilder *bld, tPort port)
{
    bld->port = port;
}

void hdfsBuilderSetUserName(struct hdfsBuilder *bld, const char *userName)
{
    bld->userName = userName;
}

void hdfsBuilderSetKerbTicketCachePath(struct hdfsBuilder *bld,
                                       const char *kerbTicketCachePath)
{
    bld->kerbTicketCachePath = kerbTicketCachePath;
}

void hdfsFreeBuilder(struct hdfsBuilder *bld)
{
    struct hdfsBuilderConfOpt *cur, *next;

    if (!bld)
        return;
    cur = bld->opts;
    for (cur = bld->opts; cur; ) {
        next = cur->next;
        free(cur);
        cur = next;
    }
    hadoop_uri_free(bld->uri);
    hconf_free(bld->hconf);
    free(bld);
}

int hdfsBuilderConfSetStr(struct hdfsBuilder *bld, const char *key,
                          const char *val)
{
    struct hdfsBuilderConfOpt *opt, *next;

    opt = calloc(1, sizeof(struct hdfsBuilderConfOpt));
    if (!opt)
        return -ENOMEM;
    next = bld->opts;
    bld->opts = opt;
    opt->next = next;
    opt->key = key;
    opt->val = val;
    return 0;
}

int hdfsConfGetStr(const char *key __attribute__((unused)),
                   char **val __attribute__((unused)))
{
    // FIXME: add configuration stuff
    errno = ENOTSUP;
    return -1;
}

int hdfsConfGetInt(const char *key, int32_t *val)
{
    char *str = NULL;
    int ret;

    ret = hdfsConfGetStr(key, &str);
    if (ret)
        return ret;
    *val = atoi(str);
    hdfsConfStrFree(str);
    return 0;
}

void hdfsConfStrFree(char *val)
{
    free(val);
}

int hdfsDisconnect(hdfsFS fs)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->disconnect(fs);
}

hdfsFile hdfsOpenFile(hdfsFS fs, const char *path, int flags,
                      int bufferSize, short replication, tSize blocksize)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->open(fs, path, flags, bufferSize,
                                 replication, blocksize);
}

int hdfsCloseFile(hdfsFS fs, hdfsFile file)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->close(fs, file);
}

int hdfsExists(hdfsFS fs, const char *path)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->exists(fs, path);
}

int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->seek(fs, file, desiredPos);
}

tOffset hdfsTell(hdfsFS fs, hdfsFile file)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->tell(fs, file);
}

tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->read(fs, file, buffer, length);
}

tSize hdfsPread(hdfsFS fs, hdfsFile file, tOffset position,
                    void* buffer, tSize length)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->pread(fs, file, position, buffer, length);
}

tSize hdfsWrite(hdfsFS fs, hdfsFile file, const void* buffer,
                tSize length)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->write(fs, file, buffer, length);
}

int hdfsFlush(hdfsFS fs, hdfsFile file)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->flush(fs, file);
}

int hdfsHFlush(hdfsFS fs, hdfsFile file)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->hflush(fs, file);
}

int hdfsHSync(hdfsFS fs, hdfsFile file)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->hsync(fs, file);
}

int hdfsAvailable(hdfsFS fs, hdfsFile file)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->available(fs, file);
}

int hdfsCopy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst)
{
    struct hadoop_fs_base *src_base = (struct hadoop_fs_base*)srcFS;
    struct hadoop_fs_base *dst_base = (struct hadoop_fs_base*)dstFS;

    if (src_base->ty != dst_base->ty) {
        errno = EINVAL;
        return -1;
    }
    return g_ops[src_base->ty]->copy(srcFS, src, dstFS, dst);
}

int hdfsMove(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst)
{
    struct hadoop_fs_base *src_base = (struct hadoop_fs_base*)srcFS;
    struct hadoop_fs_base *dst_base = (struct hadoop_fs_base*)dstFS;

    if (src_base->ty != dst_base->ty) {
        errno = EINVAL;
        return -1;
    }
    return g_ops[src_base->ty]->move(srcFS, src, dstFS, dst);
}

int hdfsDelete(hdfsFS fs, const char* path, int recursive)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->unlink(fs, path, recursive);
}

int hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->rename(fs, oldPath, newPath);
}

char* hdfsGetWorkingDirectory(hdfsFS fs, char *buffer, size_t bufferSize)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->get_working_directory(fs, buffer, bufferSize);
}

int hdfsSetWorkingDirectory(hdfsFS fs, const char* path)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->set_working_directory(fs, path);
}

int hdfsCreateDirectory(hdfsFS fs, const char* path)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->mkdir(fs, path);
}

int hdfsSetReplication(hdfsFS fs, const char* path, int16_t replication)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->set_replication(fs, path, replication);
}

hdfsFileInfo *hdfsListDirectory(hdfsFS fs, const char* path, int *numEntries)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->list_directory(fs, path, numEntries);
}

hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, const char* path)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->get_path_info(fs, path);
}

void hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries)
{
    //Free the mName, mOwner, and mGroup
    int i;
    for (i=0; i < numEntries; ++i) {
        release_file_info_entry(hdfsFileInfo + i);
    }

    //Free entire block
    free(hdfsFileInfo);
}

char*** hdfsGetHosts(hdfsFS fs, const char* path, 
          tOffset start, tOffset length)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->get_hosts(fs, path, start, length);
}

void hdfsFreeHosts(char ***blockHosts)
{
    int i, j;
    for (i=0; blockHosts[i]; i++) {
        for (j=0; blockHosts[i][j]; j++) {
            free(blockHosts[i][j]);
        }
        free(blockHosts[i]);
    }
    free(blockHosts);
}

tOffset hdfsGetDefaultBlockSize(hdfsFS fs)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->get_default_block_size(fs);
}

tOffset hdfsGetDefaultBlockSizeAtPath(hdfsFS fs, const char *path)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->get_default_block_size_at_path(fs, path);
}

tOffset hdfsGetCapacity(hdfsFS fs)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->get_capacity(fs);
}

tOffset hdfsGetUsed(hdfsFS fs)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->get_used(fs);
}

int hdfsChown(hdfsFS fs, const char* path, const char *owner,
              const char *group)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->chown(fs, path, owner, group);
}

int hdfsChmod(hdfsFS fs, const char* path, short mode)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->chmod(fs, path, mode);
}

int hdfsUtime(hdfsFS fs, const char* path, tTime mtime, tTime atime)
{
    struct hadoop_fs_base *base = (struct hadoop_fs_base*)fs;
    return g_ops[base->ty]->utime(fs, path, mtime, atime);
}

struct hadoopRzOptions *hadoopRzOptionsAlloc(void)
{
    struct hadoopRzOptions *opts;
    opts = calloc(1, sizeof(*opts));
    if (!opts) {
        errno = ENOMEM;
        return NULL;
    }
    return opts;
}

int hadoopRzOptionsSetSkipChecksum(struct hadoopRzOptions *opts, int skip)
{
    opts->skip_checksums = skip;
    return 0;
}

int hadoopRzOptionsSetByteBufferPool(
            struct hadoopRzOptions *opts, const char *className)
{
    return strdupto(&opts->pool_name, className);
}

void hadoopRzOptionsFree(struct hadoopRzOptions *opts)
{
    if (opts) {
        if (opts->cache_teardown_cb) {
            opts->cache_teardown_cb(opts->cache);
            opts->cache = NULL;
        }
        free(opts->pool_name);
        free(opts);
    }
}

struct hadoopRzBuffer* hadoopReadZero(hdfsFile file,
            struct hadoopRzOptions *opts, int32_t maxLength)
{
    struct hadoop_file_base *base = (struct hadoop_file_base*)file;
    return g_ops[base->ty]->read_zero(file, opts, maxLength);
}

int32_t hadoopRzBufferLength(const struct hadoopRzBuffer *buf)
{
    struct hadoop_rz_buffer_base *bbuf = (struct hadoop_rz_buffer_base *)buf;
    return bbuf->length;
}

const void *hadoopRzBufferGet(const struct hadoopRzBuffer *buf)
{
    struct hadoop_rz_buffer_base *bbuf = (struct hadoop_rz_buffer_base *)buf;
    return bbuf->ptr;
}

void hadoopRzBufferFree(hdfsFile file, struct hadoopRzBuffer *buffer)
{
    struct hadoop_file_base *base = (struct hadoop_file_base*)file;
    g_ops[base->ty]->rz_buffer_free(file, buffer);
}

int hdfsFileUsesDirectRead(struct hdfsFile_internal *file)
{
    struct hadoop_file_base *base = (struct hadoop_file_base*)file;
    return g_ops[base->ty]->file_uses_direct_read(file);
}

void hdfsFileDisableDirectRead(struct hdfsFile_internal *file)
{
    struct hadoop_file_base *base = (struct hadoop_file_base*)file;
    return g_ops[base->ty]->file_disable_direct_read(file);
}

// vim: ts=4:sw=4:et
