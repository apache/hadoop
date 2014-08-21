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
#include "ndfs/meta.h"
#include "ndfs/util.h"
#include "rpc/messenger.h"
#include "rpc/proxy.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <uv.h>

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

int ndfs_file_is_open_for_read(hdfsFile bfile)
{
    struct native_file_base *file = (struct native_file_base *)bfile;
    return !!(file->flags & NDFS_FILE_FLAG_RO);
}

int ndfs_file_is_open_for_write(hdfsFile bfile)
{
    struct native_file_base *file = (struct native_file_base *)bfile;
    return !(file->flags & NDFS_FILE_FLAG_RO);
}

int ndfs_file_get_read_statistics(hdfsFile bfile,
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

hdfsFile ndfs_open_file(hdfsFS bfs, const char* uri, int flags, 
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

int ndfs_close_file(hdfsFS fs __attribute__((unused)),
                    hdfsFile bfile __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

int ndfs_seek(hdfsFS bfs __attribute__((unused)),
              hdfsFile bfile __attribute__((unused)),
              tOffset desiredPos __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

tOffset ndfs_tell(hdfsFS bfs __attribute__((unused)),
                  hdfsFile bfile __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

tSize ndfs_read(hdfsFS bfs __attribute__((unused)),
                       hdfsFile bfile __attribute__((unused)),
                       void *buffer __attribute__((unused)),
                       tSize length __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

tSize ndfs_pread(hdfsFS bfs __attribute__((unused)),
            hdfsFile bfile __attribute__((unused)),
            tOffset position __attribute__((unused)),
            void* buffer __attribute__((unused)),
            tSize length __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

tSize ndfs_write(hdfsFS bfs __attribute__((unused)),
            hdfsFile bfile __attribute__((unused)),
            const void* buffer __attribute__((unused)),
            tSize length __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

int ndfs_flush(hdfsFS bfs __attribute__((unused)),
               hdfsFile bfile __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

int ndfs_hflush(hdfsFS bfs __attribute__((unused)),
                hdfsFile bfile __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

int ndfs_hsync(hdfsFS bfs __attribute__((unused)),
               hdfsFile bfile __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

int ndfs_available(hdfsFS bfs __attribute__((unused)),
                   hdfsFile bfile __attribute__((unused)))
{
    errno = ENOTSUP;
    return -1;
}

struct hadoopRzBuffer* ndfs_read_zero(
            hdfsFile bfile __attribute__((unused)),
            struct hadoopRzOptions *opts __attribute__((unused)),
            int32_t maxLength __attribute__((unused)))
{
    errno = ENOTSUP;
    return NULL;
}

void ndfs_rz_buffer_free(hdfsFile bfile __attribute__((unused)),
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

char***
ndfs_get_hosts(hdfsFS bfs __attribute__((unused)),
               const char* path __attribute__((unused)),
               tOffset start __attribute__((unused)),
               tOffset length __attribute__((unused)))
{
    errno = ENOTSUP;
    return NULL;
}

// vim: ts=4:sw=4:tw=79:et
