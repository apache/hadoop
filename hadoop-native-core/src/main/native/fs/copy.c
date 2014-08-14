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
#include "common/queue.h"
#include "fs/common.h"
#include "fs/copy.h"
#include "fs/fs.h"
#include "fs/hdfs.h"

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define COPY_BUF_LEN 131072

/**
 * A buffer created by hdfsListDirectory.
 */
struct copy_mem {
    STAILQ_ENTRY(copy_mem) entry;
    hdfsFileInfo *info;
    int len;
    int refcnt;
};

STAILQ_HEAD(copy_mem_head, copy_mem);

/**
 * A work item we need to handle.
 *
 * Multiple work items have pointers into each buffer created by
 * hdfsListDirectory.
 */
struct copy_work {
    STAILQ_ENTRY(copy_work) entry;
    hdfsFileInfo *info;
    struct copy_mem *mem;
};

STAILQ_HEAD(copy_work_head, copy_work);

/**
 * Check that the destination is not a subdirectory of the source.
 *
 * Note: it would be smarter to do this via checking for repeated inode
 * numbers.  By using paths, we could get fooled by stuff like copying from a
 * regular path into a /.reserved/.inodes path which was a subdirectory.  Or
 * cycles involving symlinks or hardlinks.  Unfortunately, HDFS doesn't (yet?)
 * expose the inode IDs of files via getFileStatus, so we don't have any good
 * way to detect cycles at runtime.
 *
 * The Java code follows the same strategy as this code.
 *
 * @param src       The source URI.
 * @param dst       The destination URI.
 *
 * @return          NULL on success; an error if the check failed.
 */
static struct hadoop_err *subdir_check(struct hadoop_uri *src,
                                       struct hadoop_uri *dst)
{
    size_t src_path_len;

    if (strcmp(src->scheme, dst->scheme) != 0) {
        return NULL;
    }
    src_path_len = strlen(src->path);
    if ((strncmp(src->path, dst->path, src_path_len) == 0) &&
        ((dst->path[src_path_len] == '/') &&
         (dst->path[src_path_len] == '0'))) {
        return hadoop_lerr_alloc(EINVAL, "subdir_check: can't copy %s into "
                "%s, because the latter is a subdirectory of the former.",
                src->path, dst->path);
    }
    return NULL;
}

/**
 * Get the working directory of a filesystem.
 * We don't know exactly how big of a buffer we need, so we keep doubling
 * until we no longer hit ENAMETOOLONG.
 *
 * @param fs      The filesystem.
 * @param out     (out param) On success, the dynamically allocated working
 *                  directory URI as a string.
 *
 * @return        NULL on success; error otherwise
 */
static struct hadoop_err *get_working_directory_dyn(hdfsFS fs, char **out)
{
    int ret, len = 16;
    char *buf = NULL, *nbuf;

    while (1) {
        nbuf = realloc(buf, len);
        if (!nbuf) {
            free(buf);
            return hadoop_lerr_alloc(ENOMEM, "get_working_directory_dyn: "
                                       "OOM");
        }
        buf = nbuf;
        if (hdfsGetWorkingDirectory(fs, buf, len)) {
            *out = buf;
            return NULL;
        }
        ret = errno;
        if (ret != ENAMETOOLONG) {
            free(buf);
            return hadoop_lerr_alloc(ret, "get_working_directory_dyn: "
                                       "error %s", terror(ret));
        }
        if (len >= 1073741824) {
            free(buf);
            return hadoop_lerr_alloc(ENAMETOOLONG, 
                "get_working_directory_dyn: working directory too long.");
        }
        len *= 2;
    }
}

/**
 * Translate a source URI into a destination URI.
 *
 * @pararm src          The source URI as text.  We need to parse this since
 *                          it may have a scheme, etc. included, and not just a
 *                          path.
 * @pararm dst          The destination URI.  We use this to get the scheme,
 *                          user_info, etc. of the URI we're creating.
 * @pararm src_base     The source base path.  This is what was passed as
 *                          'src' to the copy command.
 * @pararm out          (out param) The new URI to use.
 *
 * @return              NULL on success; error code otherwise.
 */
static struct hadoop_err *translate_src_to_dst(const char *src,
            const struct hadoop_uri *dst_uri, const char *src_base, 
            char **out)
{
    struct hadoop_uri *src_uri = NULL;
    struct hadoop_err *err = NULL;
    size_t src_base_len;
    const char *suffix;

    src_base_len = strlen(src_base);
    if (strlen(dst_uri->path) < src_base_len) {
        err = hadoop_lerr_alloc(EINVAL, "translate_src_to_dst: src_base '%s' "
                "is too long to be a base for %s", src_base, dst_uri->path);
        goto done;
    }
    err = hadoop_uri_parse(src, NULL, &src_uri, H_URI_PARSE_PATH);
    if (err)
        goto done;
    hadoop_uri_print(stderr, "src_uri", src_uri);
    hadoop_uri_print(stderr, "dst_uri", dst_uri);
    suffix = src_uri->path + src_base_len;
    err = dynprintf(out, "%s://%s%s%s%s%s%s",
              dst_uri->scheme,
              (dst_uri->user_info[0] ? dst_uri->user_info : ""),
              (dst_uri->user_info[0] ? "@" : ""),
              (dst_uri->auth[0] ? dst_uri->auth : ""),
              (dst_uri->auth[0] ? "/" : ""),
              dst_uri->path,
              suffix);
    fprintf(stderr, "out: %s\n", *out);

done:
    hadoop_uri_free(src_uri);
    return err;
}

static struct hadoop_err *list_dir_for_copy(hdfsFS fs,
                const char *path, struct copy_mem_head *mem_head,
                struct copy_work_head *work_head)
{
    int ret, i;
    struct copy_mem *mem;
    struct copy_work *work;

    mem = calloc(1, sizeof(*mem));
    if (!mem) {
        return hadoop_lerr_alloc(ENOMEM, "copy_recursive: OOM");
    }
    mem->info = hdfsListDirectory(fs, path, &mem->len);
    if (!mem->info) {
        ret = errno;
        free(mem);
        if (ret == ENOENT)
            return NULL;
        return hadoop_lerr_alloc(ret, "copy_recursive: failed to list "
            "source directory %s: %s", path, terror(ret));
    }
    STAILQ_INSERT_TAIL(mem_head, mem, entry);

    // Break up the directory listing into work items.
    for (i = 0; i < mem->len; i++) {
        work = calloc(1, sizeof(struct copy_work));
        if (!work) {
            return hadoop_lerr_alloc(ENOMEM, "copy_recursive: OOM");
        }
        work->info = &mem->info[i];
        work->mem = mem;
        mem->refcnt++;
        STAILQ_INSERT_TAIL(work_head, work, entry);
    }
    return NULL;
}

static int hdfsWriteFully(hdfsFS fs, hdfsFile out, const char *buf, tSize amt)
{
    tSize cur = 0, res;

    while (cur < amt) {
        res = hdfsWrite(fs, out, buf + cur, amt - cur);
        if (res <= 0) {
            return -1; // errno will be set
        }
        cur += res;
    }
    return 0;
}

/**
 * Copy a regular file from one FS to another.
 *
 * @param srcFS         The source filesystem.
 * @param dstFS         The destination filesystem.
 * @param buf           The buffer to use.
 * @param buf_len       Size of the buffer to use.
 * @param from_uri      Source URI in text form.
 * @param to_uri        Destination URI in text form.
 *
 * @return              NULL on success; error otherwise.
 */
static struct hadoop_err *copy_regular_file(hdfsFS srcFS, hdfsFS dstFS,
                char *buf, size_t buf_len,
                const char *from_uri, const char *to_uri) 
{
    int ret;
    hdfsFile in = NULL;
    hdfsFile out = NULL;
    tSize res;
    struct hadoop_err *err = NULL;

    in = hdfsOpenFile(srcFS, from_uri, O_RDONLY, 0, 0, 0);
    if (!in) {
        ret = errno;
        err = hadoop_lerr_alloc(ret, "copy_regular_file(%s, %s): "
                "failed to open source uri for read: %s",
                from_uri, to_uri, terror(ret));
        goto done;
    }
    out = hdfsOpenFile(dstFS, to_uri, O_WRONLY, 0, 0, 0);
    if (!in) {
        ret = errno;
        err = hadoop_lerr_alloc(ret, "copy_regular_file(%s, %s): "
                "failed to open destination uri for write: %s",
                from_uri, to_uri, terror(ret));
        goto done;
    }
    while (1) {
        res = hdfsRead(srcFS, in, buf, buf_len);
        if (res == 0) {
            break;
        } else if (res < 0) {
            ret = errno;
            err = hadoop_lerr_alloc(ret, "copy_regular_file(%s, %s): "
                "hdfsRead error: %s", from_uri, to_uri, terror(ret));
            goto done;
        }
        if (hdfsWriteFully(dstFS, out, buf, res)) {
            ret = errno;
            err = hadoop_lerr_alloc(ret, "copy_regular_file(%s, %s): "
                "hdfsWriteFully error: %s", from_uri, to_uri, terror(ret));
            goto done;
        }
    }
    // Check the return code of close for the file we're writing to.
    // hdfsCloseFile may flush buffers, and we don't want to ignore an error
    // here. 
    if (hdfsCloseFile(dstFS, out)) {
        out = NULL;
        ret = errno;
        err = hadoop_lerr_alloc(ret, "copy_regular_file(%s, %s): "
            "hdfsClose error: %s", from_uri, to_uri, terror(ret));
        goto done;
    }
    out = NULL;
    err = NULL;
done:
    if (in) {
        hdfsCloseFile(srcFS, in);
    }
    if (out) {
        hdfsCloseFile(dstFS, out);
    }
    return err;
}

static struct hadoop_err *copy_impl(hdfsFS srcFS, hdfsFS dstFS,
        const char *src, const char *dst,
        const struct hadoop_uri *src_uri, const struct hadoop_uri *dst_uri)
{
    int ret;
    struct copy_mem *mem, *nmem;
    struct copy_work *work, *nwork;
    struct copy_mem_head mem_head;
    struct copy_work_head work_head;
    char *target_uri = NULL, *buf = NULL;
    struct hadoop_err *err = NULL;
    hdfsFileInfo *info = NULL;

    STAILQ_INIT(&mem_head);
    STAILQ_INIT(&work_head);
    buf = malloc(COPY_BUF_LEN);
    if (!buf) {
        err = hadoop_lerr_alloc(ENOMEM, "copy_recursive: "
                "failed to malloc a buffer of size %d", COPY_BUF_LEN);
        goto done;
    }
    info = hdfsGetPathInfo(srcFS, src);
    if (!info) {
        ret = errno;
        err = hadoop_lerr_alloc(ENOENT, "copy_recursive: error calling "
                "hdfsGetPathInfo on source %s: %s", src, terror(ret));
        goto done;
    }
    if (info->mKind == kObjectKindFile) {
        // Handle the case where 'src' is a regular file.
        err = translate_src_to_dst(src, dst_uri,
                             src_uri->path, &target_uri);
        if (err)
            goto done;
        err = copy_regular_file(srcFS, dstFS, buf, COPY_BUF_LEN,
                src, target_uri);
        goto done;
    }
    err = list_dir_for_copy(srcFS, src, &mem_head, &work_head);
    if (err) {
        err = hadoop_err_prepend(err, 0, "copy_recursive: error listing "
                    "source directory %s: ", src);
        goto done;
    }
    // Process work items.
    while ((work = STAILQ_FIRST(&work_head))) {
        free(target_uri);
        target_uri = NULL;
        err = translate_src_to_dst(work->info->mName, dst_uri,
                             src_uri->path, &target_uri);
        if (err)
            goto done;
        if (work->info->mKind == kObjectKindDirectory) {
            mem = NULL;
            err = list_dir_for_copy(srcFS, work->info->mName,
                                    &mem_head, &work_head);
            if (err) {
                goto done;
            }
            if (hdfsCreateDirectory(dstFS, target_uri) < 0) {
                ret = errno;
                err = hadoop_lerr_alloc(ret, "copy_recursive(%s, %s): failed "
                        "to mkdir(%s): %s", src, dst, target_uri, terror(ret));
                goto done;
            }
            work->mem->refcnt--;
            if (work->mem->refcnt <= 0) {
                hdfsFreeFileInfo(work->mem->info, work->mem->len);
                STAILQ_REMOVE(&mem_head, work->mem, copy_mem, entry);
                free(work->mem);
            }
        } else {
            err = copy_regular_file(srcFS, dstFS, buf, COPY_BUF_LEN,
                    work->info->mName, target_uri);
            if (err)
                goto done;
        }
        STAILQ_REMOVE(&work_head, work, copy_work, entry);
        free(work);
    }

    err = NULL;
done:
    free(buf);
    STAILQ_FOREACH_SAFE(mem, &mem_head, entry, nmem) {
        hdfsFreeFileInfo(mem->info, mem->len);
        STAILQ_REMOVE(&mem_head, mem, copy_mem, entry);
        free(mem);
    }
    STAILQ_FOREACH_SAFE(work, &work_head, entry, nwork) {
        STAILQ_REMOVE(&work_head, work, copy_work, entry);
        free(work);
    }
    return err;
}

int copy_recursive(hdfsFS srcFS, const char *src,
                   hdfsFS dstFS, const char *dst)
{
    struct hadoop_err *err = NULL;
    char *src_working = NULL, *dst_working = NULL;
    struct hadoop_uri *src_working_uri = NULL, *dst_working_uri = NULL;
    struct hadoop_uri *src_uri = NULL, *dst_uri = NULL;

    err = get_working_directory_dyn(srcFS, &src_working);
    if (err) {
        err = hadoop_err_prepend(err, 0, "copy_recursive(%s, %s)", src, dst);
        goto done;
    }
    err = get_working_directory_dyn(dstFS, &dst_working);
    if (err) {
        err = hadoop_err_prepend(err, 0, "copy_recursive(%s, %s)", src, dst);
        goto done;
    }
    err = hadoop_uri_parse(src_working, NULL, &src_working_uri,
                     H_URI_APPEND_SLASH | H_URI_PARSE_ALL);
    if (err) {
        err = hadoop_err_prepend(err, 0, "copy_recursive(%s, %s): "
                "error parsing source working directory", src, dst);
        goto done;
    }
    fprintf(stderr, "src_working(raw)=%s\n", src_working);
    hadoop_uri_print(stderr, "src_working", src_working_uri);
    err = hadoop_uri_parse(dst_working, NULL, &dst_working_uri,
                     H_URI_APPEND_SLASH | H_URI_PARSE_ALL);
    if (err) {
        err = hadoop_err_prepend(err, 0, "copy_recursive(%s, %s): "
                "error parsing destination working directory", src, dst);
        goto done;
    }
    fprintf(stderr, "dst_working(raw)=%s\n", dst_working);
    hadoop_uri_print(stderr, "dst_working", dst_working_uri);
    err = hadoop_uri_parse(src, src_working_uri, &src_uri, H_URI_PARSE_ALL);
    if (err) {
        err = hadoop_err_prepend(err, 0, "copy_recursive(%s, %s): "
                "error parsing source URI", src, dst);
        goto done;
    }
    hadoop_uri_print(stderr, "src_uri", src_uri);
    if (!src_uri->scheme) {
        src_uri->scheme = strdup(DEFAULT_SCHEME);
        if (!src_uri->scheme) {
            err = hadoop_lerr_alloc(ENOMEM, "copy_recursive(%s, %s): "
                        "OOM", src, dst);
            goto done;
        }
    }
    err = hadoop_uri_parse(dst, dst_working_uri, &dst_uri, H_URI_PARSE_ALL);
    if (err) {
        err = hadoop_err_prepend(err, 0, "copy_recursive(%s, %s): "
                "error parsing destination URI", src, dst);
        goto done;
    }
    if (!dst_uri->scheme) {
        dst_uri->scheme = strdup(DEFAULT_SCHEME);
        if (!dst_uri->scheme) {
            err = hadoop_lerr_alloc(ENOMEM, "copy_recursive(%s, %s): "
                        "OOM", src, dst);
            goto done;
        }
    }
    err = subdir_check(src_uri, dst_uri);
    if (err) {
        goto done;
    }
    err = copy_impl(srcFS, dstFS, src, dst, src_uri, dst_uri);
    if (err)
        goto done;
    err = NULL;

done:
    hadoop_uri_free(src_working_uri);
    hadoop_uri_free(dst_working_uri);
    hadoop_uri_free(src_uri);
    hadoop_uri_free(dst_uri);
    free(src_working);
    free(dst_working);
    return hadoopfs_errno_and_retcode(err);
}

// vim: ts=4:sw=4:et
