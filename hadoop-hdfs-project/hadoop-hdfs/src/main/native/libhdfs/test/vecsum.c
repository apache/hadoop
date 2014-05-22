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

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "config.h"
#include "hdfs.h"

#define VECSUM_CHUNK_SIZE (8 * 1024 * 1024)
#define ZCR_READ_CHUNK_SIZE (1024 * 1024 * 8)
#define NORMAL_READ_CHUNK_SIZE (8 * 1024 * 1024)
#define DOUBLES_PER_LOOP_ITER 16

static double timespec_to_double(const struct timespec *ts)
{
    double sec = ts->tv_sec;
    double nsec = ts->tv_nsec;
    return sec + (nsec / 1000000000L);
}

struct stopwatch {
    struct timespec start;
    struct timespec stop;
};

static struct stopwatch *stopwatch_create(void)
{
    struct stopwatch *watch;

    watch = calloc(1, sizeof(struct stopwatch));
    if (!watch) {
        fprintf(stderr, "failed to allocate memory for stopwatch\n");
        goto error;
    }
    if (clock_gettime(CLOCK_MONOTONIC, &watch->start)) {
        int err = errno;
        fprintf(stderr, "clock_gettime(CLOCK_MONOTONIC) failed with "
            "error %d (%s)\n", err, strerror(err));
        goto error;
    }
    return watch;

error:
    free(watch);
    return NULL;
}

static void stopwatch_stop(struct stopwatch *watch,
        long long bytes_read)
{
    double elapsed, rate;

    if (clock_gettime(CLOCK_MONOTONIC, &watch->stop)) {
        int err = errno;
        fprintf(stderr, "clock_gettime(CLOCK_MONOTONIC) failed with "
            "error %d (%s)\n", err, strerror(err));
        goto done;
    }
    elapsed = timespec_to_double(&watch->stop) -
        timespec_to_double(&watch->start);
    rate = (bytes_read / elapsed) / (1024 * 1024 * 1024);
    printf("stopwatch: took %.5g seconds to read %lld bytes, "
        "for %.5g GB/s\n", elapsed, bytes_read, rate);
    printf("stopwatch:  %.5g seconds\n", elapsed);
done:
    free(watch);
}

enum vecsum_type {
    VECSUM_LOCAL = 0,
    VECSUM_LIBHDFS,
    VECSUM_ZCR,
};

#define VECSUM_TYPE_VALID_VALUES "libhdfs, zcr, or local"

int parse_vecsum_type(const char *str)
{
    if (strcasecmp(str, "local") == 0)
        return VECSUM_LOCAL;
    else if (strcasecmp(str, "libhdfs") == 0)
        return VECSUM_LIBHDFS;
    else if (strcasecmp(str, "zcr") == 0)
        return VECSUM_ZCR;
    else
        return -1;
}

struct options {
    // The path to read.
    const char *path;

    // Length of the file.
    long long length;

    // The number of times to read the path.
    int passes;

    // Type of vecsum to do
    enum vecsum_type ty;

    // RPC address to use for HDFS
    const char *rpc_address;
};

static struct options *options_create(void)
{
    struct options *opts = NULL;
    const char *pass_str;
    const char *ty_str;
    const char *length_str;
    int ty;

    opts = calloc(1, sizeof(struct options));
    if (!opts) {
        fprintf(stderr, "failed to calloc options\n");
        goto error;
    }
    opts->path = getenv("VECSUM_PATH");
    if (!opts->path) {
        fprintf(stderr, "You must set the VECSUM_PATH environment "
            "variable to the path of the file to read.\n");
        goto error;
    }
    length_str = getenv("VECSUM_LENGTH");
    if (!length_str) {
        length_str = "2147483648";
    }
    opts->length = atoll(length_str);
    if (!opts->length) {
        fprintf(stderr, "Can't parse VECSUM_LENGTH of '%s'.\n",
                length_str);
        goto error;
    }
    if (opts->length % VECSUM_CHUNK_SIZE) {
        fprintf(stderr, "VECSUM_LENGTH must be a multiple of '%lld'.  The "
                "currently specified length of '%lld' is not.\n",
                (long long)VECSUM_CHUNK_SIZE, (long long)opts->length);
        goto error;
    }
    pass_str = getenv("VECSUM_PASSES");
    if (!pass_str) {
        fprintf(stderr, "You must set the VECSUM_PASSES environment "
            "variable to the number of passes to make.\n");
        goto error;
    }
    opts->passes = atoi(pass_str);
    if (opts->passes <= 0) {
        fprintf(stderr, "Invalid value for the VECSUM_PASSES "
            "environment variable.  You must set this to a "
            "number greater than 0.\n");
        goto error;
    }
    ty_str = getenv("VECSUM_TYPE");
    if (!ty_str) {
        fprintf(stderr, "You must set the VECSUM_TYPE environment "
            "variable to " VECSUM_TYPE_VALID_VALUES "\n");
        goto error;
    }
    ty = parse_vecsum_type(ty_str);
    if (ty < 0) {
        fprintf(stderr, "Invalid VECSUM_TYPE environment variable.  "
            "Valid values are " VECSUM_TYPE_VALID_VALUES "\n");
        goto error;
    }
    opts->ty = ty;
    opts->rpc_address = getenv("VECSUM_RPC_ADDRESS");
    if (!opts->rpc_address) {
        opts->rpc_address = "default";
    }
    return opts;
error:
    free(opts);
    return NULL;
}

static int test_file_chunk_setup(double **chunk)
{
    int i;
    double *c, val;

    c = malloc(VECSUM_CHUNK_SIZE);
    if (!c) {
        fprintf(stderr, "test_file_create: failed to malloc "
                "a buffer of size '%lld'\n",
                (long long) VECSUM_CHUNK_SIZE);
        return EIO;
    }
    val = 0.0;
    for (i = 0; i < VECSUM_CHUNK_SIZE / sizeof(double); i++) {
        c[i] = val;
        val += 0.5;
    }
    *chunk = c;
    return 0;
}

static void options_free(struct options *opts)
{
    free(opts);
}

struct local_data {
    int fd;
    double *mmap;
    long long length;
};

static int local_data_create_file(struct local_data *cdata,
                                  const struct options *opts)
{
    int ret = EIO;
    int dup_fd = -1;
    FILE *fp = NULL;
    double *chunk = NULL;
    long long offset = 0;

    dup_fd = dup(cdata->fd);
    if (dup_fd < 0) {
        ret = errno;
        fprintf(stderr, "local_data_create_file: dup failed: %s (%d)\n",
                strerror(ret), ret);
        goto done;
    }
    fp = fdopen(dup_fd, "w");
    if (!fp) {
        ret = errno;
        fprintf(stderr, "local_data_create_file: fdopen failed: %s (%d)\n",
                strerror(ret), ret);
        goto done;
    }
    ret = test_file_chunk_setup(&chunk);
    if (ret)
        goto done;
    while (offset < opts->length) {
        if (fwrite(chunk, VECSUM_CHUNK_SIZE, 1, fp) != 1) {
            fprintf(stderr, "local_data_create_file: failed to write to "
                    "the local file '%s' at offset %lld\n",
                    opts->path, offset);
            ret = EIO;
            goto done;
        }
        offset += VECSUM_CHUNK_SIZE;
    }
    fprintf(stderr, "local_data_create_file: successfully re-wrote %s as "
            "a file of length %lld\n", opts->path, opts->length);
    ret = 0;

done:
    if (dup_fd >= 0) {
        close(dup_fd);
    }
    if (fp) {
        fclose(fp);
    }
    free(chunk);
    return ret;
}

static struct local_data *local_data_create(const struct options *opts)
{
    struct local_data *cdata = NULL;
    struct stat st_buf;

    cdata = malloc(sizeof(*cdata));
    if (!cdata) {
        fprintf(stderr, "Failed to allocate local test data.\n");
        goto error;
    }
    cdata->fd = -1;
    cdata->mmap = MAP_FAILED;
    cdata->length = opts->length;

    cdata->fd = open(opts->path, O_RDWR | O_CREAT, 0777);
    if (cdata->fd < 0) {
        int err = errno;
        fprintf(stderr, "local_data_create: failed to open %s "
            "for read/write: error %d (%s)\n", opts->path, err, strerror(err));
        goto error;
    }
    if (fstat(cdata->fd, &st_buf)) {
        int err = errno;
        fprintf(stderr, "local_data_create: fstat(%s) failed: "
            "error %d (%s)\n", opts->path, err, strerror(err));
        goto error;
    }
    if (st_buf.st_size != opts->length) {
        int err;
        fprintf(stderr, "local_data_create: current size of %s is %lld, but "
                "we want %lld.  Re-writing the file.\n",
                opts->path, (long long)st_buf.st_size,
                (long long)opts->length);
        err = local_data_create_file(cdata, opts);
        if (err)
            goto error;
    }
    cdata->mmap = mmap(NULL, cdata->length, PROT_READ,
                       MAP_PRIVATE, cdata->fd, 0);
    if (cdata->mmap == MAP_FAILED) {
        int err = errno;
        fprintf(stderr, "local_data_create: mmap(%s) failed: "
            "error %d (%s)\n", opts->path, err, strerror(err));
        goto error;
    }
    return cdata;

error:
    if (cdata) {
        if (cdata->fd >= 0) {
            close(cdata->fd);
        }
        free(cdata);
    }
    return NULL;
}

static void local_data_free(struct local_data *cdata)
{
    close(cdata->fd);
    munmap(cdata->mmap, cdata->length);
}

struct libhdfs_data {
    hdfsFS fs;
    hdfsFile file;
    long long length;
    double *buf;
};

static void libhdfs_data_free(struct libhdfs_data *ldata)
{
    if (ldata->fs) {
        free(ldata->buf);
        if (ldata->file) {
            hdfsCloseFile(ldata->fs, ldata->file);
        }
        hdfsDisconnect(ldata->fs);
    }
    free(ldata);
}

static int libhdfs_data_create_file(struct libhdfs_data *ldata,
                                    const struct options *opts)
{
    int ret;
    double *chunk = NULL;
    long long offset = 0;

    ldata->file = hdfsOpenFile(ldata->fs, opts->path, O_WRONLY, 0, 1, 0);
    if (!ldata->file) {
        ret = errno;
        fprintf(stderr, "libhdfs_data_create_file: hdfsOpenFile(%s, "
            "O_WRONLY) failed: error %d (%s)\n", opts->path, ret,
            strerror(ret));
        goto done;
    }
    ret = test_file_chunk_setup(&chunk);
    if (ret)
        goto done;
    while (offset < opts->length) {
        ret = hdfsWrite(ldata->fs, ldata->file, chunk, VECSUM_CHUNK_SIZE);
        if (ret < 0) {
            ret = errno;
            fprintf(stderr, "libhdfs_data_create_file: got error %d (%s) at "
                    "offset %lld of %s\n", ret, strerror(ret),
                    offset, opts->path);
            goto done;
        } else if (ret < VECSUM_CHUNK_SIZE) {
            fprintf(stderr, "libhdfs_data_create_file: got short write "
                    "of %d at offset %lld of %s\n", ret, offset, opts->path);
            goto done;
        }
        offset += VECSUM_CHUNK_SIZE;
    }
    ret = 0;
done:
    free(chunk);
    if (ldata->file) {
        if (hdfsCloseFile(ldata->fs, ldata->file)) {
            fprintf(stderr, "libhdfs_data_create_file: hdfsCloseFile error.");
            ret = EIO;
        }
        ldata->file = NULL;
    }
    return ret;
}

static struct libhdfs_data *libhdfs_data_create(const struct options *opts)
{
    struct libhdfs_data *ldata = NULL;
    struct hdfsBuilder *builder = NULL;
    hdfsFileInfo *pinfo = NULL;

    ldata = calloc(1, sizeof(struct libhdfs_data));
    if (!ldata) {
        fprintf(stderr, "Failed to allocate libhdfs test data.\n");
        goto error;
    }
    builder = hdfsNewBuilder();
    if (!builder) {
        fprintf(stderr, "Failed to create builder.\n");
        goto error;
    }
    hdfsBuilderSetNameNode(builder, opts->rpc_address);
    hdfsBuilderConfSetStr(builder,
        "dfs.client.read.shortcircuit.skip.checksum", "true");
    ldata->fs = hdfsBuilderConnect(builder);
    if (!ldata->fs) {
        fprintf(stderr, "Could not connect to default namenode!\n");
        goto error;
    }
    pinfo = hdfsGetPathInfo(ldata->fs, opts->path);
    if (!pinfo) {
        int err = errno;
        fprintf(stderr, "hdfsGetPathInfo(%s) failed: error %d (%s).  "
                "Attempting to re-create file.\n",
            opts->path, err, strerror(err));
        if (libhdfs_data_create_file(ldata, opts))
            goto error;
    } else if (pinfo->mSize != opts->length) {
        fprintf(stderr, "hdfsGetPathInfo(%s) failed: length was %lld, "
                "but we want length %lld.  Attempting to re-create file.\n",
                opts->path, (long long)pinfo->mSize, (long long)opts->length);
        if (libhdfs_data_create_file(ldata, opts))
            goto error;
    }
    ldata->file = hdfsOpenFile(ldata->fs, opts->path, O_RDONLY, 0, 0, 0);
    if (!ldata->file) {
        int err = errno;
        fprintf(stderr, "hdfsOpenFile(%s) failed: error %d (%s)\n",
            opts->path, err, strerror(err));
        goto error;
    }
    ldata->length = opts->length;
    return ldata;

error:
    if (pinfo)
        hdfsFreeFileInfo(pinfo, 1);
    if (ldata)
        libhdfs_data_free(ldata);
    return NULL;
}

static int check_byte_size(int byte_size, const char *const str)
{
    if (byte_size % sizeof(double)) {
        fprintf(stderr, "%s is not a multiple "
            "of sizeof(double)\n", str);
        return EINVAL;
    }
    if ((byte_size / sizeof(double)) % DOUBLES_PER_LOOP_ITER) {
        fprintf(stderr, "The number of doubles contained in "
            "%s is not a multiple of DOUBLES_PER_LOOP_ITER\n",
            str);
        return EINVAL;
    }
    return 0;
}

#ifdef HAVE_INTEL_SSE_INTRINSICS

#include <emmintrin.h>

static double vecsum(const double *buf, int num_doubles)
{
    int i;
    double hi, lo;
    __m128d x0, x1, x2, x3, x4, x5, x6, x7;
    __m128d sum0 = _mm_set_pd(0.0,0.0);
    __m128d sum1 = _mm_set_pd(0.0,0.0);
    __m128d sum2 = _mm_set_pd(0.0,0.0);
    __m128d sum3 = _mm_set_pd(0.0,0.0);
    __m128d sum4 = _mm_set_pd(0.0,0.0);
    __m128d sum5 = _mm_set_pd(0.0,0.0);
    __m128d sum6 = _mm_set_pd(0.0,0.0);
    __m128d sum7 = _mm_set_pd(0.0,0.0);
    for (i = 0; i < num_doubles; i+=DOUBLES_PER_LOOP_ITER) {
        x0 = _mm_load_pd(buf + i + 0);
        x1 = _mm_load_pd(buf + i + 2);
        x2 = _mm_load_pd(buf + i + 4);
        x3 = _mm_load_pd(buf + i + 6);
        x4 = _mm_load_pd(buf + i + 8);
        x5 = _mm_load_pd(buf + i + 10);
        x6 = _mm_load_pd(buf + i + 12);
        x7 = _mm_load_pd(buf + i + 14);
        sum0 = _mm_add_pd(sum0, x0);
        sum1 = _mm_add_pd(sum1, x1);
        sum2 = _mm_add_pd(sum2, x2);
        sum3 = _mm_add_pd(sum3, x3);
        sum4 = _mm_add_pd(sum4, x4);
        sum5 = _mm_add_pd(sum5, x5);
        sum6 = _mm_add_pd(sum6, x6);
        sum7 = _mm_add_pd(sum7, x7);
    }
    x0 = _mm_add_pd(sum0, sum1);
    x1 = _mm_add_pd(sum2, sum3);
    x2 = _mm_add_pd(sum4, sum5);
    x3 = _mm_add_pd(sum6, sum7);
    x4 = _mm_add_pd(x0, x1);
    x5 = _mm_add_pd(x2, x3);
    x6 = _mm_add_pd(x4, x5);
    _mm_storeh_pd(&hi, x6);
    _mm_storel_pd(&lo, x6);
    return hi + lo;
}

#else

static double vecsum(const double *buf, int num_doubles)
{
    int i;
    double sum = 0.0;
    for (i = 0; i < num_doubles; i++) {
        sum += buf[i];
    }
    return sum;
}

#endif

static int vecsum_zcr_loop(int pass, struct libhdfs_data *ldata,
        struct hadoopRzOptions *zopts,
        const struct options *opts)
{
    int32_t len;
    double sum = 0.0;
    const double *buf;
    struct hadoopRzBuffer *rzbuf = NULL;
    int ret;

    while (1) {
        rzbuf = hadoopReadZero(ldata->file, zopts, ZCR_READ_CHUNK_SIZE);
        if (!rzbuf) {
            ret = errno;
            fprintf(stderr, "hadoopReadZero failed with error "
                "code %d (%s)\n", ret, strerror(ret));
            goto done;
        }
        buf = hadoopRzBufferGet(rzbuf);
        if (!buf) break;
        len = hadoopRzBufferLength(rzbuf);
        if (len < ZCR_READ_CHUNK_SIZE) {
            fprintf(stderr, "hadoopReadZero got a partial read "
                "of length %d\n", len);
            ret = EINVAL;
            goto done;
        }
        sum += vecsum(buf,
            ZCR_READ_CHUNK_SIZE / sizeof(double));
        hadoopRzBufferFree(ldata->file, rzbuf);
    }
    printf("finished zcr pass %d.  sum = %g\n", pass, sum);
    ret = 0;

done:
    if (rzbuf)
        hadoopRzBufferFree(ldata->file, rzbuf);
    return ret;
}

static int vecsum_zcr(struct libhdfs_data *ldata,
        const struct options *opts)
{
    int ret, pass;
    struct hadoopRzOptions *zopts = NULL;

    zopts = hadoopRzOptionsAlloc();
    if (!zopts) {
        fprintf(stderr, "hadoopRzOptionsAlloc failed.\n");
        ret = ENOMEM;
        goto done;
    }
    if (hadoopRzOptionsSetSkipChecksum(zopts, 1)) {
        ret = errno;
        perror("hadoopRzOptionsSetSkipChecksum failed: ");
        goto done;
    }
    if (hadoopRzOptionsSetByteBufferPool(zopts, NULL)) {
        ret = errno;
        perror("hadoopRzOptionsSetByteBufferPool failed: ");
        goto done;
    }
    for (pass = 0; pass < opts->passes; ++pass) {
        ret = vecsum_zcr_loop(pass, ldata, zopts, opts);
        if (ret) {
            fprintf(stderr, "vecsum_zcr_loop pass %d failed "
                "with error %d\n", pass, ret);
            goto done;
        }
        hdfsSeek(ldata->fs, ldata->file, 0);
    }
    ret = 0;
done:
    if (zopts)
        hadoopRzOptionsFree(zopts);
    return ret;
}

tSize hdfsReadFully(hdfsFS fs, hdfsFile f, void* buffer, tSize length)
{
    uint8_t *buf = buffer;
    tSize ret, nread = 0;

    while (length > 0) {
        ret = hdfsRead(fs, f, buf, length);
        if (ret < 0) {
            if (errno != EINTR) {
                return -1;
            }
        }
        if (ret == 0) {
            break;
        }
        nread += ret;
        length -= ret;
        buf += ret;
    }
    return nread;
}

static int vecsum_normal_loop(int pass, const struct libhdfs_data *ldata,
            const struct options *opts)
{
    double sum = 0.0;

    while (1) {
        int res = hdfsReadFully(ldata->fs, ldata->file, ldata->buf,
                NORMAL_READ_CHUNK_SIZE);
        if (res == 0) // EOF
            break;
        if (res < 0) {
            int err = errno;
            fprintf(stderr, "hdfsRead failed with error %d (%s)\n",
                err, strerror(err));
            return err;
        }
        if (res < NORMAL_READ_CHUNK_SIZE) {
            fprintf(stderr, "hdfsRead got a partial read of "
                "length %d\n", res);
            return EINVAL;
        }
        sum += vecsum(ldata->buf,
                  NORMAL_READ_CHUNK_SIZE / sizeof(double));
    }
    printf("finished normal pass %d.  sum = %g\n", pass, sum);
    return 0;
}

static int vecsum_libhdfs(struct libhdfs_data *ldata,
            const struct options *opts)
{
    int pass;

    ldata->buf = malloc(NORMAL_READ_CHUNK_SIZE);
    if (!ldata->buf) {
        fprintf(stderr, "failed to malloc buffer of size %d\n",
            NORMAL_READ_CHUNK_SIZE);
        return ENOMEM;
    }
    for (pass = 0; pass < opts->passes; ++pass) {
        int ret = vecsum_normal_loop(pass, ldata, opts);
        if (ret) {
            fprintf(stderr, "vecsum_normal_loop pass %d failed "
                "with error %d\n", pass, ret);
            return ret;
        }
        hdfsSeek(ldata->fs, ldata->file, 0);
    }
    return 0;
}

static void vecsum_local(struct local_data *cdata, const struct options *opts)
{
    int pass;

    for (pass = 0; pass < opts->passes; pass++) {
        double sum = vecsum(cdata->mmap, cdata->length / sizeof(double));
        printf("finished vecsum_local pass %d.  sum = %g\n", pass, sum);
    }
}

static long long vecsum_length(const struct options *opts,
                const struct libhdfs_data *ldata)
{
    if (opts->ty == VECSUM_LOCAL) {
        struct stat st_buf = { 0 };
        if (stat(opts->path, &st_buf)) {
            int err = errno;
            fprintf(stderr, "vecsum_length: stat(%s) failed: "
                "error %d (%s)\n", opts->path, err, strerror(err));
            return -EIO;
        }
        return st_buf.st_size;
    } else {
        return ldata->length;
    }
}

/*
 * vecsum is a microbenchmark which measures the speed of various ways of
 * reading from HDFS.  It creates a file containing floating-point 'doubles',
 * and computes the sum of all the doubles several times.  For some CPUs,
 * assembly optimizations are used for the summation (SSE, etc).
 */
int main(void)
{
    int ret = 1;
    struct options *opts = NULL;
    struct local_data *cdata = NULL;
    struct libhdfs_data *ldata = NULL;
    struct stopwatch *watch = NULL;

    if (check_byte_size(VECSUM_CHUNK_SIZE, "VECSUM_CHUNK_SIZE") ||
        check_byte_size(ZCR_READ_CHUNK_SIZE,
                "ZCR_READ_CHUNK_SIZE") ||
        check_byte_size(NORMAL_READ_CHUNK_SIZE,
                "NORMAL_READ_CHUNK_SIZE")) {
        goto done;
    }
    opts = options_create();
    if (!opts)
        goto done;
    if (opts->ty == VECSUM_LOCAL) {
        cdata = local_data_create(opts);
        if (!cdata)
            goto done;
    } else {
        ldata = libhdfs_data_create(opts);
        if (!ldata)
            goto done;
    }
    watch = stopwatch_create();
    if (!watch)
        goto done;
    switch (opts->ty) {
    case VECSUM_LOCAL:
        vecsum_local(cdata, opts);
        ret = 0;
        break;
    case VECSUM_LIBHDFS:
        ret = vecsum_libhdfs(ldata, opts);
        break;
    case VECSUM_ZCR:
        ret = vecsum_zcr(ldata, opts);
        break;
    }
    if (ret) {
        fprintf(stderr, "vecsum failed with error %d\n", ret);
        goto done;
    }
    ret = 0;
done:
    fprintf(stderr, "cleaning up...\n");
    if (watch && (ret == 0)) {
        long long length = vecsum_length(opts, ldata);
        if (length >= 0) {
            stopwatch_stop(watch, length * opts->passes);
        }
    }
    if (cdata)
        local_data_free(cdata);
    if (ldata)
        libhdfs_data_free(ldata);
    if (opts)
        options_free(opts);
    return ret;
}

// vim: ts=4:sw=4:tw=79:et
