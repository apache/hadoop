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

#include "expect.h"
#include "exception.h"
#include "hdfs/hdfs.h"
#include "jni_helper.h"
#include "native_mini_dfs.h"
#include "os/mutexes.h"
#include "os/thread.h"

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TO_STR_HELPER(X) #X
#define TO_STR(X) TO_STR_HELPER(X)

#define TLH_MAX_THREADS 100

#define TLH_DEFAULT_BLOCK_SIZE 134217728

static struct NativeMiniDfsCluster* tlhCluster;

struct tlhThreadInfo {
    /** Thread index */
    int threadIdx;
    /** 0 = thread was successful; error code otherwise */
    int success;
    /** thread identifier */
    thread theThread;
};

static int hdfsSingleNameNodeConnect(struct NativeMiniDfsCluster *cl, hdfsFS *fs,
                                     const char *username)
{
    int ret;
    tPort port;
    hdfsFS hdfs;
    struct hdfsBuilder *bld;
    
    port = (tPort)nmdGetNameNodePort(cl);
    if (port < 0) {
        fprintf(stderr, "hdfsSingleNameNodeConnect: nmdGetNameNodePort "
                "returned error %d\n", port);
        return port;
    }
    bld = hdfsNewBuilder();
    if (!bld)
        return -ENOMEM;
    hdfsBuilderSetForceNewInstance(bld);
    hdfsBuilderSetNameNode(bld, "localhost");
    hdfsBuilderSetNameNodePort(bld, port);
    hdfsBuilderConfSetStr(bld, "dfs.block.size",
                          TO_STR(TLH_DEFAULT_BLOCK_SIZE));
    hdfsBuilderConfSetStr(bld, "dfs.blocksize",
                          TO_STR(TLH_DEFAULT_BLOCK_SIZE));
    if (username) {
        hdfsBuilderSetUserName(bld, username);
    }
    hdfs = hdfsBuilderConnect(bld);
    if (!hdfs) {
        ret = -errno;
        return ret;
    }
    *fs = hdfs;
    return 0;
}

static int doTestGetDefaultBlockSize(hdfsFS fs, const char *path)
{
    int64_t blockSize;
    int ret;

    blockSize = hdfsGetDefaultBlockSize(fs);
    if (blockSize < 0) {
        ret = errno;
        fprintf(stderr, "hdfsGetDefaultBlockSize failed with error %d\n", ret);
        return ret;
    } else if (blockSize != TLH_DEFAULT_BLOCK_SIZE) {
        fprintf(stderr, "hdfsGetDefaultBlockSize got %"PRId64", but we "
                "expected %d\n", blockSize, TLH_DEFAULT_BLOCK_SIZE);
        return EIO;
    }

    blockSize = hdfsGetDefaultBlockSizeAtPath(fs, path);
    if (blockSize < 0) {
        ret = errno;
        fprintf(stderr, "hdfsGetDefaultBlockSizeAtPath(%s) failed with "
                "error %d\n", path, ret);
        return ret;
    } else if (blockSize != TLH_DEFAULT_BLOCK_SIZE) {
        fprintf(stderr, "hdfsGetDefaultBlockSizeAtPath(%s) got "
                "%"PRId64", but we expected %d\n", 
                path, blockSize, TLH_DEFAULT_BLOCK_SIZE);
        return EIO;
    }
    return 0;
}

struct tlhPaths {
    char prefix[256];
    char file1[256];
    char file2[256];
};

static int setupPaths(const struct tlhThreadInfo *ti, struct tlhPaths *paths)
{
    memset(paths, 0, sizeof(*paths));
    if (snprintf(paths->prefix, sizeof(paths->prefix), "/tlhData%04d",
                 ti->threadIdx) >= sizeof(paths->prefix)) {
        return ENAMETOOLONG;
    }
    if (snprintf(paths->file1, sizeof(paths->file1), "%s/file1",
                 paths->prefix) >= sizeof(paths->file1)) {
        return ENAMETOOLONG;
    }
    if (snprintf(paths->file2, sizeof(paths->file2), "%s/file2",
                 paths->prefix) >= sizeof(paths->file2)) {
        return ENAMETOOLONG;
    }
    return 0;
}

static int doTestHdfsOperations(struct tlhThreadInfo *ti, hdfsFS fs,
                                const struct tlhPaths *paths)
{
    char tmp[4096];
    hdfsFile file;
    int ret, expected, numEntries;
    hdfsFileInfo *fileInfo;
    struct hdfsReadStatistics *readStats = NULL;
    struct hdfsHedgedReadMetrics *hedgedMetrics = NULL;

    if (hdfsExists(fs, paths->prefix) == 0) {
        EXPECT_ZERO(hdfsDelete(fs, paths->prefix, 1));
    }
    EXPECT_ZERO(hdfsCreateDirectory(fs, paths->prefix));

    EXPECT_ZERO(doTestGetDefaultBlockSize(fs, paths->prefix));

    /* There should be no entry in the directory. */
    errno = EACCES; // see if errno is set to 0 on success
    EXPECT_NULL_WITH_ERRNO(hdfsListDirectory(fs, paths->prefix, &numEntries), 0);
    if (numEntries != 0) {
        fprintf(stderr, "hdfsListDirectory set numEntries to "
                "%d on empty directory.", numEntries);
    }

    /* There should not be any file to open for reading. */
    EXPECT_NULL(hdfsOpenFile(fs, paths->file1, O_RDONLY, 0, 0, 0));

    /* Check if the exceptions are stored in the TLS */
    EXPECT_STR_CONTAINS(hdfsGetLastExceptionRootCause(),
                        "File does not exist");
    EXPECT_STR_CONTAINS(hdfsGetLastExceptionStackTrace(),
                        "java.io.FileNotFoundException");

    /* hdfsOpenFile should not accept mode = 3 */
    EXPECT_NULL(hdfsOpenFile(fs, paths->file1, 3, 0, 0, 0));

    file = hdfsOpenFile(fs, paths->file1, O_WRONLY, 0, 0, 0);
    EXPECT_NONNULL(file);

    /* TODO: implement writeFully and use it here */
    expected = (int)strlen(paths->prefix);
    ret = hdfsWrite(fs, file, paths->prefix, expected);
    if (ret < 0) {
        ret = errno;
        fprintf(stderr, "hdfsWrite failed and set errno %d\n", ret);
        return ret;
    }
    if (ret != expected) {
        fprintf(stderr, "hdfsWrite was supposed to write %d bytes, but "
                "it wrote %d\n", ret, expected);
        return EIO;
    }
    EXPECT_ZERO(hdfsFlush(fs, file));
    EXPECT_ZERO(hdfsHSync(fs, file));
    EXPECT_ZERO(hdfsCloseFile(fs, file));

    /* There should be 1 entry in the directory. */
    EXPECT_NONNULL(hdfsListDirectory(fs, paths->prefix, &numEntries));
    if (numEntries != 1) {
        fprintf(stderr, "hdfsListDirectory set numEntries to "
                "%d on directory containing 1 file.", numEntries);
    }

    /* Let's re-open the file for reading */
    file = hdfsOpenFile(fs, paths->file1, O_RDONLY, 0, 0, 0);
    EXPECT_NONNULL(file);

    EXPECT_ZERO(hdfsFileGetReadStatistics(file, &readStats));
    errno = 0;
    EXPECT_UINT64_EQ(UINT64_C(0), readStats->totalBytesRead);
    EXPECT_UINT64_EQ(UINT64_C(0), readStats->totalLocalBytesRead);
    EXPECT_UINT64_EQ(UINT64_C(0), readStats->totalShortCircuitBytesRead);
    hdfsFileFreeReadStatistics(readStats);

    /* Verify that we can retrieve the hedged read metrics */
    EXPECT_ZERO(hdfsGetHedgedReadMetrics(fs, &hedgedMetrics));
    errno = 0;
    EXPECT_UINT64_EQ(UINT64_C(0), hedgedMetrics->hedgedReadOps);
    EXPECT_UINT64_EQ(UINT64_C(0), hedgedMetrics->hedgedReadOpsWin);
    EXPECT_UINT64_EQ(UINT64_C(0), hedgedMetrics->hedgedReadOpsInCurThread);
    hdfsFreeHedgedReadMetrics(hedgedMetrics);

    /* TODO: implement readFully and use it here */
    ret = hdfsRead(fs, file, tmp, sizeof(tmp));
    if (ret < 0) {
        ret = errno;
        fprintf(stderr, "hdfsRead failed and set errno %d\n", ret);
        return ret;
    }
    if (ret != expected) {
        fprintf(stderr, "hdfsRead was supposed to read %d bytes, but "
                "it read %d\n", ret, expected);
        return EIO;
    }
    EXPECT_ZERO(hdfsFileGetReadStatistics(file, &readStats));
    errno = 0;
    EXPECT_UINT64_EQ((uint64_t)expected, readStats->totalBytesRead);
    hdfsFileFreeReadStatistics(readStats);
    EXPECT_ZERO(hdfsFileClearReadStatistics(file));
    EXPECT_ZERO(hdfsFileGetReadStatistics(file, &readStats));
    EXPECT_UINT64_EQ((uint64_t)0, readStats->totalBytesRead);
    hdfsFileFreeReadStatistics(readStats);
    EXPECT_ZERO(memcmp(paths->prefix, tmp, expected));
    EXPECT_ZERO(hdfsCloseFile(fs, file));

    // TODO: Non-recursive delete should fail?
    //EXPECT_NONZERO(hdfsDelete(fs, prefix, 0));
    EXPECT_ZERO(hdfsCopy(fs, paths->file1, fs, paths->file2));

    EXPECT_ZERO(hdfsChown(fs, paths->file2, NULL, NULL));
    EXPECT_ZERO(hdfsChown(fs, paths->file2, NULL, "doop"));
    fileInfo = hdfsGetPathInfo(fs, paths->file2);
    EXPECT_NONNULL(fileInfo);
    EXPECT_ZERO(strcmp("doop", fileInfo->mGroup));
    EXPECT_ZERO(hdfsFileIsEncrypted(fileInfo));
    hdfsFreeFileInfo(fileInfo, 1);

    EXPECT_ZERO(hdfsChown(fs, paths->file2, "ha", "doop2"));
    fileInfo = hdfsGetPathInfo(fs, paths->file2);
    EXPECT_NONNULL(fileInfo);
    EXPECT_ZERO(strcmp("ha", fileInfo->mOwner));
    EXPECT_ZERO(strcmp("doop2", fileInfo->mGroup));
    hdfsFreeFileInfo(fileInfo, 1);

    EXPECT_ZERO(hdfsChown(fs, paths->file2, "ha2", NULL));
    fileInfo = hdfsGetPathInfo(fs, paths->file2);
    EXPECT_NONNULL(fileInfo);
    EXPECT_ZERO(strcmp("ha2", fileInfo->mOwner));
    EXPECT_ZERO(strcmp("doop2", fileInfo->mGroup));
    hdfsFreeFileInfo(fileInfo, 1);

    snprintf(tmp, sizeof(tmp), "%s/nonexistent-file-name", paths->prefix);
    EXPECT_NEGATIVE_ONE_WITH_ERRNO(hdfsChown(fs, tmp, "ha3", NULL), ENOENT);
    return 0;
}

static int testHdfsOperationsImpl(struct tlhThreadInfo *ti)
{
    hdfsFS fs = NULL;
    struct tlhPaths paths;

    fprintf(stderr, "testHdfsOperations(threadIdx=%d): starting\n",
        ti->threadIdx);
    EXPECT_ZERO(hdfsSingleNameNodeConnect(tlhCluster, &fs, NULL));
    EXPECT_ZERO(setupPaths(ti, &paths));
    // test some operations
    EXPECT_ZERO(doTestHdfsOperations(ti, fs, &paths));
    EXPECT_ZERO(hdfsDisconnect(fs));
    // reconnect as user "foo" and verify that we get permission errors
    EXPECT_ZERO(hdfsSingleNameNodeConnect(tlhCluster, &fs, "foo"));
    EXPECT_NEGATIVE_ONE_WITH_ERRNO(hdfsChown(fs, paths.file1, "ha3", NULL), EACCES);
    EXPECT_ZERO(hdfsDisconnect(fs));
    // reconnect to do the final delete.
    EXPECT_ZERO(hdfsSingleNameNodeConnect(tlhCluster, &fs, NULL));
    EXPECT_ZERO(hdfsDelete(fs, paths.prefix, 1));
    EXPECT_ZERO(hdfsDisconnect(fs));
    return 0;
}

static void testHdfsOperations(void *v)
{
    struct tlhThreadInfo *ti = (struct tlhThreadInfo*)v;
    int ret = testHdfsOperationsImpl(ti);
    ti->success = ret;
}

static int checkFailures(struct tlhThreadInfo *ti, int tlhNumThreads)
{
    int i, threadsFailed = 0;
    const char *sep = "";

    for (i = 0; i < tlhNumThreads; i++) {
        if (ti[i].success != 0) {
            threadsFailed = 1;
        }
    }
    if (!threadsFailed) {
        fprintf(stderr, "testLibHdfs: all threads succeeded.  SUCCESS.\n");
        return EXIT_SUCCESS;
    }
    fprintf(stderr, "testLibHdfs: some threads failed: [");
    for (i = 0; i < tlhNumThreads; i++) {
        if (ti[i].success != 0) {
            fprintf(stderr, "%s%d", sep, i);
            sep = ", "; 
        }
    }
    fprintf(stderr, "].  FAILURE.\n");
    return EXIT_FAILURE;
}

int testRecursiveJvmMutex() {
    jthrowable jthr;
    JNIEnv *env = getJNIEnv();
    if (!env) {
        fprintf(stderr, "testRecursiveJvmMutex: getJNIEnv failed\n");
        return -EIO;
    }
    jthr = newRuntimeError(env, "Dummy error to print for testing");

    /* printExceptionAndFree() takes the jvmMutex within */
    mutexLock(&jvmMutex);
    printExceptionAndFree(env, jthr, PRINT_EXC_ALL, "testRecursiveJvmMutex");
    mutexUnlock(&jvmMutex);

    return 0;
}

/**
 * Test that we can write a file with libhdfs and then read it back
 */
int main(void)
{
    int i, tlhNumThreads;
    const char *tlhNumThreadsStr;
    struct tlhThreadInfo ti[TLH_MAX_THREADS];
    struct NativeMiniDfsConf conf = {
        1, /* doFormat */
    };

    /* Check that the recursive mutex works as expected */
    if (testRecursiveJvmMutex() < 0) {
        fprintf(stderr, "testRecursiveJvmMutex failed\n");
        return EXIT_FAILURE;
    }

    tlhNumThreadsStr = getenv("TLH_NUM_THREADS");
    if (!tlhNumThreadsStr) {
        tlhNumThreadsStr = "3";
    }
    tlhNumThreads = atoi(tlhNumThreadsStr);
    if ((tlhNumThreads <= 0) || (tlhNumThreads > TLH_MAX_THREADS)) {
        fprintf(stderr, "testLibHdfs: must have a number of threads "
                "between 1 and %d inclusive, not %d\n",
                TLH_MAX_THREADS, tlhNumThreads);
        return EXIT_FAILURE;
    }
    memset(&ti[0], 0, sizeof(ti));
    for (i = 0; i < tlhNumThreads; i++) {
        ti[i].threadIdx = i;
    }

    tlhCluster = nmdCreate(&conf);
    EXPECT_NONNULL(tlhCluster);
    EXPECT_ZERO(nmdWaitClusterUp(tlhCluster));

    for (i = 0; i < tlhNumThreads; i++) {
        ti[i].theThread.start = testHdfsOperations;
        ti[i].theThread.arg = &ti[i];
        EXPECT_ZERO(threadCreate(&ti[i].theThread));
    }
    for (i = 0; i < tlhNumThreads; i++) {
        EXPECT_ZERO(threadJoin(&ti[i].theThread));
    }

    EXPECT_ZERO(nmdShutdown(tlhCluster));
    nmdFree(tlhCluster);
    return checkFailures(ti, tlhNumThreads);
}
