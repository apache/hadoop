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
#include "hdfs.h"
#include "native_mini_dfs.h"

#include <errno.h>
#include <semaphore.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TLH_MAX_THREADS 100

static struct NativeMiniDfsCluster* cluster;

static const char *user;

struct tlhThreadInfo {
    /** Thread index */
    int threadIdx;
    /** 0 = thread was successful; error code otherwise */
    int success;
    /** pthread identifier */
    pthread_t thread;
};

static int hdfsSingleNameNodeConnect(struct NativeMiniDfsCluster *cluster,
                                     hdfsFS *fs)
{
    int nnPort;
    const char *nnHost;
    hdfsFS hdfs;
    
    if (nmdGetNameNodeHttpAddress(cluster, &nnPort, &nnHost)) {
        fprintf(stderr, "Error when retrieving namenode host address.\n");
        return 1;
    }
    
    hdfs = hdfsConnectAsUser(nnHost, nnPort, user);
    if(!hdfs) {
        fprintf(stderr, "Oops! Failed to connect to hdfs!\n");
        return 1;
    }

    *fs = hdfs;
    return 0;
}

static int doTestHdfsOperations(struct tlhThreadInfo *ti, hdfsFS fs)
{
    char prefix[256], tmp[256];
    hdfsFile file;
    int ret, expected;
    hdfsFileInfo *fileInfo;
    
    snprintf(prefix, sizeof(prefix), "/tlhData%04d", ti->threadIdx);
    
    if (hdfsExists(fs, prefix) == 0) {
        EXPECT_ZERO(hdfsDelete(fs, prefix, 1));
    }
    EXPECT_ZERO(hdfsCreateDirectory(fs, prefix));
    snprintf(tmp, sizeof(tmp), "%s/file", prefix);
    
    EXPECT_NONNULL(hdfsOpenFile(fs, tmp, O_RDONLY, 0, 0, 0));
    
    file = hdfsOpenFile(fs, tmp, O_WRONLY, 0, 0, 0);
    EXPECT_NONNULL(file);
    
    /* TODO: implement writeFully and use it here */
    expected = (int)strlen(prefix);
    ret = hdfsWrite(fs, file, prefix, expected);
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
    EXPECT_ZERO(hdfsCloseFile(fs, file));
    
    /* Let's re-open the file for reading */
    file = hdfsOpenFile(fs, tmp, O_RDONLY, 0, 0, 0);
    EXPECT_NONNULL(file);
    
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
    EXPECT_ZERO(memcmp(prefix, tmp, expected));
    EXPECT_ZERO(hdfsCloseFile(fs, file));
        
    snprintf(tmp, sizeof(tmp), "%s/file", prefix);
    EXPECT_NONZERO(hdfsChown(fs, tmp, NULL, NULL));
    EXPECT_ZERO(hdfsChown(fs, tmp, NULL, "doop"));
    fileInfo = hdfsGetPathInfo(fs, tmp);
    EXPECT_NONNULL(fileInfo);
    EXPECT_ZERO(strcmp("doop", fileInfo->mGroup));
    hdfsFreeFileInfo(fileInfo, 1);
    
    EXPECT_ZERO(hdfsChown(fs, tmp, "ha", "doop2"));
    fileInfo = hdfsGetPathInfo(fs, tmp);
    EXPECT_NONNULL(fileInfo);
    EXPECT_ZERO(strcmp("ha", fileInfo->mOwner));
    EXPECT_ZERO(strcmp("doop2", fileInfo->mGroup));
    hdfsFreeFileInfo(fileInfo, 1);
    
    EXPECT_ZERO(hdfsChown(fs, tmp, "ha2", NULL));
    fileInfo = hdfsGetPathInfo(fs, tmp);
    EXPECT_NONNULL(fileInfo);
    EXPECT_ZERO(strcmp("ha2", fileInfo->mOwner));
    EXPECT_ZERO(strcmp("doop2", fileInfo->mGroup));
    hdfsFreeFileInfo(fileInfo, 1);
    
    EXPECT_ZERO(hdfsDelete(fs, prefix, 1));
    return 0;
}

static void *testHdfsOperations(void *v)
{
    struct tlhThreadInfo *ti = (struct tlhThreadInfo*)v;
    hdfsFS fs = NULL;
    int ret;
    
    fprintf(stderr, "testHdfsOperations(threadIdx=%d): starting\n",
            ti->threadIdx);
    ret = hdfsSingleNameNodeConnect(cluster, &fs);
    if (ret) {
        fprintf(stderr, "testHdfsOperations(threadIdx=%d): "
                "hdfsSingleNameNodeConnect failed with error %d.\n",
                ti->threadIdx, ret);
        ti->success = EIO;
        return NULL;
    }
    ti->success = doTestHdfsOperations(ti, fs);
    if (hdfsDisconnect(fs)) {
        ret = errno;
        fprintf(stderr, "hdfsDisconnect error %d\n", ret);
        ti->success = ret;
    }
    return NULL;
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

/**
 * Test that we can write a file with libhdfs and then read it back
 */
int main(int argc, const char *args[])
{
    int i, tlhNumThreads;
    const char *tlhNumThreadsStr;
    struct tlhThreadInfo ti[TLH_MAX_THREADS];
    
    if (argc != 2) {
        fprintf(stderr, "usage: test_libwebhdfs_threaded <username>\n");
        exit(1);
    }
    user = args[1];
    
    struct NativeMiniDfsConf conf = {
        .doFormat = 1, .webhdfsEnabled = 1, .namenodeHttpPort = 50070,
    };
    cluster = nmdCreate(&conf);
    EXPECT_NONNULL(cluster);
    EXPECT_ZERO(nmdWaitClusterUp(cluster));
    
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
    
    for (i = 0; i < tlhNumThreads; i++) {
        EXPECT_ZERO(pthread_create(&ti[i].thread, NULL,
                                   testHdfsOperations, &ti[i]));
    }
    for (i = 0; i < tlhNumThreads; i++) {
        EXPECT_ZERO(pthread_join(ti[i].thread, NULL));
    }
    
    EXPECT_ZERO(nmdShutdown(cluster));
    nmdFree(cluster);
    return checkFailures(ti, tlhNumThreads);
}
