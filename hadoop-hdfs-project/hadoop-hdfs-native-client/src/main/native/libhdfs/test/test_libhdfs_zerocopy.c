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
#include "platform.h"

#include <errno.h>
#include <inttypes.h>
#include <unistd.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#define TO_STR_HELPER(X) #X
#define TO_STR(X) TO_STR_HELPER(X)

#define TEST_FILE_NAME_LENGTH 128
#define TEST_ZEROCOPY_FULL_BLOCK_SIZE 4096
#define TEST_ZEROCOPY_LAST_BLOCK_SIZE 3215
#define TEST_ZEROCOPY_NUM_BLOCKS 6
#define SMALL_READ_LEN 16
#define TEST_ZEROCOPY_FILE_LEN \
  (((TEST_ZEROCOPY_NUM_BLOCKS - 1) * TEST_ZEROCOPY_FULL_BLOCK_SIZE) + \
    TEST_ZEROCOPY_LAST_BLOCK_SIZE)

#define ZC_BUF_LEN 32768

static uint8_t *getZeroCopyBlockData(int blockIdx)
{
    uint8_t *buf = malloc(TEST_ZEROCOPY_FULL_BLOCK_SIZE);
    int i;
    if (!buf) {
        fprintf(stderr, "malloc(%d) failed\n", TEST_ZEROCOPY_FULL_BLOCK_SIZE);
        exit(1);
    }
    for (i = 0; i < TEST_ZEROCOPY_FULL_BLOCK_SIZE; i++) {
      buf[i] = (uint8_t)(blockIdx + (i % 17));
    }
    return buf;
}

static int getZeroCopyBlockLen(int blockIdx)
{
    if (blockIdx >= TEST_ZEROCOPY_NUM_BLOCKS) {
        return 0;
    } else if (blockIdx == (TEST_ZEROCOPY_NUM_BLOCKS - 1)) {
        return TEST_ZEROCOPY_LAST_BLOCK_SIZE;
    } else {
        return TEST_ZEROCOPY_FULL_BLOCK_SIZE;
    }
}

static int doTestZeroCopyReads(hdfsFS fs, const char *fileName)
{
    hdfsFile file = NULL;
    struct hadoopRzOptions *opts = NULL;
    struct hadoopRzBuffer *buffer = NULL;
    uint8_t *block;

    file = hdfsOpenFile(fs, fileName, O_RDONLY, 0, 0, 0);
    EXPECT_NONNULL(file);
    opts = hadoopRzOptionsAlloc();
    EXPECT_NONNULL(opts);
    EXPECT_ZERO(hadoopRzOptionsSetSkipChecksum(opts, 1));
    /* haven't read anything yet */
    EXPECT_ZERO(expectFileStats(file, 0LL, 0LL, 0LL, 0LL));
    block = getZeroCopyBlockData(0);
    EXPECT_NONNULL(block);
    /* first read is half of a block. */
    buffer = hadoopReadZero(file, opts, TEST_ZEROCOPY_FULL_BLOCK_SIZE / 2);
    EXPECT_NONNULL(buffer);
    EXPECT_INT_EQ(TEST_ZEROCOPY_FULL_BLOCK_SIZE / 2,
          hadoopRzBufferLength(buffer));
    EXPECT_ZERO(memcmp(hadoopRzBufferGet(buffer), block,
          TEST_ZEROCOPY_FULL_BLOCK_SIZE / 2));
    hadoopRzBufferFree(file, buffer);
    /* read the next half of the block */
    buffer = hadoopReadZero(file, opts, TEST_ZEROCOPY_FULL_BLOCK_SIZE / 2);
    EXPECT_NONNULL(buffer);
    EXPECT_INT_EQ(TEST_ZEROCOPY_FULL_BLOCK_SIZE / 2,
          hadoopRzBufferLength(buffer));
    EXPECT_ZERO(memcmp(hadoopRzBufferGet(buffer),
          block + (TEST_ZEROCOPY_FULL_BLOCK_SIZE / 2),
          TEST_ZEROCOPY_FULL_BLOCK_SIZE / 2));
    hadoopRzBufferFree(file, buffer);
    free(block);
    EXPECT_ZERO(expectFileStats(file, TEST_ZEROCOPY_FULL_BLOCK_SIZE, 
              TEST_ZEROCOPY_FULL_BLOCK_SIZE,
              TEST_ZEROCOPY_FULL_BLOCK_SIZE,
              TEST_ZEROCOPY_FULL_BLOCK_SIZE));
    /* Now let's read just a few bytes. */
    buffer = hadoopReadZero(file, opts, SMALL_READ_LEN);
    EXPECT_NONNULL(buffer);
    EXPECT_INT_EQ(SMALL_READ_LEN, hadoopRzBufferLength(buffer));
    block = getZeroCopyBlockData(1);
    EXPECT_NONNULL(block);
    EXPECT_ZERO(memcmp(block, hadoopRzBufferGet(buffer), SMALL_READ_LEN));
    hadoopRzBufferFree(file, buffer);
    EXPECT_INT64_EQ(
          (int64_t)TEST_ZEROCOPY_FULL_BLOCK_SIZE + (int64_t)SMALL_READ_LEN,
          hdfsTell(fs, file));
    EXPECT_ZERO(expectFileStats(file,
          TEST_ZEROCOPY_FULL_BLOCK_SIZE + SMALL_READ_LEN,
          TEST_ZEROCOPY_FULL_BLOCK_SIZE + SMALL_READ_LEN,
          TEST_ZEROCOPY_FULL_BLOCK_SIZE + SMALL_READ_LEN,
          TEST_ZEROCOPY_FULL_BLOCK_SIZE + SMALL_READ_LEN));

    /* Clear 'skip checksums' and test that we can't do zero-copy reads any
     * more.  Since there is no ByteBufferPool set, we should fail with
     * EPROTONOSUPPORT.
     */
    EXPECT_ZERO(hadoopRzOptionsSetSkipChecksum(opts, 0));
    EXPECT_NULL(hadoopReadZero(file, opts, TEST_ZEROCOPY_FULL_BLOCK_SIZE));
    EXPECT_INT_EQ(EPROTONOSUPPORT, errno);

    /* Verify that setting a NULL ByteBufferPool class works. */
    EXPECT_ZERO(hadoopRzOptionsSetByteBufferPool(opts, NULL));
    EXPECT_ZERO(hadoopRzOptionsSetSkipChecksum(opts, 0));
    EXPECT_NULL(hadoopReadZero(file, opts, TEST_ZEROCOPY_FULL_BLOCK_SIZE));
    EXPECT_INT_EQ(EPROTONOSUPPORT, errno);

    /* Now set a ByteBufferPool and try again.  It should succeed this time. */
    EXPECT_ZERO(hadoopRzOptionsSetByteBufferPool(opts,
          ELASTIC_BYTE_BUFFER_POOL_CLASS));
    buffer = hadoopReadZero(file, opts, TEST_ZEROCOPY_FULL_BLOCK_SIZE);
    EXPECT_NONNULL(buffer);
    EXPECT_INT_EQ(TEST_ZEROCOPY_FULL_BLOCK_SIZE, hadoopRzBufferLength(buffer));
    EXPECT_ZERO(expectFileStats(file,
          (2 * TEST_ZEROCOPY_FULL_BLOCK_SIZE) + SMALL_READ_LEN,
          (2 * TEST_ZEROCOPY_FULL_BLOCK_SIZE) + SMALL_READ_LEN,
          (2 * TEST_ZEROCOPY_FULL_BLOCK_SIZE) + SMALL_READ_LEN,
          TEST_ZEROCOPY_FULL_BLOCK_SIZE + SMALL_READ_LEN));
    EXPECT_ZERO(memcmp(block + SMALL_READ_LEN, hadoopRzBufferGet(buffer),
        TEST_ZEROCOPY_FULL_BLOCK_SIZE - SMALL_READ_LEN));
    free(block);
    block = getZeroCopyBlockData(2);
    EXPECT_NONNULL(block);
    EXPECT_ZERO(memcmp(block, (uint8_t*)hadoopRzBufferGet(buffer) +
        (TEST_ZEROCOPY_FULL_BLOCK_SIZE - SMALL_READ_LEN), SMALL_READ_LEN));
    hadoopRzBufferFree(file, buffer);

    /* Check the result of a zero-length read. */
    buffer = hadoopReadZero(file, opts, 0);
    EXPECT_NONNULL(buffer);
    EXPECT_NONNULL(hadoopRzBufferGet(buffer));
    EXPECT_INT_EQ(0, hadoopRzBufferLength(buffer));
    hadoopRzBufferFree(file, buffer);

    /* Check the result of reading past EOF */
    EXPECT_INT_EQ(0, hdfsSeek(fs, file, TEST_ZEROCOPY_FILE_LEN));
    buffer = hadoopReadZero(file, opts, 1);
    EXPECT_NONNULL(buffer);
    EXPECT_NULL(hadoopRzBufferGet(buffer));
    hadoopRzBufferFree(file, buffer);

    /* Cleanup */
    free(block);
    hadoopRzOptionsFree(opts);
    EXPECT_ZERO(hdfsCloseFile(fs, file));
    return 0;
}

static int createZeroCopyTestFile(hdfsFS fs, char *testFileName,
                                  size_t testFileNameLen)
{
    int blockIdx, blockLen;
    hdfsFile file;
    uint8_t *data;

    snprintf(testFileName, testFileNameLen, "/zeroCopyTestFile.%d.%d",
             getpid(), rand());
    file = hdfsOpenFile(fs, testFileName, O_WRONLY, 0, 1,
                        TEST_ZEROCOPY_FULL_BLOCK_SIZE);
    EXPECT_NONNULL(file);
    for (blockIdx = 0; blockIdx < TEST_ZEROCOPY_NUM_BLOCKS; blockIdx++) {
        blockLen = getZeroCopyBlockLen(blockIdx);
        data = getZeroCopyBlockData(blockIdx);
        EXPECT_NONNULL(data);
        EXPECT_INT_EQ(blockLen, hdfsWrite(fs, file, data, blockLen));
    }
    EXPECT_ZERO(hdfsCloseFile(fs, file));
    return 0;
}

static int nmdConfigureHdfsBuilder(struct NativeMiniDfsCluster *cl,
                            struct hdfsBuilder *bld) {
    int ret;
    tPort port;
    const char *domainSocket;

    hdfsBuilderSetNameNode(bld, "localhost");
    port = (tPort) nmdGetNameNodePort(cl);
    if (port < 0) {
      fprintf(stderr, "nmdGetNameNodePort failed with error %d\n", -port);
      return EIO;
    }
    hdfsBuilderSetNameNodePort(bld, port);

    domainSocket = hdfsGetDomainSocketPath(cl);

    if (domainSocket) {
      ret = hdfsBuilderConfSetStr(bld, "dfs.client.read.shortcircuit", "true");
      if (ret) {
        return ret;
      }
      ret = hdfsBuilderConfSetStr(bld, "dfs.domain.socket.path",
                                  domainSocket);
      if (ret) {
        return ret;
      }
    }
    return 0;
}


/**
 * Test that we can write a file with libhdfs and then read it back
 */
int main(void)
{
    int port;
    struct NativeMiniDfsConf conf = {
        1, /* doFormat */
        0, /* webhdfsEnabled */
        0, /* namenodeHttpPort */
        1, /* configureShortCircuit */
    };
    char testFileName[TEST_FILE_NAME_LENGTH];
    hdfsFS fs;
    struct NativeMiniDfsCluster* cl;
    struct hdfsBuilder *bld;

    cl = nmdCreate(&conf);
    EXPECT_NONNULL(cl);
    EXPECT_ZERO(nmdWaitClusterUp(cl));
    port = nmdGetNameNodePort(cl);
    if (port < 0) {
        fprintf(stderr, "TEST_ERROR: test_zerocopy: "
                "nmdGetNameNodePort returned error %d\n", port);
        return EXIT_FAILURE;
    }
    bld = hdfsNewBuilder();
    EXPECT_NONNULL(bld);
    EXPECT_ZERO(nmdConfigureHdfsBuilder(cl, bld));
    hdfsBuilderSetForceNewInstance(bld);
    hdfsBuilderConfSetStr(bld, "dfs.block.size",
                          TO_STR(TEST_ZEROCOPY_FULL_BLOCK_SIZE));
    /* ensure that we'll always get our mmaps */
    hdfsBuilderConfSetStr(bld, "dfs.client.read.shortcircuit.skip.checksum",
                          "true");
    fs = hdfsBuilderConnect(bld);
    EXPECT_NONNULL(fs);
    EXPECT_ZERO(createZeroCopyTestFile(fs, testFileName,
          TEST_FILE_NAME_LENGTH));
    EXPECT_ZERO(doTestZeroCopyReads(fs, testFileName));
    EXPECT_ZERO(hdfsDisconnect(fs));
    EXPECT_ZERO(nmdShutdown(cl));
    nmdFree(cl);
    fprintf(stderr, "TEST_SUCCESS\n"); 
    return EXIT_SUCCESS;
}
