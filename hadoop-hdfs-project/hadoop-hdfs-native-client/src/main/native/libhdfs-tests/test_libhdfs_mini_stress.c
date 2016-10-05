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
#include "hdfs/hdfs.h"
#include "hdfspp/hdfs_ext.h"
#include "native_mini_dfs.h"
#include "os/thread.h"

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TO_STR_HELPER(X) #X
#define TO_STR(X) TO_STR_HELPER(X)

#define TLH_MAX_THREADS 10000

#define TLH_MAX_DNS 16

#define TLH_DEFAULT_BLOCK_SIZE 1048576

#define TLH_DEFAULT_DFS_REPLICATION 3

#define TLH_DEFAULT_IPC_CLIENT_CONNECT_MAX_RETRIES 100

#define TLH_DEFAULT_IPC_CLIENT_CONNECT_RETRY_INTERVAL_MS 5

#ifndef RANDOM_ERROR_RATIO
#define RANDOM_ERROR_RATIO 1000000000
#endif

struct tlhThreadInfo {
  /** Thread index */
  int threadIdx;
  /** 0 = thread was successful; error code otherwise */
  int success;
  /** thread identifier */
  thread theThread;
  /** fs, shared with other threads **/
  hdfsFS hdfs;
  /** Filename */
  const char *fileNm;

};

static int hdfsNameNodeConnect(struct NativeMiniDfsCluster *cl, hdfsFS *fs,
                               const char *username)
{
  int ret;
  tPort port;
  hdfsFS hdfs;
  struct hdfsBuilder *bld;

  port = (tPort)nmdGetNameNodePort(cl);
  if (port < 0) {
    fprintf(stderr, "hdfsNameNodeConnect: nmdGetNameNodePort "
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
  hdfsBuilderConfSetStr(bld, "dfs.replication",
                        TO_STR(TLH_DEFAULT_DFS_REPLICATION));
  hdfsBuilderConfSetStr(bld, "ipc.client.connect.max.retries",
                        TO_STR(TLH_DEFAULT_IPC_CLIENT_CONNECT_MAX_RETRIES));
  hdfsBuilderConfSetStr(bld, "ipc.client.connect.retry.interval",
                        TO_STR(TLH_DEFAULT_IPC_CLIENT_CONNECT_RETRY_INTERVAL_MS));
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

static int hdfsWriteData(hdfsFS hdfs, const char *dirNm,
                         const char *fileNm, tSize fileSz)
{
  hdfsFile file;
  int ret, expected;
  const char *content;

  content = fileNm;

  if (hdfsExists(hdfs, dirNm) == 0) {
    EXPECT_ZERO(hdfsDelete(hdfs, dirNm, 1));
  }
  EXPECT_ZERO(hdfsCreateDirectory(hdfs, dirNm));

  file = hdfsOpenFile(hdfs, fileNm, O_WRONLY, 0, 0, 0);
  EXPECT_NONNULL(file);

  expected = (int)strlen(content);
  tSize sz = 0;
  while (sz < fileSz) {
    ret = hdfsWrite(hdfs, file, content, expected);
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
    sz += ret;
  }
  EXPECT_ZERO(hdfsFlush(hdfs, file));
  EXPECT_ZERO(hdfsHSync(hdfs, file));
  EXPECT_ZERO(hdfsCloseFile(hdfs, file));
  return 0;
}

static int fileEventCallback1(const char * event, const char * cluster, const char * file, int64_t value, int64_t cookie)
{
  char * randomErrRatioStr = getenv("RANDOM_ERROR_RATIO");
  int64_t randomErrRatio = RANDOM_ERROR_RATIO;
  if (randomErrRatioStr) randomErrRatio = (int64_t)atoi(randomErrRatioStr);
  if (randomErrRatio == 0) return DEBUG_SIMULATE_ERROR;
  else if (randomErrRatio < 0) return LIBHDFSPP_EVENT_OK;
  return random() % randomErrRatio == 0 ? DEBUG_SIMULATE_ERROR : LIBHDFSPP_EVENT_OK;
}

static int fileEventCallback2(const char * event, const char * cluster, const char * file, int64_t value, int64_t cookie)
{
  /* no op */
  return LIBHDFSPP_EVENT_OK;
}

static int doTestHdfsMiniStress(struct tlhThreadInfo *ti, int randomErr)
{
  char tmp[4096];
  hdfsFile file;
  int ret, expected;
  hdfsFileInfo *fileInfo;
  uint64_t readOps, nErrs=0;
  tOffset seekPos;
  const char *content;

  content = ti->fileNm;
  expected = (int)strlen(content);

  fileInfo = hdfsGetPathInfo(ti->hdfs, ti->fileNm);
  EXPECT_NONNULL(fileInfo);

  file = hdfsOpenFile(ti->hdfs, ti->fileNm, O_RDONLY, 0, 0, 0);
  EXPECT_NONNULL(file);

  libhdfspp_file_event_callback callback = (randomErr != 0) ? &fileEventCallback1 : &fileEventCallback2;

  hdfsPreAttachFileMonitor(callback, 0);

  fprintf(stderr, "testHdfsMiniStress(threadIdx=%d): starting read loop\n",
          ti->threadIdx);
  for (readOps=0; readOps < 1000; ++readOps) {
    EXPECT_ZERO(hdfsCloseFile(ti->hdfs, file));
    file = hdfsOpenFile(ti->hdfs, ti->fileNm, O_RDONLY, 0, 0, 0);
    EXPECT_NONNULL(file);
    seekPos = (((double)random()) / RAND_MAX) * (fileInfo->mSize - expected);
    seekPos = (seekPos / expected) * expected;
    ret = hdfsSeek(ti->hdfs, file, seekPos);
    if (ret < 0) {
      ret = errno;
      fprintf(stderr, "hdfsSeek to %"PRIu64" failed and set"
              " errno %d\n", seekPos, ret);
      ++nErrs;
      continue;
    }
    ret = hdfsRead(ti->hdfs, file, tmp, expected);
    if (ret < 0) {
      ret = errno;
      fprintf(stderr, "hdfsRead failed and set errno %d\n", ret);
      ++nErrs;
      continue;
    }
    if (ret != expected) {
      fprintf(stderr, "hdfsRead was supposed to read %d bytes, but "
              "it read %d\n", ret, expected);
      ++nErrs;
      continue;
    }
    ret = memcmp(content, tmp, expected);
    if (ret) {
      fprintf(stderr, "hdfsRead result (%.*s) does not match expected (%.*s)",
              expected, tmp, expected, content);
      ++nErrs;
      continue;
    }
  }
  EXPECT_ZERO(hdfsCloseFile(ti->hdfs, file));
  fprintf(stderr, "testHdfsMiniStress(threadIdx=%d): finished read loop\n",
          ti->threadIdx);
  EXPECT_ZERO(nErrs);
  return 0;
}

static int testHdfsMiniStressImpl(struct tlhThreadInfo *ti)
{
  fprintf(stderr, "testHdfsMiniStress(threadIdx=%d): starting\n",
          ti->threadIdx);
  EXPECT_NONNULL(ti->hdfs);
  // Error injection on, some failures are expected in the read path.
  // The expectation is that any memory stomps will cascade and cause
  // the following test to fail.  Ideally RPC errors would be seperated
  // from BlockReader errors (RPC is expected to recover from disconnects).
  doTestHdfsMiniStress(ti, 1);
  // No error injection
  EXPECT_ZERO(doTestHdfsMiniStress(ti, 0));
  return 0;
}

static void testHdfsMiniStress(void *v)
{
  struct tlhThreadInfo *ti = (struct tlhThreadInfo*)v;
  int ret = testHdfsMiniStressImpl(ti);
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
    fprintf(stderr, "testLibHdfsMiniStress: all threads succeeded.  SUCCESS.\n");
    return EXIT_SUCCESS;
  }
  fprintf(stderr, "testLibHdfsMiniStress: some threads failed: [");
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
 * Test intended to stress libhdfs client with concurrent requests. Currently focused
 * on concurrent reads.
 */
int main(void)
{
  int i, tlhNumThreads;
  char *dirNm, *fileNm;
  tSize fileSz;
  const char *tlhNumThreadsStr, *tlhNumDNsStr;
  hdfsFS hdfs = NULL;
  struct NativeMiniDfsCluster* tlhCluster;
  struct tlhThreadInfo ti[TLH_MAX_THREADS];
  struct NativeMiniDfsConf conf = {
      1, /* doFormat */
  };

  dirNm = "/tlhMiniStressData";
  fileNm = "/tlhMiniStressData/file";
  fileSz = 2*1024*1024;

  tlhNumDNsStr = getenv("TLH_NUM_DNS");
  if (!tlhNumDNsStr) {
    tlhNumDNsStr = "1";
  }
  conf.numDataNodes = atoi(tlhNumDNsStr);
  if ((conf.numDataNodes <= 0) || (conf.numDataNodes > TLH_MAX_DNS)) {
    fprintf(stderr, "testLibHdfsMiniStress: must have a number of datanodes "
            "between 1 and %d inclusive, not %d\n",
            TLH_MAX_DNS, conf.numDataNodes);
    return EXIT_FAILURE;
  }

  tlhNumThreadsStr = getenv("TLH_NUM_THREADS");
  if (!tlhNumThreadsStr) {
    tlhNumThreadsStr = "8";
  }
  tlhNumThreads = atoi(tlhNumThreadsStr);
  if ((tlhNumThreads <= 0) || (tlhNumThreads > TLH_MAX_THREADS)) {
    fprintf(stderr, "testLibHdfsMiniStress: must have a number of threads "
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

  EXPECT_ZERO(hdfsNameNodeConnect(tlhCluster, &hdfs, NULL));

  // Single threaded writes for now.
  EXPECT_ZERO(hdfsWriteData(hdfs, dirNm, fileNm, fileSz));

  // Multi-threaded reads.
  for (i = 0; i < tlhNumThreads; i++) {
    ti[i].theThread.start = testHdfsMiniStress;
    ti[i].theThread.arg = &ti[i];
    ti[i].hdfs = hdfs;
    ti[i].fileNm = fileNm;
    EXPECT_ZERO(threadCreate(&ti[i].theThread));
  }
  for (i = 0; i < tlhNumThreads; i++) {
    EXPECT_ZERO(threadJoin(&ti[i].theThread));
  }

  EXPECT_ZERO(hdfsDisconnect(hdfs));
  EXPECT_ZERO(nmdShutdown(tlhCluster));
  nmdFree(tlhCluster);
  return checkFailures(ti, tlhNumThreads);
}
