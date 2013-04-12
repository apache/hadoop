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

#define FUSE_USE_VERSION 26

#include "fuse-dfs/test/fuse_workload.h"
#include "libhdfs/expect.h"
#include "util/posix_util.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <unistd.h>
#include <utime.h>

typedef int (*testReadDirFn)(const struct dirent *de, void *v);

struct fileCtx {
  int fd;
  char *str;
  int strLen;
  char *path;
};

static const char *DIRS_A_AND_B[] = { "a", "b", NULL };
static const char *DIRS_B_AND_C[] = { "b", "c", NULL };

#define LONG_STR_LEN 1024
#define NUM_FILE_CTX 3
#define MAX_TRIES 120

// TODO: implement/test access, mknod, symlink
// TODO: rmdir on non-dir, non-empty dir
// TODO: test unlink-during-writing
// TODO: test weird open flags failing
// TODO: test chown, chmod

static int testReadDirImpl(DIR *dp, testReadDirFn fn, void *data)
{
  struct dirent *de;
  int ret, noda = 0;

  while (1) {
    de = readdir(dp);
    if (!de)
      return noda;
    if (!strcmp(de->d_name, "."))
      continue;
    if (!strcmp(de->d_name, ".."))
      continue;
    ret = fn(de, data);
    if (ret < 0)
      return ret;
    ++noda;
  }
  return noda;
}

static int testReadDir(const char *dirName, testReadDirFn fn, void *data)
{
  int ret;
  DIR *dp;

  dp = opendir(dirName);
  if (!dp) {
    return -errno;
  }
  ret = testReadDirImpl(dp, fn, data);
  closedir(dp);
  return ret;
}

static int expectDirs(const struct dirent *de, void *v)
{
  const char **names = v;
  const char **n;

  for (n = names; *n; ++n) {
    if (!strcmp(de->d_name, *n)) {
      return 0;
    }
  }
  return -ENOENT;
}

static int safeWrite(int fd, const void *buf, size_t amt)
{
  while (amt > 0) {
    int r = write(fd, buf, amt);
    if (r < 0) {
      if (errno != EINTR)
        return -errno;
      continue;
    }
    amt -= r;
    buf = (const char *)buf + r;
  }
  return 0;
}

static int safeRead(int fd, void *buf, int c)
{
  int res;
  size_t amt = 0;

  while (amt < c) {
    res = read(fd, buf, c - amt);
    if (res <= 0) {
      if (res == 0)
        return amt;
      if (errno != EINTR)
        return -errno;
      continue;
    }
    amt += res;
    buf = (char *)buf + res;
  }
  return amt;
}

/* Bug: HDFS-2551.
 * When a program writes a file, closes it, and immediately re-opens it,
 * it might not appear to have the correct length.  This is because FUSE
 * invokes the release() callback asynchronously.
 *
 * To work around this, we keep retrying until the file length is what we
 * expect.
 */
static int closeWorkaroundHdfs2551(int fd, const char *path, off_t expectedSize)
{
  int ret, try;
  struct stat stBuf;

  RETRY_ON_EINTR_GET_ERRNO(ret, close(fd));
  EXPECT_ZERO(ret);
  for (try = 0; try < MAX_TRIES; try++) {
    EXPECT_ZERO(stat(path, &stBuf));
    EXPECT_NONZERO(S_ISREG(stBuf.st_mode));
    if (stBuf.st_size == expectedSize) {
      return 0;
    }
    sleepNoSig(1);
  }
  fprintf(stderr, "FUSE_WORKLOAD: error: expected file %s to have length "
          "%lld; instead, it had length %lld\n",
          path, (long long)expectedSize, (long long)stBuf.st_size);
  return -EIO;
}

#ifdef FUSE_CAP_ATOMIC_O_TRUNC

/**
 * Test that we can create a file, write some contents to it, close that file,
 * and then successfully re-open with O_TRUNC.
 */
static int testOpenTrunc(const char *base)
{
  int fd, err;
  char path[PATH_MAX];
  const char * const SAMPLE1 = "this is the first file that we wrote.";
  const char * const SAMPLE2 = "this is the second file that we wrote.  "
    "It's #2!";

  snprintf(path, sizeof(path), "%s/trunc.txt", base);
  fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) {
    err = errno;
    fprintf(stderr, "TEST_ERROR: testOpenTrunc(%s): first open "
            "failed with error %d\n", path, err);
    return -err;
  }
  EXPECT_ZERO(safeWrite(fd, SAMPLE1, strlen(SAMPLE1)));
  EXPECT_ZERO(closeWorkaroundHdfs2551(fd, path, strlen(SAMPLE1)));
  fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) {
    err = errno;
    fprintf(stderr, "TEST_ERROR: testOpenTrunc(%s): second open "
            "failed with error %d\n", path, err);
    return -err;
  }
  EXPECT_ZERO(safeWrite(fd, SAMPLE2, strlen(SAMPLE2)));
  EXPECT_ZERO(closeWorkaroundHdfs2551(fd, path, strlen(SAMPLE2)));
  return 0;
}

#else

static int testOpenTrunc(const char *base)
{
  fprintf(stderr, "FUSE_WORKLOAD: We lack FUSE_CAP_ATOMIC_O_TRUNC support.  "
          "Not testing open(O_TRUNC).\n");
  return 0;
}

#endif

int runFuseWorkloadImpl(const char *root, const char *pcomp,
    struct fileCtx *ctx)
{
  char base[PATH_MAX], tmp[PATH_MAX], *tmpBuf;
  char src[PATH_MAX], dst[PATH_MAX];
  struct stat stBuf;
  int ret, i;
  struct utimbuf tbuf;
  struct statvfs stvBuf;

  // The root must be a directory
  EXPECT_ZERO(stat(root, &stBuf));
  EXPECT_NONZERO(S_ISDIR(stBuf.st_mode));

  // base = <root>/<pcomp>.  base must not exist yet
  snprintf(base, sizeof(base), "%s/%s", root, pcomp);
  EXPECT_NEGATIVE_ONE_WITH_ERRNO(stat(base, &stBuf), ENOENT);

  // mkdir <base>
  RETRY_ON_EINTR_GET_ERRNO(ret, mkdir(base, 0755));
  EXPECT_ZERO(ret);

  // rmdir <base>
  RETRY_ON_EINTR_GET_ERRNO(ret, rmdir(base));
  EXPECT_ZERO(ret);

  // mkdir <base>
  RETRY_ON_EINTR_GET_ERRNO(ret, mkdir(base, 0755));
  EXPECT_ZERO(ret);

  // stat <base>
  EXPECT_ZERO(stat(base, &stBuf));
  EXPECT_NONZERO(S_ISDIR(stBuf.st_mode));

  // mkdir <base>/a
  snprintf(tmp, sizeof(tmp), "%s/a", base);
  RETRY_ON_EINTR_GET_ERRNO(ret, mkdir(tmp, 0755));
  EXPECT_ZERO(ret);

  /* readdir test */
  EXPECT_INT_EQ(1, testReadDir(base, expectDirs, DIRS_A_AND_B));

  // mkdir <base>/b
  snprintf(tmp, sizeof(tmp), "%s/b", base);
  RETRY_ON_EINTR_GET_ERRNO(ret, mkdir(tmp, 0755));
  EXPECT_ZERO(ret);

  // readdir a and b
  EXPECT_INT_EQ(2, testReadDir(base, expectDirs, DIRS_A_AND_B));

  // rename a -> c
  snprintf(src, sizeof(src), "%s/a", base);
  snprintf(dst, sizeof(dst), "%s/c", base);
  EXPECT_ZERO(rename(src, dst));

  // readdir c and b
  EXPECT_INT_EQ(-ENOENT, testReadDir(base, expectDirs, DIRS_A_AND_B));
  EXPECT_INT_EQ(2, testReadDir(base, expectDirs, DIRS_B_AND_C));

  // statvfs
  memset(&stvBuf, 0, sizeof(stvBuf));
  EXPECT_ZERO(statvfs(root, &stvBuf));

  // set utime on base
  memset(&tbuf, 0, sizeof(tbuf));
  tbuf.actime = 123;
  tbuf.modtime = 456;
  EXPECT_ZERO(utime(base, &tbuf));

  // stat(base)
  EXPECT_ZERO(stat(base, &stBuf));
  EXPECT_NONZERO(S_ISDIR(stBuf.st_mode));
  //EXPECT_INT_EQ(456, stBuf.st_atime); // hdfs doesn't store atime on directories
  EXPECT_INT_EQ(456, stBuf.st_mtime);

  // open some files and write to them
  for (i = 0; i < NUM_FILE_CTX; i++) {
    snprintf(tmp, sizeof(tmp), "%s/b/%d", base, i);
    ctx[i].path = strdup(tmp);
    if (!ctx[i].path) {
      fprintf(stderr, "FUSE_WORKLOAD: OOM on line %d\n", __LINE__);
      return -ENOMEM;
    }
    ctx[i].strLen = strlen(ctx[i].str);
  }
  for (i = 0; i < NUM_FILE_CTX; i++) {
    ctx[i].fd = open(ctx[i].path, O_RDONLY);
    if (ctx[i].fd >= 0) {
      fprintf(stderr, "FUSE_WORKLOAD: Error: was able to open %s for read before it "
              "was created!\n", ctx[i].path);
      return -EIO; 
    }
    ctx[i].fd = creat(ctx[i].path, 0755);
    if (ctx[i].fd < 0) {
      fprintf(stderr, "FUSE_WORKLOAD: Failed to create file %s for writing!\n",
              ctx[i].path);
      return -EIO;
    }
  }
  for (i = 0; i < NUM_FILE_CTX; i++) {
    EXPECT_ZERO(safeWrite(ctx[i].fd, ctx[i].str, ctx[i].strLen));
  }
  for (i = 0; i < NUM_FILE_CTX; i++) {
    EXPECT_ZERO(closeWorkaroundHdfs2551(ctx[i].fd, ctx[i].path, ctx[i].strLen));
    ctx[i].fd = -1;
  }
  for (i = 0; i < NUM_FILE_CTX; i++) {
    ctx[i].fd = open(ctx[i].path, O_RDONLY);
    if (ctx[i].fd < 0) {
      fprintf(stderr, "FUSE_WORKLOAD: Failed to open file %s for reading!\n",
              ctx[i].path);
      return -EIO;
    }
  }
  for (i = 0; i < NUM_FILE_CTX; i++) {
    tmpBuf = calloc(1, ctx[i].strLen);
    if (!tmpBuf) {
      fprintf(stderr, "FUSE_WORKLOAD: OOM on line %d\n", __LINE__);
      return -ENOMEM;
    }
    EXPECT_INT_EQ(ctx[i].strLen, safeRead(ctx[i].fd, tmpBuf, ctx[i].strLen));
    EXPECT_ZERO(memcmp(ctx[i].str, tmpBuf, ctx[i].strLen));
    RETRY_ON_EINTR_GET_ERRNO(ret, close(ctx[i].fd));
    ctx[i].fd = -1;
    EXPECT_ZERO(ret);
    free(tmpBuf);
  }
  for (i = 0; i < NUM_FILE_CTX; i++) {
    EXPECT_ZERO(truncate(ctx[i].path, 0));
  }
  for (i = 0; i < NUM_FILE_CTX; i++) {
    EXPECT_ZERO(stat(ctx[i].path, &stBuf));
    EXPECT_NONZERO(S_ISREG(stBuf.st_mode));
    EXPECT_INT_EQ(0, stBuf.st_size);
  }
  for (i = 0; i < NUM_FILE_CTX; i++) {
    RETRY_ON_EINTR_GET_ERRNO(ret, unlink(ctx[i].path));
    EXPECT_ZERO(ret);
  }
  for (i = 0; i < NUM_FILE_CTX; i++) {
    EXPECT_NEGATIVE_ONE_WITH_ERRNO(stat(ctx[i].path, &stBuf), ENOENT);
  }
  for (i = 0; i < NUM_FILE_CTX; i++) {
    free(ctx[i].path);
  }
  EXPECT_ZERO(testOpenTrunc(base));
  EXPECT_ZERO(recursiveDelete(base));
  return 0;
}

int runFuseWorkload(const char *root, const char *pcomp)
{
  int ret, err;
  size_t i;
  char longStr[LONG_STR_LEN];
  struct fileCtx ctx[NUM_FILE_CTX] = {
    {
      .fd = -1,
      .str = "hello, world",
    },
    {
      .fd = -1,
      .str = "A",
    },
    {
      .fd = -1,
      .str = longStr,
    },
  };
  for (i = 0; i < LONG_STR_LEN - 1; i++) {
    longStr[i] = 'a' + (i % 10);
  }
  longStr[LONG_STR_LEN - 1] = '\0';

  ret = runFuseWorkloadImpl(root, pcomp, ctx);
  // Make sure all file descriptors are closed, or else we won't be able to
  // unmount
  for (i = 0; i < NUM_FILE_CTX; i++) {
    if (ctx[i].fd >= 0) {
      RETRY_ON_EINTR_GET_ERRNO(err, close(ctx[i].fd));
    }
  }
  return ret;
}
