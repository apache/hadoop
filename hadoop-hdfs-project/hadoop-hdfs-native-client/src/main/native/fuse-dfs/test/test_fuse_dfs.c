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

#include "fuse-dfs/test/fuse_workload.h"
#include "hdfs/hdfs.h"
#include "libhdfs-tests/expect.h"
#include "libhdfs-tests/native_mini_dfs.h"
#include "util/posix_util.h"

#include <ctype.h>
#include <errno.h>
#include <libgen.h>
#include <limits.h>
#include <mntent.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/** Exit status to return when there is an exec error.
 *
 * We assume that fusermount and fuse_dfs don't normally return this error code.
 */
#define EXIT_STATUS_EXEC_ERROR 240

/** Maximum number of times to try to unmount the fuse filesystem we created.
 */
#define MAX_UNMOUNT_TRIES 20

/**
 * Verify that the fuse workload works on the local FS.
 *
 * @return        0 on success; error code otherwise
 */
static int verifyFuseWorkload(void)
{
  char tempDir[PATH_MAX];

  EXPECT_ZERO(createTempDir(tempDir, sizeof(tempDir), 0755));
  EXPECT_ZERO(runFuseWorkload(tempDir, "test"));
  EXPECT_ZERO(recursiveDelete(tempDir));

  return 0;
}

static int fuserMount(int *procRet, ...) __attribute__((sentinel));

/**
 * Invoke the fusermount binary.
 *
 * @param retVal  (out param) The return value from fusermount
 *
 * @return        0 on success; error code if the fork fails
 */
static int fuserMount(int *procRet, ...)
{
  int ret, status;
  size_t i = 0;
  char *args[64], *c;
  va_list ap;
  pid_t pid, pret;

  args[i++] = "fusermount";
  va_start(ap, procRet);
  while (1) {
    c = va_arg(ap, char *);
    args[i++] = c;
    if (!c)
      break;
    if (i > sizeof(args)/sizeof(args[0])) {
      va_end(ap);
      return -EINVAL;
    }
  }
  va_end(ap);
  pid = fork();
  if (pid < 0) {
    ret = errno;
    fprintf(stderr, "FUSE_TEST: failed to fork: error %d: %s\n",
            ret, strerror(ret));
    return -ret;
  } else if (pid == 0) {
    if (execvp("fusermount", args)) {
      ret = errno;
      fprintf(stderr, "FUSE_TEST: failed to execute fusermount: "
              "error %d: %s\n", ret, strerror(ret));
      exit(EXIT_STATUS_EXEC_ERROR);
    }
  }
  pret = waitpid(pid, &status, 0);
  if (pret != pid) {
    ret = errno;
    fprintf(stderr, "FUSE_TEST: failed to wait for pid %d: returned %d "
            "(error %d: %s)\n", pid, pret, ret, strerror(ret));
    return -ret;
  }
  if (WIFEXITED(status)) {
    *procRet = WEXITSTATUS(status);
  } else if (WIFSIGNALED(status)) {
    fprintf(stderr, "FUSE_TEST: fusermount exited with signal %d\n",
            WTERMSIG(status));
    *procRet = -1;
  } else {
    fprintf(stderr, "FUSE_TEST: fusermount exited with unknown exit type\n");
    *procRet = -1;
  }
  return 0;
}

static int isMounted(const char *mntPoint)
{
  int ret;
  FILE *fp;
  struct mntent *mntEnt;

  fp = setmntent("/proc/mounts", "r");
  if (!fp) {
    ret = errno;
    fprintf(stderr, "FUSE_TEST: isMounted(%s) failed to open /proc/mounts: "
            "error %d: %s", mntPoint, ret, strerror(ret));
    return -ret;
  }
  while ((mntEnt = getmntent(fp))) {
    if (!strcmp(mntEnt->mnt_dir, mntPoint)) {
      endmntent(fp);
      return 1;
    }
  }
  endmntent(fp);
  return 0;
}

static int waitForMount(const char *mntPoint, int retries)
{
  int ret, try = 0;

  while (try++ < retries) {
    ret = isMounted(mntPoint);
    if (ret < 0) {
      fprintf(stderr, "FUSE_TEST: waitForMount(%s, %d): isMounted returned "
              "error %d\n", mntPoint, retries, ret);
    } else if (ret == 1) {
      return 0;
    }
    sleepNoSig(2);
  }
  return -ETIMEDOUT;
}

/**
 * Try to unmount the fuse filesystem we mounted.
 *
 * Normally, only one try should be sufficient to unmount the filesystem.
 * However, if our tests needs to exit right after starting the fuse_dfs process,
 * the fuse_dfs process might not have gotten around to mounting itself yet.  The
 * retry loop in this function ensures that we always unmount FUSE before
 * exiting, rather than leaving around a zombie fuse_dfs.
 *
 * @param mntPoint            Where the FUSE filesystem is mounted
 * @return                    0 on success; error code otherwise
 */
static int cleanupFuse(const char *mntPoint)
{
  int ret, pret, tries = 0;

  while (1) {
    ret = fuserMount(&pret, "-u", mntPoint, NULL);
    if (ret) {
      fprintf(stderr, "FUSE_TEST: unmountFuse: failed to invoke fuserMount: "
              "error %d\n", ret);
      return ret;
    }
    if (pret == 0) {
      fprintf(stderr, "FUSE_TEST: successfully unmounted FUSE filesystem.\n");
      return 0;
    }
    if (tries++ > MAX_UNMOUNT_TRIES) {
      return -EIO;
    }
    fprintf(stderr, "FUSE_TEST: retrying unmount in 2 seconds...\n");
    sleepNoSig(2);
  }
}

/**
 * Create a fuse_dfs process using the miniDfsCluster we set up.
 *
 * @param argv0                 argv[0], as passed into main
 * @param cluster               The NativeMiniDfsCluster to connect to
 * @param mntPoint              The mount point
 * @param pid                   (out param) the fuse_dfs process
 *
 * @return                      0 on success; error code otherwise
 */
static int spawnFuseServer(const char *argv0,
    const struct NativeMiniDfsCluster *cluster, const char *mntPoint,
    pid_t *pid)
{
  int ret, procRet;
  char scratch[PATH_MAX], *dir, fusePath[PATH_MAX], portOpt[128];

  snprintf(scratch, sizeof(scratch), "%s", argv0);
  dir = dirname(scratch);
  snprintf(fusePath, sizeof(fusePath), "%s/fuse_dfs", dir);
  if (access(fusePath, X_OK)) {
    fprintf(stderr, "FUSE_TEST: spawnFuseServer: failed to find fuse_dfs "
            "binary at %s!\n", fusePath);
    return -ENOENT;
  }
  /* Let's make sure no other FUSE filesystem is mounted at mntPoint */
  ret = fuserMount(&procRet, "-u", mntPoint, NULL);
  if (ret) {
    fprintf(stderr, "FUSE_TEST: fuserMount -u %s failed with error %d\n",
            mntPoint, ret);
    return -EIO;
  }
  if (procRet == EXIT_STATUS_EXEC_ERROR) {
    fprintf(stderr, "FUSE_TEST: fuserMount probably could not be executed\n");
    return -EIO;
  }
  /* fork and exec the fuse_dfs process */
  *pid = fork();
  if (*pid < 0) {
    ret = errno;
    fprintf(stderr, "FUSE_TEST: spawnFuseServer: failed to fork: "
            "error %d: %s\n", ret, strerror(ret));
    return -ret;
  } else if (*pid == 0) {
    snprintf(portOpt, sizeof(portOpt), "-oport=%d",
             nmdGetNameNodePort(cluster));
    if (execl(fusePath, fusePath, "-obig_writes", "-oserver=hdfs://localhost",
          portOpt, "-onopermissions", "-ononempty", "-oinitchecks",
          mntPoint, "-f", NULL)) {
      ret = errno;
      fprintf(stderr, "FUSE_TEST: spawnFuseServer: failed to execv %s: "
              "error %d: %s\n", fusePath, ret, strerror(ret));
      exit(EXIT_STATUS_EXEC_ERROR);
    }
  }
  return 0;
}

/**
 * Test that we can start up fuse_dfs and do some stuff.
 */
int main(int argc, char **argv)
{
  int ret, pret, status;
  pid_t fusePid;
  const char *mntPoint;
  char mntTmp[PATH_MAX] = "";
  struct NativeMiniDfsCluster* tlhCluster;
  struct NativeMiniDfsConf conf = {
      .doFormat = 1,
  };

  mntPoint = getenv("TLH_FUSE_MNT_POINT");
  if (!mntPoint) {
    if (createTempDir(mntTmp, sizeof(mntTmp), 0755)) {
      fprintf(stderr, "FUSE_TEST: failed to create temporary directory for "
              "fuse mount point.\n");
      ret = EXIT_FAILURE;
      goto done;
    }
    fprintf(stderr, "FUSE_TEST: creating mount point at '%s'\n", mntTmp);
    mntPoint = mntTmp;
  }
  if (verifyFuseWorkload()) {
    fprintf(stderr, "FUSE_TEST: failed to verify fuse workload on "
            "local FS.\n");
    ret = EXIT_FAILURE;
    goto done_rmdir;
  }
  tlhCluster = nmdCreate(&conf);
  if (!tlhCluster) {
    ret = EXIT_FAILURE;
    goto done_rmdir;
  }
  if (nmdWaitClusterUp(tlhCluster)) {
    ret = EXIT_FAILURE;
    goto done_nmd_shutdown;
  }
  ret = spawnFuseServer(argv[0], tlhCluster, mntPoint, &fusePid);
  if (ret) {
    fprintf(stderr, "FUSE_TEST: spawnFuseServer failed with error "
            "code %d\n", ret);
    ret = EXIT_FAILURE;
    goto done_nmd_shutdown;
  }
  ret = waitForMount(mntPoint, 20);
  if (ret) {
    fprintf(stderr, "FUSE_TEST: waitForMount(%s) failed with error "
            "code %d\n", mntPoint, ret);
    cleanupFuse(mntPoint);
    ret = EXIT_FAILURE;
    goto done_nmd_shutdown;
  }
  ret = runFuseWorkload(mntPoint, "test");
  if (ret) {
    fprintf(stderr, "FUSE_TEST: runFuseWorkload failed with error "
            "code %d\n", ret);
    cleanupFuse(mntPoint);
    ret = EXIT_FAILURE;
    goto done_nmd_shutdown;
  }
  if (cleanupFuse(mntPoint)) {
    fprintf(stderr, "FUSE_TEST: fuserMount -u %s failed with error "
            "code %d\n", mntPoint, ret);
    ret = EXIT_FAILURE;
    goto done_nmd_shutdown;
  }
  alarm(120);
  pret = waitpid(fusePid, &status, 0);
  if (pret != fusePid) {
    ret = errno;
    fprintf(stderr, "FUSE_TEST: failed to wait for fusePid %d: "
            "returned %d: error %d (%s)\n",
            fusePid, pret, ret, strerror(ret));
    ret = EXIT_FAILURE;
    goto done_nmd_shutdown;
  }
  if (WIFEXITED(status)) {
    ret = WEXITSTATUS(status);
    if (ret) {
      fprintf(stderr, "FUSE_TEST: fuse exited with failure status "
              "%d!\n", ret);
      ret = EXIT_FAILURE;
      goto done_nmd_shutdown;
    }
  } else if (WIFSIGNALED(status)) {
    ret = WTERMSIG(status);
    if (ret != SIGTERM) {
      fprintf(stderr, "FUSE_TEST: fuse exited with unexpected "
              "signal %d!\n", ret);
      ret = EXIT_FAILURE;
      goto done_nmd_shutdown;
    }
  } else {
    fprintf(stderr, "FUSE_TEST: fusermount exited with unknown exit type\n");
    ret = EXIT_FAILURE;
    goto done_nmd_shutdown;
  }
  ret = EXIT_SUCCESS;

done_nmd_shutdown:
  EXPECT_ZERO(nmdShutdown(tlhCluster));
  nmdFree(tlhCluster);
done_rmdir:
  if (mntTmp[0]) {
    rmdir(mntTmp);
  }
done:
  if (ret == EXIT_SUCCESS) {
    fprintf(stderr, "FUSE_TEST: SUCCESS.\n");
  } else {
    fprintf(stderr, "FUSE_TEST: FAILURE!\n");
  }
  return ret;
}
