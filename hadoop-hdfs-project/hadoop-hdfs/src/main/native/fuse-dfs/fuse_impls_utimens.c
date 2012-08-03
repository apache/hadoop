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

#include "fuse_dfs.h"
#include "fuse_impls.h"
#include "fuse_connect.h"

int dfs_utimens(const char *path, const struct timespec ts[2])
{
  struct hdfsConn *conn = NULL;
  hdfsFS fs;
  int ret = 0;
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  TRACE1("utimens", path)

  assert(path);
  assert(dfs);
  assert('/' == *path);

  time_t aTime = ts[0].tv_sec;
  time_t mTime = ts[1].tv_sec;

  ret = fuseConnectAsThreadUid(&conn);
  if (ret) {
    fprintf(stderr, "fuseConnectAsThreadUid: failed to open a libhdfs "
            "connection!  error %d.\n", ret);
    ret = -EIO;
    goto cleanup;
  }
  fs = hdfsConnGetFs(conn);

  if (hdfsUtime(fs, path, mTime, aTime)) {
    hdfsFileInfo *info = hdfsGetPathInfo(fs, path);
    if (info == NULL) {
      ret = (errno > 0) ? -errno : -ENOENT;
      goto cleanup;
    }
    // Silently ignore utimens failure for directories, otherwise 
    // some programs like tar will fail.
    if (info->mKind == kObjectKindDirectory) {
      ret = 0;
    } else {
      ret = (errno > 0) ? -errno : -EACCES;
    }
    goto cleanup;
  }
  ret = 0;

cleanup:
  if (conn) {
    hdfsConnRelease(conn);
  }
  return ret;
}
