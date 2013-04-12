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
#include "fuse_trash.h"

int dfs_unlink(const char *path)
{
  struct hdfsConn *conn = NULL;
  hdfsFS fs;
  int ret = 0;
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  TRACE1("unlink", path)

  assert(path);
  assert(dfs);
  assert('/' == *path);

  if (is_protected(path)) {
    ERROR("Trying to delete protected directory %s", path);
    ret = -EACCES;
    goto cleanup;
  }

  ret = fuseConnectAsThreadUid(&conn);
  if (ret) {
    fprintf(stderr, "fuseConnectAsThreadUid: failed to open a libhdfs "
            "connection!  error %d.\n", ret);
    ret = -EIO;
    goto cleanup;
  }
  fs = hdfsConnGetFs(conn);

  if (hdfsDeleteWithTrash(fs, path, dfs->usetrash)) {
    ERROR("Could not delete file %s", path);
    ret = (errno > 0) ? -errno : -EIO;
    goto cleanup;
  }
  ret = 0;

cleanup:
  if (conn) {
    hdfsConnRelease(conn);
  }
  return ret;

}
