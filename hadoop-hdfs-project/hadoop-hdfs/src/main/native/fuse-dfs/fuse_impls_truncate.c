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

/**
 * For now implement truncate here and only for size == 0.
 * Weak implementation in that we just delete the file and 
 * then re-create it, but don't set the user, group, and times to the old
 * file's metadata. 
 */
int dfs_truncate(const char *path, off_t size)
{
  struct hdfsConn *conn = NULL;
  hdfsFS fs;
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  TRACE1("truncate", path)

  assert(path);
  assert('/' == *path);
  assert(dfs);

  if (size != 0) {
    return 0;
  }

  int ret = dfs_unlink(path);
  if (ret != 0) {
    return ret;
  }

  ret = fuseConnectAsThreadUid(&conn);
  if (ret) {
    fprintf(stderr, "fuseConnectAsThreadUid: failed to open a libhdfs "
            "connection!  error %d.\n", ret);
    ret = -EIO;
    goto cleanup;
  }
  fs = hdfsConnGetFs(conn);

  int flags = O_WRONLY | O_CREAT;

  hdfsFile file;
  if ((file = (hdfsFile)hdfsOpenFile(fs, path, flags,  0, 0, 0)) == NULL) {
    ERROR("Could not connect open file %s", path);
    ret = -EIO;
    goto cleanup;
  }

  if (hdfsCloseFile(fs, file) != 0) {
    ERROR("Could not close file %s", path);
    ret = -EIO;
    goto cleanup;
  }

cleanup:
  if (conn) {
    hdfsConnRelease(conn);
  }
  return ret;
}
