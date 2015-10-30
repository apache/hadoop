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
#include "fuse_users.h"
#include "fuse_impls.h"
#include "fuse_connect.h"

#include <stdlib.h>

int dfs_chown(const char *path, uid_t uid, gid_t gid)
{
  struct hdfsConn *conn = NULL;
  int ret = 0;
  char *user = NULL;
  char *group = NULL;

  TRACE1("chown", path)

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);
  assert('/' == *path);

  if ((uid == -1) && (gid == -1)) {
    ret = 0;
    goto cleanup;
  }
  if (uid != -1) {
    user = getUsername(uid);
    if (NULL == user) {
      ERROR("Could not lookup the user id string %d",(int)uid);
      ret = -EIO;
      goto cleanup;
    }
  }
  if (gid != -1) {
    group = getGroup(gid);
    if (group == NULL) {
      ERROR("Could not lookup the group id string %d",(int)gid);
      ret = -EIO;
      goto cleanup;
    }
  }

  ret = fuseConnectAsThreadUid(&conn);
  if (ret) {
    fprintf(stderr, "fuseConnectAsThreadUid: failed to open a libhdfs "
            "connection!  error %d.\n", ret);
    ret = -EIO;
    goto cleanup;
  }

  if (hdfsChown(hdfsConnGetFs(conn), path, user, group)) {
    ret = errno;
    ERROR("Could not chown %s to %d:%d: error %d", path, (int)uid, gid, ret);
    ret = (ret > 0) ? -ret : -EIO;
    goto cleanup;
  }

cleanup:
  if (conn) {
    hdfsConnRelease(conn);
  }
  free(user);
  free(group);

  return ret;
}
