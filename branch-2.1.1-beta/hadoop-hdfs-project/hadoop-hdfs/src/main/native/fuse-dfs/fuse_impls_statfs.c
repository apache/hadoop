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


int dfs_statfs(const char *path, struct statvfs *st)
{
  struct hdfsConn *conn = NULL;
  hdfsFS fs;
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;
  int ret;

  TRACE1("statfs",path)

  assert(path);
  assert(st);
  assert(dfs);

  memset(st,0,sizeof(struct statvfs));

  ret = fuseConnectAsThreadUid(&conn);
  if (ret) {
    fprintf(stderr, "fuseConnectAsThreadUid: failed to open a libhdfs "
            "connection!  error %d.\n", ret);
    ret = -EIO;
    goto cleanup;
  }
  fs = hdfsConnGetFs(conn);

  const tOffset cap   = hdfsGetCapacity(fs);
  const tOffset used  = hdfsGetUsed(fs);
  const tOffset bsize = hdfsGetDefaultBlockSize(fs);

  st->f_bsize   =  bsize;
  st->f_frsize  =  bsize;
  st->f_blocks  =  cap/bsize;
  st->f_bfree   =  (cap-used)/bsize;
  st->f_bavail  =  (cap-used)/bsize;
  st->f_files   =  1000;
  st->f_ffree   =  500;
  st->f_favail  =  500;
  st->f_fsid    =  1023;
  st->f_flag    =  ST_RDONLY | ST_NOSUID;
  st->f_namemax =  1023;
  ret = 0;

cleanup:
  if (conn) {
    hdfsConnRelease(conn);
  }
  return ret;
}
