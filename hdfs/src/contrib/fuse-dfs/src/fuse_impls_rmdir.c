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
#include "fuse_trash.h"
#include "fuse_connect.h"

extern const char *const TrashPrefixDir;

int dfs_rmdir(const char *path)
{
  TRACE1("rmdir", path)

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert(dfs);
  assert('/' == *path);

  if (is_protected(path)) {
    syslog(LOG_ERR,"ERROR: hdfs trying to delete a protected directory: %s ",path);
    return -EACCES;
  }

  if (dfs->read_only) {
    syslog(LOG_ERR,"ERROR: hdfs is configured as read-only, cannot delete the directory %s\n",path);
    return -EACCES;
  }

  hdfsFS userFS;
  // if not connected, try to connect and fail out if we can't.
  if ((userFS = doConnectAsUser(dfs->nn_hostname,dfs->nn_port))== NULL) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  int numEntries = 0;
  hdfsFileInfo *info = hdfsListDirectory(userFS,path,&numEntries);

  // free the info pointers
  hdfsFreeFileInfo(info,numEntries);

  if (numEntries) {
    return -ENOTEMPTY;
  }

  if (hdfsDeleteWithTrash(userFS, path, dfs->usetrash)) {
    syslog(LOG_ERR,"ERROR: hdfs error trying to delete the directory %s\n",path);
    return -EIO;
  }

  return 0;
}
