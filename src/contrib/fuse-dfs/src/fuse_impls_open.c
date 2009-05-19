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
#include "fuse_file_handle.h"

int dfs_open(const char *path, struct fuse_file_info *fi)
{
  TRACE1("open", path)

  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;

  // check params and the context var
  assert(path);
  assert('/' == *path);
  assert(dfs);

  int ret = 0;

  // 0x8000 is always passed in and hadoop doesn't like it, so killing it here
  // bugbug figure out what this flag is and report problem to Hadoop JIRA
  int flags = (fi->flags & 0x7FFF);

  // retrieve dfs specific data
  dfs_fh *fh = (dfs_fh*)malloc(sizeof (dfs_fh));
  if (fh == NULL) {
    syslog(LOG_ERR, "ERROR: malloc of new file handle failed %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  if ((fh->fs = doConnectAsUser(dfs->nn_hostname,dfs->nn_port)) == NULL) {
    syslog(LOG_ERR, "ERROR: could not connect to dfs %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  if ((fh->hdfsFH = hdfsOpenFile(fh->fs, path, flags,  0, 3, 0)) == NULL) {
    syslog(LOG_ERR, "ERROR: could not connect open file %s:%d\n", __FILE__, __LINE__);
    return -EIO;
  }

  // 
  // mutex needed for reads/writes
  //
  pthread_mutex_init(&fh->mutex, NULL);

  if (fi->flags & O_WRONLY || fi->flags & O_CREAT) {
    // write specific initialization
    fh->buf = NULL;
  } else  {
    // read specific initialization

    assert(dfs->rdbuffer_size > 0);

    if (NULL == (fh->buf = (char*)malloc(dfs->rdbuffer_size*sizeof (char)))) {
      syslog(LOG_ERR, "ERROR: could not allocate memory for file buffer for a read for file %s dfs %s:%d\n", path,__FILE__, __LINE__);
      ret = -EIO;
    }

    fh->buffersStartOffset = 0;
    fh->bufferSize = 0;
  }

  fi->fh = (uint64_t)fh;

  return ret;
}
