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
#include "fuse_file_handle.h"

int dfs_write(const char *path, const char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi)
{
  TRACE1("write", path)

  // retrieve dfs specific data
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;
  int ret = 0;

  // check params and the context var
  assert(path);
  assert(dfs);
  assert('/' == *path);
  assert(fi);

  dfs_fh *fh = (dfs_fh*)fi->fh;
  assert(fh);

  hdfsFile file_handle = (hdfsFile)fh->hdfsFH;
  assert(file_handle);

  //
  // Critical section - make the sanity check (tell to see the writes are sequential) and the actual write 
  // (no returns until end)
  //
  pthread_mutex_lock(&fh->mutex);

  tSize length = 0;
  assert(fh->fs);

  tOffset cur_offset = hdfsTell(fh->fs, file_handle);
  if (cur_offset != offset) {
    syslog(LOG_ERR, "ERROR: user trying to random access write to a file %d!=%d for %s %s:%d\n",(int)cur_offset, (int)offset,path, __FILE__, __LINE__);
    ret =  -EIO;
  } else {
    length = hdfsWrite(fh->fs, file_handle, buf, size);
    if (length <= 0) {
      syslog(LOG_ERR, "ERROR: fuse problem - could not write all the bytes for %s %d!=%d%s:%d\n",path,length,(int)size, __FILE__, __LINE__);
      ret = -EIO;
    } 
    if (length != size) {
      syslog(LOG_ERR, "WARN: fuse problem - could not write all the bytes for %s %d!=%d%s:%d\n",path,length,(int)size, __FILE__, __LINE__);
    }
  }

  //
  // Critical section end 
  //

  pthread_mutex_unlock(&fh->mutex);

  return ret == 0 ? length : ret;
}
