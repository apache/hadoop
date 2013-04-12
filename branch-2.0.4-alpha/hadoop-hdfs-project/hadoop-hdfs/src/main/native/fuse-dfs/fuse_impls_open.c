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

#include <stdio.h>
#include <stdlib.h>

static int get_hdfs_open_flags_from_info(hdfsFS fs, const char *path,
                  int flags, int *outflags, const hdfsFileInfo *info);

/**
 * Given a set of FUSE flags, determine the libhdfs flags we need.
 *
 * This is complicated by two things:
 * 1. libhdfs doesn't support O_RDWR at all;
 * 2. when given O_WRONLY, libhdfs will truncate the file unless O_APPEND is
 * also given.  In other words, there is an implicit O_TRUNC.
 *
 * Probably the next iteration of the libhdfs interface should not use the POSIX
 * flags at all, since, as you can see, they don't really match up very closely
 * to the POSIX meaning.  However, for the time being, this is the API.
 *
 * @param fs               The libhdfs object
 * @param path             The path we're opening
 * @param flags            The FUSE flags
 *
 * @return                 negative error code on failure; flags otherwise.
 */
static int64_t get_hdfs_open_flags(hdfsFS fs, const char *path, int flags)
{
  int hasContent;
  int64_t ret;
  hdfsFileInfo *info;

  if ((flags & O_ACCMODE) == O_RDONLY) {
    return O_RDONLY;
  }
  if (flags & O_TRUNC) {
    /* If we're opening for write or read/write, O_TRUNC means we should blow
     * away the file which is there and create our own file.
     * */
    return O_WRONLY;
  }
  info = hdfsGetPathInfo(fs, path);
  if (info) {
    if (info->mSize == 0) {
      // If the file has zero length, we shouldn't feel bad about blowing it
      // away.
      ret = O_WRONLY;
    } else if ((flags & O_ACCMODE) == O_RDWR) {
      // HACK: translate O_RDWR requests into O_RDONLY if the file already
      // exists and has non-zero length.
      ret = O_RDONLY;
    } else { // O_WRONLY
      // HACK: translate O_WRONLY requests into append if the file already
      // exists.
      ret = O_WRONLY | O_APPEND;
    }
  } else { // !info
    if (flags & O_CREAT) {
      ret = O_WRONLY;
    } else {
      ret = -ENOENT;
    }
  }
  if (info) {
    hdfsFreeFileInfo(info, 1);
  }
  return ret;
}

int dfs_open(const char *path, struct fuse_file_info *fi)
{
  hdfsFS fs = NULL;
  dfs_context *dfs = (dfs_context*)fuse_get_context()->private_data;
  dfs_fh *fh = NULL;
  int mutexInit = 0, ret, flags = 0;
  int64_t flagRet;

  TRACE1("open", path)

  // check params and the context var
  assert(path);
  assert('/' == *path);
  assert(dfs);

  // retrieve dfs specific data
  fh = (dfs_fh*)calloc(1, sizeof (dfs_fh));
  if (!fh) {
    ERROR("Malloc of new file handle failed");
    ret = -EIO;
    goto error;
  }
  ret = fuseConnectAsThreadUid(&fh->conn);
  if (ret) {
    fprintf(stderr, "fuseConnectAsThreadUid: failed to open a libhdfs "
            "connection!  error %d.\n", ret);
    ret = -EIO;
    goto error;
  }
  fs = hdfsConnGetFs(fh->conn);
  flagRet = get_hdfs_open_flags(fs, path, fi->flags);
  if (flagRet < 0) {
    ret = -flagRet;
    goto error;
  }
  flags = flagRet;
  if ((fh->hdfsFH = hdfsOpenFile(fs, path, flags,  0, 0, 0)) == NULL) {
    ERROR("Could not open file %s (errno=%d)", path, errno);
    if (errno == 0 || errno == EINTERNAL) {
      ret = -EIO;
      goto error;
    }
    ret = -errno;
    goto error;
  }

  ret = pthread_mutex_init(&fh->mutex, NULL);
  if (ret) {
    fprintf(stderr, "dfs_open: error initializing mutex: error %d\n", ret); 
    ret = -EIO;
    goto error;
  }
  mutexInit = 1;

  if ((flags & O_ACCMODE) == O_WRONLY) {
    fh->buf = NULL;
  } else  {
    assert(dfs->rdbuffer_size > 0);
    fh->buf = (char*)malloc(dfs->rdbuffer_size * sizeof(char));
    if (NULL == fh->buf) {
      ERROR("Could not allocate memory for a read for file %s\n", path);
      ret = -EIO;
      goto error;
    }
    fh->buffersStartOffset = 0;
    fh->bufferSize = 0;
  }
  fi->fh = (uint64_t)fh;
  return 0;

error:
  if (fh) {
    if (mutexInit) {
      pthread_mutex_destroy(&fh->mutex);
    }
    free(fh->buf);
    if (fh->hdfsFH) {
      hdfsCloseFile(fs, fh->hdfsFH);
    }
    if (fh->conn) {
      hdfsConnRelease(fh->conn);
    }
    free(fh);
  }
  return ret;
}
