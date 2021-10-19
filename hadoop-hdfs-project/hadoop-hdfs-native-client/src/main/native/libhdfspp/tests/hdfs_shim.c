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

#include "libhdfs_wrapper.h"
#include "libhdfspp_wrapper.h"
#include "hdfs/hdfs.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Shim structs and functions that delegate to libhdfspp and libhdfs. */
struct hdfs_internal {
  libhdfs_hdfsFS libhdfsRep;
  libhdfspp_hdfsFS libhdfsppRep;
};
typedef struct hdfs_internal* hdfsFS;

struct hdfsFile_internal {
  libhdfs_hdfsFile libhdfsRep;
  libhdfspp_hdfsFile libhdfsppRep;
};
typedef struct hdfsFile_internal* hdfsFile;

struct hdfsBuilder {
  struct hdfsBuilder *  libhdfs_builder;
  struct hdfsBuilder * libhdfspp_builder;
};

#define REPORT_FUNCTION_NOT_IMPLEMENTED                     \
  fprintf(stderr, "%s failed: function not implemented by " \
    "libhdfs++ test shim", __PRETTY_FUNCTION__);

int hdfsFileIsOpenForWrite(hdfsFile file) {
  return libhdfs_hdfsFileIsOpenForWrite(file->libhdfsRep);
}

int hdfsFileGetReadStatistics(hdfsFile file, struct hdfsReadStatistics **stats) {
  //We do not track which bytes were remote or local, so we assume all are local
  int ret = libhdfspp_hdfsFileGetReadStatistics(file->libhdfsppRep, (struct libhdfspp_hdfsReadStatistics **)stats);
  if(!ret) {
    (*stats)->totalLocalBytesRead = (*stats)->totalBytesRead;
  }
  return ret;
}

int64_t hdfsReadStatisticsGetRemoteBytesRead(const struct hdfsReadStatistics *stats) {
  return libhdfspp_hdfsReadStatisticsGetRemoteBytesRead((struct libhdfspp_hdfsReadStatistics *)stats);
}

int hdfsFileClearReadStatistics(hdfsFile file) {
  return libhdfspp_hdfsFileClearReadStatistics(file->libhdfsppRep);
}

void hdfsFileFreeReadStatistics(struct hdfsReadStatistics *stats) {
  libhdfspp_hdfsFileFreeReadStatistics((struct libhdfspp_hdfsReadStatistics *)stats);
}

hdfsFS hdfsConnectAsUser(const char* nn, tPort port, const char *user) {
  hdfsFS ret = calloc(1, sizeof(struct hdfs_internal));
  ret->libhdfsRep = libhdfs_hdfsConnectAsUser(nn, port, user);
    if (!ret->libhdfsRep) {
      libhdfs_hdfsDisconnect(ret->libhdfsRep);
      free(ret);
      return NULL;
    }
  ret->libhdfsppRep = libhdfspp_hdfsConnectAsUser(nn, port, user);
  if (!ret->libhdfsppRep) {
    libhdfs_hdfsDisconnect(ret->libhdfsRep);
    free(ret);
    return NULL;
  }
  return ret;
}

hdfsFS hdfsConnect(const char* nn, tPort port) {
  hdfsFS ret = calloc(1, sizeof(struct hdfs_internal));
  ret->libhdfsRep = libhdfs_hdfsConnect(nn, port);
    if (!ret->libhdfsRep) {
      libhdfs_hdfsDisconnect(ret->libhdfsRep);
      free(ret);
      return NULL;
    }
  ret->libhdfsppRep = libhdfspp_hdfsConnect(nn, port);
  if (!ret->libhdfsppRep) {
    libhdfs_hdfsDisconnect(ret->libhdfsRep);
    free(ret);
    return NULL;
  }
  return ret;
}

hdfsFS hdfsConnectAsUserNewInstance(const char* nn, tPort port, const char *user ) {
  hdfsFS ret = calloc(1, sizeof(struct hdfs_internal));
  ret->libhdfsRep = libhdfs_hdfsConnectAsUserNewInstance(nn, port, user);
    if (!ret->libhdfsRep) {
      libhdfs_hdfsDisconnect(ret->libhdfsRep);
      free(ret);
      return NULL;
    }
  ret->libhdfsppRep = libhdfspp_hdfsConnectAsUserNewInstance(nn, port, user);
  if (!ret->libhdfsppRep) {
    libhdfs_hdfsDisconnect(ret->libhdfsRep);
    free(ret);
    return NULL;
  }
  return ret;
}

hdfsFS hdfsConnectNewInstance(const char* nn, tPort port) {
  hdfsFS ret = calloc(1, sizeof(struct hdfs_internal));
  ret->libhdfsRep = libhdfs_hdfsConnectNewInstance(nn, port);
    if (!ret->libhdfsRep) {
      libhdfs_hdfsDisconnect(ret->libhdfsRep);
      free(ret);
      return NULL;
    }
  ret->libhdfsppRep = libhdfspp_hdfsConnectNewInstance(nn, port);
  if (!ret->libhdfsppRep) {
    libhdfs_hdfsDisconnect(ret->libhdfsRep);
    free(ret);
    return NULL;
  }
  return ret;
}

hdfsFS hdfsBuilderConnect(struct hdfsBuilder *bld) {
  hdfsFS ret = calloc(1, sizeof(struct hdfs_internal));
  ret->libhdfsRep = libhdfs_hdfsBuilderConnect(bld->libhdfs_builder);
  if (!ret->libhdfsRep) {
    free(ret);
    return NULL;
  }
  /* Destroys bld object. */
  ret->libhdfsppRep = libhdfspp_hdfsBuilderConnect(bld->libhdfspp_builder);
  if (!ret->libhdfsppRep) {
    libhdfs_hdfsDisconnect(ret->libhdfsRep);
    free(ret);
    return NULL;
  }
  return ret;
}

struct hdfsBuilder *hdfsNewBuilder(void) {
  struct hdfsBuilder * result = calloc(1, sizeof(struct hdfsBuilder));
  result -> libhdfs_builder = libhdfs_hdfsNewBuilder();
  result -> libhdfspp_builder = libhdfspp_hdfsNewBuilder();
  return result;
}

void hdfsBuilderSetForceNewInstance(struct hdfsBuilder *bld) {
  libhdfs_hdfsBuilderSetForceNewInstance(bld->libhdfs_builder);
  libhdfspp_hdfsBuilderSetForceNewInstance(bld->libhdfspp_builder);
}

void hdfsBuilderSetNameNode(struct hdfsBuilder *bld, const char *nn) {
  libhdfs_hdfsBuilderSetNameNode(bld->libhdfs_builder, nn);
  libhdfspp_hdfsBuilderSetNameNode(bld->libhdfspp_builder, nn);
}

void hdfsBuilderSetNameNodePort(struct hdfsBuilder *bld, tPort port) {
  libhdfs_hdfsBuilderSetNameNodePort(bld->libhdfs_builder, port);
  libhdfspp_hdfsBuilderSetNameNodePort(bld->libhdfspp_builder, port);
}

void hdfsBuilderSetUserName(struct hdfsBuilder *bld, const char *userName) {
  libhdfs_hdfsBuilderSetUserName(bld->libhdfs_builder, userName);
  libhdfspp_hdfsBuilderSetUserName(bld->libhdfspp_builder, userName);
}

void hdfsBuilderSetKerbTicketCachePath(struct hdfsBuilder *bld,
                               const char *kerbTicketCachePath) {
  REPORT_FUNCTION_NOT_IMPLEMENTED
}

void hdfsFreeBuilder(struct hdfsBuilder *bld) {
  libhdfs_hdfsFreeBuilder(bld->libhdfs_builder);
  libhdfspp_hdfsFreeBuilder(bld->libhdfspp_builder);
  free(bld);
}

int hdfsBuilderConfSetStr(struct hdfsBuilder *bld, const char *key,
                          const char *val) {
  int ret = libhdfs_hdfsBuilderConfSetStr(bld->libhdfs_builder, key, val);
  if (ret) {
    return ret;
  }
  ret = libhdfspp_hdfsBuilderConfSetStr(bld->libhdfspp_builder, key, val);
  if (ret) {
    return ret;
  }
  return 0;
}

int hdfsConfGetStr(const char *key, char **val) {
  return libhdfspp_hdfsConfGetStr(key, val);
}

int hdfsConfGetInt(const char *key, int32_t *val) {
  return libhdfspp_hdfsConfGetInt(key, val);
}

void hdfsConfStrFree(char *val) {
  libhdfspp_hdfsConfStrFree(val);
}

int hdfsDisconnect(hdfsFS fs) {
  int ret1 = libhdfs_hdfsDisconnect(fs->libhdfsRep);
  int ret2 = libhdfspp_hdfsDisconnect(fs->libhdfsppRep);
  free(fs);
  if (ret1){
    return ret1;
  } else if (ret2){
    return ret2;
  } else {
    return 0;
  }
}

hdfsFile hdfsOpenFile(hdfsFS fs, const char* path, int flags,
                      int bufferSize, short replication, tSize blocksize) {
  hdfsFile ret = calloc(1, sizeof(struct hdfsFile_internal));
  /* Currently only open libhdf++ for reads. */
  ret->libhdfsppRep = 0;
  if (flags == O_RDONLY) {
    ret->libhdfsppRep = libhdfspp_hdfsOpenFile(fs->libhdfsppRep, path, flags,
        bufferSize, replication, blocksize);
  }
  ret->libhdfsRep = libhdfs_hdfsOpenFile(fs->libhdfsRep, path,
      flags, bufferSize, replication, blocksize);
  if (!ret->libhdfsRep) {
    free(ret);
    ret = NULL;
  }
  return ret;
}

int hdfsTruncateFile(hdfsFS fs, const char* path, tOffset newlength) {
  return libhdfs_hdfsTruncateFile(fs->libhdfsRep, path, newlength);
}

int hdfsUnbufferFile(hdfsFile file) {
  return libhdfs_hdfsUnbufferFile(file->libhdfsRep);
}

int hdfsCloseFile(hdfsFS fs, hdfsFile file) {
  int ret;
  if (file->libhdfsppRep) {
    libhdfspp_hdfsCloseFile(fs->libhdfsppRep, file->libhdfsppRep);
  }
  ret = libhdfs_hdfsCloseFile(fs->libhdfsRep, file->libhdfsRep);
  free(file);
  return ret;
}

int hdfsExists(hdfsFS fs, const char *path) {
  return libhdfspp_hdfsExists(fs->libhdfsppRep, path);
}

int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos) {
  int ret1 = libhdfs_hdfsSeek(fs->libhdfsRep, file->libhdfsRep, desiredPos);
  int ret2 = libhdfspp_hdfsSeek(fs->libhdfsppRep, file->libhdfsppRep, desiredPos);
  if (ret1) {
    return ret1;
  } else if (ret2) {
    return ret2;
  } else {
    return 0;
  }
}

tOffset hdfsTell(hdfsFS fs, hdfsFile file) {
  tOffset ret1 = libhdfs_hdfsTell(fs->libhdfsRep, file->libhdfsRep);
  tOffset ret2 = libhdfspp_hdfsTell(fs->libhdfsppRep, file->libhdfsppRep);
  if (ret1 != ret2) {
    errno = EIO;
    return -1;
  } else {
    return ret1;
  }
}

tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length) {
  // Read to update stats.
  tSize nRead = libhdfs_hdfsRead(fs->libhdfsRep, file->libhdfsRep, buffer, length);
  // Clear to avoid false positives.
  if (nRead > 0) memset(buffer, 0, nRead);
  return libhdfspp_hdfsRead(fs->libhdfsppRep, file->libhdfsppRep, buffer, length);
}

tSize hdfsPread(hdfsFS fs, hdfsFile file, tOffset position,
                void* buffer, tSize length) {
  tSize ret = -1;
  if (!fs->libhdfsppRep) {
    fprintf(stderr, "hdfsPread failed: no libhdfs++ file system");
  } else if (!file->libhdfsppRep) {
    fprintf(stderr, "hdfsPread failed: no libhdfs++ file");
  } else {
    ret = libhdfspp_hdfsPread(fs->libhdfsppRep, file->libhdfsppRep,
        position, buffer, length);
  }
  return ret;
}

int hdfsPreadFully(hdfsFS fs, hdfsFile file, tOffset position,
                void* buffer, tSize length) {
  return libhdfs_hdfsPreadFully(fs->libhdfsRep, file->libhdfsRep, position,
          buffer, length);
}

tSize hdfsWrite(hdfsFS fs, hdfsFile file, const void* buffer,
                tSize length) {
  return libhdfs_hdfsWrite(fs->libhdfsRep, file->libhdfsRep, buffer, length);
}

int hdfsFlush(hdfsFS fs, hdfsFile file) {
  return libhdfs_hdfsFlush(fs->libhdfsRep, file->libhdfsRep);
}

int hdfsHFlush(hdfsFS fs, hdfsFile file) {
  return libhdfs_hdfsHFlush(fs->libhdfsRep, file->libhdfsRep);
}

int hdfsHSync(hdfsFS fs, hdfsFile file) {
  return libhdfs_hdfsHSync(fs->libhdfsRep, file->libhdfsRep);
}

int hdfsAvailable(hdfsFS fs, hdfsFile file) {
  return libhdfspp_hdfsAvailable(fs->libhdfsppRep, file->libhdfsppRep);
}

int hdfsCopy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst) {
  return libhdfs_hdfsCopy(srcFS->libhdfsRep, src, dstFS->libhdfsRep, dst);
}

int hdfsMove(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst) {
  return libhdfs_hdfsMove(srcFS->libhdfsRep, src, dstFS->libhdfsRep, dst);
}

int hdfsDelete(hdfsFS fs, const char* path, int recursive) {
  return libhdfspp_hdfsDelete(fs->libhdfsppRep, path, recursive);
}

int hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath) {
  return libhdfspp_hdfsRename(fs->libhdfsppRep, oldPath, newPath);
}

char* hdfsGetWorkingDirectory(hdfsFS fs, char *buffer, size_t bufferSize) {
  return libhdfspp_hdfsGetWorkingDirectory(fs->libhdfsppRep, buffer, bufferSize);
}

int hdfsSetWorkingDirectory(hdfsFS fs, const char* path) {
  int ret1 = libhdfspp_hdfsSetWorkingDirectory(fs->libhdfsppRep, path);
  int ret2 = libhdfs_hdfsSetWorkingDirectory(fs->libhdfsRep, path);
  if (ret1) {
    return ret1;
  } else if (ret2) {
    return ret2;
  } else {
    return 0;
  }
}

int hdfsCreateDirectory(hdfsFS fs, const char* path) {
  return libhdfspp_hdfsCreateDirectory(fs->libhdfsppRep, path);
}

int hdfsSetReplication(hdfsFS fs, const char* path, int16_t replication) {
  return libhdfspp_hdfsSetReplication(fs->libhdfsppRep, path, replication);
}

hdfsFileInfo *hdfsListDirectory(hdfsFS fs, const char* path,
                                int *numEntries) {
  return (hdfsFileInfo *)libhdfspp_hdfsListDirectory(fs->libhdfsppRep, path, numEntries);
}

hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, const char* path) {
  return (hdfsFileInfo *)libhdfspp_hdfsGetPathInfo(fs->libhdfsppRep, path);
}

void hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries) {
  return libhdfspp_hdfsFreeFileInfo
      ((libhdfspp_hdfsFileInfo *) hdfsFileInfo, numEntries);
}

int hdfsFileIsEncrypted(hdfsFileInfo *hdfsFileInfo) {
  return libhdfs_hdfsFileIsEncrypted
      ((libhdfs_hdfsFileInfo *) hdfsFileInfo);
}

char*** hdfsGetHosts(hdfsFS fs, const char* path,
        tOffset start, tOffset length) {
  return libhdfspp_hdfsGetHosts(fs->libhdfsppRep, path, start, length);
}

void hdfsFreeHosts(char ***blockHosts) {
  return libhdfspp_hdfsFreeHosts(blockHosts);
}

tOffset hdfsGetDefaultBlockSize(hdfsFS fs) {
  return libhdfspp_hdfsGetDefaultBlockSize(fs->libhdfsppRep);
}

tOffset hdfsGetDefaultBlockSizeAtPath(hdfsFS fs, const char *path) {
  return libhdfspp_hdfsGetDefaultBlockSizeAtPath(fs->libhdfsppRep, path);
}

tOffset hdfsGetCapacity(hdfsFS fs) {
  return libhdfspp_hdfsGetCapacity(fs->libhdfsppRep);
}

tOffset hdfsGetUsed(hdfsFS fs) {
  return libhdfspp_hdfsGetUsed(fs->libhdfsppRep);
}

int hdfsChown(hdfsFS fs, const char* path, const char *owner,
              const char *group) {
  return libhdfspp_hdfsChown(fs->libhdfsppRep, path, owner, group);
}

int hdfsChmod(hdfsFS fs, const char* path, short mode) {
  return libhdfspp_hdfsChmod(fs->libhdfsppRep, path, mode);
}

int hdfsUtime(hdfsFS fs, const char* path, tTime mtime, tTime atime) {
  return libhdfspp_hdfsUtime(fs->libhdfsppRep, path, mtime, atime);
}

struct hadoopRzOptions *hadoopRzOptionsAlloc(void) {
  return libhdfs_hadoopRzOptionsAlloc();
}

int hadoopRzOptionsSetSkipChecksum(
        struct hadoopRzOptions *opts, int skip) {
  return libhdfs_hadoopRzOptionsSetSkipChecksum(opts, skip);
}

int hadoopRzOptionsSetByteBufferPool(
        struct hadoopRzOptions *opts, const char *className) {
  return libhdfs_hadoopRzOptionsSetByteBufferPool(opts, className);
}

void hadoopRzOptionsFree(struct hadoopRzOptions *opts) {
  libhdfs_hadoopRzOptionsFree(opts);
}

struct hadoopRzBuffer* hadoopReadZero(hdfsFile file,
        struct hadoopRzOptions *opts, int32_t maxLength) {
  return libhdfs_hadoopReadZero(file->libhdfsRep, opts, maxLength);
}

int32_t hadoopRzBufferLength(const struct hadoopRzBuffer *buffer) {
  return libhdfs_hadoopRzBufferLength(buffer);
}

const void *hadoopRzBufferGet(const struct hadoopRzBuffer *buffer) {
  return libhdfs_hadoopRzBufferGet(buffer);
}

void hadoopRzBufferFree(hdfsFile file, struct hadoopRzBuffer *buffer) {
  return libhdfs_hadoopRzBufferFree(file->libhdfsRep, buffer);
}

int hdfsGetHedgedReadMetrics(hdfsFS fs, struct hdfsHedgedReadMetrics **metrics) {
  return  libhdfs_hdfsGetHedgedReadMetrics(fs->libhdfsRep, (struct libhdfs_hdfsHedgedReadMetrics **) metrics);
}

void hdfsFreeHedgedReadMetrics(struct hdfsHedgedReadMetrics *metrics) {
  return  libhdfs_hdfsFreeHedgedReadMetrics((struct libhdfs_hdfsHedgedReadMetrics *) metrics);
}

/*************
 * hdfs_ext functions
 */

int hdfsGetLastError(char *buf, int len) {
  return libhdfspp_hdfsGetLastError(buf, len);
}

int hdfsCancel(hdfsFS fs, hdfsFile file) {
  return libhdfspp_hdfsCancel(fs->libhdfsppRep, file->libhdfsppRep);
}


int hdfsGetBlockLocations(hdfsFS fs, const char *path, struct hdfsBlockLocations ** locations) {
  return libhdfspp_hdfsGetBlockLocations(fs->libhdfsppRep, path, locations);
}

int hdfsFreeBlockLocations(struct hdfsBlockLocations * locations) {
  return libhdfspp_hdfsFreeBlockLocations(locations);
}

hdfsFileInfo *hdfsFind(hdfsFS fs, const char* path, const char* name, uint32_t *numEntries) {
  return (hdfsFileInfo *)libhdfspp_hdfsFind(fs->libhdfsppRep, path, name, numEntries);
}

int hdfsCreateSnapshot(hdfsFS fs, const char* path, const char* name) {
  return libhdfspp_hdfsCreateSnapshot(fs->libhdfsppRep, path, name);
}

int hdfsDeleteSnapshot(hdfsFS fs, const char* path, const char* name) {
  return libhdfspp_hdfsDeleteSnapshot(fs->libhdfsppRep, path, name);
}

int hdfsRenameSnapshot(hdfsFS fs, const char* path, const char* old_name, const char* new_name) {
  return libhdfspp_hdfsRenameSnapshot(fs->libhdfsppRep, path, old_name, new_name);
}

int hdfsAllowSnapshot(hdfsFS fs, const char* path) {
  return libhdfspp_hdfsAllowSnapshot(fs->libhdfsppRep, path);
}

int hdfsDisallowSnapshot(hdfsFS fs, const char* path) {
  return libhdfspp_hdfsDisallowSnapshot(fs->libhdfsppRep, path);
}
