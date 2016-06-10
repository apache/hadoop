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

/* Cheat for now and use the same hdfsBuilder as libhdfs */
/* (libhdfspp doesn't have an hdfsBuilder yet). */
struct hdfsBuilder {
    int forceNewInstance;
    const char *nn;
    tPort port;
    const char *kerbTicketCachePath;
    const char *userName;
    struct hdfsBuilderConfOpt *opts;
};

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

#define REPORT_FUNCTION_NOT_IMPLEMENTED                     \
  fprintf(stderr, "%s failed: function not implemented by " \
    "libhdfs++ test shim", __PRETTY_FUNCTION__);

int hdfsFileIsOpenForWrite(hdfsFile file) {
  return libhdfs_hdfsFileIsOpenForWrite(file->libhdfsRep);
}

int hdfsFileGetReadStatistics(hdfsFile file,
                              struct hdfsReadStatistics **stats) {
  return libhdfs_hdfsFileGetReadStatistics
      (file->libhdfsRep, (struct libhdfs_hdfsReadStatistics **)stats);
}

int64_t hdfsReadStatisticsGetRemoteBytesRead(
                        const struct hdfsReadStatistics *stats) {
  return libhdfs_hdfsReadStatisticsGetRemoteBytesRead
      ((struct libhdfs_hdfsReadStatistics *)stats);
}

int hdfsFileClearReadStatistics(hdfsFile file) {
  return libhdfs_hdfsFileClearReadStatistics(file->libhdfsRep);
}

void hdfsFileFreeReadStatistics(struct hdfsReadStatistics *stats) {
  libhdfs_hdfsFileFreeReadStatistics(
      (struct libhdfs_hdfsReadStatistics *)stats);
}

hdfsFS hdfsConnectAsUser(const char* nn, tPort port, const char *user) {
  return (hdfsFS) libhdfspp_hdfsConnectAsUser(nn, port, user);
}

hdfsFS hdfsConnect(const char* nn, tPort port) {
  REPORT_FUNCTION_NOT_IMPLEMENTED
  return NULL;
}

hdfsFS hdfsConnectAsUserNewInstance(const char* nn, tPort port, const char *user ) {
  REPORT_FUNCTION_NOT_IMPLEMENTED
  return NULL;
}

hdfsFS hdfsConnectNewInstance(const char* nn, tPort port) {
  REPORT_FUNCTION_NOT_IMPLEMENTED
  return NULL;
}

hdfsFS hdfsBuilderConnect(struct hdfsBuilder *bld) {
  hdfsFS ret = calloc(1, sizeof(struct hdfs_internal));
  ret->libhdfsppRep = libhdfspp_hdfsConnect(bld->nn, bld->port);
  if (!ret->libhdfsppRep) {
    free(ret);
    ret = NULL;
  } else {
    /* Destroys bld object. */
    ret->libhdfsRep = libhdfs_hdfsBuilderConnect(bld);
    if (!ret->libhdfsRep) {
      libhdfspp_hdfsDisconnect(ret->libhdfsppRep);
      free(ret);
      ret = NULL;
    }
  }
  return ret;
}

struct hdfsBuilder *hdfsNewBuilder(void) {
  return libhdfs_hdfsNewBuilder();
}

void hdfsBuilderSetForceNewInstance(struct hdfsBuilder *bld) {
  libhdfs_hdfsBuilderSetForceNewInstance(bld);
}

void hdfsBuilderSetNameNode(struct hdfsBuilder *bld, const char *nn) {
  libhdfs_hdfsBuilderSetNameNode(bld, nn);
}

void hdfsBuilderSetNameNodePort(struct hdfsBuilder *bld, tPort port) {
  libhdfs_hdfsBuilderSetNameNodePort(bld, port);
}

void hdfsBuilderSetUserName(struct hdfsBuilder *bld, const char *userName) {
  libhdfs_hdfsBuilderSetUserName(bld, userName);
}

void hdfsBuilderSetKerbTicketCachePath(struct hdfsBuilder *bld,
                               const char *kerbTicketCachePath) {
  libhdfs_hdfsBuilderSetKerbTicketCachePath(bld, kerbTicketCachePath);
}

void hdfsFreeBuilder(struct hdfsBuilder *bld) {
  libhdfs_hdfsFreeBuilder(bld);
}

int hdfsBuilderConfSetStr(struct hdfsBuilder *bld, const char *key,
                          const char *val) {
  return libhdfs_hdfsBuilderConfSetStr(bld, key, val);
}

int hdfsConfGetStr(const char *key, char **val) {
  return libhdfs_hdfsConfGetStr(key, val);
}

int hdfsConfGetInt(const char *key, int32_t *val) {
  return libhdfs_hdfsConfGetInt(key, val);
}

void hdfsConfStrFree(char *val) {
  libhdfs_hdfsConfStrFree(val);
}

int hdfsDisconnect(hdfsFS fs) {
  int ret;
  libhdfspp_hdfsDisconnect(fs->libhdfsppRep);
  ret = libhdfs_hdfsDisconnect(fs->libhdfsRep);
  free(fs);
  return ret;
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
  return libhdfs_hdfsExists(fs->libhdfsRep, path);
}

int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos) {
  return libhdfs_hdfsSeek(fs->libhdfsRep, file->libhdfsRep, desiredPos);
}

tOffset hdfsTell(hdfsFS fs, hdfsFile file) {
  return libhdfs_hdfsTell(fs->libhdfsRep, file->libhdfsRep);
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
  return libhdfs_hdfsAvailable(fs->libhdfsRep, file->libhdfsRep);
}

int hdfsCopy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst) {
  return libhdfs_hdfsCopy(srcFS->libhdfsRep, src, dstFS->libhdfsRep, dst);
}

int hdfsMove(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst) {
  return libhdfs_hdfsMove(srcFS->libhdfsRep, src, dstFS->libhdfsRep, dst);
}

int hdfsDelete(hdfsFS fs, const char* path, int recursive) {
  return libhdfs_hdfsDelete(fs->libhdfsRep, path, recursive);
}

int hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath) {
  return libhdfs_hdfsRename(fs->libhdfsRep, oldPath, newPath);
}

char* hdfsGetWorkingDirectory(hdfsFS fs, char *buffer, size_t bufferSize) {
  return libhdfs_hdfsGetWorkingDirectory(fs->libhdfsRep, buffer, bufferSize);
}

int hdfsSetWorkingDirectory(hdfsFS fs, const char* path) {
  return libhdfs_hdfsSetWorkingDirectory(fs->libhdfsRep, path);
}

int hdfsCreateDirectory(hdfsFS fs, const char* path) {
  return libhdfs_hdfsCreateDirectory(fs->libhdfsRep, path);
}

int hdfsSetReplication(hdfsFS fs, const char* path, int16_t replication) {
  return libhdfs_hdfsSetReplication(fs->libhdfsRep, path, replication);
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
  return libhdfs_hdfsGetHosts(fs->libhdfsRep, path, start, length);
}

void hdfsFreeHosts(char ***blockHosts) {
  return libhdfs_hdfsFreeHosts(blockHosts);
}

tOffset hdfsGetDefaultBlockSize(hdfsFS fs) {
  return libhdfs_hdfsGetDefaultBlockSize(fs->libhdfsRep);
}

tOffset hdfsGetDefaultBlockSizeAtPath(hdfsFS fs, const char *path) {
  return libhdfs_hdfsGetDefaultBlockSizeAtPath(fs->libhdfsRep, path);
}

tOffset hdfsGetCapacity(hdfsFS fs) {
  return libhdfspp_hdfsGetCapacity(fs->libhdfsppRep);
}

tOffset hdfsGetUsed(hdfsFS fs) {
  return libhdfspp_hdfsGetUsed(fs->libhdfsppRep);
}

int hdfsChown(hdfsFS fs, const char* path, const char *owner,
              const char *group) {
  return libhdfs_hdfsChown(fs->libhdfsRep, path, owner, group);
}

int hdfsChmod(hdfsFS fs, const char* path, short mode) {
  return libhdfs_hdfsChmod(fs->libhdfsRep, path, mode);
}

int hdfsUtime(hdfsFS fs, const char* path, tTime mtime, tTime atime) {
  return libhdfs_hdfsUtime(fs->libhdfsRep, path, mtime, atime);
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


/*************
 * hdfs_ext functions
 */

void hdfsGetLastError(char *buf, int len) {
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

int hdfsCreateSnapshot(hdfsFS fs, const char* path, const char* name) {
  return libhdfspp_hdfsCreateSnapshot(fs->libhdfsppRep, path, name);
}

int hdfsDeleteSnapshot(hdfsFS fs, const char* path, const char* name) {
  return libhdfspp_hdfsDeleteSnapshot(fs->libhdfsppRep, path, name);
}

int hdfsAllowSnapshot(hdfsFS fs, const char* path) {
  return libhdfspp_hdfsAllowSnapshot(fs->libhdfsppRep, path);
}

int hdfsDisallowSnapshot(hdfsFS fs, const char* path) {
  return libhdfspp_hdfsDisallowSnapshot(fs->libhdfsppRep, path);
}
