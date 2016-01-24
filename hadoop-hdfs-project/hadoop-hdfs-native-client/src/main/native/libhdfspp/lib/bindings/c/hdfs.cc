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

#include "hdfspp/hdfspp.h"

#include "fs/filesystem.h"
#include "common/hdfs_configuration.h"
#include "common/configuration_loader.h"

#include <hdfs/hdfs.h>
#include <hdfspp/hdfs_ext.h>

#include <string>
#include <cstring>
#include <iostream>
#include <algorithm>

using namespace hdfs;

/* Separate the handles used by the C api from the C++ API*/
struct hdfs_internal {
  hdfs_internal(FileSystem *p) : filesystem_(p) {}
  hdfs_internal(std::unique_ptr<FileSystem> p)
      : filesystem_(std::move(p)) {}
  virtual ~hdfs_internal(){};
  FileSystem *get_impl() { return filesystem_.get(); }
  const FileSystem *get_impl() const { return filesystem_.get(); }

 private:
  std::unique_ptr<FileSystem> filesystem_;
};

struct hdfsFile_internal {
  hdfsFile_internal(FileHandle *p) : file_(p) {}
  hdfsFile_internal(std::unique_ptr<FileHandle> p) : file_(std::move(p)) {}
  virtual ~hdfsFile_internal(){};
  FileHandle *get_impl() { return file_.get(); }
  const FileHandle *get_impl() const { return file_.get(); }

 private:
  std::unique_ptr<FileHandle> file_;
};

/* Keep thread local copy of last error string */
thread_local std::string errstr;

/* Fetch last error that happened in this thread */
void hdfsGetLastError(char *buf, int len) {
  if(nullptr == buf || len < 1 || errstr.empty()) {
    return;
  }

  /* leave space for a trailing null */
  size_t copylen = std::min((size_t)errstr.size(), (size_t)len);
  if(copylen == (size_t)len) {
    copylen--;
  }

  strncpy(buf, errstr.c_str(), copylen);

  /* stick in null */
  buf[copylen] = 0;
}

struct hdfsBuilder {
  hdfsBuilder();
  hdfsBuilder(const char * directory);
  virtual ~hdfsBuilder() {}
  ConfigurationLoader loader;
  HdfsConfiguration config;

  std::string overrideHost;
  tPort       overridePort; // 0 --> use default

  static constexpr tPort kUseDefaultPort = 0;
  static constexpr tPort kDefaultPort = 8020;
};

/* Error handling with optional debug to stderr */
static void ReportError(int errnum, std::string msg) {
  errno = errnum;
  errstr = msg;
#ifdef LIBHDFSPP_C_API_ENABLE_DEBUG
  std::cerr << "Error: errno=" << strerror(errnum) << " message=\"" << msg
            << "\"" << std::endl;
#else
  (void)msg;
#endif
}

/* Convert Status wrapped error into appropriate errno and return code */
static int Error(const Status &stat) {
  int code = stat.code();
  switch (code) {
    case Status::Code::kOk:
      return 0;
    case Status::Code::kInvalidArgument:
      ReportError(EINVAL, "Invalid argument");
      break;
    case Status::Code::kResourceUnavailable:
      ReportError(EAGAIN, "Resource temporarily unavailable");
      break;
    case Status::Code::kUnimplemented:
      ReportError(ENOSYS, "Function not implemented");
      break;
    case Status::Code::kException:
      ReportError(EINTR, "Exception raised");
      break;
    default:
      ReportError(ENOSYS, "Error: unrecognised code");
  }
  return -1;
}

/* return false on failure */
bool CheckSystemAndHandle(hdfsFS fs, hdfsFile file) {
  if (!fs) {
    ReportError(ENODEV, "Cannot perform FS operations with null FS handle.");
    return false;
  }
  if (!file) {
    ReportError(EBADF, "Cannot perform FS operations with null File handle.");
    return false;
  }
  return true;
}

/**
 * C API implementations
 **/

int hdfsFileIsOpenForRead(hdfsFile file) {
  /* files can only be open for reads at the moment, do a quick check */
  if (file) {
    return true; // Update implementation when we get file writing
  }
  return false;
}

hdfsFS hdfsConnect(const char *nn, tPort port) {
  std::string port_as_string = std::to_string(port);
  IoService * io_service = IoService::New();
  FileSystem *fs = FileSystem::New(io_service, Options());
  if (!fs) {
    return nullptr;
  }

  if (!fs->Connect(nn, port_as_string).ok()) {
    ReportError(ENODEV, "Unable to connect to NameNode.");

    // FileSystem's ctor might take ownership of the io_service; if it does,
    //    it will null out the pointer
    if (io_service)
      delete io_service;

    delete fs;

    return nullptr;
  }
  return new hdfs_internal(fs);
}

int hdfsDisconnect(hdfsFS fs) {
  if (!fs) {
    ReportError(ENODEV, "Cannot disconnect null FS handle.");
    return -1;
  }

  delete fs;
  return 0;
}

hdfsFile hdfsOpenFile(hdfsFS fs, const char *path, int flags, int bufferSize,
                      short replication, tSize blocksize) {
  (void)flags;
  (void)bufferSize;
  (void)replication;
  (void)blocksize;
  if (!fs) {
    ReportError(ENODEV, "Cannot perform FS operations with null FS handle.");
    return nullptr;
  }
  FileHandle *f = nullptr;
  Status stat = fs->get_impl()->Open(path, &f);
  if (!stat.ok()) {
    return nullptr;
  }
  return new hdfsFile_internal(f);
}

int hdfsCloseFile(hdfsFS fs, hdfsFile file) {
  if (!CheckSystemAndHandle(fs, file)) {
    return -1;
  }
  delete file;
  return 0;
}

tSize hdfsPread(hdfsFS fs, hdfsFile file, tOffset position, void *buffer,
                tSize length) {
  if (!CheckSystemAndHandle(fs, file)) {
    return -1;
  }

  size_t len = length;
  Status stat = file->get_impl()->PositionRead(buffer, &len, position);
  if(!stat.ok()) {
    return Error(stat);
  }
  return (tSize)len;
}

tSize hdfsRead(hdfsFS fs, hdfsFile file, void *buffer, tSize length) {
  if (!CheckSystemAndHandle(fs, file)) {
    return -1;
  }

  size_t len = length;
  Status stat = file->get_impl()->Read(buffer, &len);
  if (!stat.ok()) {
    return Error(stat);
  }

  return (tSize)len;
}

int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos) {
  if (!CheckSystemAndHandle(fs, file)) {
    return -1;
  }

  off_t desired = desiredPos;
  Status stat = file->get_impl()->Seek(&desired, std::ios_base::beg);
  if (!stat.ok()) {
    return Error(stat);
  }

  return (int)desired;
}

tOffset hdfsTell(hdfsFS fs, hdfsFile file) {
  if (!CheckSystemAndHandle(fs, file)) {
    return -1;
  }

  ssize_t offset = 0;
  Status stat = file->get_impl()->Seek(&offset, std::ios_base::cur);
  if (!stat.ok()) {
    return Error(stat);
  }

  return offset;
}

int hdfsCancel(hdfsFS fs, hdfsFile file) {
  if (!CheckSystemAndHandle(fs, file)) {
    return -1;
  }
  static_cast<FileHandleImpl*>(file->get_impl())->CancelOperations();
  return 0;
}

/*******************************************************************
 *                BUILDER INTERFACE
 *******************************************************************/

HdfsConfiguration LoadDefault(ConfigurationLoader & loader)
{
  optional<HdfsConfiguration> result = loader.LoadDefaultResources<HdfsConfiguration>();
  if (result)
  {
    return result.value();
  }
  else
  {
    return loader.New<HdfsConfiguration>();
  }
}

hdfsBuilder::hdfsBuilder() : config(LoadDefault(loader)), overridePort(kUseDefaultPort)
{
}

hdfsBuilder::hdfsBuilder(const char * directory) :
      config(loader.New<HdfsConfiguration>()), overridePort(kUseDefaultPort)
{
  loader.SetSearchPath(directory);
  config = LoadDefault(loader);
}

struct hdfsBuilder *hdfsNewBuilder(void)
{
  return new struct hdfsBuilder();
}

void hdfsBuilderSetNameNode(struct hdfsBuilder *bld, const char *nn)
{
  bld->overrideHost = nn;
}

void hdfsBuilderSetNameNodePort(struct hdfsBuilder *bld, tPort port)
{
  bld->overridePort = port;
}

void hdfsFreeBuilder(struct hdfsBuilder *bld)
{
  delete bld;
}

int hdfsBuilderConfSetStr(struct hdfsBuilder *bld, const char *key,
                          const char *val)
{
  optional<HdfsConfiguration> newConfig = bld->loader.OverlayValue(bld->config, key, val);
  if (newConfig)
  {
    bld->config = newConfig.value();
    return 0;
  }
  else
  {
    return 1;
  }
}

void hdfsConfStrFree(char *val)
{
  if (val)
  {
    free(val);
  }
}

hdfsFS hdfsBuilderConnect(struct hdfsBuilder *bld) {

  if (!bld->overrideHost.empty())
  {
    // TODO: pass rest of config once we get that done (HDFS-9556)
    tPort port = bld->overridePort;
    if (port == hdfsBuilder::kUseDefaultPort)
    {
      port = hdfsBuilder::kDefaultPort;
    }
    return hdfsConnect(bld->overrideHost.c_str(), port);
  }
  else
  {
    //TODO: allow construction from default port once that is done (HDFS-9556)
    ReportError(EINVAL, "No host provided to builder in hdfsBuilderConnect");
    return nullptr;
  }
}

int hdfsConfGetStr(const char *key, char **val)
{
  hdfsBuilder builder;
  return hdfsBuilderConfGetStr(&builder, key, val);
}

int hdfsConfGetInt(const char *key, int32_t *val)
{
  hdfsBuilder builder;
  return hdfsBuilderConfGetInt(&builder, key, val);
}

//
//  Extended builder interface
//
struct hdfsBuilder *hdfsNewBuilderFromDirectory(const char * configDirectory)
{
  return new struct hdfsBuilder(configDirectory);
}

int hdfsBuilderConfGetStr(struct hdfsBuilder *bld, const char *key,
                          char **val)
{
  optional<std::string> value = bld->config.Get(key);
  if (value)
  {
    size_t len = value->length() + 1;
    *val = static_cast<char *>(malloc(len));
    strncpy(*val, value->c_str(), len);
  }
  else
  {
    *val = nullptr;
  }
  return 0;
}

// If we're running on a 32-bit platform, we might get 64-bit values that
//    don't fit in an int, and int is specified by the java hdfs.h interface
bool isValidInt(int64_t value)
{
  return (value >= std::numeric_limits<int>::min() &&
          value <= std::numeric_limits<int>::max());
}

int hdfsBuilderConfGetInt(struct hdfsBuilder *bld, const char *key, int32_t *val)
{
  // Pull from default configuration
  optional<int64_t> value = bld->config.GetInt(key);
  if (value)
  {
    if (!isValidInt(*value))
      return 1;

    *val = *value;
  }
  // If not found, don't change val
  return 0;
}
