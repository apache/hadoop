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
#include "hdfspp/hdfs_ext.h"

#include "common/hdfs_configuration.h"
#include "common/configuration_loader.h"
#include "common/logging.h"
#include "fs/filesystem.h"
#include "fs/filehandle.h"


#include <libgen.h>
#include "limits.h"
#include <string>
#include <cstring>
#include <iostream>
#include <algorithm>
#include <functional>

using namespace hdfs;
using std::experimental::nullopt;
using namespace std::placeholders;

static constexpr tPort kDefaultPort = 8020;

/** Annotate what parts of the code below are implementatons of API functions
 *  and if they are normal vs. extended API.
 */
#define LIBHDFS_C_API
#define LIBHDFSPP_EXT_API

/* Separate the handles used by the C api from the C++ API*/
struct hdfs_internal {
  hdfs_internal(FileSystem *p) : filesystem_(p), working_directory_("/") {}
  hdfs_internal(std::unique_ptr<FileSystem> p)
      : filesystem_(std::move(p)), working_directory_("/") {}
  virtual ~hdfs_internal(){};
  FileSystem *get_impl() { return filesystem_.get(); }
  const FileSystem *get_impl() const { return filesystem_.get(); }
  std::string get_working_directory() {
    std::lock_guard<std::mutex> read_guard(wd_lock_);
    return working_directory_;
  }
  void set_working_directory(std::string new_directory) {
    std::lock_guard<std::mutex> write_guard(wd_lock_);
    working_directory_ = new_directory;
  }

 private:
  std::unique_ptr<FileSystem> filesystem_;
  std::string working_directory_;      //has to always start and end with '/'
  std::mutex wd_lock_;                 //synchronize access to the working directory
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
LIBHDFSPP_EXT_API
int hdfsGetLastError(char *buf, int len) {
  //No error message
  if(errstr.empty()){
    return -1;
  }

  //There is an error, but no room for the error message to be copied to
  if(nullptr == buf || len < 1) {
    return -1;
  }

  /* leave space for a trailing null */
  size_t copylen = std::min((size_t)errstr.size(), (size_t)len);
  if(copylen == (size_t)len) {
    copylen--;
  }

  strncpy(buf, errstr.c_str(), copylen);

  /* stick in null */
  buf[copylen] = 0;

  return 0;
}

/* Event callbacks for next open calls */
thread_local std::experimental::optional<fs_event_callback> fsEventCallback;
thread_local std::experimental::optional<file_event_callback> fileEventCallback;

struct hdfsBuilder {
  hdfsBuilder();
  hdfsBuilder(const char * directory);
  virtual ~hdfsBuilder() {}
  ConfigurationLoader loader;
  HdfsConfiguration config;

  optional<std::string> overrideHost;
  optional<tPort>       overridePort;
  optional<std::string> user;

  static constexpr tPort kUseDefaultPort = 0;
};

/* Error handling with optional debug to stderr */
static void ReportError(int errnum, const std::string & msg) {
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
  const char * default_message;
  int errnum;

  int code = stat.code();
  switch (code) {
    case Status::Code::kOk:
      return 0;
    case Status::Code::kInvalidArgument:
      errnum = EINVAL;
      default_message = "Invalid argument";
      break;
    case Status::Code::kResourceUnavailable:
      errnum = EAGAIN;
      default_message = "Resource temporarily unavailable";
      break;
    case Status::Code::kUnimplemented:
      errnum = ENOSYS;
      default_message = "Function not implemented";
      break;
    case Status::Code::kException:
      errnum = EINTR;
      default_message = "Exception raised";
      break;
    case Status::Code::kOperationCanceled:
      errnum = EINTR;
      default_message = "Operation canceled";
      break;
    case Status::Code::kPermissionDenied:
      errnum = EACCES;
      default_message = "Permission denied";
      break;
    case Status::Code::kPathNotFound:
      errnum = ENOENT;
      default_message = "No such file or directory";
      break;
    case Status::Code::kNotADirectory:
      errnum = ENOTDIR;
      default_message = "Not a directory";
      break;
    case Status::Code::kFileAlreadyExists:
      errnum = EEXIST;
      default_message = "File already exists";
      break;
    case Status::Code::kPathIsNotEmptyDirectory:
      errnum = ENOTEMPTY;
      default_message = "Directory is not empty";
      break;
    case Status::Code::kInvalidOffset:
      errnum = Status::Code::kInvalidOffset;
      default_message = "Trying to begin a read past the EOF";
      break;
    default:
      errnum = ENOSYS;
      default_message = "Error: unrecognised code";
  }
  if (stat.ToString().empty())
    ReportError(errnum, default_message);
  else
    ReportError(errnum, stat.ToString());
  return -1;
}

static int ReportException(const std::exception & e)
{
  return Error(Status::Exception("Uncaught exception", e.what()));
}

static int ReportCaughtNonException()
{
  return Error(Status::Exception("Uncaught value not derived from std::exception", ""));
}

/* return false on failure */
bool CheckSystem(hdfsFS fs) {
  if (!fs) {
    ReportError(ENODEV, "Cannot perform FS operations with null FS handle.");
    return false;
  }

  return true;
}

/* return false on failure */
bool CheckHandle(hdfsFile file) {
  if (!file) {
    ReportError(EBADF, "Cannot perform FS operations with null File handle.");
    return false;
  }
  return true;
}

/* return false on failure */
bool CheckSystemAndHandle(hdfsFS fs, hdfsFile file) {
  if (!CheckSystem(fs))
    return false;

  if (!CheckHandle(file))
    return false;

  return true;
}

optional<std::string> getAbsolutePath(hdfsFS fs, const char* path) {
  //Does not support . (dot) and .. (double dot) semantics
  if (!path || path[0] == '\0') {
    Error(Status::InvalidArgument("getAbsolutePath: argument 'path' cannot be NULL or empty"));
    return optional<std::string>();
  }
  if (path[0] != '/') {
    //we know that working directory always ends with '/'
    return fs->get_working_directory().append(path);
  }
  return optional<std::string>(path);
}

/**
 * C API implementations
 **/

LIBHDFS_C_API
int hdfsFileIsOpenForRead(hdfsFile file) {
  /* files can only be open for reads at the moment, do a quick check */
  if (!CheckHandle(file)){
    return 0;
  }
  return 1; // Update implementation when we get file writing
}

LIBHDFS_C_API
int hdfsFileIsOpenForWrite(hdfsFile file) {
  /* files can only be open for reads at the moment, so return false */
  CheckHandle(file);
  return -1; // Update implementation when we get file writing
}

int hdfsConfGetLong(const char *key, int64_t *val)
{
  try
  {
    errno = 0;
    hdfsBuilder builder;
    return hdfsBuilderConfGetLong(&builder, key, val);
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

hdfsFS doHdfsConnect(optional<std::string> nn, optional<tPort> port, optional<std::string> user, const Options & options) {
  try
  {
    errno = 0;
    IoService * io_service = IoService::New();

    FileSystem *fs = FileSystem::New(io_service, user.value_or(""), options);
    if (!fs) {
      ReportError(ENODEV, "Could not create FileSystem object");
      return nullptr;
    }

    if (fsEventCallback) {
      fs->SetFsEventCallback(fsEventCallback.value());
    }

    Status status;
    if (nn || port) {
      if (!port) {
        port = kDefaultPort;
      }
      std::string port_as_string = std::to_string(*port);
      status = fs->Connect(nn.value_or(""), port_as_string);
    } else {
      status = fs->ConnectToDefaultFs();
    }

    if (!status.ok()) {
      Error(status);

      // FileSystem's ctor might take ownership of the io_service; if it does,
      //    it will null out the pointer
      if (io_service)
        delete io_service;

      delete fs;

      return nullptr;
    }
    return new hdfs_internal(fs);
  } catch (const std::exception & e) {
    ReportException(e);
    return nullptr;
  } catch (...) {
    ReportCaughtNonException();
    return nullptr;
  }
}

LIBHDFSPP_EXT_API
hdfsFS hdfsAllocateFileSystem(struct hdfsBuilder *bld) {
  // Same idea as the first half of doHdfsConnect, but return the wrapped FS before
  // connecting.
  try {
    errno = 0;
    std::shared_ptr<IoService> io_service = IoService::MakeShared();

    int io_thread_count = bld->config.GetOptions().io_threads_;
    if(io_thread_count < 1) {
      io_service->InitDefaultWorkers();
    } else {
      io_service->InitWorkers(io_thread_count);
    }

    FileSystem *fs = FileSystem::New(io_service, bld->user.value_or(""), bld->config.GetOptions());
    if (!fs) {
      ReportError(ENODEV, "Could not create FileSystem object");
      return nullptr;
    }

    if (fsEventCallback) {
      fs->SetFsEventCallback(fsEventCallback.value());
    }

    return new hdfs_internal(fs);
  } catch (const std::exception &e) {
    ReportException(e);
    return nullptr;
  } catch (...) {
    ReportCaughtNonException();
    return nullptr;
  }
  return nullptr;
}

LIBHDFSPP_EXT_API
int hdfsConnectAllocated(hdfsFS fs, struct hdfsBuilder *bld) {
  if(!CheckSystem(fs)) {
    return ENODEV;
  }

  if(!bld) {
    ReportError(ENODEV, "No hdfsBuilder object supplied");
    return ENODEV;
  }

  // Get C++ FS to do connect
  FileSystem *fsImpl = fs->get_impl();
  if(!fsImpl) {
    ReportError(ENODEV, "Null FileSystem implementation");
    return ENODEV;
  }

  // Unpack the required bits of the hdfsBuilder
  optional<std::string> nn = bld->overrideHost;
  optional<tPort> port = bld->overridePort;
  optional<std::string> user = bld->user;

  // try-catch in case some of the third-party stuff throws
  try {
    Status status;
    if (nn || port) {
      if (!port) {
        port = kDefaultPort;
      }
      std::string port_as_string = std::to_string(*port);
      status = fsImpl->Connect(nn.value_or(""), port_as_string);
    } else {
      status = fsImpl->ConnectToDefaultFs();
    }

    if (!status.ok()) {
      Error(status);
      return ENODEV;
    }

    // 0 to indicate a good connection
    return 0;
  } catch (const std::exception & e) {
    ReportException(e);
    return ENODEV;
  } catch (...) {
    ReportCaughtNonException();
    return ENODEV;
  }

  return 0;
}

LIBHDFS_C_API
hdfsFS hdfsConnect(const char *nn, tPort port) {
  return hdfsConnectAsUser(nn, port, "");
}

LIBHDFS_C_API
hdfsFS hdfsConnectAsUser(const char* nn, tPort port, const char *user) {
  return doHdfsConnect(std::string(nn), port, std::string(user), Options());
}

LIBHDFS_C_API
hdfsFS hdfsConnectAsUserNewInstance(const char* nn, tPort port, const char *user ) {
  //libhdfspp always returns a new instance
  return doHdfsConnect(std::string(nn), port, std::string(user), Options());
}

LIBHDFS_C_API
hdfsFS hdfsConnectNewInstance(const char* nn, tPort port) {
  //libhdfspp always returns a new instance
  return hdfsConnectAsUser(nn, port, "");
}

LIBHDFSPP_EXT_API
int hdfsCancelPendingConnection(hdfsFS fs) {
  // todo: stick an enum in hdfs_internal to check the connect state
  if(!CheckSystem(fs)) {
    return ENODEV;
  }

  FileSystem *fsImpl = fs->get_impl();
  if(!fsImpl) {
    ReportError(ENODEV, "Null FileSystem implementation");
    return ENODEV;
  }

  bool canceled = fsImpl->CancelPendingConnect();
  if(canceled) {
    return 0;
  } else {
    return EINTR;
  }
}

LIBHDFS_C_API
int hdfsDisconnect(hdfsFS fs) {
  try
  {
    errno = 0;
    if (!fs) {
      ReportError(ENODEV, "Cannot disconnect null FS handle.");
      return -1;
    }

    delete fs;
    return 0;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

LIBHDFS_C_API
hdfsFile hdfsOpenFile(hdfsFS fs, const char *path, int flags, int bufferSize,
                      short replication, tSize blocksize) {
  try
  {
    errno = 0;
    (void)flags;
    (void)bufferSize;
    (void)replication;
    (void)blocksize;
    if (!fs) {
      ReportError(ENODEV, "Cannot perform FS operations with null FS handle.");
      return nullptr;
    }
    const optional<std::string> abs_path = getAbsolutePath(fs, path);
    if(!abs_path) {
      return nullptr;
    }
    FileHandle *f = nullptr;
    Status stat = fs->get_impl()->Open(*abs_path, &f);
    if (!stat.ok()) {
      Error(stat);
      return nullptr;
    }
    if (f && fileEventCallback) {
      f->SetFileEventCallback(fileEventCallback.value());
    }
    return new hdfsFile_internal(f);
  } catch (const std::exception & e) {
    ReportException(e);
    return nullptr;
  } catch (...) {
    ReportCaughtNonException();
    return nullptr;
  }
}

LIBHDFS_C_API
int hdfsCloseFile(hdfsFS fs, hdfsFile file) {
  try
  {
    errno = 0;
    if (!CheckSystemAndHandle(fs, file)) {
      return -1;
    }
    delete file;
    return 0;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

LIBHDFS_C_API
char* hdfsGetWorkingDirectory(hdfsFS fs, char *buffer, size_t bufferSize) {
  try
  {
    errno = 0;
    if (!CheckSystem(fs)) {
      return nullptr;
    }
    std::string wd = fs->get_working_directory();
    size_t size = wd.size();
    if (size + 1 > bufferSize) {
      std::stringstream ss;
      ss << "hdfsGetWorkingDirectory: bufferSize is " << bufferSize <<
          ", which is not enough to fit working directory of size " << (size + 1);
      Error(Status::InvalidArgument(ss.str().c_str()));
      return nullptr;
    }
    wd.copy(buffer, size);
    buffer[size] = '\0';
    return buffer;
  } catch (const std::exception & e) {
    ReportException(e);
    return nullptr;
  } catch (...) {
    ReportCaughtNonException();
    return nullptr;
  }
}

LIBHDFS_C_API
int hdfsSetWorkingDirectory(hdfsFS fs, const char* path) {
  try
  {
    errno = 0;
    if (!CheckSystem(fs)) {
      return -1;
    }
    optional<std::string> abs_path = getAbsolutePath(fs, path);
    if(!abs_path) {
      return -1;
    }
    //Enforce last character to be '/'
    std::string withSlash = *abs_path;
    char last = withSlash.back();
    if (last != '/'){
      withSlash += '/';
    }
    fs->set_working_directory(withSlash);
    return 0;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

LIBHDFS_C_API
int hdfsAvailable(hdfsFS fs, hdfsFile file) {
  //Since we do not have read ahead implemented, return 0 if fs and file are good;
  errno = 0;
  if (!CheckSystemAndHandle(fs, file)) {
    return -1;
  }
  return 0;
}

LIBHDFS_C_API
tOffset hdfsGetDefaultBlockSize(hdfsFS fs) {
  try {
    errno = 0;
    return fs->get_impl()->get_options().block_size;
  } catch (const std::exception & e) {
    ReportException(e);
    return -1;
  } catch (...) {
    ReportCaughtNonException();
    return -1;
  }
}

LIBHDFS_C_API
tOffset hdfsGetDefaultBlockSizeAtPath(hdfsFS fs, const char *path) {
  try {
    errno = 0;
    if (!CheckSystem(fs)) {
      return -1;
    }
    const optional<std::string> abs_path = getAbsolutePath(fs, path);
    if(!abs_path) {
      return -1;
    }
    uint64_t block_size;
    Status stat = fs->get_impl()->GetPreferredBlockSize(*abs_path, block_size);
    if (!stat.ok()) {
      if (stat.pathNotFound()){
        return fs->get_impl()->get_options().block_size;
      } else {
        return Error(stat);
      }
    }
    return block_size;
  } catch (const std::exception & e) {
    ReportException(e);
    return -1;
  } catch (...) {
    ReportCaughtNonException();
    return -1;
  }
}

LIBHDFS_C_API
int hdfsSetReplication(hdfsFS fs, const char* path, int16_t replication) {
    try {
      errno = 0;
      if (!CheckSystem(fs)) {
        return -1;
      }
      const optional<std::string> abs_path = getAbsolutePath(fs, path);
      if(!abs_path) {
        return -1;
      }
      if(replication < 1){
        return Error(Status::InvalidArgument("SetReplication: argument 'replication' cannot be less than 1"));
      }
      Status stat;
      stat = fs->get_impl()->SetReplication(*abs_path, replication);
      if (!stat.ok()) {
        return Error(stat);
      }
      return 0;
    } catch (const std::exception & e) {
      return ReportException(e);
    } catch (...) {
      return ReportCaughtNonException();
    }
}

LIBHDFS_C_API
int hdfsUtime(hdfsFS fs, const char* path, tTime mtime, tTime atime) {
    try {
      errno = 0;
      if (!CheckSystem(fs)) {
        return -1;
      }
      const optional<std::string> abs_path = getAbsolutePath(fs, path);
      if(!abs_path) {
        return -1;
      }
      Status stat;
      stat = fs->get_impl()->SetTimes(*abs_path, mtime, atime);
      if (!stat.ok()) {
        return Error(stat);
      }
      return 0;
    } catch (const std::exception & e) {
      return ReportException(e);
    } catch (...) {
      return ReportCaughtNonException();
    }
}

LIBHDFS_C_API
tOffset hdfsGetCapacity(hdfsFS fs) {
  try {
    errno = 0;
    if (!CheckSystem(fs)) {
      return -1;
    }

    hdfs::FsInfo fs_info;
    Status stat = fs->get_impl()->GetFsStats(fs_info);
    if (!stat.ok()) {
      Error(stat);
      return -1;
    }
    return fs_info.capacity;
  } catch (const std::exception & e) {
    ReportException(e);
    return -1;
  } catch (...) {
    ReportCaughtNonException();
    return -1;
  }
}

LIBHDFS_C_API
tOffset hdfsGetUsed(hdfsFS fs) {
  try {
    errno = 0;
    if (!CheckSystem(fs)) {
      return -1;
    }

    hdfs::FsInfo fs_info;
    Status stat = fs->get_impl()->GetFsStats(fs_info);
    if (!stat.ok()) {
      Error(stat);
      return -1;
    }
    return fs_info.used;
  } catch (const std::exception & e) {
    ReportException(e);
    return -1;
  } catch (...) {
    ReportCaughtNonException();
    return -1;
  }
}

void StatInfoToHdfsFileInfo(hdfsFileInfo * file_info,
                            const hdfs::StatInfo & stat_info) {
  /* file or directory */
  if (stat_info.file_type == StatInfo::IS_DIR) {
    file_info->mKind = kObjectKindDirectory;
  } else if (stat_info.file_type == StatInfo::IS_FILE) {
    file_info->mKind = kObjectKindFile;
  } else {
    file_info->mKind = kObjectKindFile;
    LOG_WARN(kFileSystem, << "Symlink is not supported! Reporting as a file: ");
  }

  /* the name of the file */
  char copyOfPath[PATH_MAX];
  strncpy(copyOfPath, stat_info.path.c_str(), PATH_MAX);
  copyOfPath[PATH_MAX - 1] = '\0'; // in case strncpy ran out of space

  char * mName = basename(copyOfPath);
  size_t mName_size = strlen(mName);
  file_info->mName = new char[mName_size+1];
  strncpy(file_info->mName, basename(copyOfPath), mName_size + 1);

  /* the last modification time for the file in seconds */
  file_info->mLastMod = (tTime) stat_info.modification_time;

  /* the size of the file in bytes */
  file_info->mSize = (tOffset) stat_info.length;

  /* the count of replicas */
  file_info->mReplication = (short) stat_info.block_replication;

  /* the block size for the file */
  file_info->mBlockSize = (tOffset) stat_info.blocksize;

  /* the owner of the file */
  file_info->mOwner = new char[stat_info.owner.size() + 1];
  strncpy(file_info->mOwner, stat_info.owner.c_str(), stat_info.owner.size() + 1);

  /* the group associated with the file */
  file_info->mGroup = new char[stat_info.group.size() + 1];
  strncpy(file_info->mGroup, stat_info.group.c_str(), stat_info.group.size() + 1);

  /* the permissions associated with the file encoded as an octal number (0777)*/
  file_info->mPermissions = (short) stat_info.permissions;

  /* the last access time for the file in seconds since the epoch*/
  file_info->mLastAccess = stat_info.access_time;
}

LIBHDFS_C_API
int hdfsExists(hdfsFS fs, const char *path) {
  try {
    errno = 0;
    if (!CheckSystem(fs)) {
      return -1;
    }
    const optional<std::string> abs_path = getAbsolutePath(fs, path);
    if(!abs_path) {
      return -1;
    }
    hdfs::StatInfo stat_info;
    Status stat = fs->get_impl()->GetFileInfo(*abs_path, stat_info);
    if (!stat.ok()) {
      return Error(stat);
    }
    return 0;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

LIBHDFS_C_API
hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, const char* path) {
  try {
    errno = 0;
    if (!CheckSystem(fs)) {
       return nullptr;
    }
    const optional<std::string> abs_path = getAbsolutePath(fs, path);
    if(!abs_path) {
      return nullptr;
    }
    hdfs::StatInfo stat_info;
    Status stat = fs->get_impl()->GetFileInfo(*abs_path, stat_info);
    if (!stat.ok()) {
      Error(stat);
      return nullptr;
    }
    hdfsFileInfo *file_info = new hdfsFileInfo[1];
    StatInfoToHdfsFileInfo(file_info, stat_info);
    return file_info;
  } catch (const std::exception & e) {
    ReportException(e);
    return nullptr;
  } catch (...) {
    ReportCaughtNonException();
    return nullptr;
  }
}

LIBHDFS_C_API
hdfsFileInfo *hdfsListDirectory(hdfsFS fs, const char* path, int *numEntries) {
  try {
      errno = 0;
      if (!CheckSystem(fs)) {
        *numEntries = 0;
        return nullptr;
      }
      const optional<std::string> abs_path = getAbsolutePath(fs, path);
      if(!abs_path) {
        return nullptr;
      }
      std::vector<StatInfo> stat_infos;
      Status stat = fs->get_impl()->GetListing(*abs_path, &stat_infos);
      if (!stat.ok()) {
        Error(stat);
        *numEntries = 0;
        return nullptr;
      }
      if(stat_infos.empty()){
        *numEntries = 0;
        return nullptr;
      }
      *numEntries = stat_infos.size();
      hdfsFileInfo *file_infos = new hdfsFileInfo[stat_infos.size()];
      for(std::vector<StatInfo>::size_type i = 0; i < stat_infos.size(); i++) {
        StatInfoToHdfsFileInfo(&file_infos[i], stat_infos.at(i));
      }

      return file_infos;
    } catch (const std::exception & e) {
      ReportException(e);
      *numEntries = 0;
      return nullptr;
    } catch (...) {
      ReportCaughtNonException();
      *numEntries = 0;
      return nullptr;
    }
}

LIBHDFS_C_API
void hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries)
{
    errno = 0;
    int i;
    for (i = 0; i < numEntries; ++i) {
        delete[] hdfsFileInfo[i].mName;
        delete[] hdfsFileInfo[i].mOwner;
        delete[] hdfsFileInfo[i].mGroup;
    }
    delete[] hdfsFileInfo;
}

LIBHDFS_C_API
int hdfsCreateDirectory(hdfsFS fs, const char* path) {
  try {
    errno = 0;
    if (!CheckSystem(fs)) {
      return -1;
    }
    const optional<std::string> abs_path = getAbsolutePath(fs, path);
    if(!abs_path) {
      return -1;
    }
    Status stat;
    //Use default permissions and set true for creating all non-existant parent directories
    stat = fs->get_impl()->Mkdirs(*abs_path, FileSystem::GetDefaultPermissionMask(), true);
    if (!stat.ok()) {
      return Error(stat);
    }
    return 0;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

LIBHDFS_C_API
int hdfsDelete(hdfsFS fs, const char* path, int recursive) {
  try {
      errno = 0;
      if (!CheckSystem(fs)) {
        return -1;
      }
      const optional<std::string> abs_path = getAbsolutePath(fs, path);
      if(!abs_path) {
        return -1;
      }
      Status stat;
      stat = fs->get_impl()->Delete(*abs_path, recursive);
      if (!stat.ok()) {
        return Error(stat);
      }
      return 0;
    } catch (const std::exception & e) {
      return ReportException(e);
    } catch (...) {
      return ReportCaughtNonException();
    }
}

LIBHDFS_C_API
int hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath) {
  try {
    errno = 0;
    if (!CheckSystem(fs)) {
      return -1;
    }
    const optional<std::string> old_abs_path = getAbsolutePath(fs, oldPath);
    const optional<std::string> new_abs_path = getAbsolutePath(fs, newPath);
    if(!old_abs_path || !new_abs_path) {
      return -1;
    }
    Status stat;
    stat = fs->get_impl()->Rename(*old_abs_path, *new_abs_path);
    if (!stat.ok()) {
      return Error(stat);
    }
    return 0;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

LIBHDFS_C_API
int hdfsChmod(hdfsFS fs, const char* path, short mode){
  try {
      errno = 0;
      if (!CheckSystem(fs)) {
        return -1;
      }
      const optional<std::string> abs_path = getAbsolutePath(fs, path);
      if(!abs_path) {
        return -1;
      }
      Status stat = FileSystem::CheckValidPermissionMask(mode);
      if (!stat.ok()) {
        return Error(stat);
      }
      stat = fs->get_impl()->SetPermission(*abs_path, mode);
      if (!stat.ok()) {
        return Error(stat);
      }
      return 0;
    } catch (const std::exception & e) {
      return ReportException(e);
    } catch (...) {
      return ReportCaughtNonException();
    }
}

LIBHDFS_C_API
int hdfsChown(hdfsFS fs, const char* path, const char *owner, const char *group){
  try {
      errno = 0;
      if (!CheckSystem(fs)) {
        return -1;
      }
      const optional<std::string> abs_path = getAbsolutePath(fs, path);
      if(!abs_path) {
        return -1;
      }
      std::string own = (owner) ? owner : "";
      std::string grp = (group) ? group : "";

      Status stat;
      stat = fs->get_impl()->SetOwner(*abs_path, own, grp);
      if (!stat.ok()) {
        return Error(stat);
      }
      return 0;
    } catch (const std::exception & e) {
      return ReportException(e);
    } catch (...) {
      return ReportCaughtNonException();
    }
}

LIBHDFSPP_EXT_API
hdfsFileInfo * hdfsFind(hdfsFS fs, const char* path, const char* name, uint32_t * numEntries){
  try {
      errno = 0;
      if (!CheckSystem(fs)) {
        *numEntries = 0;
        return nullptr;
      }

      std::vector<StatInfo>  stat_infos;
      Status stat = fs->get_impl()->Find(path, name, hdfs::FileSystem::GetDefaultFindMaxDepth(), &stat_infos);
      if (!stat.ok()) {
        Error(stat);
        *numEntries = 0;
        return nullptr;
      }
      //Existing API expects nullptr if size is 0
      if(stat_infos.empty()){
        *numEntries = 0;
        return nullptr;
      }
      *numEntries = stat_infos.size();
      hdfsFileInfo *file_infos = new hdfsFileInfo[stat_infos.size()];
      for(std::vector<StatInfo>::size_type i = 0; i < stat_infos.size(); i++) {
        StatInfoToHdfsFileInfo(&file_infos[i], stat_infos.at(i));
      }

      return file_infos;
    } catch (const std::exception & e) {
      ReportException(e);
      *numEntries = 0;
      return nullptr;
    } catch (...) {
      ReportCaughtNonException();
      *numEntries = 0;
      return nullptr;
    }
}

LIBHDFSPP_EXT_API
int hdfsCreateSnapshot(hdfsFS fs, const char* path, const char* name) {
  try {
    errno = 0;
    if (!CheckSystem(fs)) {
      return -1;
    }
    const optional<std::string> abs_path = getAbsolutePath(fs, path);
    if(!abs_path) {
      return -1;
    }
    Status stat;
    if(!name){
      stat = fs->get_impl()->CreateSnapshot(*abs_path, "");
    } else {
      stat = fs->get_impl()->CreateSnapshot(*abs_path, name);
    }
    if (!stat.ok()) {
      return Error(stat);
    }
    return 0;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

LIBHDFSPP_EXT_API
int hdfsDeleteSnapshot(hdfsFS fs, const char* path, const char* name) {
  try {
    errno = 0;
    if (!CheckSystem(fs)) {
      return -1;
    }
    const optional<std::string> abs_path = getAbsolutePath(fs, path);
    if(!abs_path) {
      return -1;
    }
    if (!name) {
      return Error(Status::InvalidArgument("hdfsDeleteSnapshot: argument 'name' cannot be NULL"));
    }
    Status stat;
    stat = fs->get_impl()->DeleteSnapshot(*abs_path, name);
    if (!stat.ok()) {
      return Error(stat);
    }
    return 0;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}


int hdfsRenameSnapshot(hdfsFS fs, const char* path, const char* old_name, const char* new_name) {
  try {
    errno = 0;
    if (!CheckSystem(fs)) {
      return -1;
    }
    const optional<std::string> abs_path = getAbsolutePath(fs, path);
    if(!abs_path) {
      return -1;
    }
    if (!old_name) {
      return Error(Status::InvalidArgument("hdfsRenameSnapshot: argument 'old_name' cannot be NULL"));
    }
    if (!new_name) {
      return Error(Status::InvalidArgument("hdfsRenameSnapshot: argument 'new_name' cannot be NULL"));
    }
    Status stat;
    stat = fs->get_impl()->RenameSnapshot(*abs_path, old_name, new_name);
    if (!stat.ok()) {
      return Error(stat);
    }
    return 0;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }

}

LIBHDFSPP_EXT_API
int hdfsAllowSnapshot(hdfsFS fs, const char* path) {
  try {
    errno = 0;
    if (!CheckSystem(fs)) {
      return -1;
    }
    const optional<std::string> abs_path = getAbsolutePath(fs, path);
    if(!abs_path) {
      return -1;
    }
    Status stat;
    stat = fs->get_impl()->AllowSnapshot(*abs_path);
    if (!stat.ok()) {
      return Error(stat);
    }
    return 0;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

LIBHDFSPP_EXT_API
int hdfsDisallowSnapshot(hdfsFS fs, const char* path) {
  try {
    errno = 0;
    if (!CheckSystem(fs)) {
      return -1;
    }
    const optional<std::string> abs_path = getAbsolutePath(fs, path);
    if(!abs_path) {
      return -1;
    }
    Status stat;
    stat = fs->get_impl()->DisallowSnapshot(*abs_path);
    if (!stat.ok()) {
      return Error(stat);
    }
    return 0;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

LIBHDFS_C_API
tSize hdfsPread(hdfsFS fs, hdfsFile file, tOffset position, void *buffer,
                tSize length) {
  try
  {
    errno = 0;
    if (!CheckSystemAndHandle(fs, file)) {
      return -1;
    }

    size_t len = 0;
    Status stat = file->get_impl()->PositionRead(buffer, length, position, &len);
    if(!stat.ok()) {
      return Error(stat);
    }
    return (tSize)len;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

LIBHDFS_C_API
tSize hdfsRead(hdfsFS fs, hdfsFile file, void *buffer, tSize length) {
  try
  {
    errno = 0;
    if (!CheckSystemAndHandle(fs, file)) {
      return -1;
    }

    size_t len = 0;
    Status stat = file->get_impl()->Read(buffer, length, &len);
    if (!stat.ok()) {
      return Error(stat);
    }

    return (tSize)len;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

LIBHDFS_C_API
int hdfsUnbufferFile(hdfsFile file) {
  //Currently we are not doing any buffering
  CheckHandle(file);
  return -1;
}

LIBHDFS_C_API
int hdfsFileGetReadStatistics(hdfsFile file, struct hdfsReadStatistics **stats) {
  try
    {
      errno = 0;
      if (!CheckHandle(file)) {
        return -1;
      }
      *stats = new hdfsReadStatistics;
      memset(*stats, 0, sizeof(hdfsReadStatistics));
      (*stats)->totalBytesRead = file->get_impl()->get_bytes_read();
      return 0;
    } catch (const std::exception & e) {
      return ReportException(e);
    } catch (...) {
      return ReportCaughtNonException();
    }
}

LIBHDFS_C_API
int hdfsFileClearReadStatistics(hdfsFile file) {
  try
    {
      errno = 0;
      if (!CheckHandle(file)) {
        return -1;
      }
      file->get_impl()->clear_bytes_read();
      return 0;
    } catch (const std::exception & e) {
      return ReportException(e);
    } catch (...) {
      return ReportCaughtNonException();
    }
}

LIBHDFS_C_API
int64_t hdfsReadStatisticsGetRemoteBytesRead(const struct hdfsReadStatistics *stats) {
    return stats->totalBytesRead - stats->totalLocalBytesRead;
}

LIBHDFS_C_API
void hdfsFileFreeReadStatistics(struct hdfsReadStatistics *stats) {
    errno = 0;
    delete stats;
}

/* 0 on success, -1 on error*/
LIBHDFS_C_API
int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos) {
  try
  {
    errno = 0;
    if (!CheckSystemAndHandle(fs, file)) {
      return -1;
    }

    off_t desired = desiredPos;
    Status stat = file->get_impl()->Seek(&desired, std::ios_base::beg);
    if (!stat.ok()) {
      return Error(stat);
    }

    return 0;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

LIBHDFS_C_API
tOffset hdfsTell(hdfsFS fs, hdfsFile file) {
  try
  {
    errno = 0;
    if (!CheckSystemAndHandle(fs, file)) {
      return -1;
    }

    off_t offset = 0;
    Status stat = file->get_impl()->Seek(&offset, std::ios_base::cur);
    if (!stat.ok()) {
      return Error(stat);
    }

    return (tOffset)offset;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

/* extended API */
int hdfsCancel(hdfsFS fs, hdfsFile file) {
  try
  {
    errno = 0;
    if (!CheckSystemAndHandle(fs, file)) {
      return -1;
    }
    static_cast<FileHandleImpl*>(file->get_impl())->CancelOperations();
    return 0;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

LIBHDFSPP_EXT_API
int hdfsGetBlockLocations(hdfsFS fs, const char *path, struct hdfsBlockLocations ** locations_out)
{
  try
  {
    errno = 0;
    if (!CheckSystem(fs)) {
      return -1;
    }
    if (locations_out == nullptr) {
      ReportError(EINVAL, "Null pointer passed to hdfsGetBlockLocations");
      return -1;
    }
    const optional<std::string> abs_path = getAbsolutePath(fs, path);
    if(!abs_path) {
      return -1;
    }
    std::shared_ptr<FileBlockLocation> ppLocations;
    Status stat = fs->get_impl()->GetBlockLocations(*abs_path, 0, std::numeric_limits<int64_t>::max(), &ppLocations);
    if (!stat.ok()) {
      return Error(stat);
    }

    hdfsBlockLocations *locations = new struct hdfsBlockLocations();
    (*locations_out) = locations;

    bzero(locations, sizeof(*locations));
    locations->fileLength = ppLocations->getFileLength();
    locations->isLastBlockComplete = ppLocations->isLastBlockComplete();
    locations->isUnderConstruction = ppLocations->isUnderConstruction();

    const std::vector<BlockLocation> & ppBlockLocations = ppLocations->getBlockLocations();
    locations->num_blocks = ppBlockLocations.size();
    locations->blocks = new struct hdfsBlockInfo[locations->num_blocks];
    for (size_t i=0; i < ppBlockLocations.size(); i++) {
      auto ppBlockLocation = ppBlockLocations[i];
      auto block = &locations->blocks[i];

      block->num_bytes = ppBlockLocation.getLength();
      block->start_offset = ppBlockLocation.getOffset();

      const std::vector<DNInfo> & ppDNInfos = ppBlockLocation.getDataNodes();
      block->num_locations = ppDNInfos.size();
      block->locations = new hdfsDNInfo[block->num_locations];
      for (size_t j=0; j < block->num_locations; j++) {
        auto ppDNInfo = ppDNInfos[j];
        auto dn_info = &block->locations[j];

        dn_info->xfer_port = ppDNInfo.getXferPort();
        dn_info->info_port = ppDNInfo.getInfoPort();
        dn_info->IPC_port  = ppDNInfo.getIPCPort();
        dn_info->info_secure_port = ppDNInfo.getInfoSecurePort();

        char * buf;
        buf = new char[ppDNInfo.getHostname().size() + 1];
        strncpy(buf, ppDNInfo.getHostname().c_str(), ppDNInfo.getHostname().size() + 1);
        dn_info->hostname = buf;

        buf = new char[ppDNInfo.getIPAddr().size() + 1];
        strncpy(buf, ppDNInfo.getIPAddr().c_str(), ppDNInfo.getIPAddr().size() + 1);
        dn_info->ip_address = buf;

        buf = new char[ppDNInfo.getNetworkLocation().size() + 1];
        strncpy(buf, ppDNInfo.getNetworkLocation().c_str(), ppDNInfo.getNetworkLocation().size() + 1);
        dn_info->network_location = buf;
      }
    }

    return 0;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

LIBHDFSPP_EXT_API
int hdfsFreeBlockLocations(struct hdfsBlockLocations * blockLocations) {
  errno = 0;
  if (blockLocations == nullptr)
    return 0;

  for (size_t i=0; i < blockLocations->num_blocks; i++) {
    auto block = &blockLocations->blocks[i];
    for (size_t j=0; j < block->num_locations; j++) {
      auto location = &block->locations[j];
      delete[] location->hostname;
      delete[] location->ip_address;
      delete[] location->network_location;
    }
  }
  delete[] blockLocations->blocks;
  delete blockLocations;

  return 0;
}

LIBHDFS_C_API
char*** hdfsGetHosts(hdfsFS fs, const char* path, tOffset start, tOffset length) {
  try
    {
      errno = 0;
      if (!CheckSystem(fs)) {
        return nullptr;
      }
      const optional<std::string> abs_path = getAbsolutePath(fs, path);
      if(!abs_path) {
        return nullptr;
      }
      std::shared_ptr<FileBlockLocation> ppLocations;
      Status stat = fs->get_impl()->GetBlockLocations(*abs_path, start, length, &ppLocations);
      if (!stat.ok()) {
        Error(stat);
        return nullptr;
      }
      const std::vector<BlockLocation> & ppBlockLocations = ppLocations->getBlockLocations();
      char ***hosts = new char**[ppBlockLocations.size() + 1];
      for (size_t i=0; i < ppBlockLocations.size(); i++) {
        const std::vector<DNInfo> & ppDNInfos = ppBlockLocations[i].getDataNodes();
        hosts[i] = new char*[ppDNInfos.size() + 1];
        for (size_t j=0; j < ppDNInfos.size(); j++) {
          auto ppDNInfo = ppDNInfos[j];
          hosts[i][j] = new char[ppDNInfo.getHostname().size() + 1];
          strncpy(hosts[i][j], ppDNInfo.getHostname().c_str(), ppDNInfo.getHostname().size() + 1);
        }
        hosts[i][ppDNInfos.size()] = nullptr;
      }
      hosts[ppBlockLocations.size()] = nullptr;
      return hosts;
    } catch (const std::exception & e) {
      ReportException(e);
      return nullptr;
    } catch (...) {
      ReportCaughtNonException();
      return nullptr;
    }
}

LIBHDFS_C_API
void hdfsFreeHosts(char ***blockHosts) {
  errno = 0;
  if (blockHosts == nullptr)
    return;

  for (size_t i = 0; blockHosts[i]; i++) {
    for (size_t j = 0; blockHosts[i][j]; j++) {
      delete[] blockHosts[i][j];
    }
    delete[] blockHosts[i];
  }
  delete blockHosts;
}

/*******************************************************************
 *                EVENT CALLBACKS
 *******************************************************************/

const char * FS_NN_CONNECT_EVENT = hdfs::FS_NN_CONNECT_EVENT;
const char * FS_NN_READ_EVENT = hdfs::FS_NN_READ_EVENT;
const char * FS_NN_WRITE_EVENT = hdfs::FS_NN_WRITE_EVENT;

const char * FILE_DN_CONNECT_EVENT = hdfs::FILE_DN_CONNECT_EVENT;
const char * FILE_DN_READ_EVENT = hdfs::FILE_DN_READ_EVENT;
const char * FILE_DN_WRITE_EVENT = hdfs::FILE_DN_WRITE_EVENT;


event_response fs_callback_glue(libhdfspp_fs_event_callback handler,
                      int64_t cookie,
                      const char * event,
                      const char * cluster,
                      int64_t value) {
  int result = handler(event, cluster, value, cookie);
  if (result == LIBHDFSPP_EVENT_OK) {
    return event_response::make_ok();
  }
#ifndef LIBHDFSPP_SIMULATE_ERROR_DISABLED
  if (result == DEBUG_SIMULATE_ERROR) {
    return event_response::test_err(Status::Error("Simulated error"));
  }
#endif

  return event_response::make_ok();
}

event_response file_callback_glue(libhdfspp_file_event_callback handler,
                      int64_t cookie,
                      const char * event,
                      const char * cluster,
                      const char * file,
                      int64_t value) {
  int result = handler(event, cluster, file, value, cookie);
  if (result == LIBHDFSPP_EVENT_OK) {
    return event_response::make_ok();
  }
#ifndef LIBHDFSPP_SIMULATE_ERROR_DISABLED
  if (result == DEBUG_SIMULATE_ERROR) {
    return event_response::test_err(Status::Error("Simulated error"));
  }
#endif

  return event_response::make_ok();
}

LIBHDFSPP_EXT_API
int hdfsPreAttachFSMonitor(libhdfspp_fs_event_callback handler, int64_t cookie)
{
  fs_event_callback callback = std::bind(fs_callback_glue, handler, cookie, _1, _2, _3);
  fsEventCallback = callback;
  return 0;
}

LIBHDFSPP_EXT_API
int hdfsPreAttachFileMonitor(libhdfspp_file_event_callback handler, int64_t cookie)
{
  file_event_callback callback = std::bind(file_callback_glue, handler, cookie, _1, _2, _3, _4);
  fileEventCallback = callback;
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
    return loader.NewConfig<HdfsConfiguration>();
  }
}

hdfsBuilder::hdfsBuilder() : config(loader.NewConfig<HdfsConfiguration>())
{
  errno = 0;
  config = LoadDefault(loader);
}

hdfsBuilder::hdfsBuilder(const char * directory) :
      config(loader.NewConfig<HdfsConfiguration>())
{
  errno = 0;
  loader.SetSearchPath(directory);
  config = LoadDefault(loader);
}

LIBHDFS_C_API
struct hdfsBuilder *hdfsNewBuilder(void)
{
  try
  {
    errno = 0;
    return new struct hdfsBuilder();
  } catch (const std::exception & e) {
    ReportException(e);
    return nullptr;
  } catch (...) {
    ReportCaughtNonException();
    return nullptr;
  }
}

LIBHDFS_C_API
void hdfsBuilderSetNameNode(struct hdfsBuilder *bld, const char *nn)
{
  errno = 0;
  bld->overrideHost = std::string(nn);
}

LIBHDFS_C_API
void hdfsBuilderSetNameNodePort(struct hdfsBuilder *bld, tPort port)
{
  errno = 0;
  bld->overridePort = port;
}

LIBHDFS_C_API
void hdfsBuilderSetUserName(struct hdfsBuilder *bld, const char *userName)
{
  errno = 0;
  if (userName && *userName) {
    bld->user = std::string(userName);
  }
}

LIBHDFS_C_API
void hdfsBuilderSetForceNewInstance(struct hdfsBuilder *bld) {
  //libhdfspp always returns a new instance, so nothing to do
  (void)bld;
  errno = 0;
}

LIBHDFS_C_API
void hdfsFreeBuilder(struct hdfsBuilder *bld)
{
  try
  {
    errno = 0;
    delete bld;
  } catch (const std::exception & e) {
    ReportException(e);
  } catch (...) {
    ReportCaughtNonException();
  }
}

LIBHDFS_C_API
int hdfsBuilderConfSetStr(struct hdfsBuilder *bld, const char *key,
                          const char *val)
{
  try
  {
    errno = 0;
    optional<HdfsConfiguration> newConfig = bld->loader.OverlayValue(bld->config, key, val);
    if (newConfig)
    {
      bld->config = newConfig.value();
      return 0;
    }
    else
    {
      ReportError(EINVAL, "Could not change Builder value");
      return -1;
    }
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

LIBHDFS_C_API
void hdfsConfStrFree(char *val)
{
  errno = 0;
  free(val);
}

LIBHDFS_C_API
hdfsFS hdfsBuilderConnect(struct hdfsBuilder *bld) {
  hdfsFS fs = doHdfsConnect(bld->overrideHost, bld->overridePort, bld->user, bld->config.GetOptions());
  // Always free the builder
  hdfsFreeBuilder(bld);
  return fs;
}

LIBHDFS_C_API
int hdfsConfGetStr(const char *key, char **val)
{
  try
  {
    errno = 0;
    hdfsBuilder builder;
    return hdfsBuilderConfGetStr(&builder, key, val);
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

LIBHDFS_C_API
int hdfsConfGetInt(const char *key, int32_t *val)
{
  try
  {
    errno = 0;
    hdfsBuilder builder;
    return hdfsBuilderConfGetInt(&builder, key, val);
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

//
//  Extended builder interface
//
struct hdfsBuilder *hdfsNewBuilderFromDirectory(const char * configDirectory)
{
  try
  {
    errno = 0;
    return new struct hdfsBuilder(configDirectory);
  } catch (const std::exception & e) {
    ReportException(e);
    return nullptr;
  } catch (...) {
    ReportCaughtNonException();
    return nullptr;
  }
}

LIBHDFSPP_EXT_API
int hdfsBuilderConfGetStr(struct hdfsBuilder *bld, const char *key,
                          char **val)
{
  try
  {
    errno = 0;
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
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

// If we're running on a 32-bit platform, we might get 64-bit values that
//    don't fit in an int, and int is specified by the java hdfs.h interface
bool isValidInt(int64_t value)
{
  return (value >= std::numeric_limits<int>::min() &&
          value <= std::numeric_limits<int>::max());
}

LIBHDFSPP_EXT_API
int hdfsBuilderConfGetInt(struct hdfsBuilder *bld, const char *key, int32_t *val)
{
  try
  {
    errno = 0;
    // Pull from default configuration
    optional<int64_t> value = bld->config.GetInt(key);
    if (value)
    {
      if (!isValidInt(*value)){
        ReportError(EINVAL, "Builder value is not valid");
        return -1;
      }
      *val = *value;
      return 0;
    }
    // If not found, don't change val
    ReportError(EINVAL, "Could not get Builder value");
    return 0;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

LIBHDFSPP_EXT_API
int hdfsBuilderConfGetLong(struct hdfsBuilder *bld, const char *key, int64_t *val)
{
  try
  {
    errno = 0;
    // Pull from default configuration
    optional<int64_t> value = bld->config.GetInt(key);
    if (value)
    {
      *val = *value;
      return 0;
    }
    // If not found, don't change val
    ReportError(EINVAL, "Could not get Builder value");
    return 0;
  } catch (const std::exception & e) {
    return ReportException(e);
  } catch (...) {
    return ReportCaughtNonException();
  }
}

/**
 * Logging functions
 **/
class CForwardingLogger : public LoggerInterface {
 public:
  CForwardingLogger() : callback_(nullptr) {};

  // Converts LogMessage into LogData, a POD type,
  // and invokes callback_ if it's not null.
  void Write(const LogMessage& msg);

  // pass in NULL to clear the hook
  void SetCallback(void (*callback)(LogData*));

  //return a copy, or null on failure.
  static LogData *CopyLogData(const LogData*);
  //free LogData allocated with CopyLogData
  static void FreeLogData(LogData*);
 private:
  void (*callback_)(LogData*);
};

/**
 *  Plugin to forward message to a C function pointer
 **/
void CForwardingLogger::Write(const LogMessage& msg) {
  if(!callback_)
    return;

  const std::string text = msg.MsgString();

  LogData data;
  data.level = msg.level();
  data.component = msg.component();
  data.msg = text.c_str();
  data.file_name = msg.file_name();
  data.file_line = msg.file_line();
  callback_(&data);
}

void CForwardingLogger::SetCallback(void (*callback)(LogData*)) {
  callback_ = callback;
}

LogData *CForwardingLogger::CopyLogData(const LogData *orig) {
  if(!orig)
    return nullptr;

  LogData *copy = (LogData*)malloc(sizeof(LogData));
  if(!copy)
    return nullptr;

  copy->level = orig->level;
  copy->component = orig->component;
  if(orig->msg)
    copy->msg = strdup(orig->msg);
  copy->file_name = orig->file_name;
  copy->file_line = orig->file_line;
  return copy;
}

void CForwardingLogger::FreeLogData(LogData *data) {
  if(!data)
    return;
  if(data->msg)
    free((void*)data->msg);

  // Inexpensive way to help catch use-after-free
  memset(data, 0, sizeof(LogData));
  free(data);
}

LIBHDFSPP_EXT_API
LogData *hdfsCopyLogData(LogData *data) {
  return CForwardingLogger::CopyLogData(data);
}

LIBHDFSPP_EXT_API
void hdfsFreeLogData(LogData *data) {
  CForwardingLogger::FreeLogData(data);
}

LIBHDFSPP_EXT_API
void hdfsSetLogFunction(void (*callback)(LogData*)) {
  CForwardingLogger *logger = new CForwardingLogger();
  logger->SetCallback(callback);
  LogManager::SetLoggerImplementation(std::unique_ptr<LoggerInterface>(logger));
}

static bool IsLevelValid(int component) {
  if(component < HDFSPP_LOG_LEVEL_TRACE || component > HDFSPP_LOG_LEVEL_ERROR)
    return false;
  return true;
}


//  should use  __builtin_popcnt as optimization on some platforms
static int popcnt(int val) {
  int bits = sizeof(val) * 8;
  int count = 0;
  for(int i=0; i<bits; i++) {
    if((val >> i) & 0x1)
      count++;
  }
  return count;
}

static bool IsComponentValid(int component) {
  if(component < HDFSPP_LOG_COMPONENT_UNKNOWN || component > HDFSPP_LOG_COMPONENT_FILESYSTEM)
    return false;
  if(popcnt(component) != 1)
    return false;
  return true;
}

LIBHDFSPP_EXT_API
int hdfsEnableLoggingForComponent(int component) {
  errno = 0;
  if(!IsComponentValid(component))
    return -1;
  LogManager::EnableLogForComponent(static_cast<LogSourceComponent>(component));
  return 0;
}

LIBHDFSPP_EXT_API
int hdfsDisableLoggingForComponent(int component) {
  errno = 0;
  if(!IsComponentValid(component))
    return -1;
  LogManager::DisableLogForComponent(static_cast<LogSourceComponent>(component));
  return 0;
}

LIBHDFSPP_EXT_API
int hdfsSetLoggingLevel(int level) {
  errno = 0;
  if(!IsLevelValid(level))
    return -1;
  LogManager::SetLogLevel(static_cast<LogLevel>(level));
  return 0;
}

#undef LIBHDFS_C_API
#undef LIBHDFSPP_EXT_API


