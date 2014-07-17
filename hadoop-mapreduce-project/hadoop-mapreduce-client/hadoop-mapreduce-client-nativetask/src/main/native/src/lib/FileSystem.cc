/*
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

#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>
#include <jni.h>
#include "commons.h"
#include "util/StringUtil.h"
#include "jniutils.h"
#include "NativeTask.h"
#include "TaskCounters.h"
#include "NativeObjectFactory.h"
#include "Path.h"
#include "FileSystem.h"

namespace NativeTask {

/////////////////////////////////////////////////////////////

FileInputStream::FileInputStream(const string & path) {
  _handle = fopen(path.c_str(), "rb");
  if (_handle != NULL) {
    _fd = fileno(_handle);
    _path = path;
  } else {
    _fd = -1;
    THROW_EXCEPTION_EX(IOException, "Can't open raw file: [%s]", path.c_str());
  }
  _bytesRead = NativeObjectFactory::GetCounter(TaskCounters::FILESYSTEM_COUNTER_GROUP,
      TaskCounters::FILE_BYTES_READ);
}

FileInputStream::~FileInputStream() {
  close();
}

void FileInputStream::seek(uint64_t position) {
  ::lseek(_fd, position, SEEK_SET);
}

uint64_t FileInputStream::tell() {
  return ::lseek(_fd, 0, SEEK_CUR);
}

int32_t FileInputStream::read(void * buff, uint32_t length) {
  int32_t ret = ::read(_fd, buff, length);
  if (ret > 0) {
    _bytesRead->increase(ret);
  }
  return ret;
}

void FileInputStream::close() {
  if (_handle != NULL) {
    fclose(_handle);
    _handle = NULL;
    _fd = -1;
  }
}

/////////////////////////////////////////////////////////////

FileOutputStream::FileOutputStream(const string & path, bool overwite) {
  _handle = fopen(path.c_str(), "wb");
  if (_handle != NULL) {
    _fd = fileno(_handle);
    _path = path;
  } else {
    _fd = -1;
    THROW_EXCEPTION_EX(IOException, "Open raw file failed: [%s]", path.c_str());
  }
  _bytesWrite = NativeObjectFactory::GetCounter(TaskCounters::FILESYSTEM_COUNTER_GROUP,
      TaskCounters::FILE_BYTES_WRITTEN);
}

FileOutputStream::~FileOutputStream() {
  close();
}

uint64_t FileOutputStream::tell() {
  return ::lseek(_fd, 0, SEEK_CUR);
}

void FileOutputStream::write(const void * buff, uint32_t length) {
  if (::write(_fd, buff, length) < length) {
    THROW_EXCEPTION(IOException, "::write error");
  }
  _bytesWrite->increase(length);
}

void FileOutputStream::flush() {
}

void FileOutputStream::close() {
  if (_handle != NULL) {
    fclose(_handle);
    _handle = NULL;
    _fd = -1;
  }
}

/////////////////////////////////////////////////////////////

class RawFileSystem : public FileSystem {
protected:
  string getRealPath(const string & path) {
    if (StringUtil::StartsWith(path, "file:")) {
      return path.substr(5);
    }
    return path;
  }
public:
  InputStream * open(const string & path) {
    return new FileInputStream(getRealPath(path));
  }

  OutputStream * create(const string & path, bool overwrite) {
    string np = getRealPath(path);
    string parent = Path::GetParent(np);
    if (parent.length() > 0) {
      if (!exists(parent)) {
        mkdirs(parent);
      }
    }
    return new FileOutputStream(np, overwrite);
  }

  uint64_t getLength(const string & path) {
    struct stat st;
    if (::stat(getRealPath(path).c_str(), &st) != 0) {
      char buff[256];
      strerror_r(errno, buff, 256);
      THROW_EXCEPTION(IOException,
          StringUtil::Format("stat path %s failed, %s", path.c_str(), buff));
    }
    return st.st_size;
  }

  bool list(const string & path, vector<FileEntry> & status) {
    DIR * dp;
    struct dirent * dirp;
    if ((dp = opendir(path.c_str())) == NULL) {
      return false;
    }

    FileEntry temp;
    while ((dirp = readdir(dp)) != NULL) {
      temp.name = dirp->d_name;
      temp.isDirectory = dirp->d_type & DT_DIR;
      if (temp.name == "." || temp.name == "..") {
        continue;
      }
      status.push_back(temp);
    }
    closedir(dp);
    return true;
  }

  void remove(const string & path) {
    if (!exists(path)) {
      LOG("[FileSystem] remove file %s not exists, ignore", path.c_str());
      return;
    }
    if (::remove(getRealPath(path).c_str()) != 0) {
      int err = errno;
      if (::system(StringUtil::Format("rm -rf %s", path.c_str()).c_str()) == 0) {
        return;
      }
      char buff[256];
      strerror_r(err, buff, 256);
      THROW_EXCEPTION(IOException,
          StringUtil::Format("FileSystem: remove path %s failed, %s", path.c_str(), buff));
    }
  }

  bool exists(const string & path) {
    struct stat st;
    if (::stat(getRealPath(path).c_str(), &st) != 0) {
      return false;
    }
    return true;
  }

  int mkdirs(const string & path, mode_t nmode) {
    string np = getRealPath(path);
    struct stat sb;

    if (stat(np.c_str(), &sb) == 0) {
      if (S_ISDIR (sb.st_mode) == 0) {
        return 1;
      }
      return 0;
    }

    string npathstr = np;
    char * npath = const_cast<char*>(npathstr.c_str());

    /* Skip leading slashes. */
    char * p = npath;
    while (*p == '/')
      p++;

    while (NULL != (p = strchr(p, '/'))) {
      *p = '\0';
      if (stat(npath, &sb) != 0) {
        if (mkdir(npath, nmode)) {
          return 1;
        }
      } else if (S_ISDIR (sb.st_mode) == 0) {
        return 1;
      }
      *p++ = '/'; /* restore slash */
      while (*p == '/')
        p++;
    }

    /* Create the final directory component. */
    if (stat(npath, &sb) && mkdir(npath, nmode)) {
      return 1;
    }
    return 0;
  }

  void mkdirs(const string & path) {
    int ret = mkdirs(path, 0755);
    if (ret != 0) {
      THROW_EXCEPTION_EX(IOException, "mkdirs [%s] failed", path.c_str());
    }
  }
};

///////////////////////////////////////////////////////////

extern RawFileSystem RawFileSystemInstance;

RawFileSystem RawFileSystemInstance = RawFileSystem();

string FileSystem::getDefaultFsUri(Config * config) {
  const char * nm = config->get(FS_DEFAULT_NAME);
  if (nm == NULL) {
    nm = config->get("fs.defaultFS");
  }
  if (nm == NULL) {
    return string("file:///");
  } else {
    return string(nm);
  }
}

FileSystem & FileSystem::getLocal() {
  return RawFileSystemInstance;
}


FileSystem & FileSystem::get(Config * config) {
  string uri = getDefaultFsUri(config);
  if (uri == "file:///") {
    return RawFileSystemInstance;
  }
}

} // namespace Hadoap
