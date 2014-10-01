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

#ifndef _HDFS_LIBHDFS3_COMMON_FILEWRAPPER_H_
#define _HDFS_LIBHDFS3_COMMON_FILEWRAPPER_H_

#include <cassert>
#include <cstdio>
#include <stdint.h>
#include <string>
#include <string>
#include <vector>

namespace hdfs {
namespace internal {

class FileWrapper {
public:
    virtual ~FileWrapper() {
    }

    virtual bool open(int fd, bool delegate) = 0;
    virtual bool open(const std::string &path) = 0;
    virtual void close() = 0;
    virtual const char *read(std::vector<char> &buffer, int32_t size) = 0;
    virtual void copy(char *buffer, int32_t size) = 0;
    virtual void seek(int64_t position) = 0;
};

class CFileWrapper: public FileWrapper {
public:
    CFileWrapper();
    ~CFileWrapper();
    bool open(int fd, bool delegate);
    bool open(const std::string &path);
    void close();
    const char *read(std::vector<char> &buffer, int32_t size);
    void copy(char *buffer, int32_t size);
    void seek(int64_t offset);

private:
    FILE *file;
    std::string path;
};

class MappedFileWrapper: public FileWrapper {
public:
    MappedFileWrapper();
    ~MappedFileWrapper();
    bool open(int fd, bool delegate);
    bool open(const std::string &path);
    void close();
    const char *read(std::vector<char> &buffer, int32_t size);
    void copy(char *buffer, int32_t size);
    void seek(int64_t offset);

private:
    bool openInternal(int fd, bool delegate, size_t size);

private:
    bool delegate;
    const char *begin;
    const char *position;
    int fd;
    int64_t size;
    std::string path;
};

}
}

#endif /* _HDFS_LIBHDFS3_COMMON_FILEWRAPPER_H_ */
