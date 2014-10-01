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

#include <errno.h>
#include <fcntl.h>
#include <limits>
#include <sstream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "Exception.h"
#include "ExceptionInternal.h"
#include "FileWrapper.h"

namespace hdfs {
namespace internal {

MappedFileWrapper::MappedFileWrapper() :
    delegate(true), begin(NULL), position(NULL), fd(-1), size(0) {
}

MappedFileWrapper::~MappedFileWrapper() {
    close();
}

bool MappedFileWrapper::openInternal(int fd, bool delegate, size_t size) {
    this->delegate = delegate;
    void *retval = mmap(NULL, size, PROT_READ, MAP_FILE | MAP_PRIVATE, fd, 0);
    begin = position = static_cast<const char *>(retval);

    if (MAP_FAILED == retval) {
        begin = position = NULL;
        close();
        return false;
    }

    if (posix_madvise(const_cast<char *>(begin), size, POSIX_MADV_SEQUENTIAL)) {
        close();
        return false;
    }

    return true;
}

bool MappedFileWrapper::open(int fd, bool delegate) {
    size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);
    std::stringstream ss;
    ss << "FileDescriptor " << fd;
    path = ss.str();

    if (static_cast<uint64_t>(size) >
        static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        THROW(HdfsIOException,
              "Cannot create memory mapped file for \"%s\", file is too large.",
              path.c_str());
    }

    return openInternal(fd, delegate, static_cast<size_t>(size));
}

bool MappedFileWrapper::open(const std::string &path) {
    struct stat st;

    if (stat(path.c_str(), &st)) {
        return false;
    }

    size = st.st_size;

    if (static_cast<uint64_t>(size) >
        static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        THROW(HdfsIOException,
              "Cannot create memory mapped file for \"%s\", file is too large.",
              path.c_str());
    }

    fd = ::open(path.c_str(), O_RDONLY);

    if (fd < 0) {
        return false;
    }

    this->path = path;
    return openInternal(fd, true, st.st_size);
}

void MappedFileWrapper::close() {
    if (NULL != begin) {
        ::munmap(const_cast<char *>(begin), static_cast<size_t>(size));
        begin = position = NULL;
    }
    if (fd >= 0 && delegate) {
        ::close(fd);
    }

    fd = -1;
    size = 0;
    delegate = true;
    path.clear();
}

const char * MappedFileWrapper::read(std::vector<char> &buffer, int32_t size) {
    assert(NULL != begin && NULL != position);
    const char * retval = position;
    position += size;
    return retval;
}

void MappedFileWrapper::copy(char *buffer, int32_t size) {
    assert(NULL != begin && NULL != position);
    memcpy(buffer, position, size);
    position += size;
}

void MappedFileWrapper::seek(int64_t offset) {
    assert(NULL != begin && NULL != position);
    position = begin + offset;
}

}
}
