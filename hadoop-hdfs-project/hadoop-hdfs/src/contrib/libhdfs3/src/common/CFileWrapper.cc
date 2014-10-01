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

#include "FileWrapper.h"

#include <limits>
#include <string>

#include "Exception.h"
#include "ExceptionInternal.h"

namespace hdfs {
namespace internal {

CFileWrapper::CFileWrapper() :
    file(NULL) {
}

CFileWrapper::~CFileWrapper() {
    close();
}

bool CFileWrapper::open(int fd, bool delegate) {
    assert(false && "not implemented");
    abort();
    return false;
}

bool CFileWrapper::open(const std::string &path) {
    this->path = path;
    file = fopen(path.c_str(), "rb");
    return NULL != file;
}

void CFileWrapper::close() {
    if (NULL != file) {
        fclose(file);
        file = NULL;
    }
}

const char *CFileWrapper::read(std::vector<char> &buffer, int32_t size) {
    buffer.resize(size);
    copy(&buffer[0], size);
    return &buffer[0];
}

void CFileWrapper::copy(char *buffer, int32_t size) {
    int32_t todo = size, done;

    while (todo > 0) {
        done = fread(buffer + (size - todo), sizeof(char), todo, file);

        if (done < 0) {
            THROW(HdfsIOException, "Cannot read file \"%s\", %s.", path.c_str(),
                  GetSystemErrorInfo(errno));
        } else if (0 == done) {
            THROW(HdfsIOException, "Cannot read file \"%s\", End of file.",
                  path.c_str());
        }

        todo -= done;
    }
}

void CFileWrapper::seek(int64_t offset) {
    assert(offset > 0);
    int64_t todo = offset, batch;
    bool seek_set = true;

    while (todo > 0) {
        batch = todo < std::numeric_limits<long>::max() ?
                todo : std::numeric_limits<long>::max();
        off_t rc = fseek(file, static_cast<long>(batch),
                         seek_set ? SEEK_SET : SEEK_CUR);
        seek_set = false;

        if (rc != 0) {
            THROW(HdfsIOException, "Cannot lseek file: %s, %s", path.c_str(),
                  GetSystemErrorInfo(errno));
        }

        todo -= batch;
    }
}

}
}
