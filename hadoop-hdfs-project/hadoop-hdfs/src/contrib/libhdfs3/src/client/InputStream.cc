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

#include "FileSystemImpl.h"
#include "InputStream.h"
#include "InputStreamImpl.h"
#include "StatusInternal.h"

using namespace hdfs::internal;

namespace hdfs {

InputStream::InputStream() {
    impl = new internal::InputStreamImpl;
}

InputStream::~InputStream() {
    delete impl;
}

Status InputStream::open(FileSystem &fs, const std::string &path,
                         bool verifyChecksum) {
    if (!fs.impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    try {
        impl->open(fs.impl, path.c_str(), verifyChecksum);
    } catch (...) {
        return lastError = CreateStatusFromException(current_exception());
    }

    return lastError = Status::OK();
}

int32_t InputStream::read(char *buf, int32_t size) {
    int32_t retval = -1;

    try {
        retval = impl->read(buf, size);
        lastError = Status::OK();
    } catch (...) {
        lastError = CreateStatusFromException(current_exception());
    }

    return retval;
}

Status InputStream::readFully(char *buf, int64_t size) {
    try {
        impl->readFully(buf, size);
    } catch (...) {
        return lastError = CreateStatusFromException(current_exception());
    }

    return lastError = Status::OK();
}

int64_t InputStream::available() {
    int64_t retval = -1;

    try {
        retval = impl->available();
        lastError = Status::OK();
    } catch (...) {
        lastError = CreateStatusFromException(current_exception());
    }

    return retval;
}

Status InputStream::seek(int64_t pos) {
    try {
        impl->seek(pos);
    } catch (...) {
        return lastError = CreateStatusFromException(current_exception());
    }

    return lastError = Status::OK();
}

int64_t InputStream::tell() {
    int64_t retval = -1;

    try {
        retval = impl->tell();
        lastError = Status::OK();
    } catch (...) {
        lastError = CreateStatusFromException(current_exception());
    }

    return retval;
}

Status InputStream::close() {
    try {
        impl->close();
    } catch (...) {
        return lastError = CreateStatusFromException(current_exception());
    }

    return lastError = Status::OK();
}

Status InputStream::getLastError() {
    return lastError;
}
}
