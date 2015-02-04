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
    CHECK_PARAMETER(fs.impl, EIO, "FileSystem: not connected.");

    try {
        impl->open(fs.impl, path.c_str(), verifyChecksum);
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status InputStream::read(char *buf, int32_t size, int32_t *done) {
    CHECK_PARAMETER(NULL != done, EINVAL, "invalid parameter \"output\"");

    try {
        *done = impl->read(buf, size);
    } catch (const HdfsEndOfStream &e) {
        *done = 0;
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status InputStream::readFully(char *buf, int64_t size) {
    try {
        impl->readFully(buf, size);
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status InputStream::available(int64_t *output) {
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->available();
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status InputStream::seek(int64_t pos) {
    try {
        impl->seek(pos);
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status InputStream::tell(int64_t *output) {
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->tell();
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status InputStream::close() {
    try {
        impl->close();
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}
}
