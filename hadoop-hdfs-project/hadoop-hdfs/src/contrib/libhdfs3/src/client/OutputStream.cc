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

#include "Atomic.h"
#include "FileSystemImpl.h"
#include "OutputStream.h"
#include "OutputStreamImpl.h"
#include "SharedPtr.h"
#include "StatusInternal.h"

#include <stdint.h>

using namespace hdfs::internal;

namespace hdfs {

OutputStream::OutputStream() {
    impl = new internal::OutputStreamImpl;
}

OutputStream::~OutputStream() {
    delete impl;
}

Status OutputStream::open(FileSystem &fs, const std::string &path, int flag,
                          const Permission permission, bool createParent,
                          int replication, int64_t blockSize) {
    CHECK_PARAMETER(fs.impl, EIO, "FileSystem: not connected.");

    try {
        impl->open(fs.impl, path.c_str(), flag, permission, createParent,
                   replication, blockSize);
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status OutputStream::append(const char *buf, uint32_t size) {
    try {
        impl->append(buf, size);
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status OutputStream::flush() {
    try {
        impl->flush();
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status OutputStream::tell(int64_t *output) {
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->tell();
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status OutputStream::sync() {
    try {
        impl->sync();
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status OutputStream::close() {
    try {
        impl->close();
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}
}
