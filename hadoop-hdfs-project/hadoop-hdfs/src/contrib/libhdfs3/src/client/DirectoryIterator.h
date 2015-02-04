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

#ifndef _HDFS_LIBHFDS3_CLIENT_DIRECTORY_ITERATOR_H_
#define _HDFS_LIBHFDS3_CLIENT_DIRECTORY_ITERATOR_H_

#include "FileStatus.h"
#include "Status.h"

#include <vector>

namespace hdfs {
namespace internal {
class FileSystemImpl;
}

class DirectoryIterator {
public:
    DirectoryIterator();
    bool hasNext();
    Status getNext(FileStatus *output);

private:
    DirectoryIterator(hdfs::internal::FileSystemImpl *const fs,
                      const std::string &path, bool needLocations);
    bool getListing();

    bool needLocations;
    bool hasNextItem;
    hdfs::internal::FileSystemImpl *filesystem;
    size_t next;
    std::string path;
    std::string startAfter;
    std::vector<FileStatus> lists;

    friend class hdfs::internal::FileSystemImpl;
};
}

#endif /* _HDFS_LIBHFDS3_CLIENT_DIRECTORY_ITERATOR_H_ */
