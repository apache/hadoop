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
#ifndef _HDFS_LIBHDFS3_CLIENT_FILESTATUS_H_
#define _HDFS_LIBHDFS3_CLIENT_FILESTATUS_H_

#include "Permission.h"

#include <stdint.h>
#include <string>

namespace hdfs {

class FileStatus {
public:
    FileStatus() :
        isdir(false), atime(0), blocksize(0), length(0), mtime(
            0), permission(0644), replications(0) {
    }

    int64_t getAccessTime() const {
        return atime;
    }

    void setAccessTime(int64_t accessTime) {
        atime = accessTime;
    }

    short getReplication() const {
        return replications;
    }

    void setReplication(short blockReplication) {
        replications = blockReplication;
    }

    int64_t getBlockSize() const {
        return blocksize;
    }

    void setBlocksize(int64_t blocksize) {
        this->blocksize = blocksize;
    }

    const char *getGroup() const {
        return group.c_str();
    }

    void setGroup(const char * group) {
        this->group = group;
    }

    /**
     * Is this a directory?
     * @return true if this is a directory
     */
    bool isDirectory() const {
        return isdir;
    }

    void setIsdir(bool isdir) {
        this->isdir = isdir;
    }

    int64_t getLength() const {
        return length;
    }

    void setLength(int64_t length) {
        this->length = length;
    }

    int64_t getModificationTime() const {
        return mtime;
    }

    void setModificationTime(int64_t modificationTime) {
        mtime = modificationTime;
    }

    const char *getOwner() const {
        return owner.c_str();
    }

    void setOwner(const char * owner) {
        this->owner = owner;
    }

    const char *getPath() const {
        return path.c_str();
    }

    void setPath(const char * path) {
        this->path = path;
    }

    const Permission &getPermission() const {
        return permission;
    }

    void setPermission(const Permission & permission) {
        this->permission = permission;
    }

    const char *getSymlink() const {
        return symlink.c_str();
    }

    void setSymlink(const char *symlink) {
        this->symlink = symlink;
    }

    /**
     * Is this a file?
     * @return true if this is a file
     */
    bool isFile() {
        return !isdir && !isSymlink();
    }

    /**
     * Is this a symbolic link?
     * @return true if this is a symbolic link
     */
    bool isSymlink() {
        return !symlink.empty();
    }

private:
    bool isdir;
    int64_t atime;
    int64_t blocksize;
    int64_t length;
    int64_t mtime;
    Permission permission;
    short replications;
    std::string group;
    std::string owner;
    std::string path;
    std::string symlink;
};

}

#endif
