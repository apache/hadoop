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

#ifndef _HDFS_LIBHDFS3_CLIENT_FILESYSTEM_H_
#define _HDFS_LIBHDFS3_CLIENT_FILESYSTEM_H_

#include "BlockLocation.h"
#include "Config.h"
#include "DirectoryIterator.h"
#include "FileStatus.h"
#include "FileSystemStats.h"
#include "Permission.h"
#include "SharedPtr.h"
#include "Status.h"
#include "Token.h"

#include <vector>

namespace hdfs {
namespace internal {
struct FileSystemImpl;
}

class FileSystem {
public:
    /**
     * Construct a FileSystem
     * @param conf hdfs configuration
     */
    FileSystem(const Config &conf);

    /**
     * Destroy a HdfsFileSystem instance
     */
    ~FileSystem();

    /**
     * Connect to default hdfs.
     * @return the result status of this operation
     */
    Status connect();

    /**
     * Connect to hdfs
     * @param uri hdfs connection uri, hdfs://host:port
     * @return the result status of this operation
     */
    Status connect(const std::string &uri);

    /**
     * Connect to hdfs with user or token
     * 	username and token cannot be set at the same time
     * @param uri connection uri.
     * @param username user used to connect to hdfs
     * @param token token used to connect to hdfs
     * @return the result status of this operation
     */
    Status connect(const std::string &uri, const std::string &username,
                   const std::string &token);

    /**
     * disconnect from hdfs
     */
    void disconnect();

    /**
     * To get default number of replication.
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getDefaultReplication(int *output) const;

    /**
     * To get the default number of block size.
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getDefaultBlockSize(int64_t *output) const;

    /**
     * To get the home directory.
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getHomeDirectory(std::string *output) const;

    /**
     * To delete a file or directory.
     * @param path the path to be deleted.
     * @param recursive if path is a directory, delete the contents recursively.
     * @return the result status of this operation
     */
    Status deletePath(const std::string &path, bool recursive);

    /**
     * To create a directory which given permission.
     * @param path the directory path which is to be created.
     * @param permission directory permission.
     * @return the result status of this operation
     */
    Status mkdir(const std::string &path, const Permission &permission);

    /**
     * To create a directory which given permission.
     * If parent path does not exits, create it.
     * @param path the directory path which is to be created.
     * @param permission directory permission.
     * @return the result status of this operation
     */
    Status mkdirs(const std::string &path, const Permission &permission);

    /**
     * To get path information.
     * @param path the path which information is to be returned.
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getFileStatus(const std::string &path, FileStatus *output) const;

    /**
     * Return an array containing hostnames, offset and size of
     * portions of the given file.
     *
     * This call is most helpful with DFS, where it returns
     * hostnames of machines that contain the given file.
     *
     * The FileSystem will simply return an elt containing 'localhost'.
     *
     * @param path path is used to identify an FS since an FS could have
     *          another FS that it could be delegating the call to
     * @param start offset into the given file
     * @param len length for which to get locations for
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getFileBlockLocations(const std::string &path, int64_t start,
                                 int64_t len,
                                 std::vector<BlockLocation> *output);

    /**
     * list the contents of a directory.
     * @param path The directory path.
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status listDirectory(const std::string &path, DirectoryIterator *output);

    /**
     * To set the owner and the group of the path.
     * username and groupname cannot be empty at the same time.
     * @param path the path which owner of group is to be changed.
     * @param username new user name.
     * @param groupname new group.
     * @return the result status of this operation
     */
    Status setOwner(const std::string &path, const std::string &username,
                    const std::string &groupname);

    /**
     * To set the access time or modification time of a path.
     * @param path the path which access time or modification time is to be
     * changed.
     * @param mtime new modification time.
     * @param atime new access time.
     * @return the result status of this operation
     */
    Status setTimes(const std::string &path, int64_t mtime, int64_t atime);

    /**
     * To set the permission of a path.
     * @param path the path which permission is to be changed.
     * @param permission new permission.
     * @return the result status of this operation
     */
    Status setPermission(const std::string &path, const Permission &permission);

    /**
     * To set the number of replication.
     * @param path the path which number of replication is to be changed.
     * @param replication new number of replication.
     * @return return true if success.
     * @return the result status of this operation
     */
    Status setReplication(const std::string &path, short replication);

    /**
     * To rename a path.
     * @param src old path.
     * @param dst new path.
     * @return the result status of this operation
     */
    Status rename(const std::string &src, const std::string &dst);

    /**
     * To set working directory.
     * @param path new working directory.
     * @return the result status of this operation
     */
    Status setWorkingDirectory(const std::string &path);

    /**
     * To get working directory.
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getWorkingDirectory(std::string *output) const;

    /**
     * To test if the path exist.
     * @param path the path which is to be tested.
     * @return the result status of this operation
     */
    Status exist(const std::string &path) const;

    /**
     * To get the file system status.
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getStats(FileSystemStats *output) const;

    /**
     * Get a valid Delegation Token.
     * @param renewer the designated renewer for the token
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getDelegationToken(const std::string &renewer, std::string *output);

    /**
     * Get a valid Delegation Token using the default user as renewer.
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getDelegationToken(std::string *output);

    /**
     * Renew an existing delegation token.
     * @param token delegation token obtained earlier
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status renewDelegationToken(const std::string &token, int64_t *output);

    /**
     * Cancel an existing delegation token.
     * @param token delegation token
     * @return the result status of this operation
     */
    Status cancelDelegationToken(const std::string &token);

private:
    FileSystem(const FileSystem &other);
    FileSystem &operator=(const FileSystem &other);
    Config conf_;
    hdfs::internal::shared_ptr<internal::FileSystemImpl> impl;

    friend class InputStream;
    friend class OutputStream;
};
}
#endif /* _HDFS_LIBHDFS3_CLIENT_FILESYSTEM_H_ */
