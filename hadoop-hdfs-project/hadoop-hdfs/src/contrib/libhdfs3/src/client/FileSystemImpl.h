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

#ifndef _HDFS_LIBHDFS3_CLIENT_FILESYSTEMIMPL_H_
#define _HDFS_LIBHDFS3_CLIENT_FILESYSTEMIMPL_H_

#include "BlockLocation.h"
#include "Config.h"
#include "DirectoryIterator.h"
#include "FileStatus.h"
#include "FileSystemKey.h"
#include "FileSystemStats.h"
#include "LeaseRenewer.h"
#include "server/LocatedBlocks.h"
#include "server/Namenode.h"
#include "SessionConfig.h"
#include "UnorderedMap.h"
#include "UserInfo.h"
#ifdef MOCK
#include "NamenodeStub.h"
#endif

#include <string>
#include <vector>

namespace hdfs {
class Permission;
}

namespace hdfs {
namespace internal {

class FileSystemImpl {
public:
    /**
     * Construct a FileSystemImpl instance.
     * @param key a key which can be uniquely identify a FileSystemImpl
     * instance.
     * @param c a configuration objecto used to initialize the instance.
     */
    FileSystemImpl(const FileSystemKey &key, const Config &c);

    /**
     * Destroy a FileSystemBase instance
     */
    ~FileSystemImpl();

    /**
     * Format the path to a absolute canonicalized path.
     * @param path target path to be hendled.
     * @return return a absolute canonicalized path.
     */
    const std::string getStandardPath(const char *path);

    /**
     * To get the client unique ID.
     * @return return the client unique ID.
     */
    const char *getClientName();

    /**
     * Connect to hdfs
     */
    void connect();

    /**
     * disconnect from hdfs
     */
    void disconnect();

    /**
     * To get default number of replication.
     * @return the default number of replication.
     */
    int getDefaultReplication() const;

    /**
     * To get the default block size.
     * @return the default block size.
     */
    int64_t getDefaultBlockSize() const;

    /**
     * To get the home directory.
     * @return home directory.
     */
    std::string getHomeDirectory() const;

    /**
     * To delete a file or directory.
     * @param path the path to be deleted.
     * @param recursive if path is a directory, delete the contents recursively.
     * @return return true if success.
     */
    bool deletePath(const char *path, bool recursive);

    /**
     * To create a directory with given permission.
     * @param path the directory path which is to be created.
     * @param permission directory permission.
     * @return return true if success.
     */
    bool mkdir(const char *path, const Permission &permission);

    /**
     * To create a directory which given permission.
     * If parent path does not exits, create it.
     * @param path the directory path which is to be created.
     * @param permission directory permission.
     * @return return true if success.
     */
    bool mkdirs(const char *path, const Permission &permission);

    /**
     * To get path information.
     * @param path the path which information is to be returned.
     * @return the path information.
     */
    FileStatus getFileStatus(const char *path);

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
     */
    std::vector<BlockLocation> getFileBlockLocations(const char *path,
                                                     int64_t start,
                                                     int64_t len);

    /**
     * list the contents of a directory.
     * @param path the directory path.
     * @return Return a iterator to visit all elements in this directory.
     */
    DirectoryIterator listDirectory(const char *path, bool needLocation);

    /**
     * To set the owner and the group of the path.
     * username and groupname cannot be empty at the same time.
     * @param path the path which owner of group is to be changed.
     * @param username new user name.
     * @param groupname new group.
     */
    void setOwner(const char *path, const char *username,
                  const char *groupname);

    /**
     * To set the access time or modification time of a path.
     * @param path the path which access time or modification time is to be
     * changed.
     * @param mtime new modification time.
     * @param atime new access time.
     */
    void setTimes(const char *path, int64_t mtime, int64_t atime);

    /**
     * To set the permission of a path.
     * @param path the path which permission is to be changed.
     * @param permission new permission.
     */
    void setPermission(const char *path, const Permission &permission);

    /**
     * To set the number of replication.
     * @param path the path which number of replication is to be changed.
     * @param replication new number of replication.
     * @return return true if success.
     */
    bool setReplication(const char *path, short replication);

    /**
     * To rename a path.
     * @param src old path.
     * @param dst new path.
     * @return return true if success.
     */
    bool rename(const char *src, const char *dst);

    /**
     * To set working directory.
     * @param path new working directory.
     */
    void setWorkingDirectory(const char *path);

    /**
     * To get working directory.
     * @return working directory.
     */
    std::string getWorkingDirectory() const;

    /**
     * To test if the path exist.
     * @param path the path which is to be tested.
     * @return return true if the path exist.
     */
    bool exist(const char *path);

    /**
     * To get the file system status.
     * @return the file system status.
     */
    FileSystemStats getFsStats();

    /**
     * Get a valid Delegation Token.
     *
     * @param renewer the designated renewer for the token
     * @return Token
     * @throws IOException
     */
    std::string getDelegationToken(const char *renewer);

    /**
     * Get a valid Delegation Token using default user as renewer.
     *
     * @return Token
     * @throws IOException
     */
    std::string getDelegationToken();

    /**
     * Renew an existing delegation token.
     *
     * @param token delegation token obtained earlier
     * @return the new expiration time
     * @throws IOException
     */
    int64_t renewDelegationToken(const std::string &token);

    /**
     * Cancel an existing delegation token.
     *
     * @param token delegation token
     * @throws IOException
     */
    void cancelDelegationToken(const std::string &token);

    /**
     * Get locations of the blocks of the specified file within the specified
     *range.
     * DataNode locations for each block are sorted by
     * the proximity to the client.
     *
     * The client will then have to contact
     * one of the indicated DataNodes to obtain the actual data.
     *
     * @param src file name
     * @param offset range start offset
     * @param length range length
     * @param lbs output the returned blocks
     */
    void getBlockLocations(const std::string &src, int64_t offset,
                           int64_t length, LocatedBlocks &lbs);

    /**
     * Create a new file entry in the namespace.
     *
     * @param src path of the file being created.
     * @param masked masked permission.
     * @param flag indicates whether the file should be
     *  overwritten if it already exists or create if it does not exist or
     *append.
     * @param createParent create missing parent directory if true
     * @param replication block replication factor.
     * @param blockSize maximum block size.
     */
    void create(const std::string &src, const Permission &masked, int flag,
                bool createParent, short replication, int64_t blockSize);

    /**
     * Append to the end of the file.
     *
     * @param src path of the file being created.
     * @return return the last partial block if any
     */
    shared_ptr<LocatedBlock> append(const std::string &src);

    /**
     * The client can give up on a block by calling abandonBlock().
     * The client can then either obtain a new block, or complete or abandon the
     *file.
     * Any partial writes to the block will be discarded.
     *
     * @param b the block to be abandoned.
     * @param src the file which the block belongs to.
     */
    void abandonBlock(const ExtendedBlock &b, const std::string &src);

    /**
     * A client that wants to write an additional block to the
     * indicated filename (which must currently be open for writing)
     * should call addBlock().
     *
     * addBlock() allocates a new block and datanodes the block data
     * should be replicated to.
     *
     * addBlock() also commits the previous block by reporting
     * to the name-node the actual generation stamp and the length
     * of the block that the client has transmitted to data-nodes.
     *
     * @param src the file being created
     * @param previous  previous block
     * @param excludeNodes a list of nodes that should not be allocated for the
     *current block.
     * @return return the new block.
     */
    shared_ptr<LocatedBlock> addBlock(
        const std::string &src, const ExtendedBlock *previous,
        const std::vector<DatanodeInfo> &excludeNodes);

    /**
     * Get a datanode for an existing pipeline.
     *
     * @param src the file being written
     * @param blk the block being written
     * @param existings the existing nodes in the pipeline
     * @param excludes the excluded nodes
     * @param numAdditionalNodes number of additional datanodes
     * @return return a new block information which contains new datanode.
     */
    shared_ptr<LocatedBlock> getAdditionalDatanode(
        const std::string &src, const ExtendedBlock &blk,
        const std::vector<DatanodeInfo> &existings,
        const std::vector<std::string> &storageIDs,
        const std::vector<DatanodeInfo> &excludes, int numAdditionalNodes);

    /**
     * The client is done writing data to the given filename, and would
     * like to complete it.
     *
     * The function returns whether the file has been closed successfully.
     * If the function returns false, the caller should try again.
     *
     * close() also commits the last block of file by reporting
     * to the name-node the actual generation stamp and the length
     * of the block that the client has transmitted to data-nodes.
     *
     * A call to complete() will not return true until all the file's
     * blocks have been replicated the minimum number of times.  Thus,
     * DataNode failures may cause a client to call complete() several
     * times before succeeding.
     *
     * @param src the file being written.
     * @param last last block to be committed.
     * @return return false if the client should retry.
     */
    bool complete(const std::string &src, const ExtendedBlock *last);

    /**
     * The client wants to report corrupted blocks (blocks with specified
     * locations on datanodes).
     * @param blocks Array of located blocks to report
     */
    /*void reportBadBlocks(const std::vector<LocatedBlock> &blocks);*/

    /**
     * Write all metadata for this file into persistent storage.
     * The file must be currently open for writing.
     * @param src The const std::string &representation of the path
     */
    void fsync(const std::string &src);

    /**
     * Get a new generation stamp together with an access token for
     * a block under construction
     *
     * This method is called only when a client needs to recover a failed
     * pipeline or set up a pipeline for appending to a block.
     *
     * @param block a block
     * @return return a located block with a new generation stamp and an access
     *token
     */
    shared_ptr<LocatedBlock> updateBlockForPipeline(const ExtendedBlock &block);

    /**
     * Update a pipeline for a block under construction
     *
     * @param oldBlock the old block
     * @param newBlock the new block containing new generation stamp and length
     * @param newNodes datanodes in the pipeline
     */
    void updatePipeline(const ExtendedBlock &oldBlock,
                        const ExtendedBlock &newBlock,
                        const std::vector<DatanodeInfo> &newNodes,
                        const std::vector<std::string> &storageIDs);

    /**
     * register the output stream in filespace when it is opened.
     */
    void registerOpenedOutputStream();

    /**
     * unregister the output stream from filespace when it is closed.
     */
    void unregisterOpenedOutputStream();

    /**
     * Get the configuration used in filesystem.
     * @return return the configuration instance.
     */
    const SessionConfig &getConf() const {
        return sconf;
    }

    /**
     * Get the user used in filesystem.
     * @return return the user information.
     */
    const UserInfo &getUserInfo() const {
        return user;
    }

    /**
     * Get a partial listing of the indicated directory
     *
     * @param src the directory name
     * @param startAfter the name to start listing after encoded in java UTF8
     * @param needLocation if the FileStatus should contain block locations
     * @param dl append the returned directories.
     * @return return true if there are more items.
     */
    bool getListing(const std::string &src, const std::string &startAfter,
                    bool needLocation, std::vector<FileStatus> &dl);

    /**
     * To renew the lease.
     */
    void renewLease();

private:
    FileSystemImpl(const FileSystemImpl &other);
    FileSystemImpl &operator=(const FileSystemImpl &other);

    Config conf;
    FileSystemKey key;
    LeaseRenewer leaseRenewer;
    mutex mutWorkingDir;
    Namenode *nn;
    SessionConfig sconf;
    std::string clientName;
    std::string tokenService;
    std::string workingDir;
    UserInfo user;
#ifdef MOCK
private:
    Hdfs::Mock::NamenodeStub *stub;
#endif
};
}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_FILESYSTEMIMPL_H_ */
