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

#ifndef _HDFS_LIBHDFS3_SERVER_NAMENODE_H_
#define _HDFS_LIBHDFS3_SERVER_NAMENODE_H_

#include "client/FileStatus.h"
#include "client/Permission.h"
#include "DatanodeInfo.h"
#include "Exception.h"
#include "ExtendedBlock.h"
#include "LocatedBlock.h"
#include "LocatedBlocks.h"
#include "rpc/RpcAuth.h"
#include "rpc/RpcCall.h"
#include "rpc/RpcClient.h"
#include "rpc/RpcConfig.h"
#include "rpc/RpcProtocolInfo.h"
#include "rpc/RpcServerInfo.h"
#include "SessionConfig.h"

#include <stdint.h>
#include <vector>

namespace hdfs {
namespace internal {

class Namenode {
public:
    /**
     * Destroy the namenode.
     */
    virtual ~Namenode() {
    }

    /**
     * Get locations of the blocks of the specified file within the specified range.
     * DataNode locations for each block are sorted by
     * the proximity to the client.
     * <p>
     * Return {//link LocatedBlocks} which contains
     * file length, blocks and their locations.
     * DataNode locations for each block are sorted by
     * the distance to the client's address.
     * <p>
     * The client will then have to contact
     * one of the indicated DataNodes to obtain the actual data.
     *
     * @param src file name
     * @param offset range start offset
     * @param length range length
     * @param file length and array of blocks with their locations
     * @param lbs output the returned blocks
     *
     * @throw AccessControlException If access is denied
     * @throw FileNotFoundException If file <code>src</code> does not exist
     * @throw UnresolvedLinkException If <code>src</code> contains a symlink
     * @throw HdfsIOException If an I/O error occurred
     */
    //Idempotent
    virtual void getBlockLocations(const std::string &src, int64_t offset,
                                   int64_t length, LocatedBlocks &lbs)
      /* throw (AccessControlException,
             FileNotFoundException, UnresolvedLinkException,
             HdfsIOException) */ = 0;

    /**
     * Create a new file entry in the namespace.
     * <p>
     * This will create an empty file specified by the source path.
     * The path should reflect a full path originated at the root.
     * The name-node does not have a notion of "current" directory for a client.
     * <p>
     * Once created, the file is visible and available for read to other clients.
     * Although, other clients cannot {//link #delete(const std::string &, bool)}, re-create or
     * {//link #rename(const std::string &, const std::string &)} it until the file is completed
     * or explicitly as a result of lease expiration.
     * <p>
     * Blocks have a maximum size.  Clients that intend to create
     * multi-block files must also use
     * {//link #addBlock(const std::string &, const std::string &, ExtendedBlock, DatanodeInfo[])}
     *
     * @param src path of the file being created.
     * @param masked masked permission.
     * @param clientName name of the current client.
     * @param flag indicates whether the file should be
     * overwritten if it already exists or create if it does not exist or append.
     * @param createParent create missing parent directory if true
     * @param replication block replication factor.
     * @param blockSize maximum block size.
     *
     * @throw AccessControlException If access is denied
     * @throw AlreadyBeingCreatedException if the path does not exist.
     * @throw DSQuotaExceededException If file creation violates disk space
     *           quota restriction
     * @throw FileAlreadyExistsException If file <code>src</code> already exists
     * @throw FileNotFoundException If parent of <code>src</code> does not exist
     *           and <code>createParent</code> is false
     * @throw ParentNotDirectoryException If parent of <code>src</code> is not a
     *           directory.
     * @throw NSQuotaExceededException If file creation violates name space
     *           quota restriction
     * @throw SafeModeException create not allowed in safemode
     * @throw UnresolvedLinkException If <code>src</code> contains a symlink
     * @throw HdfsIOException If an I/O error occurred
     *
     * RuntimeExceptions:
     * @throw InvalidPathException Path <code>src</code> is invalid
     */
    virtual void create(const std::string &src, const Permission &masked,
                        const std::string &clientName, int flag, bool createParent,
                        short replication, int64_t blockSize)
      /* throw (AccessControlException,
             AlreadyBeingCreatedException, DSQuotaExceededException,
             FileAlreadyExistsException, FileNotFoundException,
             NSQuotaExceededException, ParentNotDirectoryException,
             SafeModeException, UnresolvedLinkException, HdfsIOException) */ = 0;

    /**
     * Append to the end of the file.
     * @param src path of the file being created.
     * @param clientName name of the current client.
     * @param information about the last partial block if any.
     * @param lb output the returned block.s
     *
     * @throw AccessControlException if permission to append file is
     * denied by the system. As usually on the client side the exception will
     * be wrapped into {//link org.apache.hadoop.ipc.RemoteException}.
     * Allows appending to an existing file if the server is
     * configured with the parameter dfs.support.append set to true, otherwise
     * throw an HdfsIOException.
     *
     * @throw AccessControlException If permission to append to file is denied
     * @throw FileNotFoundException If file <code>src</code> is not found
     * @throw DSQuotaExceededException If append violates disk space quota
     *           restriction
     * @throw SafeModeException append not allowed in safemode
     * @throw UnresolvedLinkException If <code>src</code> contains a symlink
     * @throw HdfsIOException If an I/O error occurred.
     *
     * RuntimeExceptions:
     * @throw UnsupportedOperationException if append is not supported
     */
    virtual shared_ptr<LocatedBlock> append(const std::string &src,
                                            const std::string &clientName)
    /* throw (AccessControlException,
             DSQuotaExceededException, FileNotFoundException,
             SafeModeException, UnresolvedLinkException, HdfsIOException) */ = 0;

    /**
     * Set replication for an existing file.
     * <p>
     * The NameNode sets replication to the new value and returns.
     * The actual block replication is not expected to be performed during
     * this method call. The blocks will be populated or removed in the
     * background as the result of the routine block maintenance procedures.
     *
     * @param src file name
     * @param replication new replication
     *
     * @param true if successful) = 0;
     *         false if file does not exist or is a directory
     *
     * @throw AccessControlException If access is denied
     * @throw DSQuotaExceededException If replication violates disk space
     *           quota restriction
     * @throw FileNotFoundException If file <code>src</code> is not found
     * @throw SafeModeException not allowed in safemode
     * @throw UnresolvedLinkException if <code>src</code> contains a symlink
     * @throw HdfsIOException If an I/O error occurred
     */
    //Idempotent
    virtual bool setReplication(const std::string &src, short replication)
    /* throw (AccessControlException, DSQuotaExceededException,
     FileNotFoundException, SafeModeException, UnresolvedLinkException,
     HdfsIOException) */ = 0;

    /**
     * Set permissions for an existing file/directory.
     *
     * @throw AccessControlException If access is denied
     * @throw FileNotFoundException If file <code>src</code> is not found
     * @throw SafeModeException not allowed in safemode
     * @throw UnresolvedLinkException If <code>src</code> contains a symlink
     * @throw HdfsIOException If an I/O error occurred
     */
    //Idempotent
    virtual void setPermission(const std::string &src,
                               const Permission &permission) /* throw (AccessControlException,
             FileNotFoundException, SafeModeException,
             UnresolvedLinkException, HdfsIOException) */ = 0;

    /**
     * Set Owner of a path (i.e. a file or a directory).
     * The parameters username and groupname cannot both be null.
     * @param src
     * @param username If it is null, the original username remains unchanged.
     * @param groupname If it is null, the original groupname remains unchanged.
     *
     * @throw AccessControlException If access is denied
     * @throw FileNotFoundException If file <code>src</code> is not found
     * @throw SafeModeException not allowed in safemode
     * @throw UnresolvedLinkException If <code>src</code> contains a symlink
     * @throw HdfsIOException If an I/O error occurred
     */
    //Idempotent
    virtual void setOwner(const std::string &src, const std::string &username,
                          const std::string &groupname) /* throw (AccessControlException,
             FileNotFoundException, SafeModeException,
             UnresolvedLinkException, HdfsIOException) */ = 0;

    /**
     * The client can give up on a block by calling abandonBlock().
     * The client can then
     * either obtain a new block, or complete or abandon the file.
     * Any partial writes to the block will be discarded.
     *
     * @throw AccessControlException If access is denied
     * @throw FileNotFoundException file <code>src</code> is not found
     * @throw UnresolvedLinkException If <code>src</code> contains a symlink
     * @throw HdfsIOException If an I/O error occurred
     */
    virtual void abandonBlock(const ExtendedBlock &b, const std::string &src,
                              const std::string &holder) /* throw (AccessControlException,
             FileNotFoundException, UnresolvedLinkException,
             HdfsIOException) */ = 0;

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
     * @param clientName the name of the client that adds the block
     * @param previous  previous block
     * @param excludeNodes a list of nodes that should not be
     * allocated for the current block
     *
     * @param LocatedBlock allocated block information.
     * @param lb output the returned block.
     *
     * @throw AccessControlException If access is denied
     * @throw FileNotFoundException If file <code>src</code> is not found
     * @throw NotReplicatedYetException previous blocks of the file are not
     *           replicated yet. Blocks cannot be added until replication
     *           completes.
     * @throw SafeModeException create not allowed in safemode
     * @throw UnresolvedLinkException If <code>src</code> contains a symlink
     * @throw HdfsIOException If an I/O error occurred
     */
    virtual shared_ptr<LocatedBlock> addBlock(const std::string &src,
            const std::string &clientName, const ExtendedBlock *previous,
            const std::vector<DatanodeInfo> &excludeNodes)
    /* throw (AccessControlException, FileNotFoundException,
     NotReplicatedYetException, SafeModeException,
     UnresolvedLinkException, HdfsIOException) */ = 0;

    /**
     * Get a datanode for an existing pipeline.
     *
     * @param src the file being written
     * @param blk the block being written
     * @param existings the existing nodes in the pipeline
     * @param excludes the excluded nodes
     * @param numAdditionalNodes number of additional datanodes
     * @param clientName the name of the client
     *
     * @param the located block.
     * @param output the returned block.
     *
     * @throw AccessControlException If access is denied
     * @throw FileNotFoundException If file <code>src</code> is not found
     * @throw SafeModeException create not allowed in safemode
     * @throw UnresolvedLinkException If <code>src</code> contains a symlink
     * @throw HdfsIOException If an I/O error occurred
     */
    //Idempotent
    virtual shared_ptr<LocatedBlock> getAdditionalDatanode(const std::string &src,
            const ExtendedBlock &blk,
            const std::vector<DatanodeInfo> &existings,
            const std::vector<std::string> &storageIDs,
            const std::vector<DatanodeInfo> &excludes, int numAdditionalNodes,
            const std::string &clientName)
    /* throw (AccessControlException, FileNotFoundException,
     SafeModeException, UnresolvedLinkException, HdfsIOException) */ = 0;

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
     * @throw AccessControlException If access is denied
     * @throw FileNotFoundException If file <code>src</code> is not found
     * @throw SafeModeException create not allowed in safemode
     * @throw UnresolvedLinkException If <code>src</code> contains a symlink
     * @throw HdfsIOException If an I/O error occurred
     */
    virtual bool complete(const std::string &src,
                          const std::string &clientName, const ExtendedBlock *last)
    /* throw (AccessControlException, FileNotFoundException,
     SafeModeException, UnresolvedLinkException, HdfsIOException) */ = 0;

    /**
     * The client wants to report corrupted blocks (blocks with specified
     * locations on datanodes).
     * @param blocks Array of located blocks to report
     */
    //Idempotent
    /*    virtual void reportBadBlocks(const std::vector<LocatedBlock> &blocks)
         throw (HdfsIOException)  = 0;*/

    /**
     * Rename an item in the file system namespace.
     * @param src existing file or directory name.
     * @param dst new name.
     * @param true if successful, or false if the old name does not exist
     * or if the new name already belongs to the namespace.
     *
     * @throw HdfsIOException an I/O error occurred
     */
    virtual bool rename(const std::string &src, const std::string &dst)
    /* throw (UnresolvedLinkException, HdfsIOException) */ = 0;

    /**
     * Moves blocks from srcs to trg and delete srcs
     *
     * @param trg existing file
     * @param srcs - list of existing files (same block size, same replication)
     * @throw HdfsIOException if some arguments are invalid
     * @throw UnresolvedLinkException if <code>trg</code> or <code>srcs</code>
     *           contains a symlink
     */
    /*    virtual void concat(const std::string &trg,
                            const std::vector<std::string> &srcs)  throw (HdfsIOException,
                 UnresolvedLinkException)  = 0;*/

    /**
     * Rename src to dst.
     * <ul>
     * <li>Fails if src is a file and dst is a directory.
     * <li>Fails if src is a directory and dst is a file.
     * <li>Fails if the parent of dst does not exist or is a file.
     * </ul>
     * <p>
     * Without OVERWRITE option, rename fails if the dst already exists.
     * With OVERWRITE option, rename overwrites the dst, if it is a file
     * or an empty directory. Rename fails if dst is a non-empty directory.
     * <p>
     * This implementation of rename is atomic.
     * <p>
     * @param src existing file or directory name.
     * @param dst new name.
     * @param options Rename options
     *
     * @throw AccessControlException If access is denied
     * @throw DSQuotaExceededException If rename violates disk space
     *           quota restriction
     * @throw FileAlreadyExistsException If <code>dst</code> already exists and
     *           <code>options</options> has {//link Rename#OVERWRITE} option
     *           false.
     * @throw FileNotFoundException If <code>src</code> does not exist
     * @throw NSQuotaExceededException If rename violates namespace
     *           quota restriction
     * @throw ParentNotDirectoryException If parent of <code>dst</code>
     *           is not a directory
     * @throw SafeModeException rename not allowed in safemode
     * @throw UnresolvedLinkException If <code>src</code> or
     *           <code>dst</code> contains a symlink
     * @throw HdfsIOException If an I/O error occurred
     */
    /*virtual void rename2(const std::string &src, const std::string &dst)
     throw (AccessControlException, DSQuotaExceededException,
     FileAlreadyExistsException, FileNotFoundException,
     NSQuotaExceededException, ParentNotDirectoryException,
     SafeModeException, UnresolvedLinkException, HdfsIOException) = 0;*/

    /**
     * Delete the given file or directory from the file system.
     * <p>
     * same as delete but provides a way to avoid accidentally
     * deleting non empty directories programmatically.
     * @param src existing name
     * @param recursive if true deletes a non empty directory recursively,
     * else throw( an exception.
     * @param true only if the existing file or directory was actually removed
     * from the file system.
     *
     * @throw AccessControlException If access is denied
     * @throw FileNotFoundException If file <code>src</code> is not found
     * @throw SafeModeException create not allowed in safemode
     * @throw UnresolvedLinkException If <code>src</code> contains a symlink
     * @throw HdfsIOException If an I/O error occurred
     */
    virtual bool deleteFile(const std::string &src, bool recursive)
    /* throw (AccessControlException, FileNotFoundException,
     SafeModeException, UnresolvedLinkException, HdfsIOException) */ = 0;

    /**
     * Create a directory (or hierarchy of directories) with the given
     * name and permission.
     *
     * @param src The path of the directory being created
     * @param masked The masked permission of the directory being created
     * @param createParent create missing parent directory if true
     *
     * @param True if the operation success.
     *
     * @throw AccessControlException If access is denied
     * @throw FileAlreadyExistsException If <code>src</code> already exists
     * @throw FileNotFoundException If parent of <code>src</code> does not exist
     *           and <code>createParent</code> is false
     * @throw NSQuotaExceededException If file creation violates quota restriction
     * @throw ParentNotDirectoryException If parent of <code>src</code>
     *           is not a directory
     * @throw SafeModeException create not allowed in safemode
     * @throw UnresolvedLinkException If <code>src</code> contains a symlink
     * @throw HdfsIOException If an I/O error occurred.
     *
     * RunTimeExceptions:
     * @throw InvalidPathException If <code>src</code> is invalid
     */
    //Idempotent
    virtual bool mkdirs(const std::string &src, const Permission &masked,
                        bool createParent) /* throw (AccessControlException,
             FileAlreadyExistsException, FileNotFoundException,
             NSQuotaExceededException, ParentNotDirectoryException,
             SafeModeException, UnresolvedLinkException, HdfsIOException) */ = 0;

    /**
     * Get a partial listing of the indicated directory
     *
     * @param src the directory name
     * @param startAfter the name to start listing after encoded in java UTF8
     * @param needLocation if the FileStatus should contain block locations
     *
     * @param a partial listing starting after startAfter
     * @param dl append the returned directories.
     *
     * @throw AccessControlException permission denied
     * @throw FileNotFoundException file <code>src</code> is not found
     * @throw UnresolvedLinkException If <code>src</code> contains a symlink
     * @throw HdfsIOException If an I/O error occurred
     */
    //Idempotent
    virtual bool getListing(const std::string &src,
                            const std::string &startAfter, bool needLocation,
                            std::vector<FileStatus> &dl) /* throw (AccessControlException,
             FileNotFoundException, UnresolvedLinkException,
             HdfsIOException) */ = 0;

    /**
     * Client programs can cause stateful changes in the NameNode
     * that affect other clients.  A client may obtain a file and
     * neither abandon nor complete it.  A client might hold a series
     * of locks that prevent other clients from proceeding.
     * Clearly, it would be bad if a client held a bunch of locks
     * that it never gave up.  This can happen easily if the client
     * dies unexpectedly.
     * <p>
     * So, the NameNode will revoke the locks and live file-creates
     * for clients that it thinks have died.  A client tells the
     * NameNode that it is still alive by periodically calling
     * renewLease().  If a certain amount of time passes since
     * the last call to renewLease(), the NameNode assumes the
     * client has died.
     *
     * @throw AccessControlException permission denied
     * @throw HdfsIOException If an I/O error occurred
     */
    //Idempotent
    virtual void renewLease(const std::string &clientName)
    /* throw (AccessControlException, HdfsIOException) */ = 0;

    /**
     * Start lease recovery.
     * Lightweight NameNode operation to trigger lease recovery
     *
     * @param src path of the file to start lease recovery
     * @param clientName name of the current client
     * @param true if the file is already closed
     * @throw HdfsIOException
     */
    //Idempotent
    virtual bool recoverLease(const std::string &src,
                              const std::string &clientName) = 0;

    /**
     * Get a set of statistics about the filesystem.
     * Right now, only seven values are returned.
     * <ul>
     * <li> [0] contains the total storage capacity of the system, in bytes.</li>
     * <li> [1] contains the total used space of the system, in bytes.</li>
     * <li> [2] contains the available storage of the system, in bytes.</li>
     * <li> [3] contains number of under replicated blocks in the system.</li>
     * <li> [4] contains number of blocks with a corrupt replica. </li>
     * <li> [5] contains number of blocks without any good replicas left. </li>
     * </ul>
     * Use  constants like {//link #GET_STATS_CAPACITY_IDX} in place of
     * actual numbers to index into the array.
     */
    //Idempotent
    virtual std::vector<int64_t> getFsStats() /* throw (HdfsIOException) */ = 0;

    /**
     * Dumps namenode data structures into specified file. If the file
     * already exists, then append.
     *
     * @throw HdfsIOException
     */
    /*    virtual void metaSave(
            const std::string &filename)  throw (HdfsIOException)  = 0;*/

    /**
     * Get the file info for a specific file or directory.
     * @param src The const std::string &representation of the path to the file
     *
     * @param object containing information regarding the file
     *         or null if file not found
     * @throw AccessControlException permission denied
     * @throw FileNotFoundException file <code>src</code> is not found
     * @throw UnresolvedLinkException if the path contains a symlink.
     * @throw HdfsIOException If an I/O error occurred
     */
    //Idempotent
    virtual FileStatus getFileInfo(const std::string &src)
    /* throw (AccessControlException, FileNotFoundException,
     UnresolvedLinkException, HdfsIOException) */ = 0;

    /**
     * Get the file info for a specific file or directory. If the path
     * refers to a symlink then the FileStatus of the symlink is returned.
     * @param src The const std::string &representation of the path to the file
     *
     * @param object containing information regarding the file
     *         or null if file not found
     *
     * @throw AccessControlException permission denied
     * @throw UnresolvedLinkException if <code>src</code> contains a symlink
     * @throw HdfsIOException If an I/O error occurred
     */
    //Idempotent
    /*    virtual FileStatus getFileLinkInfo(const std::string &src)
         throw (AccessControlException, UnresolvedLinkException,
         HdfsIOException)  = 0;*/

    /**
     * Get {//link ContentSummary} rooted at the specified directory.
     * @param path The const std::string &representation of the path
     *
     * @throw AccessControlException permission denied
     * @throw FileNotFoundException file <code>path</code> is not found
     * @throw UnresolvedLinkException if <code>path</code> contains a symlink.
     * @throw HdfsIOException If an I/O error occurred
     */
    //Idempotent
    /*    virtual ContentSummary getContentSummary(const std::string &path)
         throw (AccessControlException, FileNotFoundException,
         UnresolvedLinkException, HdfsIOException)  = 0;*/

    /**
     * Set the quota for a directory.
     * @param path  The const std::string &representation of the path to the directory
     * @param namespaceQuota Limit on the number of names in the tree rooted
     *                       at the directory
     * @param diskspaceQuota Limit on disk space occupied all the files under
     *                       this directory.
     * <br><br>
     *
     * The quota can have three types of values : (1) 0 or more will set
     * the quota to that value, (2) {//link HdfsConstants#QUOTA_DONT_SET}  implies
     * the quota will not be changed, and (3) {//link HdfsConstants#QUOTA_RESET}
     * implies the quota will be reset. Any other value is a runtime error.
     *
     * @throw AccessControlException permission denied
     * @throw FileNotFoundException file <code>path</code> is not found
     * @throw QuotaExceededException if the directory size
     *           is greater than the given quota
     * @throw UnresolvedLinkException if the <code>path</code> contains a symlink.
     * @throw HdfsIOException If an I/O error occurred
     */
    //Idempotent
    /*
        virtual void setQuota(const std::string &path, int64_t namespaceQuota,
                              int64_t diskspaceQuota)  throw (AccessControlException,
                 FileNotFoundException, UnresolvedLinkException,
                 HdfsIOException)  = 0;
    */

    /**
     * Write all metadata for this file into persistent storage.
     * The file must be currently open for writing.
     * @param src The const std::string &representation of the path
     * @param client The const std::string &representation of the client
     *
     * @throw AccessControlException permission denied
     * @throw FileNotFoundException file <code>src</code> is not found
     * @throw UnresolvedLinkException if <code>src</code> contains a symlink.
     * @throw HdfsIOException If an I/O error occurred
     */
    //Idempotent
    virtual void fsync(const std::string &src, const std::string &client)
    /* throw (AccessControlException, FileNotFoundException,
     UnresolvedLinkException, HdfsIOException) */ = 0;

    /**
     * Sets the modification and access time of the file to the specified time.
     * @param src The const std::string &representation of the path
     * @param mtime The number of milliseconds since Jan 1, 1970.
     *              Setting mtime to -1 means that modification time should not be set
     *              by this call.
     * @param atime The number of milliseconds since Jan 1, 1970.
     *              Setting atime to -1 means that access time should not be set
     *              by this call.
     *
     * @throw AccessControlException permission denied
     * @throw FileNotFoundException file <code>src</code> is not found
     * @throw UnresolvedLinkException if <code>src</code> contains a symlink.
     * @throw HdfsIOException If an I/O error occurred
     */
    //Idempotent
    virtual void setTimes(const std::string &src, int64_t mtime, int64_t atime)
    /* throw (AccessControlException, FileNotFoundException,
     UnresolvedLinkException, HdfsIOException) */ = 0;

    /**
     * Create symlink to a file or directory.
     * @param target The path of the destination that the
     *               link points to.
     * @param link The path of the link being created.
     * @param dirPerm permissions to use when creating parent directories
     * @param createParent - if true then missing parent dirs are created
     *                       if false then parent must exist
     *
     * @throw AccessControlException permission denied
     * @throw FileAlreadyExistsException If file <code>link</code> already exists
     * @throw FileNotFoundException If parent of <code>link</code> does not exist
     *           and <code>createParent</code> is false
     * @throw ParentNotDirectoryException If parent of <code>link</code> is not a
     *           directory.
     * @throw UnresolvedLinkException if <code>link</target> contains a symlink.
     * @throw HdfsIOException If an I/O error occurred
     */
    /*    virtual void createSymlink(const std::string &target,
                                   const std::string &link, const Permission &dirPerm,
                                   bool createParent)  throw (AccessControlException,
                 FileAlreadyExistsException, FileNotFoundException,
                 ParentNotDirectoryException, SafeModeException,
                 UnresolvedLinkException, HdfsIOException)  = 0;*/

    /**
     * Return the target of the given symlink. If there is an intermediate
     * symlink in the path (ie a symlink leading up to the final path component)
     * then the given path is returned with this symlink resolved.
     *
     * @param path The path with a link that needs resolution.
     * @param The path after resolving the first symbolic link in the path.
     * @throw AccessControlException permission denied
     * @throw FileNotFoundException If <code>path</code> does not exist
     * @throw HdfsIOException If the given path does not refer to a symlink
     *           or an I/O error occurred
     */
    //Idempotent
    /*    virtual std::string getLinkTarget(const std::string &path)
         throw (AccessControlException, FileNotFoundException,
         HdfsIOException)  = 0;*/

    /**
     * Get a new generation stamp together with an access token for
     * a block under construction
     *
     * This method is called only when a client needs to recover a failed
     * pipeline or set up a pipeline for appending to a block.
     *
     * @param block a block
     * @param clientName the name of the client
     * @param a located block with a new generation stamp and an access token
     * @param lb output the returned block.
     * @throw HdfsIOException if any error occurs
     */
    //Idempotent
    virtual shared_ptr<LocatedBlock> updateBlockForPipeline(const ExtendedBlock &block,
            const std::string &clientName)
    /* throw (HdfsIOException) */ = 0;

    /**
     * Update a pipeline for a block under construction
     *
     * @param clientName the name of the client
     * @param oldBlock the old block
     * @param newBlock the new block containing new generation stamp and length
     * @param newNodes datanodes in the pipeline
     * @throw HdfsIOException if any error occurs
     */
    virtual void updatePipeline(const std::string &clientName,
                                const ExtendedBlock &oldBlock, const ExtendedBlock &newBlock,
                                const std::vector<DatanodeInfo> &newNodes,
                                const std::vector<std::string> &storageIDs) /* throw (HdfsIOException) */ = 0;

    /**
      * Get a valid Delegation Token.
      *
      * @param renewer the designated renewer for the token
      * @return Token<DelegationTokenIdentifier>
      * @throws IOException
      */
    virtual Token getDelegationToken(const std::string &renewer)
    /* throws IOException*/ = 0;

    /**
     * Renew an existing delegation token.
     *
     * @param token delegation token obtained earlier
     * @return the new expiration time
     * @throws IOException
     */
    virtual int64_t renewDelegationToken(const Token &token)
    /*throws IOException*/ = 0;

    /**
     * Cancel an existing delegation token.
     *
     * @param token delegation token
     * @throws IOException
     */
    virtual void cancelDelegationToken(const Token &token)
    /*throws IOException*/ = 0;

    /**
     * close the namenode connection.
     */
    virtual void close() {};
};

}
}

#endif /* _HDFS_LIBHDFS3_SERVER_NAMENODE_H_ */
