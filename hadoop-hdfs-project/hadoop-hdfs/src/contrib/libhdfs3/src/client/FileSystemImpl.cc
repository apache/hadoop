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
#include "BlockLocation.h"
#include "DirectoryIterator.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "FileStatus.h"
#include "FileSystemImpl.h"
#include "FileSystemStats.h"
#include "InputStream.h"
#include "Logger.h"
#include "StringUtil.h"
#include "server/LocatedBlocks.h"
#include "server/NamenodeInfo.h"
#include "server/NamenodeProxy.h"

#include <cstring>
#include <deque>
#include <inttypes.h>
#include <strings.h>

using std::string;
using std::deque;
using std::vector;

namespace hdfs {
namespace internal {

static const std::string GetAbsPath(const std::string &prefix,
                                    const std::string &path) {
    if (path.empty()) {
        return prefix;
    }

    if ('/' == path[0]) {
        return path;
    } else {
        return prefix + "/" + path;
    }
}

/*
 * Return the canonical absolute name of file NAME.
 * A canonical name does not contain any `.', `..' components nor any repeated
 * path separators ('/')
 */
static const std::string CanonicalizePath(const std::string &path) {
    int skip = 0;
    string retval;
    vector<string> components = StringSplit(path, "/");
    deque<string> tmp;
    vector<string>::reverse_iterator s = components.rbegin();

    while (s != components.rend()) {
        if (s->empty() || *s == ".") {
            ++s;
        } else if (*s == "..") {
            ++skip;
            ++s;
        } else {
            if (skip <= 0) {
                tmp.push_front(*s);
            } else {
                --skip;
            }

            ++s;
        }
    }

    for (size_t i = 0; i < tmp.size(); ++i) {
        retval += "/";
        retval += tmp[i];
    }

    return retval.empty() ? "/" : retval;
}

FileSystemImpl::FileSystemImpl(const FileSystemKey &key, const Config &c)
    : conf(c),
      key(key),
      leaseRenewer(this),
      nn(NULL),
      sconf(c),
      user(key.getUser()) {
    static atomic<uint32_t> count(0);
    std::stringstream ss;
    srand((unsigned int)time(NULL));
    ss << "libhdfs3_client_random_" << rand() << "_count_" << ++count << "_pid_"
       << getpid() << "_tid_" << pthread_self();
    clientName = ss.str();
    workingDir = std::string("/user/") + user.getEffectiveUser();
#ifdef MOCK
    stub = NULL;
#endif
    // set log level
    RootLogger.setLogSeverity(sconf.getLogSeverity());
}

/**
 * Destroy a FileSystemBase instance
 */
FileSystemImpl::~FileSystemImpl() {
    try {
        disconnect();
    } catch (...) {
    }
}

const std::string FileSystemImpl::getStandardPath(const char *path) {
    std::string base;
    {
        lock_guard<mutex> lock(mutWorkingDir);
        base = workingDir;
    }
    return CanonicalizePath(GetAbsPath(base, path));
}

const char *FileSystemImpl::getClientName() {
    return clientName.c_str();
}

void FileSystemImpl::connect() {
    std::string host, port, uri;
    std::vector<NamenodeInfo> namenodeInfos;

    if (nn) {
        THROW(HdfsIOException, "FileSystemImpl: already connected.");
    }

    host = key.getHost();
    port = key.getPort();
    uri += key.getScheme() + "://" + host;

    if (port.empty()) {
        try {
            Status status = NamenodeInfo::GetHANamenodeInfo(key.getHost(), conf,
                                                            &namenodeInfos);
        } catch (const HdfsConfigNotFound &e) {
            NESTED_THROW(InvalidParameter,
                         "Cannot parse URI: %s, missing port or invalid HA "
                         "configuration",
                         uri.c_str());
        }

        tokenService = "ha-hdfs:";
        tokenService += host;
    } else {
        std::stringstream ss;
        ss << host << ":" << port;
        namenodeInfos.resize(1);
        namenodeInfos[0].setRpcAddr(ss.str());
        tokenService = namenodeInfos[0].getRpcAddr();
    }

#ifdef MOCK
    nn = stub->getNamenode();
#else
    nn = new NamenodeProxy(
        namenodeInfos, tokenService, sconf,
        RpcAuth(user, RpcAuth::ParseMethod(sconf.getRpcAuthMethod())));
#endif
    /*
     * To test if the connection is ok
     */
    getFsStats();
}

/**
 * disconnect from hdfs
 */
void FileSystemImpl::disconnect() {
    if (nn) {
        nn->close();
        delete nn;
    }

    nn = NULL;
}

/**
 * To get default number of replication.
 * @return the default number of replication.
 */
int FileSystemImpl::getDefaultReplication() const {
    return sconf.getDefaultReplica();
}

/**
 * To get the default number of block size.
 * @return the default block size.
 */
int64_t FileSystemImpl::getDefaultBlockSize() const {
    return sconf.getDefaultBlockSize();
}

/**
 * To get the home directory.
 * @return home directory.
 */
std::string FileSystemImpl::getHomeDirectory() const {
    return std::string("/user/") + user.getEffectiveUser();
}

/**
 * To delete a file or directory.
 * @param path the path to be deleted.
 * @param recursive if path is a directory, delete the contents recursively.
 * @return return true if success.
 */

bool FileSystemImpl::deletePath(const char *path, bool recursive) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    if (NULL == path || !strlen(path)) {
        THROW(InvalidParameter, "Invalid input: path should not be empty");
    }

    return nn->deleteFile(getStandardPath(path), recursive);
}

/**
 * To create a directory which given permission.
 * @param path the directory path which is to be created.
 * @param permission directory permission.
 * @return return true if success.
 */

bool FileSystemImpl::mkdir(const char *path, const Permission &permission) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    if (NULL == path || !strlen(path)) {
        THROW(InvalidParameter, "Invalid input: path should not be empty");
    }

    return nn->mkdirs(getStandardPath(path), permission, false);
}

/**
 * To create a directory which given permission.
 * If parent path does not exits, create it.
 * @param path the directory path which is to be created.
 * @param permission directory permission.
 * @return return true if success.
 */

bool FileSystemImpl::mkdirs(const char *path, const Permission &permission) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    if (NULL == path || !strlen(path)) {
        THROW(InvalidParameter, "Invalid input: path should not be empty");
    }

    return nn->mkdirs(getStandardPath(path), permission, true);
}

/**
 * To get path information.
 * @param path the path which information is to be returned.
 * @return the path information.
 */
FileStatus FileSystemImpl::getFileStatus(const char *path) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    if (NULL == path || !strlen(path)) {
        THROW(InvalidParameter, "Invalid input: path should not be empty");
    }

    return nn->getFileInfo(getStandardPath(path));
}

static void Convert(BlockLocation &bl, const LocatedBlock &lb) {
    const std::vector<DatanodeInfo> &nodes = lb.getLocations();
    bl.setCorrupt(lb.isCorrupt());
    bl.setLength(lb.getNumBytes());
    bl.setOffset(lb.getOffset());
    std::vector<std::string> hosts(nodes.size());
    std::vector<std::string> names(nodes.size());
    std::vector<std::string> topologyPaths(nodes.size());

    for (size_t i = 0; i < nodes.size(); ++i) {
        hosts[i] = nodes[i].getHostName();
        names[i] = nodes[i].getXferAddr();
        topologyPaths[i] =
            nodes[i].getLocation() + '/' + nodes[i].getXferAddr();
    }

    bl.setNames(names);
    bl.setHosts(hosts);
    bl.setTopologyPaths(topologyPaths);
}

std::vector<BlockLocation> FileSystemImpl::getFileBlockLocations(
    const char *path, int64_t start, int64_t len) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    if (NULL == path || !strlen(path)) {
        THROW(InvalidParameter, "Invalid input: path should not be empty");
    }

    if (start < 0) {
        THROW(InvalidParameter,
              "Invalid input: start offset should be positive");
    }

    if (len < 0) {
        THROW(InvalidParameter, "Invalid input: length should be positive");
    }

    LocatedBlocks lbs;
    nn->getBlockLocations(getStandardPath(path), start, len, lbs);
    std::vector<LocatedBlock> blocks = lbs.getBlocks();
    std::vector<BlockLocation> retval(blocks.size());

    for (size_t i = 0; i < blocks.size(); ++i) {
        Convert(retval[i], blocks[i]);
    }

    return retval;
}

/**
 * list the contents of a directory.
 * @param path the directory path.
 * @return return the path informations in the given directory.
 */
DirectoryIterator FileSystemImpl::listDirectory(const char *path,
                                                bool needLocation) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    if (NULL == path || !strlen(path)) {
        THROW(InvalidParameter, "Invalid input: path should not be empty");
    }

    return DirectoryIterator(this, getStandardPath(path), needLocation);
}

/**
 * To set the owner and the group of the path.
 * username and groupname cannot be empty at the same time.
 * @param path the path which owner of group is to be changed.
 * @param username new user name.
 * @param groupname new group.
 */
void FileSystemImpl::setOwner(const char *path, const char *username,
                              const char *groupname) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    if (NULL == path || !strlen(path)) {
        THROW(InvalidParameter, "Invalid input: path should not be empty");
    }

    if ((NULL == username || !strlen(username)) &&
        (NULL == groupname || !strlen(groupname))) {
        THROW(InvalidParameter,
              "Invalid input: username and groupname should not be empty");
    }

    nn->setOwner(getStandardPath(path), username != NULL ? username : "",
                 groupname != NULL ? groupname : "");
}

/**
 * To set the access time or modification time of a path.
 * @param path the path which access time or modification time is to be changed.
 * @param mtime new modification time.
 * @param atime new access time.
 */
void FileSystemImpl::setTimes(const char *path, int64_t mtime, int64_t atime) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    if (NULL == path || !strlen(path)) {
        THROW(InvalidParameter, "Invalid input: path should not be empty");
    }

    nn->setTimes(getStandardPath(path), mtime, atime);
}

/**
 * To set the permission of a path.
 * @param path the path which permission is to be changed.
 * @param permission new permission.
 */
void FileSystemImpl::setPermission(const char *path,
                                   const Permission &permission) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    if (NULL == path || !strlen(path)) {
        THROW(InvalidParameter, "Invalid input: path should not be empty");
    }

    nn->setPermission(getStandardPath(path), permission);
}

/**
 * To set the number of replication.
 * @param path the path which number of replication is to be changed.
 * @param replication new number of replication.
 * @return return true if success.
 */

bool FileSystemImpl::setReplication(const char *path, short replication) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    if (NULL == path || !strlen(path)) {
        THROW(InvalidParameter, "Invalid input: path should not be empty");
    }

    return nn->setReplication(getStandardPath(path), replication);
}

/**
 * To rename a path.
 * @param src old path.
 * @param dst new path.
 * @return return true if success.
 */

bool FileSystemImpl::rename(const char *src, const char *dst) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    if (NULL == src || !strlen(src)) {
        THROW(InvalidParameter, "Invalid input: src should not be empty");
    }

    if (NULL == dst || !strlen(dst)) {
        THROW(InvalidParameter, "Invalid input: dst should not be empty");
    }

    return nn->rename(getStandardPath(src), getStandardPath(dst));
}

/**
 * To set working directory.
 * @param path new working directory.
 */
void FileSystemImpl::setWorkingDirectory(const char *path) {
    if (NULL == path) {
        THROW(InvalidParameter, "Invalid input: path should not be empty");
    }

    if (!strlen(path) || '/' != path[0]) {
        THROW(InvalidParameter,
              "Invalid input: path should be an absolute path");
    }

    lock_guard<mutex> lock(mutWorkingDir);
    workingDir = path;
}

/**
 * To get working directory.
 * @return working directory.
 */
std::string FileSystemImpl::getWorkingDirectory() const {
    return workingDir;
}

/**
 * To test if the path exist.
 * @param path the path which is to be tested.
 * @return return true if the path exist.
 */

bool FileSystemImpl::exist(const char *path) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    if (NULL == path || !strlen(path)) {
        THROW(InvalidParameter, "Invalid input: path should not be empty");
    }

    try {
        getFileStatus(path);
    } catch (const FileNotFoundException &e) {
        return false;
    }

    return true;
}

/**
 * To get the file system status.
 * @return the file system status.
 */
FileSystemStats FileSystemImpl::getFsStats() {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    std::vector<int64_t> retval = nn->getFsStats();
    assert(retval.size() >= 3);
    return FileSystemStats(retval[0], retval[1], retval[2]);
}

static std::string ConstructTempFilePath(const std::string &path,
                                         const std::string clientName) {
    std::stringstream ss;
    srand((unsigned int)time(NULL));
    static atomic<uint32_t> count(0);
    std::vector<std::string> components = StringSplit(path, "/");
    ss << '/';

    for (size_t i = components.size(); i > 0; --i) {
        if (!components[i - 1].empty()) {
            components[i - 1].clear();
            break;
        }
    }

    for (size_t i = 0; i < components.size(); ++i) {
        if (!components[i].empty()) {
            ss << components[i] << '/';
        }
    }

    ss << "._client_" << clientName << "_random_" << rand() << "_count_"
       << ++count << "_tid_" << pthread_self() << "_TRUNCATE_TMP";
    return ss.str();
}

std::string FileSystemImpl::getDelegationToken(const char *renewer) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    if (NULL == renewer || !strlen(renewer)) {
        THROW(InvalidParameter, "Invalid input: renewer should not be empty.");
    }

    Token retval = nn->getDelegationToken(renewer);
    retval.setService(tokenService);
    return retval.toString();
}

std::string FileSystemImpl::getDelegationToken() {
    return getDelegationToken(key.getUser().getPrincipal().c_str());
}

int64_t FileSystemImpl::renewDelegationToken(const std::string &token) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    Token t;
    t.fromString(token);
    return nn->renewDelegationToken(t);
}

void FileSystemImpl::cancelDelegationToken(const std::string &token) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    Token t;
    t.fromString(token);
    nn->cancelDelegationToken(t);
}

void FileSystemImpl::getBlockLocations(const std::string &src, int64_t offset,
                                       int64_t length, LocatedBlocks &lbs) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    nn->getBlockLocations(src, offset, length, lbs);
}

void FileSystemImpl::create(const std::string &src, const Permission &masked,
                            int flag, bool createParent, short replication,
                            int64_t blockSize) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    nn->create(src, masked, clientName, flag, createParent, replication,
               blockSize);
}

shared_ptr<LocatedBlock> FileSystemImpl::append(const std::string &src) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    return nn->append(src, clientName);
}

void FileSystemImpl::abandonBlock(const ExtendedBlock &b,
                                  const std::string &src) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    nn->abandonBlock(b, src, clientName);
}

shared_ptr<LocatedBlock> FileSystemImpl::addBlock(
    const std::string &src, const ExtendedBlock *previous,
    const std::vector<DatanodeInfo> &excludeNodes) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    return nn->addBlock(src, clientName, previous, excludeNodes);
}

shared_ptr<LocatedBlock> FileSystemImpl::getAdditionalDatanode(
    const std::string &src, const ExtendedBlock &blk,
    const std::vector<DatanodeInfo> &existings,
    const std::vector<std::string> &storageIDs,
    const std::vector<DatanodeInfo> &excludes, int numAdditionalNodes) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    return nn->getAdditionalDatanode(src, blk, existings, storageIDs, excludes,
                                     numAdditionalNodes, clientName);
}

bool FileSystemImpl::complete(const std::string &src,
                              const ExtendedBlock *last) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    return nn->complete(src, clientName, last);
}

/*void FileSystemImpl::reportBadBlocks(const std::vector<LocatedBlock> &blocks)
{
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    nn->reportBadBlocks(blocks);
}*/

void FileSystemImpl::fsync(const std::string &src) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    nn->fsync(src, clientName);
}

shared_ptr<LocatedBlock> FileSystemImpl::updateBlockForPipeline(
    const ExtendedBlock &block) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    return nn->updateBlockForPipeline(block, clientName);
}

void FileSystemImpl::updatePipeline(
    const ExtendedBlock &oldBlock, const ExtendedBlock &newBlock,
    const std::vector<DatanodeInfo> &newNodes,
    const std::vector<std::string> &storageIDs) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    nn->updatePipeline(clientName, oldBlock, newBlock, newNodes, storageIDs);
}

bool FileSystemImpl::getListing(const std::string &src,
                                const std::string &startAfter,
                                bool needLocation,
                                std::vector<FileStatus> &dl) {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    return nn->getListing(src, startAfter, needLocation, dl);
}

void FileSystemImpl::renewLease() {
    if (!nn) {
        THROW(HdfsIOException, "FileSystemImpl: not connected.");
    }

    try {
        nn->renewLease(clientName);
    } catch (const HdfsException &e) {
        LOG(LOG_ERROR,
            "Failed to renew lease for filesystem which client name "
            "is %s, since:\n%s",
            getClientName(), GetExceptionDetail(e));
    } catch (const std::exception &e) {
        LOG(LOG_ERROR,
            "Failed to renew lease for filesystem which client name is "
            "%s, since:\n%s",
            getClientName(), e.what());
    }
}

void FileSystemImpl::registerOpenedOutputStream() {
    leaseRenewer.StartRenew();
}

void FileSystemImpl::unregisterOpenedOutputStream() {
    leaseRenewer.StopRenew();
}
}
}
