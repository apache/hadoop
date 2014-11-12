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

#include "DirectoryIterator.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "FileSystem.h"
#include "FileSystemImpl.h"
#include "FileSystemKey.h"
#include "Hash.h"
#include "SessionConfig.h"
#include "StatusInternal.h"
#include "Thread.h"
#include "UnorderedMap.h"
#include "WritableUtils.h"

#include <algorithm>
#include <string>
#include <krb5/krb5.h>

using namespace hdfs::internal;

namespace hdfs {

namespace internal {

static std::string ExtractPrincipalFromTicketCache(
    const std::string &cachePath) {
    krb5_context cxt = NULL;
    krb5_ccache ccache = NULL;
    krb5_principal principal = NULL;
    krb5_error_code ec = 0;
    std::string errmsg, retval;
    char *priName = NULL;

    if (!cachePath.empty()) {
        if (0 != setenv("KRB5CCNAME", cachePath.c_str(), 1)) {
            THROW(HdfsIOException, "Cannot set env parameter \"KRB5CCNAME\"");
        }
    }

    do {
        if (0 != (ec = krb5_init_context(&cxt))) {
            break;
        }

        if (0 != (ec = krb5_cc_default(cxt, &ccache))) {
            break;
        }

        if (0 != (ec = krb5_cc_get_principal(cxt, ccache, &principal))) {
            break;
        }

        if (0 != (ec = krb5_unparse_name(cxt, principal, &priName))) {
            break;
        }
    } while (0);

    if (!ec) {
        retval = priName;
    } else {
        if (cxt) {
            errmsg = krb5_get_error_message(cxt, ec);
        } else {
            errmsg = "Cannot initialize kerberos context";
        }
    }

    if (priName != NULL) {
        krb5_free_unparsed_name(cxt, priName);
    }

    if (principal != NULL) {
        krb5_free_principal(cxt, principal);
    }

    if (ccache != NULL) {
        krb5_cc_close(cxt, ccache);
    }

    if (cxt != NULL) {
        krb5_free_context(cxt);
    }

    if (!errmsg.empty()) {
        THROW(HdfsIOException,
              "FileSystem: Filed to extract principal from ticket cache: %s",
              errmsg.c_str());
    }

    return retval;
}

static std::string ExtractPrincipalFromToken(const Token &token) {
    std::string realUser, owner;
    std::string identifier = token.getIdentifier();
    WritableUtils cin(&identifier[0], identifier.size());
    char version;

    try {
        version = cin.readByte();

        if (version != 0) {
            THROW(HdfsIOException, "Unknown version of delegation token");
        }

        owner = cin.ReadText();
        cin.ReadText();
        realUser = cin.ReadText();
        return realUser.empty() ? owner : realUser;
    } catch (const std::range_error &e) {
    }

    THROW(HdfsIOException, "Cannot extract principal from token");
}
}

FileSystem::FileSystem(const Config &conf) : conf_(conf) {
}

FileSystem::~FileSystem() {
    impl.reset();
}

Status FileSystem::connect() {
    try {
        internal::SessionConfig sconf(conf_);
        return connect(sconf.getDefaultUri().c_str(), NULL, NULL);
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::connect(const std::string &uri) {
    try {
        connect(uri, NULL, NULL);
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

static shared_ptr<FileSystemImpl> ConnectInternal(const std::string &uri,
                                                  const std::string &principal,
                                                  const Token *token,
                                                  Config &conf) {
    if (uri.empty()) {
        THROW(InvalidParameter, "Invalid HDFS uri.");
    }

    FileSystemKey key(uri, principal.c_str());

    if (token) {
        key.addToken(*token);
    }

    return shared_ptr<FileSystemImpl>(new FileSystemImpl(key, conf));
}

Status FileSystem::connect(const std::string &uri, const std::string &username,
                           const std::string &token) {
    AuthMethod auth;
    std::string principal;
    CHECK_PARAMETER(!impl, EIO, "FileSystem: already connected.");

    try {
        SessionConfig sconf(conf_);
        auth = RpcAuth::ParseMethod(sconf.getRpcAuthMethod());

        if (!token.empty() && auth != AuthMethod::SIMPLE) {
            Token t;
            t.fromString(token);
            principal = ExtractPrincipalFromToken(t);
            impl = ConnectInternal(uri, principal, &t, conf_);
            impl->connect();
            return Status::OK();
        } else if (!username.empty()) {
            principal = username;
        }

        if (auth == AuthMethod::KERBEROS) {
            principal =
                ExtractPrincipalFromTicketCache(sconf.getKerberosCachePath());
        }

        impl = ConnectInternal(uri, principal, NULL, conf_);
        impl->connect();
    } catch (...) {
        impl.reset();
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

void FileSystem::disconnect() {
    impl.reset();
}

Status FileSystem::getDefaultReplication(int *output) const {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getDefaultReplication();
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::getDefaultBlockSize(int64_t *output) const {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getDefaultBlockSize();
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::getHomeDirectory(std::string *output) const {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getHomeDirectory();
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::deletePath(const std::string &path, bool recursive) {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");

    try {
        if (false == impl->deletePath(path.c_str(), recursive)) {
            return Status(EIO);
        }
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::mkdir(const std::string &path,
                         const Permission &permission) {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");

    try {
        if (false == impl->mkdir(path.c_str(), permission)) {
            return Status(EIO);
        }
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::mkdirs(const std::string &path,
                          const Permission &permission) {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");

    try {
        if (false == impl->mkdirs(path.c_str(), permission)) {
            return Status(EIO);
        }
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::getFileStatus(const std::string &path,
                                 FileStatus *output) const {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getFileStatus(path.c_str());
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::getFileBlockLocations(const std::string &path, int64_t start,
                                         int64_t len,
                                         std::vector<BlockLocation> *output) {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getFileBlockLocations(path.c_str(), start, len);
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::listDirectory(const std::string &path,
                                 DirectoryIterator *output) {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->listDirectory(path.c_str(), false);
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::setOwner(const std::string &path,
                            const std::string &username,
                            const std::string &groupname) {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");

    try {
        impl->setOwner(path.c_str(), username.c_str(), groupname.c_str());
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::setTimes(const std::string &path, int64_t mtime,
                            int64_t atime) {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");

    try {
        impl->setTimes(path.c_str(), mtime, atime);
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::setPermission(const std::string &path,
                                 const Permission &permission) {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");

    try {
        impl->setPermission(path.c_str(), permission);
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::setReplication(const std::string &path, short replication) {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");

    try {
        if (false == impl->setReplication(path.c_str(), replication)) {
            return Status(EIO);
        }
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::rename(const std::string &src, const std::string &dst) {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");

    try {
        if (false == impl->rename(src.c_str(), dst.c_str())) {
            return Status(EIO);
        }
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::setWorkingDirectory(const std::string &path) {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");

    try {
        impl->setWorkingDirectory(path.c_str());
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::getWorkingDirectory(std::string *output) const {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getWorkingDirectory();
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::exist(const std::string &path) const {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");

    try {
        if (false == impl->exist(path.c_str())) {
            return Status(ENOENT);
        }
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::getStats(FileSystemStats *output) const {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getFsStats();
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::getDelegationToken(const std::string &renewer,
                                      std::string *output) {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getDelegationToken(renewer.c_str());
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::getDelegationToken(std::string *output) {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getDelegationToken();
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::renewDelegationToken(const std::string &token,
                                        int64_t *output) {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->renewDelegationToken(token.c_str());
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status FileSystem::cancelDelegationToken(const std::string &token) {
    CHECK_PARAMETER(impl, EIO, "FileSystem: not connected.");

    try {
        impl->cancelDelegationToken(token.c_str());
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}
}
