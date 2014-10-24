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

#ifndef _HDFS_LIBHDFS3_COMMON_STATUSINTERNAL_H_
#define _HDFS_LIBHDFS3_COMMON_STATUSINTERNAL_H_

#include "Exception.h"
#include "ExceptionInternal.h"
#include "Status.h"

namespace hdfs {
namespace internal {

#define CHECK_PARAMETER(cond, code, msg) \
    if (!(cond)) {                       \
        return Status((code), (msg));    \
    }

static inline Status CreateStatusFromException(exception_ptr e) {
    try {
        hdfs::rethrow_exception(e);
    } catch (const hdfs::AccessControlException &e) {
        errno = EACCES;
        return Status(errno, e.what());
    } catch (const hdfs::AlreadyBeingCreatedException &e) {
        errno = EACCES;
        return Status(errno, e.what());
    } catch (const hdfs::ChecksumException &e) {
        errno = EIO;
        return Status(errno, e.what());
    } catch (const hdfs::DSQuotaExceededException &e) {
        errno = ENOSPC;
        return Status(errno, e.what());
    } catch (const hdfs::FileAlreadyExistsException &e) {
        errno = EEXIST;
        return Status(errno, e.what());
    } catch (const hdfs::FileNotFoundException &e) {
        errno = ENOENT;
        return Status(errno, e.what());
    } catch (const hdfs::HdfsBadBoolFoumat &e) {
        errno = EINVAL;
        return Status(errno, e.what());
    } catch (const hdfs::HdfsBadConfigFoumat &e) {
        errno = EINVAL;
        return Status(errno, e.what());
    } catch (const hdfs::HdfsBadNumFoumat &e) {
        errno = EINVAL;
        return Status(errno, e.what());
    } catch (const hdfs::HdfsCanceled &e) {
        errno = EIO;
        return Status(errno, e.what());
    } catch (const hdfs::HdfsConfigInvalid &e) {
        errno = EINVAL;
        return Status(errno, e.what());
    } catch (const hdfs::HdfsConfigNotFound &e) {
        errno = EINVAL;
        return Status(errno, e.what());
    } catch (const hdfs::HdfsEndOfStream &e) {
        errno = EOVERFLOW;
        return Status(errno, e.what());
    } catch (const hdfs::HdfsInvalidBlockToken &e) {
        errno = EPERM;
        return Status(errno, e.what());
    } catch (const hdfs::HdfsTimeoutException &e) {
        errno = EIO;
        return Status(errno, e.what());
    } catch (const hdfs::InvalidParameter &e) {
        errno = EINVAL;
        return Status(errno, e.what());
    } catch (const hdfs::InvalidPath &e) {
        errno = EINVAL;
        return Status(errno, e.what());
    } catch (const hdfs::NotReplicatedYetException &e) {
        errno = EINVAL;
        return Status(errno, e.what());
    } catch (const hdfs::NSQuotaExceededException &e) {
        errno = EINVAL;
        return Status(errno, e.what());
    } catch (const hdfs::ParentNotDirectoryException &e) {
        errno = EACCES;
        return Status(errno, e.what());
    } catch (const hdfs::ReplicaNotFoundException &e) {
        errno = EACCES;
        return Status(errno, e.what());
    } catch (const hdfs::SafeModeException &e) {
        errno = EIO;
        return Status(errno, e.what());
    } catch (const hdfs::UnresolvedLinkException &e) {
        errno = EACCES;
        return Status(errno, e.what());
    } catch (const hdfs::HdfsRpcException &e) {
        errno = EIO;
        return Status(errno, e.what());
    } catch (const hdfs::HdfsNetworkException &e) {
        errno = EIO;
        return Status(errno, e.what());
    } catch (const hdfs::RpcNoSuchMethodException &e) {
        errno = ENOTSUP;
        return Status(errno, e.what());
    } catch (const hdfs::SaslException &e) {
        errno = EACCES;
        return Status(errno, e.what());
    } catch (const hdfs::NameNodeStandbyException &e) {
        errno = EIO;
        return Status(errno, e.what());
    } catch (const hdfs::HdfsIOException &e) {
        errno = EIO;
        return Status(errno, e.what());
    } catch (const hdfs::HdfsException &e) {
        errno = EIO;
        return Status(errno, e.what());
    } catch (std::exception &e) {
        errno = EIO;
        return Status(errno, e.what());
    }

    return Status(EIO, "Unknown Error");
}
}
}

#endif /* _HDFS_LIBHDFS3_COMMON_STATUSINTERNAL_H_ */
