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

#ifndef _HDFS_LIBHDFS3_COMMON_EXCEPTION_H_
#define _HDFS_LIBHDFS3_COMMON_EXCEPTION_H_

#include <stdexcept>
#include <string>

namespace hdfs {

class HdfsException: public std::runtime_error {
public:
    HdfsException(const std::string &arg, const char *file, int line,
                  const char *stack);

    ~HdfsException() throw () {
    }

    virtual const char *msg() const {
        return detail.c_str();
    }

protected:
    std::string detail;
};

class HdfsIOException: public HdfsException {
public:
    HdfsIOException(const std::string &arg, const char *file, int line,
                    const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~HdfsIOException() throw () {
    }

public:
    static const char *ReflexName;
};

class HdfsNetworkException: public HdfsIOException {
public:
    HdfsNetworkException(const std::string &arg, const char *file, int line,
                         const char *stack) :
        HdfsIOException(arg, file, line, stack) {
    }

    ~HdfsNetworkException() throw () {
    }
};

class HdfsNetworkConnectException: public HdfsNetworkException {
public:
    HdfsNetworkConnectException(const std::string &arg, const char *file, int line,
                                const char *stack) :
        HdfsNetworkException(arg, file, line, stack) {
    }

    ~HdfsNetworkConnectException() throw () {
    }
};

class AccessControlException: public HdfsException {
public:
    AccessControlException(const std::string &arg, const char *file, int line,
                           const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~AccessControlException() throw () {
    }

public:
    static const char *ReflexName;
};

class AlreadyBeingCreatedException: public HdfsException {
public:
    AlreadyBeingCreatedException(const std::string &arg, const char *file,
                                 int line, const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~AlreadyBeingCreatedException() throw () {
    }

public:
    static const char *ReflexName;
};

class ChecksumException: public HdfsException {
public:
    ChecksumException(const std::string &arg, const char *file, int line,
                      const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~ChecksumException() throw () {
    }
};

class DSQuotaExceededException: public HdfsException {
public:
    DSQuotaExceededException(const std::string &arg, const char *file,
                             int line, const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~DSQuotaExceededException() throw () {
    }

public:
    static const char *ReflexName;
};

class FileAlreadyExistsException: public HdfsException {
public:
    FileAlreadyExistsException(const std::string &arg, const char *file,
                               int line, const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~FileAlreadyExistsException() throw () {
    }

public:
    static const char *ReflexName;
};

class FileNotFoundException: public HdfsException {
public:
    FileNotFoundException(const std::string &arg, const char *file, int line,
                          const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~FileNotFoundException() throw () {
    }

public:
    static const char *ReflexName;
};

class HdfsBadBoolFoumat: public HdfsException {
public:
    HdfsBadBoolFoumat(const std::string &arg, const char *file, int line,
                      const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~HdfsBadBoolFoumat() throw () {
    }
};

class HdfsBadConfigFoumat: public HdfsException {
public:
    HdfsBadConfigFoumat(const std::string &arg, const char *file, int line,
                        const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~HdfsBadConfigFoumat() throw () {
    }
};

class HdfsBadNumFoumat: public HdfsException {
public:
    HdfsBadNumFoumat(const std::string &arg, const char *file, int line,
                     const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~HdfsBadNumFoumat() throw () {
    }
};

class HdfsCanceled: public HdfsException {
public:
    HdfsCanceled(const std::string &arg, const char *file, int line,
                 const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~HdfsCanceled() throw () {
    }
};

class HdfsFileSystemClosed: public HdfsException {
public:
    HdfsFileSystemClosed(const std::string &arg, const char *file, int line,
                         const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~HdfsFileSystemClosed() throw () {
    }
};

class HdfsConfigInvalid: public HdfsException {
public:
    HdfsConfigInvalid(const std::string &arg, const char *file, int line,
                      const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~HdfsConfigInvalid() throw () {
    }
};

class HdfsConfigNotFound: public HdfsException {
public:
    HdfsConfigNotFound(const std::string &arg, const char *file, int line,
                       const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~HdfsConfigNotFound() throw () {
    }
};

class HdfsEndOfStream: public HdfsIOException {
public:
    HdfsEndOfStream(const std::string &arg, const char *file, int line,
                    const char *stack) :
        HdfsIOException(arg, file, line, stack) {
    }

    ~HdfsEndOfStream() throw () {
    }
};

class HdfsInvalidBlockToken: public HdfsException {
public:
    HdfsInvalidBlockToken(const std::string &arg, const char *file, int line,
                          const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~HdfsInvalidBlockToken() throw () {
    }

public:
    static const char *ReflexName;
};

/**
 * This will wrap HdfsNetworkConnectionException and HdfsTimeoutException.
 * This exception will be caught and attempt will be performed to recover in HA case.
 */
class HdfsFailoverException: public HdfsException {
public:
    HdfsFailoverException(const std::string &arg, const char *file, int line,
                          const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~HdfsFailoverException() throw () {
    }
};

/**
 * Fatal error during the rpc call. It may wrap other exceptions.
 */
class HdfsRpcException: public HdfsIOException {
public:
    HdfsRpcException(const std::string &arg, const char *file, int line,
                     const char *stack) :
        HdfsIOException(arg, file, line, stack) {
    }

    ~HdfsRpcException() throw () {
    }
};

/**
 * Server throw an error during the rpc call.
 * It should be used internally and parsed for details.
 */
class HdfsRpcServerException: public HdfsIOException {
public:
    HdfsRpcServerException(const std::string &arg, const char *file, int line,
                           const char *stack) :
        HdfsIOException(arg, file, line, stack) {
    }

    ~HdfsRpcServerException() throw () {
    }

    const std::string &getErrClass() const {
        return errClass;
    }

    void setErrClass(const std::string &errClass) {
        this->errClass = errClass;
    }

    const std::string &getErrMsg() const {
        return errMsg;
    }

    void setErrMsg(const std::string &errMsg) {
        this->errMsg = errMsg;
    }

private:
    std::string errClass;
    std::string errMsg;
};

class HdfsTimeoutException: public HdfsException {
public:
    HdfsTimeoutException(const std::string &arg, const char *file, int line,
                         const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~HdfsTimeoutException() throw () {
    }
};

class InvalidParameter: public HdfsException {
public:
    InvalidParameter(const std::string &arg, const char *file, int line,
                     const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~InvalidParameter() throw () {
    }

public:
    static const char *ReflexName;
};

class InvalidPath: public HdfsException {
public:
    InvalidPath(const std::string &arg, const char *file, int line,
                const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~InvalidPath() throw () {
    }
};

class NotReplicatedYetException: public HdfsException {
public:
    NotReplicatedYetException(const std::string &arg, const char *file,
                              int line, const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~NotReplicatedYetException() throw () {
    }

public:
    static const char *ReflexName;
};

class NSQuotaExceededException: public HdfsException {
public:
    NSQuotaExceededException(const std::string &arg, const char *file,
                             int line, const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~NSQuotaExceededException() throw () {
    }

public:
    static const char *ReflexName;
};

class ParentNotDirectoryException: public HdfsException {
public:
    ParentNotDirectoryException(const std::string &arg, const char *file,
                                int line, const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~ParentNotDirectoryException() throw () {
    }

public:
    static const char *ReflexName;
};

class ReplicaNotFoundException: public HdfsException {
public:
    ReplicaNotFoundException(const std::string &arg, const char *file,
                             int line, const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~ReplicaNotFoundException() throw () {
    }

public:
    static const char *ReflexName;
};

class SafeModeException: public HdfsException {
public:
    SafeModeException(const std::string &arg, const char *file, int line,
                      const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~SafeModeException() throw () {
    }

public:
    static const char *ReflexName;
};

class UnresolvedLinkException: public HdfsException {
public:
    UnresolvedLinkException(const std::string &arg, const char *file,
                            int line, const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~UnresolvedLinkException() throw () {
    }

public:
    static const char *ReflexName;
};

class UnsupportedOperationException: public HdfsException {
public:
    UnsupportedOperationException(const std::string &arg, const char *file,
                                  int line, const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~UnsupportedOperationException() throw () {
    }

public:
    static const char *ReflexName;
};

class SaslException: public HdfsException {
public:
    SaslException(const std::string &arg, const char *file, int line,
                  const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~SaslException() throw () {
    }

public:
    static const char *ReflexName;
};

class NameNodeStandbyException: public HdfsException {
public:
    NameNodeStandbyException(const std::string &arg, const char *file,
                             int line, const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~NameNodeStandbyException() throw () {
    }

public:
    static const char *ReflexName;
};

class RpcNoSuchMethodException: public HdfsException {
public:
    RpcNoSuchMethodException(const std::string &arg, const char *file,
                             int line, const char *stack) :
        HdfsException(arg, file, line, stack) {
    }

    ~RpcNoSuchMethodException() throw () {
    }

public:
    static const char *ReflexName;
};

}

#endif /* _HDFS_LIBHDFS3_COMMON_EXCEPTION_H_ */
