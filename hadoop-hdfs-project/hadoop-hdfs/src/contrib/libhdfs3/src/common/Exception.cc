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

#include "Exception.h"

#include <sstream>

namespace hdfs {

const char *HdfsIOException::ReflexName = "java.io.IOException";

const char *AlreadyBeingCreatedException::ReflexName =
    "org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException";

const char *AccessControlException::ReflexName =
    "org.apache.hadoop.security.AccessControlException";

const char *FileAlreadyExistsException::ReflexName =
    "org.apache.hadoop.fs.FileAlreadyExistsException";

const char *DSQuotaExceededException::ReflexName =
    "org.apache.hadoop.hdfs.protocol.DSQuotaExceededException";

const char *NSQuotaExceededException::ReflexName =
    "org.apache.hadoop.hdfs.protocol.NSQuotaExceededException";

const char *ParentNotDirectoryException::ReflexName =
    "org.apache.hadoop.fs.ParentNotDirectoryException";

const char *SafeModeException::ReflexName =
    "org.apache.hadoop.hdfs.server.namenode.SafeModeException";

const char *NotReplicatedYetException::ReflexName =
    "org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException";

const char *FileNotFoundException::ReflexName = "java.io.FileNotFoundException";

const char *UnresolvedLinkException::ReflexName =
    "org.apache.hadoop.fs.UnresolvedLinkException";

const char *UnsupportedOperationException::ReflexName =
    "java.lang.UnsupportedOperationException";

const char *ReplicaNotFoundException::ReflexName =
    "org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException";

const char *NameNodeStandbyException::ReflexName =
    "org.apache.hadoop.ipc.StandbyException";

const char *HdfsInvalidBlockToken::ReflexName =
    "org.apache.hadoop.security.token.SecretManager$InvalidToken";

const char *SaslException::ReflexName = "javax.security.sasl.SaslException";

const char *RpcNoSuchMethodException::ReflexName = "org.apache.hadoop.ipc.RpcNoSuchMethodException";

const char *InvalidParameter::ReflexName = "java.lang.IllegalArgumentException";

HdfsException::HdfsException(const std::string &arg, const char *file,
                             int line, const char *stack) :
    std::runtime_error(arg) {
    std::ostringstream ss;
    ss << file << ": " << line << ": " << arg << std::endl << stack;
    detail = ss.str();
}

}
