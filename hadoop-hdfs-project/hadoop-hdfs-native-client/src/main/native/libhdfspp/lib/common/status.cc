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

#include "hdfspp/status.h"

#include <cassert>
#include <sstream>
#include <cstring>
#include <map>
#include <set>

namespace hdfs {

//  Server side exceptions that we capture from the RpcResponseHeaderProto
const char * kStatusAccessControlException     = "org.apache.hadoop.security.AccessControlException";
const char * kPathIsNotDirectoryException      = "org.apache.hadoop.fs.PathIsNotDirectoryException";
const char * kSnapshotException                = "org.apache.hadoop.hdfs.protocol.SnapshotException";
const char * kStatusStandbyException           = "org.apache.hadoop.ipc.StandbyException";
const char * kStatusSaslException              = "javax.security.sasl.SaslException";
const char * kPathNotFoundException            = "org.apache.hadoop.fs.InvalidPathException";
const char * kPathNotFoundException2           = "java.io.FileNotFoundException";
const char * kFileAlreadyExistsException       = "org.apache.hadoop.fs.FileAlreadyExistsException";
const char * kPathIsNotEmptyDirectoryException = "org.apache.hadoop.fs.PathIsNotEmptyDirectoryException";


const static std::map<std::string, int> kKnownServerExceptionClasses = {
                                            {kStatusAccessControlException, Status::kAccessControlException},
                                            {kPathIsNotDirectoryException, Status::kNotADirectory},
                                            {kSnapshotException, Status::kSnapshotProtocolException},
                                            {kStatusStandbyException, Status::kStandbyException},
                                            {kStatusSaslException, Status::kAuthenticationFailed},
                                            {kPathNotFoundException, Status::kPathNotFound},
                                            {kPathNotFoundException2, Status::kPathNotFound},
                                            {kFileAlreadyExistsException, Status::kFileAlreadyExists},
                                            {kPathIsNotEmptyDirectoryException, Status::kPathIsNotEmptyDirectory}
                                        };

// Errors that retry cannot fix. TODO: complete the list.
const static std::set<int> noRetryExceptions = {
  Status::kPermissionDenied,
  Status::kAuthenticationFailed,
  Status::kAccessControlException
};

Status::Status(int code, const char *msg1)
               : code_(code) {
  if(msg1) {
    msg_ = msg1;
  }
}

Status::Status(int code, const char *exception_class_name, const char *exception_details)
               : code_(code) {
  // If we can assure this never gets nullptr args this can be
  // in the initializer list.
  if(exception_class_name)
    exception_class_ = exception_class_name;
  if(exception_details)
    msg_ = exception_details;

  std::map<std::string, int>::const_iterator it = kKnownServerExceptionClasses.find(exception_class_);
  if(it != kKnownServerExceptionClasses.end()) {
    code_ = it->second;
  }
}


Status Status::OK() {
  return Status();
}

Status Status::InvalidArgument(const char *msg) {
  return Status(kInvalidArgument, msg);
}

Status Status::PathNotFound(const char *msg){
  return Status(kPathNotFound, msg);
}

Status Status::ResourceUnavailable(const char *msg) {
  return Status(kResourceUnavailable, msg);
}

Status Status::PathIsNotDirectory(const char *msg) {
  return Status(kNotADirectory, msg);
}

Status Status::Unimplemented() {
  return Status(kUnimplemented, "");
}

Status Status::Exception(const char *exception_class_name, const char *error_message) {
  // Server side exception but can be represented by std::errc codes
  if (exception_class_name && (strcmp(exception_class_name, kStatusAccessControlException) == 0) )
    return Status(kPermissionDenied, error_message);
  else if (exception_class_name && (strcmp(exception_class_name, kStatusSaslException) == 0))
    return AuthenticationFailed();
  else if (exception_class_name && (strcmp(exception_class_name, kPathNotFoundException) == 0))
    return Status(kPathNotFound, error_message);
  else if (exception_class_name && (strcmp(exception_class_name, kPathNotFoundException2) == 0))
    return Status(kPathNotFound, error_message);
  else if (exception_class_name && (strcmp(exception_class_name, kPathIsNotDirectoryException) == 0))
    return Status(kNotADirectory, error_message);
  else if (exception_class_name && (strcmp(exception_class_name, kSnapshotException) == 0))
    return Status(kInvalidArgument, error_message);
  else if (exception_class_name && (strcmp(exception_class_name, kFileAlreadyExistsException) == 0))
    return Status(kFileAlreadyExists, error_message);
  else if (exception_class_name && (strcmp(exception_class_name, kPathIsNotEmptyDirectoryException) == 0))
    return Status(kPathIsNotEmptyDirectory, error_message);
  else
    return Status(kException, exception_class_name, error_message);
}

Status Status::Error(const char *error_message) {
  return Exception("Exception", error_message);
}

Status Status::AuthenticationFailed() {
  return Status::AuthenticationFailed(nullptr);
}

Status Status::AuthenticationFailed(const char *msg) {
  std::string formatted = "AuthenticationFailed";
  if(msg) {
    formatted += ": ";
    formatted += msg;
  }
  return Status(kAuthenticationFailed, formatted.c_str());
}

Status Status::AuthorizationFailed() {
  return Status::AuthorizationFailed(nullptr);
}

Status Status::AuthorizationFailed(const char *msg) {
  std::string formatted = "AuthorizationFailed";
  if(msg) {
    formatted += ": ";
    formatted += msg;
  }
  return Status(kPermissionDenied, formatted.c_str());
}

Status Status::Canceled() {
  return Status(kOperationCanceled, "Operation canceled");
}

Status Status::InvalidOffset(const char *msg){
  return Status(kInvalidOffset, msg);
}

std::string Status::ToString() const {
  if (code_ == kOk) {
    return "OK";
  }
  std::stringstream ss;
  if(!exception_class_.empty()) {
    ss << exception_class_ << ":";
  }
  ss << msg_;
  return ss.str();
}

bool Status::notWorthRetry() const {
  return noRetryExceptions.find(code_) != noRetryExceptions.end();
}

Status Status::MutexError(const char *msg) {
  std::string formatted = "MutexError";
  if(msg) {
    formatted += ": ";
    formatted += msg;
  }
  return Status(kBusy/*try_lock failure errno*/, msg);
}

}
