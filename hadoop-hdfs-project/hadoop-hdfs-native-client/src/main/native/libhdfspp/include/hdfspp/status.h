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
#ifndef LIBHDFSPP_STATUS_H_
#define LIBHDFSPP_STATUS_H_

#include <string>
#include <system_error>

namespace hdfs {

class Status {
 public:
  // Create a success status.
  Status() : code_(0) {}

  // Note: Avoid calling the Status constructors directly, call the factory methods instead

  // Used for common status  types
  Status(int code, const char *msg);
  // Used for server side exceptions reported through RpcResponseProto and similar
  Status(int code, const char *exception_class, const char *exception_details);

  // Factory methods
  static Status OK();
  static Status InvalidArgument(const char *msg);
  static Status ResourceUnavailable(const char *msg);
  static Status Unimplemented();
  static Status Exception(const char *exception_class_name, const char *exception_details);
  static Status Error(const char *error_message);
  static Status AuthenticationFailed();
  static Status AuthenticationFailed(const char *msg);
  static Status AuthorizationFailed();
  static Status AuthorizationFailed(const char *msg);
  static Status Canceled();
  static Status PathNotFound(const char *msg);
  static Status InvalidOffset(const char *msg);
  static Status PathIsNotDirectory(const char *msg);
  static Status MutexError(const char *msg);

  // success
  bool ok() const { return code_ == 0; }

  bool is_invalid_offset() const { return code_ == kInvalidOffset; }

  // contains ENOENT error
  bool pathNotFound() const { return code_ == kPathNotFound; }

  // Returns the string "OK" for success.
  std::string ToString() const;

  // get error code
  int code() const { return code_; }

  // if retry can possibly recover an error
  bool notWorthRetry() const;

  enum Code {
    kOk = 0,
    kInvalidArgument = static_cast<unsigned>(std::errc::invalid_argument),
    kResourceUnavailable = static_cast<unsigned>(std::errc::resource_unavailable_try_again),
    kUnimplemented = static_cast<unsigned>(std::errc::function_not_supported),
    kOperationCanceled = static_cast<unsigned>(std::errc::operation_canceled),
    kPermissionDenied = static_cast<unsigned>(std::errc::permission_denied),
    kPathNotFound = static_cast<unsigned>(std::errc::no_such_file_or_directory),
    kNotADirectory = static_cast<unsigned>(std::errc::not_a_directory),
    kFileAlreadyExists = static_cast<unsigned>(std::errc::file_exists),
    kPathIsNotEmptyDirectory = static_cast<unsigned>(std::errc::directory_not_empty),
    kBusy = static_cast<unsigned>(std::errc::device_or_resource_busy),

    // non-errc codes start at 256
    kException = 256,
    kAuthenticationFailed = 257,
    kAccessControlException = 258,
    kStandbyException = 259,
    kSnapshotProtocolException = 260,
    kInvalidOffset = 261,
  };

  std::string get_exception_class_str() const {
    return exception_class_;
  }

  int get_server_exception_type() const {
    return code_;
  }

 private:
  int code_;
  std::string msg_;

  std::string exception_class_;
};

}

#endif
