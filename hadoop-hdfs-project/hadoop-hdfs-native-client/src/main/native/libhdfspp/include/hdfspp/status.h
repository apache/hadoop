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
  Status() : code_(0) {};
  Status(int code, const char *msg);
  Status(int code, const char *msg1, const char *msg2);

  // Factory methods
  static Status OK();
  static Status InvalidArgument(const char *msg);
  static Status ResourceUnavailable(const char *msg);
  static Status Unimplemented();
  static Status Exception(const char *expception_class_name, const char *error_message);
  static Status Error(const char *error_message);
  static Status AuthenticationFailed();
  static Status Canceled();
  static Status PathNotFound(const char *msg);

  // success
  bool ok() const { return code_ == 0; }

  // Returns the string "OK" for success.
  std::string ToString() const;

  // get error code
  int code() const { return code_; }

  enum Code {
    kOk = 0,
    kInvalidArgument = static_cast<unsigned>(std::errc::invalid_argument),
    kResourceUnavailable = static_cast<unsigned>(std::errc::resource_unavailable_try_again),
    kUnimplemented = static_cast<unsigned>(std::errc::function_not_supported),
    kOperationCanceled = static_cast<unsigned>(std::errc::operation_canceled),
    kPermissionDenied = static_cast<unsigned>(std::errc::permission_denied),
    kPathNotFound = static_cast<unsigned>(std::errc::no_such_file_or_directory),
    kException = 256,
    kAuthenticationFailed = 257,
  };

 private:
  int code_;
  std::string msg_;
};

}

#endif
