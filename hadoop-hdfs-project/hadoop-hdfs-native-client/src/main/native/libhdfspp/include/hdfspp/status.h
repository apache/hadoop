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

class StatusHelper;
class Status {
 public:
  // Create a success status.
  Status() : state_(NULL) { }
  ~Status() { delete[] state_; }
  explicit Status(int code, const char *msg);

  // Copy the specified status.
  Status(const Status& s);
  void operator=(const Status& s);

  // Return a success status.
  static Status OK() { return Status(); }
  static Status InvalidArgument(const char *msg)
  { return Status(kInvalidArgument, msg); }
  static Status ResourceUnavailable(const char *msg)
  { return Status(kResourceUnavailable, msg); }
  static Status Unimplemented()
  { return Status(kUnimplemented, ""); }
  static Status Exception(const char *expception_class_name, const char *error_message)
  { return Status(kException, expception_class_name, error_message); }
  static Status Error(const char *error_message)
  { return Exception("Exception", error_message); }

  // Returns true iff the status indicates success.
  bool ok() const { return (state_ == NULL); }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

  int code() const {
    return (state_ == NULL) ? kOk : static_cast<int>(state_[4]);
  }

  enum Code {
    kOk = 0,
    kInvalidArgument = static_cast<unsigned>(std::errc::invalid_argument),
    kResourceUnavailable = static_cast<unsigned>(std::errc::resource_unavailable_try_again),
    kUnimplemented = static_cast<unsigned>(std::errc::function_not_supported),
    kException = 255,
  };

 private:
  // OK status has a NULL state_.  Otherwise, state_ is a new[] array
  // of the following form:
  //    state_[0..3] == length of message
  //    state_[4]    == code
  //    state_[5..]  == message
  const char* state_;

  explicit Status(int code, const char *msg1, const char *msg2);
  static const char *CopyState(const char* s);
  static const char *ConstructState(int code, const char *msg1, const char *msg2);
};

inline Status::Status(const Status& s) {
  state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_);
}

inline void Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  if (state_ != s.state_) {
    delete[] state_;
    state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_);
  }
}

}

#endif
