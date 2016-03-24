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

#ifndef HDFSPP_EVENTS
#define HDFSPP_EVENTS

#include "hdfspp/status.h"

#include <functional>

namespace hdfs {

/*
 * Supported event names.  These names will stay consistent in libhdfs callbacks.
 *
 * Other events not listed here may be seen, but they are not stable and
 * should not be counted on.
 */

static constexpr const char * FS_NN_CONNECT_EVENT = "NN::connect";
static constexpr const char * FS_NN_READ_EVENT = "NN::read";
static constexpr const char * FS_NN_WRITE_EVENT = "NN::write";

static constexpr const char * FILE_DN_CONNECT_EVENT = "DN::connect";
static constexpr const char * FILE_DN_READ_EVENT = "DN::read";
static constexpr const char * FILE_DN_WRITE_EVENT = "DN::write";



class event_response {
public:
// Create a response
enum event_response_type {
  kOk = 0,

#ifndef NDEBUG
  // Responses to be used in testing only
  kTest_Error = 100
#endif
};


  // The default ok response; libhdfspp should continue normally
  static event_response ok() { return event_response(); }
  event_response_type response() { return response_; }

private:
  event_response() : response_(event_response_type::kOk) {};

  event_response_type response_;



///////////////////////////////////////////////
//
//   Testing support
//
// If running a debug build, the consumer can stimulate errors
// within libhdfdspp by returning a Status from the callback.
///////////////////////////////////////////////
#ifndef NDEBUG
public:
  static event_response test_err(const Status &status) {
    return event_response(status);
  }

  Status status() { return error_status_; }

private:
  event_response(const Status & status) :
    response_(event_response_type::kTest_Error), error_status_(status) {}

  Status error_status_; // To be used with kTest_Error
#endif
};



/* callback signature */
typedef std::function<
  event_response (const char * event,
                  const char * cluster,
                  int64_t value)>
  fs_event_callback;

typedef std::function<
  event_response (const char * event,
                  const char * cluster,
                  const char * file,
                  int64_t value)>
  file_event_callback;


}
#endif
