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
 * should not be counted on.  May need to be broken up into more components
 * as more events are added.
 */

static constexpr const char * FS_NN_CONNECT_EVENT = "NN::connect";
static constexpr const char * FS_NN_READ_EVENT = "NN::read";
static constexpr const char * FS_NN_WRITE_EVENT = "NN::write";

static constexpr const char * FILE_DN_CONNECT_EVENT = "DN::connect";
static constexpr const char * FILE_DN_READ_EVENT = "DN::read";
static constexpr const char * FILE_DN_WRITE_EVENT = "DN::write";


// NN failover event due to issues with the current NN; might be standby, might be dead.
// Invokes the fs_event_callback using the nameservice name in the cluster string.
// The uint64_t value argument holds an address that can be reinterpreted as a const char *
// and provides the full URI of the node the failover will attempt to connect to next.
static constexpr const char * FS_NN_FAILOVER_EVENT = "NN::failover";

// Invoked when RpcConnection tries to use an empty set of endpoints to figure out
// which NN in a HA cluster to connect to.
static constexpr const char * FS_NN_EMPTY_ENDPOINTS_EVENT = "NN::bad_failover::no_endpoints";

// Invoked prior to determining if failed NN rpc calls should be retried or discarded.
static constexpr const char * FS_NN_PRE_RPC_RETRY_EVENT = "NN::rpc::get_retry_action";

class event_response {
public:
  // Helper factories
  // The default ok response; libhdfspp should continue normally
  static event_response make_ok() {
    return event_response(kOk);
  }
  static event_response make_caught_std_exception(const char *what) {
    return event_response(kCaughtStdException, what);
  }
  static event_response make_caught_unknown_exception() {
    return event_response(kCaughtUnknownException);
  }

  // High level classification of responses
  enum event_response_type {
    kOk = 0,
    // User supplied callback threw.
    // Std exceptions will copy the what() string
    kCaughtStdException = 1,
    kCaughtUnknownException = 2,

    // Responses to be used in testing only
    kTest_Error = 100
  };

  event_response_type response_type() { return response_type_; }

private:
  // Use factories to construct for now
  event_response();
  event_response(event_response_type type)
            : response_type_(type)
  {
    if(type == kCaughtUnknownException) {
      status_ = Status::Exception("c++ unknown exception", "");
    }
  }
  event_response(event_response_type type, const char *what)
            : response_type_(type),
              exception_msg_(what==nullptr ? "" : what)
  {
    status_ = Status::Exception("c++ std::exception", exception_msg_.c_str());
  }


  event_response_type response_type_;

  // use to hold what str if event handler threw
  std::string exception_msg_;


///////////////////////////////////////////////
//
//   Testing support
//
// The consumer can stimulate errors
// within libhdfdspp by returning a Status from the callback.
///////////////////////////////////////////////
public:
  static event_response test_err(const Status &status) {
    return event_response(status);
  }

  Status status() { return status_; }

private:
  event_response(const Status & status) :
    response_type_(event_response_type::kTest_Error), status_(status) {}

  Status status_; // To be used with kTest_Error
};

/* callback signature */
typedef std::function<event_response (const char * event,
                                      const char * cluster,
                                      int64_t value)> fs_event_callback;

typedef std::function<event_response (const char * event,
                                      const char * cluster,
                                      const char * file,
                                      int64_t value)>file_event_callback;
}
#endif
