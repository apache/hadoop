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

#ifndef LIB_RPC_SASLENGINE_H
#define LIB_RPC_SASLENGINE_H

#include "hdfspp/status.h"
#include "common/optional_wrapper.h"

#include <vector>

namespace hdfs {

class SaslProtocol;

template <class T>
using optional = std::experimental::optional<T>;

class SaslMethod {
public:
  std::string protocol;
  std::string mechanism;
  std::string serverid;
  std::string challenge;
};

class SaslEngine {
public:
    enum State {
        kUnstarted,
        kWaitingForData,
        kSuccess,
        kFailure,
        kErrorState,
    };

    // State transitions:
    //                    \--------------------------/
    // kUnstarted --start--> kWaitingForData --step-+--> kSuccess --finish--v
    //                                               \-> kFailure -/

    // State transitions:
    //                    \--------------------------/
    // kUnstarted --start--> kWaitingForData --step-+--> kSuccess --finish--v
    //                                               \-> kFailure -/

    SaslEngine(): state_ (kUnstarted) {}
    virtual ~SaslEngine();

    // Must be called when state is kUnstarted
    Status SetKerberosInfo(const std::string &principal);
    // Must be called when state is kUnstarted
    Status SetPasswordInfo(const std::string &id,
                           const std::string &password);

    // Choose a mechanism from the available ones.  Will set the
    //    chosen_mech_ member and return true if we found one we
    //    can process
    bool ChooseMech(const std::vector<SaslMethod> &avail_auths);

    // Returns the current state
    State GetState();

    // Must be called when state is kUnstarted
    virtual std::pair<Status,std::string>  Start() = 0;

    // Must be called when state is kWaitingForData
    // Returns kOK and any data that should be sent to the server
    virtual std::pair<Status,std::string> Step(const std::string data) = 0;

    // Must only be called when state is kSuccess, kFailure, or kErrorState
    virtual Status Finish() = 0;

    // main repository of generic Sasl config data:
    SaslMethod chosen_mech_;
protected:
  State state_;
  SaslProtocol * sasl_protocol_;

  optional<std::string> principal_;
  optional<std::string> realm_;
  optional<std::string> id_;
  optional<std::string> password_;

}; // class SaslEngine

} // namespace hdfs

#endif /* LIB_RPC_SASLENGINE_H */
