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
#include "optional.hpp"

#ifdef USE_GSASL
#include "gsasl.h"
#endif

#include <vector>

namespace hdfs {

template <class T>
using optional = std::experimental::optional<T>;

class SaslMethod {
public:
  std::string protocol;
  std::string mechanism;
  std::string serverid;
  void *      data;
};

class SaslEngine {
public:
    enum State {
        kUnstarted,
        kWaitingForData,
        kSuccess,
        kFailure,
        kError,
    };

    // State transitions:
    //                    \--------------------------/
    // kUnstarted --start--> kWaitingForData --step-+--> kSuccess --finish--v
    //                                               \-> kFailure -/

    SaslEngine() : state_ (kUnstarted) {}
    virtual ~SaslEngine();

    // Must be called when state is kUnstarted
    Status setKerberosInfo(const std::string &principal);
    // Must be called when state is kUnstarted
    Status setPasswordInfo(const std::string &id,
                           const std::string &password);

    // Returns the current state
    State getState();

    // Must be called when state is kUnstarted
    virtual std::pair<Status,SaslMethod>  start(
              const std::vector<SaslMethod> &protocols) = 0;

    // Must be called when state is kWaitingForData
    // Returns kOK and any data that should be sent to the server
    virtual std::pair<Status,std::string> step(const std::string data) = 0;

    // Must only be called when state is kSuccess, kFailure, or kError
    virtual Status finish() = 0;
protected:
  State state_;

  optional<std::string> principal_;
  optional<std::string> id_;
  optional<std::string> password_;

};

#ifdef USE_GSASL
class GSaslEngine : public SaslEngine
{
public:
  GSaslEngine() : SaslEngine(), ctx_(nullptr), session_(nullptr) {}
  virtual ~GSaslEngine();

  virtual std::pair<Status,SaslMethod>  start(
            const std::vector<SaslMethod> &protocols);
  virtual std::pair<Status,std::string> step(const std::string data);
  virtual Status finish();
private:
  Gsasl * ctx_;
  Gsasl_session * session_;

  Status init_kerberos(const SaslMethod & mechanism);
};
#endif

#ifdef USE_CYRUS_SASL
class CyrusSaslEngine : public SaslEngine
{
public:
  GSaslEngine() : SaslEngine(), ctx_(nullptr), session_(nullptr) {}
  virtual ~GSaslEngine();

  virtual std::pair<Status,SaslMethod>  start(
            const std::vector<SaslMethod> &protocols);
  virtual std::pair<Status,std::string> step(const std::string data);
  virtual Status finish();
private:
};
#endif

}
#endif /* LIB_RPC_SASLENGINE_H */
