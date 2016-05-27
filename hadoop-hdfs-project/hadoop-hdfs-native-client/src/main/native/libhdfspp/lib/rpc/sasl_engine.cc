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

#include "sasl_engine.h"

#include "common/logging.h"

#include <sstream>


namespace hdfs {

/*****************************************************************************
 *                     BASE CLASS
 */

SaslEngine::State SaslEngine::getState()
{
    return state_;
}

SaslEngine::~SaslEngine() {
}

Status SaslEngine::setKerberosInfo(const std::string &principal)
{
  principal_ = principal;
  return Status::OK();
}

Status SaslEngine::setPasswordInfo(const std::string &id,
                       const std::string &password)
{
  id_ = id;
  password_ = password;
  return Status::OK();
}


#ifdef USE_GSASL
/*****************************************************************************
 *                     GSASL
 */

#include <gsasl.h>


/*****************************************************************************
 *                     UTILITY FUNCTIONS
 */

Status gsasl_rc_to_status(int rc)
{
  if (rc == GSASL_OK) {
    return Status::OK();
  } else {
    std::ostringstream ss;
    ss << "Cannot initialize client (" << rc << "): " << gsasl_strerror(rc);
    return Status::Error(ss.str().c_str());
  }
}


GSaslEngine::~GSaslEngine()
{
  if (session_ != nullptr) {
      gsasl_finish(session_);
  }

  if (ctx_ != nullptr) {
      gsasl_done(ctx_);
  }
}

std::pair<Status, SaslMethod> GSaslEngine::start(const std::vector<SaslMethod> &protocols)
{
  int rc = gsasl_init(&ctx_);
  if (rc != GSASL_OK) {
    state_ = kError;
    return std::make_pair(gsasl_rc_to_status(rc), SaslMethod());
  }

  // Hack to only do GSSAPI at the moment
  for (auto protocol: protocols) {
    if (protocol.mechanism == "GSSAPI") {
      Status init = init_kerberos(protocol);
      if (init.ok()) {
        state_ = kWaitingForData;
        return std::make_pair(init, protocol);
      } else {
        state_ = kError;
        return std::make_pair(init, SaslMethod());
      }
    }
  }

  state_ = kError;
  return std::make_pair(Status::Error("No good protocol"), SaslMethod());
}

Status GSaslEngine::init_kerberos(const SaslMethod & mechanism) {
  /* Create new authentication session. */
  int rc = gsasl_client_start(ctx_, mechanism.mechanism.c_str(), &session_);
  if (rc != GSASL_OK) {
    return gsasl_rc_to_status(rc);
  }

  if (!principal_) {
    return Status::Error("Attempted kerberos authentication with no principal");
  }

  gsasl_property_set(session_, GSASL_SERVICE, mechanism.protocol.c_str());
  gsasl_property_set(session_, GSASL_AUTHID, principal_.value().c_str());
  gsasl_property_set(session_, GSASL_HOSTNAME, mechanism.serverid.c_str());
  return Status::OK();
  }

  std::pair<Status, std::string> GSaslEngine::step(const std::string data)
  {
    if (state_ != kWaitingForData)
      LOG_WARN(kRPC, << "GSaslEngine::step when state is " << state_);

    char * output = NULL;
    size_t outputSize;
    int rc = gsasl_step(session_, data.c_str(), data.size(), &output,
                        &outputSize);

    if (rc == GSASL_NEEDS_MORE || rc == GSASL_OK) {
      std::string retval(output, output ? outputSize : 0);
      if (output) {
        free(output);
      }

      if (rc == GSASL_OK) {
        state_ = kSuccess;
      }

      return std::make_pair(Status::OK(), retval);
    }
    else {
      if (output) {
        free(output);
      }
      state_ = kFailure;
      return std::make_pair(gsasl_rc_to_status(rc), "");
    }
  }

Status GSaslEngine::finish()
{
  if (state_ != kSuccess && state_ != kFailure && state_ != kError )
    LOG_WARN(kRPC, << "GSaslEngine::finish when state is " << state_);

  if (session_ != nullptr) {
      gsasl_finish(session_);
      session_ = NULL;
  }

  if (ctx_ != nullptr) {
      gsasl_done(ctx_);
      ctx_ = nullptr;
  }

  return Status::OK();
}
#endif // USE_GSASL



}
