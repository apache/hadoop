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

#include <sstream>
#include <string.h> // memcpy()

#include "sasl_engine.h"
#include "common/logging.h"

namespace hdfs {

/*****************************************************************************
 *                    SASL ENGINE BASE CLASS
 */

SaslEngine::State SaslEngine::GetState()
{
    return state_;
}

SaslEngine::~SaslEngine() {
}

Status SaslEngine::SetKerberosInfo(const std::string &principal)
{
  principal_ = principal;
  return Status::OK();
}

Status SaslEngine::SetPasswordInfo(const std::string &id,
                                   const std::string &password)
{
  id_ = id;
  password_ = password;
  return Status::OK();
}

bool SaslEngine::ChooseMech(const std::vector<SaslMethod> &resp_auths) {
  Status status = Status::OK();

  if (resp_auths.empty()) return false;

  for (SaslMethod auth: resp_auths) {
     if ( auth.mechanism != "GSSAPI") continue; // Hack: only GSSAPI for now

     // do a proper deep copy of the vector element
     // that we like, because the original v ector will go away:
     chosen_mech_.mechanism = auth.mechanism;
     chosen_mech_.protocol  = auth.protocol;
     chosen_mech_.serverid  = auth.serverid;
     chosen_mech_.challenge = auth.challenge;

     return auth.mechanism.c_str();
  }

  state_ = kErrorState;
  status = Status::Error("SaslEngine::chooseMech(): No good protocol.");

  // Clear out the chosen mech
  chosen_mech_ = SaslMethod();

  return false;
} // choose_mech()

} // namespace hdfs
