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

#ifndef LIB_RPC_CYRUS_SASLENGINE_H
#define LIB_RPC_CYRUS_SASLENGINE_H

#include "sasl/sasl.h"
#include "sasl_engine.h"

namespace hdfs
{

class CySaslEngine : public SaslEngine
{
public:
  CySaslEngine();
  virtual ~CySaslEngine();

  virtual std::pair<Status, std::string> Start();
  virtual std::pair<Status, std::string> Step(const std::string data);
  virtual Status Finish();
private:
  Status InitCyrusSasl();
  Status SaslError(int rc);

  friend int get_name(void *, int, const char **, unsigned *);
  friend int getrealm(void *, int, const char **availrealms, const char **);

  sasl_conn_t * conn_;
  std::vector<sasl_callback_t> per_connection_callbacks_;
}; //class CySaslEngine

} // namespace hdfs

#endif /* LIB_RPC_CYRUS_SASLENGINE_H */
