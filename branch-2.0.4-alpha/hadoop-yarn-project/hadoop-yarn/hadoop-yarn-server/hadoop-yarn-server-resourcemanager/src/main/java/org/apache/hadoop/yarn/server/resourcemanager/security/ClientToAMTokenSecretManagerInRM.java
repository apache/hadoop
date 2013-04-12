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

package org.apache.hadoop.yarn.server.resourcemanager.security;

import java.util.HashMap;
import java.util.Map;

import javax.crypto.SecretKey;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.security.client.BaseClientToAMTokenSecretManager;

public class ClientToAMTokenSecretManagerInRM extends
    BaseClientToAMTokenSecretManager {

  // Per application master-keys for managing client-tokens
  private Map<ApplicationAttemptId, SecretKey> masterKeys =
      new HashMap<ApplicationAttemptId, SecretKey>();

  public synchronized void registerApplication(
      ApplicationAttemptId applicationAttemptID) {
    this.masterKeys.put(applicationAttemptID, generateSecret());
  }

  public synchronized void unRegisterApplication(
      ApplicationAttemptId applicationAttemptID) {
    this.masterKeys.remove(applicationAttemptID);
  }

  @Override
  public synchronized SecretKey getMasterKey(
      ApplicationAttemptId applicationAttemptID) {
    return this.masterKeys.get(applicationAttemptID);
  }
}
