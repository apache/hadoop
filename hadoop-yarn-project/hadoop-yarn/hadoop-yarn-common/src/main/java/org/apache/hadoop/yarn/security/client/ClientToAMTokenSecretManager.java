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

package org.apache.hadoop.yarn.security.client;

import javax.crypto.SecretKey;

import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;

public class ClientToAMTokenSecretManager extends
    BaseClientToAMTokenSecretManager {

  // Only one client-token and one master-key for AM
  private final SecretKey masterKey;

  public ClientToAMTokenSecretManager(
      ApplicationAttemptId applicationAttemptID, byte[] secretKeyBytes) {
    super();
    this.masterKey = SecretManager.createSecretKey(secretKeyBytes);
  }

  @Override
  public SecretKey getMasterKey(ApplicationAttemptId applicationAttemptID) {
    // Only one client-token and one master-key for AM, just return that.
    return this.masterKey;
  }

}