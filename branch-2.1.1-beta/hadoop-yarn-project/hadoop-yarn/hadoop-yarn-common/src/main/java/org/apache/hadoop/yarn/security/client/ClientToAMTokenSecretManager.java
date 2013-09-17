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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;

/**
 * A simple {@link SecretManager} for AMs to validate Client-RM tokens issued to
 * clients by the RM using the underlying master-key shared by RM to the AMs on
 * their launch. All the methods are called by either Hadoop RPC or YARN, so
 * this class is strictly for the purpose of inherit/extend and register with
 * Hadoop RPC.
 */
@Public
@Evolving
public class ClientToAMTokenSecretManager extends
    BaseClientToAMTokenSecretManager {

  // Only one master-key for AM
  private SecretKey masterKey;

  public ClientToAMTokenSecretManager(
      ApplicationAttemptId applicationAttemptID, byte[] key) {
    super();
    if (key !=  null) {
      this.masterKey = SecretManager.createSecretKey(key);
    } else {
      this.masterKey = null;
    }
    
  }

  @Override
  public SecretKey getMasterKey(ApplicationAttemptId applicationAttemptID) {
    // Only one master-key for AM, just return that.
    return this.masterKey;
  }

  public void setMasterKey(byte[] key) {
    this.masterKey = SecretManager.createSecretKey(key);
  }
}