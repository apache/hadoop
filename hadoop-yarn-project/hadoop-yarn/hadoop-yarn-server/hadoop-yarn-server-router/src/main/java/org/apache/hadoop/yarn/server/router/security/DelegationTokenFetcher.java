/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.router.security;

import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;

import java.io.IOException;

public abstract class DelegationTokenFetcher {
  private RouterDelegationTokenSecretManager secretManager;

  public abstract void start() throws Exception;

  public DelegationTokenFetcher(RouterDelegationTokenSecretManager secretManager) {
    this.secretManager = secretManager;
  }

  protected void updateToken(RMDelegationTokenIdentifier identifier, long renewDate)
      throws IOException {
    secretManager.addPersistedDelegationToken(identifier, renewDate);
  }

  protected void removeToken(Token<RMDelegationTokenIdentifier> token, String user)
      throws IOException {
    secretManager.cancelToken(token, user);
  }

  protected void updateMasterKey(DelegationKey key) throws IOException {
    secretManager.addKey(key);
  }


}
