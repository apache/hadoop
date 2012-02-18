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

import java.util.HashMap;
import java.util.Map;

import javax.crypto.SecretKey;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.SecretManager;

public class ClientToAMSecretManager extends
    SecretManager<ClientTokenIdentifier> {

  private static Log LOG = LogFactory.getLog(ClientToAMSecretManager.class);

  // Per application masterkeys for managing client-tokens
  private Map<Text, SecretKey> masterKeys = new HashMap<Text, SecretKey>();

  public void setMasterKey(ClientTokenIdentifier identifier, byte[] key) {
    SecretKey sk = SecretManager.createSecretKey(key);
    Text applicationID = identifier.getApplicationID();
    this.masterKeys.put(applicationID, sk);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Setting master key for "
          + applicationID
          + " as "
          + new String(Base64.encodeBase64(this.masterKeys.get(applicationID)
              .getEncoded())));
    }
  }

  private void addMasterKey(ClientTokenIdentifier identifier) {
    Text applicationID = identifier.getApplicationID();
    this.masterKeys.put(applicationID, generateSecret());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating master key for "
          + applicationID
          + " as "
          + new String(Base64.encodeBase64(this.masterKeys.get(applicationID)
              .getEncoded())));}
  }

  // TODO: Handle the masterKey invalidation.
  public synchronized SecretKey getMasterKey(
      ClientTokenIdentifier identifier) {
    Text applicationID = identifier.getApplicationID();
    if (!this.masterKeys.containsKey(applicationID)) {
      addMasterKey(identifier);
    }
    return this.masterKeys.get(applicationID);
  }

  @Override
  public synchronized byte[] createPassword(
      ClientTokenIdentifier identifier) {
    byte[] password =
        createPassword(identifier.getBytes(), getMasterKey(identifier));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Password created is "
          + new String(Base64.encodeBase64(password)));
    }
    return password;
  }

  @Override
  public byte[] retrievePassword(ClientTokenIdentifier identifier)
      throws SecretManager.InvalidToken {
    byte[] password =
        createPassword(identifier.getBytes(), getMasterKey(identifier));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Password retrieved is "
          + new String(Base64.encodeBase64(password)));
    }
    return password;
  }

  @Override
  public ClientTokenIdentifier createIdentifier() {
    return new ClientTokenIdentifier();
  }

}
