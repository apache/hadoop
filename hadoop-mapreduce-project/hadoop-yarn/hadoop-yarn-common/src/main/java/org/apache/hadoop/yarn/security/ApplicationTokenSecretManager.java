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

package org.apache.hadoop.yarn.security;

import javax.crypto.SecretKey;

import org.apache.hadoop.security.token.SecretManager;

public class ApplicationTokenSecretManager extends
    SecretManager<ApplicationTokenIdentifier> {

  // TODO: mark as final
  private SecretKey masterKey; // For now only one masterKey, for ever.

  // TODO: add expiry for masterKey
  // TODO: add logic to handle with multiple masterKeys, only one being used for
  // creating new tokens at any time.
  // TODO: Make he masterKey more secure, non-transferrable etc.

  /**
   * Default constructor
   */
  public ApplicationTokenSecretManager() {
    this.masterKey = generateSecret();
  }

  // TODO: this should go away.
  public void setMasterKey(SecretKey mk) {
    this.masterKey = mk;
  }

  // TODO: this should go away.
  public SecretKey getMasterKey() {
    return masterKey;
  }

  /**
   * Convert the byte[] to a secret key
   * @param key the byte[] to create the secret key from
   * @return the secret key
   */
  public static SecretKey createSecretKey(byte[] key) {
    return SecretManager.createSecretKey(key);
  }

  @Override
  public byte[] createPassword(ApplicationTokenIdentifier identifier) {
    return createPassword(identifier.getBytes(), masterKey);
  }

  @Override
  public byte[] retrievePassword(ApplicationTokenIdentifier identifier)
      throws SecretManager.InvalidToken {
    return createPassword(identifier.getBytes(), masterKey);
  }

  @Override
  public ApplicationTokenIdentifier createIdentifier() {
    return new ApplicationTokenIdentifier();
  }

}
