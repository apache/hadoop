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
package org.apache.hadoop.lib.service;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

/**
 * Service interface to manage HttpFS delegation tokens.
 */
@InterfaceAudience.Private
public interface DelegationTokenManager {

  /**
   * Creates a delegation token.
   *
   * @param ugi UGI creating the token.
   * @param renewer token renewer.
   * @return new delegation token.
   * @throws DelegationTokenManagerException thrown if the token could not be
   * created.
   */
  public Token<DelegationTokenIdentifier> createToken(UserGroupInformation ugi,
                                                      String renewer)
    throws DelegationTokenManagerException;

  /**
   * Renews a delegation token.
   *
   * @param token delegation token to renew.
   * @param renewer token renewer.
   * @return epoc expiration time.
   * @throws DelegationTokenManagerException thrown if the token could not be
   * renewed.
   */
  public long renewToken(Token<DelegationTokenIdentifier> token, String renewer)
    throws DelegationTokenManagerException;

  /**
   * Cancels a delegation token.
   *
   * @param token delegation token to cancel.
   * @param canceler token canceler.
   * @throws DelegationTokenManagerException thrown if the token could not be
   * canceled.
   */
  public void cancelToken(Token<DelegationTokenIdentifier> token,
                          String canceler)
    throws DelegationTokenManagerException;

  /**
   * Verifies a delegation token.
   *
   * @param token delegation token to verify.
   * @return the UGI for the token.
   * @throws DelegationTokenManagerException thrown if the token could not be
   * verified.
   */
  public UserGroupInformation verifyToken(Token<DelegationTokenIdentifier> token)
    throws DelegationTokenManagerException;

}
