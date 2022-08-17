/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.federation.store;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;

import java.io.IOException;

/**
 * FederationDelegationTokenStateStore maintains the state of all
 * <em>DelegationToken</em> that have been submitted to the federated cluster.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface FederationDelegationTokenStateStore {

  /**
   * The Router Supports Store the New Master Key.
   * During this Process, Facade will call the specific StateStore to store the MasterKey.
   *
   * @param request The request contains RouterMasterKey, which is an abstraction for DelegationKey
   * @return routerMasterKeyResponse
   * @throws YarnException An internal conversion error occurred when store the Token
   * @throws IOException An IO Error occurred
   */
  RouterMasterKeyResponse storeNewMasterKey(RouterMasterKeyRequest request)
      throws YarnException, IOException;

  /**
   * The Router Supports Remove the master key.
   *
   * @param request DelegationKey.
   * @return RemoveStoredMasterKeyResponse.
   * @throws YarnException exception occurred.
   */
  RouterMasterKeyResponse removeStoredMasterKey(RouterMasterKeyRequest request)
      throws YarnException, IOException;

  /**
   * The Router supports obtaining MasterKey based on KeyId.
   *
   * @param request
   * @return
   * @throws YarnException
   * @throws IOException
   */
  RouterMasterKeyResponse getMasterKeyByDelegationKey(RouterMasterKeyRequest request)
      throws YarnException, IOException;

  /**
   * The Router Supports Store new Token.
   *
   * @param request DelegationKey.
   * @return RouterRMTokenResponse.
   * @throws YarnException exception occurred.
   */
  RouterRMTokenResponse storeNewToken(RouterRMTokenRequest request)
      throws YarnException, IOException;

  /**
   * The Router Supports Update Token.
   *
   * @param request DelegationKey.
   * @return RouterRMTokenResponse.
   * @throws YarnException exception occurred.
   */
  RouterRMTokenResponse updateStoredToken(RouterRMTokenRequest request)
      throws YarnException, IOException;

  /**
   * The Router Supports Remove Token.
   *
   * @param request DelegationKey.
   * @return RouterRMTokenResponse.
   * @throws YarnException exception occurred.
   */
  RouterRMTokenResponse removeStoredToken(RouterRMTokenRequest request)
      throws YarnException, IOException;

  /**
   * The Router Supports Remove Token.
   *
   * @param request DelegationKey.
   * @return RouterRMTokenResponse.
   * @throws YarnException exception occurred.
   */
  RouterRMTokenResponse getTokenByRouterStoreToken(RouterRMTokenRequest request)
      throws YarnException, IOException;
}
