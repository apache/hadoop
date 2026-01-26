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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;

import java.io.IOException;

/**
 * FederationDelegationTokenStateStore maintains the state of all
 * <em>DelegationToken</em> that have been submitted to the federated cluster.
 */
@Private
@Unstable
public interface FederationDelegationTokenStateStore {

  /**
   * The Router Supports Store NewMasterKey.
   * During this Process, Facade will call the specific StateStore to store the MasterKey.
   *
   * @param request The request contains RouterMasterKey, which is an abstraction for DelegationKey
   * @return routerMasterKeyResponse
   * @throws YarnException if the call to the state store is unsuccessful
   * @throws IOException An IO Error occurred
   */
  RouterMasterKeyResponse storeNewMasterKey(RouterMasterKeyRequest request)
      throws YarnException, IOException;

  /**
   * The Router Supports Remove MasterKey.
   * During this Process, Facade will call the specific StateStore to remove the MasterKey.
   *
   * @param request The request contains RouterMasterKey, which is an abstraction for DelegationKey
   * @return routerMasterKeyResponse
   * @throws YarnException if the call to the state store is unsuccessful
   * @throws IOException An IO Error occurred
   */
  RouterMasterKeyResponse removeStoredMasterKey(RouterMasterKeyRequest request)
      throws YarnException, IOException;

  /**
   * The Router Supports GetMasterKeyByDelegationKey.
   *
   * @param request The request contains RouterMasterKey, which is an abstraction for DelegationKey
   * @return routerMasterKeyResponse
   * @throws YarnException if the call to the state store is unsuccessful
   * @throws IOException An IO Error occurred
   */
  RouterMasterKeyResponse getMasterKeyByDelegationKey(RouterMasterKeyRequest request)
      throws YarnException, IOException;

  /**
   * The Router Supports Store RMDelegationTokenIdentifier.
   *
   * @param request The request contains RouterRMToken (RMDelegationTokenIdentifier and renewDate)
   * @return routerRMTokenResponse.
   * @throws YarnException if the call to the state store is unsuccessful
   * @throws IOException An IO Error occurred
   */
  RouterRMTokenResponse storeNewToken(RouterRMTokenRequest request)
      throws YarnException, IOException;

  /**
   * The Router Supports Update RMDelegationTokenIdentifier.
   *
   * @param request The request contains RouterRMToken (RMDelegationTokenIdentifier and renewDate)
   * @return RouterRMTokenResponse.
   * @throws YarnException if the call to the state store is unsuccessful
   * @throws IOException An IO Error occurred
   */
  RouterRMTokenResponse updateStoredToken(RouterRMTokenRequest request)
      throws YarnException, IOException;

  /**
   * The Router Supports Remove RMDelegationTokenIdentifier.
   *
   * @param request The request contains RouterRMToken (RMDelegationTokenIdentifier and renewDate)
   * @return RouterRMTokenResponse.
   * @throws YarnException if the call to the state store is unsuccessful
   * @throws IOException An IO Error occurred
   */
  RouterRMTokenResponse removeStoredToken(RouterRMTokenRequest request)
      throws YarnException, IOException;

  /**
   * The Router Supports GetTokenByRouterStoreToken.
   *
   * @param request The request contains RouterRMToken (RMDelegationTokenIdentifier and renewDate)
   * @return RouterRMTokenResponse.
   * @throws YarnException if the call to the state store is unsuccessful
   * @throws IOException An IO Error occurred
   */
  RouterRMTokenResponse getTokenByRouterStoreToken(RouterRMTokenRequest request)
      throws YarnException, IOException;

  /**
   * The Router Supports incrementDelegationTokenSeqNum.
   *
   * @return DelegationTokenSeqNum.
   */
  int incrementDelegationTokenSeqNum();

  /**
   * The Router Supports getDelegationTokenSeqNum.
   *
   * @return DelegationTokenSeqNum.
   */
  int getDelegationTokenSeqNum();

  /**
   * The Router Supports setDelegationTokenSeqNum.
   *
   * @param seqNum DelegationTokenSeqNum.
   */
  void setDelegationTokenSeqNum(int seqNum);

  /**
   * The Router Supports getCurrentKeyId.
   *
   * @return CurrentKeyId.
   */
  int getCurrentKeyId();

  /**
   * The Router Supports incrementCurrentKeyId.
   *
   * @return CurrentKeyId.
   */
  int incrementCurrentKeyId();
}
