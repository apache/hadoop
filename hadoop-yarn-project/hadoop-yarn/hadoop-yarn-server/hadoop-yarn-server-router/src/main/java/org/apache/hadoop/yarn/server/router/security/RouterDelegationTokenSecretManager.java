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
package org.apache.hadoop.yarn.server.router.security;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.security.token.delegation.RouterDelegationTokenSupport;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;

/**
 * A Router specific delegation token secret manager and is designed to be stateless.
 * The secret manager is responsible for generating and accepting the password
 * for each token.
 *
 * Behavioural Differences from AbstractDelegationTokenSecretManager
 * 1) Master Key - Each instance of Router will have its own master key and each instance rolls its own master key.
 *    Thus there is no concept of a global current key.
 *    The requirement to generate new master keys / delegation tokens is to generate unique INTEGER keys,
 *    which the state store is responsible for (Autoincrement is one of the ways to achieve this).
 *    This key will be regenerated on service restart and thus there is no requirement of an explicit restore mechanism.
 *    Current master key will be stored in memory on each instance and will be used to generate new tokens.
 *    Master key will be looked up from the state store for Validation / renewal, etc of tokens.
 *
 * 2) Token Expiry - It doesn't take care of token removal on expiry.
 *    Each state store can implement its own way to manage token deletion on expiry.
 *
 * This pretty much replaces all methods of AbstractDelegationTokenSecretManager which is designed for stateful managers
 * TODO - Refactor Secret Manager interfaces to support stateful and stateless secret management
 */
public class RouterDelegationTokenSecretManager
    extends AbstractDelegationTokenSecretManager<RMDelegationTokenIdentifier> {

  private static final Logger LOG = LoggerFactory
      .getLogger(RouterDelegationTokenSecretManager.class);

  private FederationStateStoreFacade federationFacade;

  /**
   * Create a Router Secret manager.
   *
   * @param delegationKeyUpdateInterval        the number of milliseconds for rolling
   *                                           new secret keys.
   * @param delegationTokenMaxLifetime         the maximum lifetime of the delegation
   *                                           tokens in milliseconds
   * @param delegationTokenRenewInterval       how often the tokens must be renewed
   *                                           in milliseconds
   * @param delegationTokenRemoverScanInterval how often the tokens are scanned
   */
  public RouterDelegationTokenSecretManager(long delegationKeyUpdateInterval,
      long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
      long delegationTokenRemoverScanInterval) {
    super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
        delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    this.federationFacade = FederationStateStoreFacade.getInstance();
  }

  @Override
  public void reset() {
    // no-op
  }

  @Override
  public long getCurrentTokensSize() {
    throw new NotImplementedException("Get current token size is not implemented");
  }

  /**
   * no-op as this method is required for stateful secret managers only
   */
  @Override
  protected void addKey(DelegationKey key) {

  }

  @Override
  public DelegationKey[] getAllKeys() {
    throw new NotImplementedException("Get all keys is not implemented");
  }

  @Override
  public RMDelegationTokenIdentifier createIdentifier() {
    return new RMDelegationTokenIdentifier();
  }

  /**
   * The Router Supports Store the New Master Key.
   * During this Process, Facade will call the specific StateStore to store the MasterKey.
   *
   * @param newKey DelegationKey
   */
  @Override
  protected void storeNewMasterKey(DelegationKey newKey) throws IOException {
    try {
      federationFacade.storeNewMasterKey(newKey);
    } catch (YarnException e) {
      e.printStackTrace();
      throw new IOException(e); // Wrap YarnException as an IOException to adhere to storeNewMasterKey contract
    }
  }

  /**
   * no-op as expiry of stored keys is upto the state store in a stateless secret manager
   */
  @Override
  public void removeStoredMasterKey(DelegationKey delegationKey) {

  }

  /**
   * no-op as we are storing entire token with info in storeToken()
   */
  @Override
  public void storeNewToken(RMDelegationTokenIdentifier identifier, long renewDate) {

  }

  /**
   * no-op as removal of tokens is handled in removeToken()
   */
  @Override
  public void removeStoredToken(RMDelegationTokenIdentifier identifier) {

  }

  /**
   * no-op as we are storing entire token with info in updateToken()
   */
  @Override
  public void updateStoredToken(RMDelegationTokenIdentifier id, long renewDate) {

  }

  @Override
  public DelegationKey getDelegationKey(int keyId) throws IOException {
    try {
      RouterMasterKeyResponse response = federationFacade.getMasterKey(keyId);
      RouterMasterKey masterKey = response.getRouterMasterKey();
      ByteBuffer keyByteBuf = masterKey.getKeyBytes();
      byte[] keyBytes = new byte[keyByteBuf.remaining()];
      keyByteBuf.get(keyBytes);
      return new DelegationKey(masterKey.getKeyId(), masterKey.getExpiryDate(), keyBytes);
    } catch (YarnException ex) {
      ex.printStackTrace();
      throw new IOException(ex);
    }
  }

  public long getRenewDate(RMDelegationTokenIdentifier ident) throws IOException {
    DelegationTokenInformation info = getTokenInfo(ident);
    return info.getRenewDate();
  }

  @Override
  protected int incrementDelegationTokenSeqNum() {
    return federationFacade.incrementDelegationTokenSeqNum();
  }

  @Override
  protected void storeToken(RMDelegationTokenIdentifier rmDelegationTokenIdentifier,
      DelegationTokenInformation tokenInfo) throws IOException {
    try {
      storeNewToken(rmDelegationTokenIdentifier, tokenInfo);
    } catch (YarnException e) {
      e.printStackTrace();
      throw new IOException(e); // Wrap YarnException as an IOException to adhere to storeToken contract
    }
  }

  @Override
  protected void updateToken(RMDelegationTokenIdentifier rmDelegationTokenIdentifier,
      DelegationTokenInformation tokenInfo) throws IOException {
    try {
      updateStoredToken(rmDelegationTokenIdentifier, tokenInfo);
    } catch (YarnException e) {
      e.printStackTrace();
      throw new IOException(e); // Wrap YarnException as an IOException to adhere to updateToken contract
    }
  }

  @Override
  protected void removeToken(RMDelegationTokenIdentifier identifier) throws IOException {
    try {
      federationFacade.removeStoredToken(identifier);
    } catch (YarnException e) {
      e.printStackTrace();
      throw new IOException(e); // Wrap YarnException as an IOException to adhere to removeToken contract
    }
  }

  @Override
  protected DelegationTokenInformation getTokenInfo(RMDelegationTokenIdentifier ident) throws IOException {
      try {
        RouterRMTokenResponse response = federationFacade.getTokenByRouterStoreToken(ident);
        RouterStoreToken routerStoreToken = response.getRouterStoreToken();
        String tokenStr = routerStoreToken.getTokenInfo();
        byte[] tokenBytes = Base64.getUrlDecoder().decode(tokenStr);
        return RouterDelegationTokenSupport.decodeDelegationTokenInformation(tokenBytes);
      } catch (YarnException ex) {
        ex.printStackTrace();
        throw new IOException(ex);
      }
  }

  @Override
  protected void rollMasterKey() throws IOException {
    updateCurrentKey();
  }

  @Override
  public void addPersistedDelegationToken(RMDelegationTokenIdentifier identifier, long renewDate) {
    throw new NotImplementedException("Recovery of tokens is not a valid use case for stateless secret managers");
  }

  @Override
  protected int getDelegationTokenSeqNum() {
    throw new NotImplementedException("Get sequence number is not a valid use case for stateless secret managers");
  }

  @Override
  protected void setDelegationTokenSeqNum(int seqNum) {
    throw new NotImplementedException("Set sequence number is not a valid use case for stateless secret managers");
  }

  @Override
  protected int getCurrentKeyId() {
    throw new NotImplementedException("Get current key id is not a valid use case for stateless secret managers");
  }

  @Override
  protected int incrementCurrentKeyId() {
    return federationFacade.incrementCurrentKeyId();
  }

  @Override
  protected void setCurrentKeyId(int keyId) {
    throw new NotImplementedException("Set current key id is not a valid use case for stateless secret managers");
  }

  @Override
  protected void storeDelegationKey(DelegationKey key) throws IOException {
    storeNewMasterKey(key);
  }

  @Override
  protected void updateDelegationKey(DelegationKey key) {
    throw new NotImplementedException("Update delegation key is not a valid use case for stateless secret managers");
  }

  private void storeNewToken(RMDelegationTokenIdentifier identifier, DelegationTokenInformation tokenInfo) throws YarnException, IOException {
    long renewDate = tokenInfo.getRenewDate();
    String token = RouterDelegationTokenSupport.encodeDelegationTokenInformation(tokenInfo);
    federationFacade.storeNewToken(identifier, renewDate, token);
  }

  private void updateStoredToken(RMDelegationTokenIdentifier identifier, DelegationTokenInformation tokenInfo) throws YarnException, IOException {
    long renewDate = tokenInfo.getRenewDate();
    String token = RouterDelegationTokenSupport.encodeDelegationTokenInformation(tokenInfo);
    federationFacade.updateStoredToken(identifier, renewDate, token);
  }

}
