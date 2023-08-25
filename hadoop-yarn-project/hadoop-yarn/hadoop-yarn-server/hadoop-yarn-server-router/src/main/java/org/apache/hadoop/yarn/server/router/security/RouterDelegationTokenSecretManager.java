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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.security.token.delegation.RouterDelegationTokenSupport;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Base64;

/**
 * A Router specific delegation token secret manager.
 * The secret manager is responsible for generating and accepting the password
 * for each token.
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
  public RMDelegationTokenIdentifier createIdentifier() {
    return new RMDelegationTokenIdentifier();
  }

  private boolean shouldIgnoreException(Exception e) {
    return !running && e.getCause() instanceof InterruptedException;
  }

  /**
   * The Router Supports Store the New Master Key.
   * During this Process, Facade will call the specific StateStore to store the MasterKey.
   *
   * @param newKey DelegationKey
   */
  @Override
  public void storeNewMasterKey(DelegationKey newKey) {
    try {
      federationFacade.storeNewMasterKey(newKey);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in storing master key with KeyID: {}.", newKey.getKeyId());
        ExitUtil.terminate(1, e);
      }
    }
  }

  /**
   * The Router Supports Remove the master key.
   * During this Process, Facade will call the specific StateStore to remove the MasterKey.
   *
   * @param delegationKey DelegationKey
   */
  @Override
  public void removeStoredMasterKey(DelegationKey delegationKey) {
    try {
      federationFacade.removeStoredMasterKey(delegationKey);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in removing master key with KeyID: {}.", delegationKey.getKeyId());
        ExitUtil.terminate(1, e);
      }
    }
  }

  /**
   * The Router Supports Store new Token.
   *
   * @param identifier RMDelegationToken
   * @param renewDate renewDate
   * @throws IOException IO exception occurred.
   */
  @Override
  public void storeNewToken(RMDelegationTokenIdentifier identifier,
      long renewDate) throws IOException {
    try {
      federationFacade.storeNewToken(identifier, renewDate);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in storing RMDelegationToken with sequence number: {}.",
            identifier.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  /**
   * The Router Supports Store new Token.
   *
   * @param identifier RMDelegationToken.
   * @param tokenInfo DelegationTokenInformation.
   */
  public void storeNewToken(RMDelegationTokenIdentifier identifier,
      DelegationTokenInformation tokenInfo) {
    try {
      String token =
          RouterDelegationTokenSupport.encodeDelegationTokenInformation(tokenInfo);
      long renewDate = tokenInfo.getRenewDate();

      federationFacade.storeNewToken(identifier, renewDate, token);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in storing RMDelegationToken with sequence number: {}.",
            identifier.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  /**
   * The Router Supports Update Token.
   *
   * @param id RMDelegationToken
   * @param renewDate renewDate
   * @throws IOException IO exception occurred
   */
  @Override
  public void updateStoredToken(RMDelegationTokenIdentifier id, long renewDate) throws IOException {
    try {
      federationFacade.updateStoredToken(id, renewDate);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in updating persisted RMDelegationToken with sequence number: {}.",
            id.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  /**
   * The Router Supports Update Token.
   *
   * @param identifier RMDelegationToken.
   * @param tokenInfo DelegationTokenInformation.
   */
  public void updateStoredToken(RMDelegationTokenIdentifier identifier,
      DelegationTokenInformation tokenInfo) {
    try {
      long renewDate = tokenInfo.getRenewDate();
      String token = RouterDelegationTokenSupport.encodeDelegationTokenInformation(tokenInfo);
      federationFacade.updateStoredToken(identifier, renewDate, token);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in updating persisted RMDelegationToken with sequence number: {}.",
            identifier.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  /**
   * The Router Supports Remove Token.
   *
   * @param identifier Delegation Token
   * @throws IOException IO exception occurred.
   */
  @Override
  public void removeStoredToken(RMDelegationTokenIdentifier identifier) throws IOException {
    try {
      federationFacade.removeStoredToken(identifier);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in removing RMDelegationToken with sequence number: {}",
            identifier.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  /**
   * The Router supports obtaining the DelegationKey stored in the Router StateStote
   * according to the DelegationKey.
   *
   * @param key Param DelegationKey
   * @return Delegation Token
   * @throws YarnException An internal conversion error occurred when getting the Token
   * @throws IOException IO exception occurred
   */
  public DelegationKey getMasterKeyByDelegationKey(DelegationKey key)
      throws YarnException, IOException {
    try {
      RouterMasterKeyResponse response = federationFacade.getMasterKeyByDelegationKey(key);
      RouterMasterKey masterKey = response.getRouterMasterKey();
      ByteBuffer keyByteBuf = masterKey.getKeyBytes();
      byte[] keyBytes = new byte[keyByteBuf.remaining()];
      keyByteBuf.get(keyBytes);
      DelegationKey delegationKey =
          new DelegationKey(masterKey.getKeyId(), masterKey.getExpiryDate(), keyBytes);
      return delegationKey;
    } catch (IOException ex) {
      throw new IOException(ex);
    } catch (YarnException ex) {
      throw new YarnException(ex);
    }
  }

  /**
   * Get RMDelegationTokenIdentifier according to RouterStoreToken.
   *
   * @param identifier RMDelegationTokenIdentifier
   * @return RMDelegationTokenIdentifier
   * @throws YarnException An internal conversion error occurred when getting the Token
   * @throws IOException IO exception occurred
   */
  public RMDelegationTokenIdentifier getTokenByRouterStoreToken(
      RMDelegationTokenIdentifier identifier) throws YarnException, IOException {
    try {
      RouterRMTokenResponse response = federationFacade.getTokenByRouterStoreToken(identifier);
      YARNDelegationTokenIdentifier responseIdentifier =
          response.getRouterStoreToken().getTokenIdentifier();
      return (RMDelegationTokenIdentifier) responseIdentifier;
    } catch (Exception ex) {
      throw new YarnException(ex);
    }
  }

  public void setFederationFacade(FederationStateStoreFacade federationFacade) {
    this.federationFacade = federationFacade;
  }

  @Public
  @VisibleForTesting
  public int getLatestDTSequenceNumber() {
    return delegationTokenSequenceNumber;
  }

  @Public
  @VisibleForTesting
  public synchronized Set<DelegationKey> getAllMasterKeys() {
    return new HashSet<>(allKeys.values());
  }

  @Public
  @VisibleForTesting
  public synchronized Map<RMDelegationTokenIdentifier, Long> getAllTokens() {
    Map<RMDelegationTokenIdentifier, Long> allTokens = new HashMap<>();
    for (Map.Entry<RMDelegationTokenIdentifier,
         DelegationTokenInformation> entry : currentTokens.entrySet()) {
      RMDelegationTokenIdentifier keyIdentifier = entry.getKey();
      DelegationTokenInformation tokenInformation = entry.getValue();
      allTokens.put(keyIdentifier, tokenInformation.getRenewDate());
    }
    return allTokens;
  }

  public long getRenewDate(RMDelegationTokenIdentifier ident)
      throws InvalidToken {
    DelegationTokenInformation info = currentTokens.get(ident);
    if (info == null) {
      throw new InvalidToken("token (" + ident.toString()
          + ") can't be found in cache");
    }
    return info.getRenewDate();
  }

  @Override
  protected synchronized int incrementDelegationTokenSeqNum() {
    return federationFacade.incrementDelegationTokenSeqNum();
  }

  @Override
  protected void storeToken(RMDelegationTokenIdentifier rmDelegationTokenIdentifier,
      DelegationTokenInformation tokenInfo) throws IOException {
    this.currentTokens.put(rmDelegationTokenIdentifier, tokenInfo);
    this.addTokenForOwnerStats(rmDelegationTokenIdentifier);
    storeNewToken(rmDelegationTokenIdentifier, tokenInfo);
  }

  @Override
  protected void updateToken(RMDelegationTokenIdentifier rmDelegationTokenIdentifier,
      DelegationTokenInformation tokenInfo) throws IOException {
    this.currentTokens.put(rmDelegationTokenIdentifier, tokenInfo);
    updateStoredToken(rmDelegationTokenIdentifier, tokenInfo);
  }

  @Override
  protected DelegationTokenInformation getTokenInfo(
      RMDelegationTokenIdentifier ident) {
    // First check if I have this..
    DelegationTokenInformation tokenInfo = currentTokens.get(ident);
    if (tokenInfo == null) {
      try {
        RouterRMTokenResponse response = federationFacade.getTokenByRouterStoreToken(ident);
        RouterStoreToken routerStoreToken = response.getRouterStoreToken();
        String tokenStr = routerStoreToken.getTokenInfo();
        byte[] tokenBytes = Base64.getUrlDecoder().decode(tokenStr);
        tokenInfo = RouterDelegationTokenSupport.decodeDelegationTokenInformation(tokenBytes);
      } catch (Exception e) {
        LOG.error("Error retrieving tokenInfo [" + ident.getSequenceNumber()
            + "] from StateStore.", e);
        throw new YarnRuntimeException(e);
      }
    }
    return tokenInfo;
  }

  @Override
  protected synchronized int getDelegationTokenSeqNum() {
    return federationFacade.getDelegationTokenSeqNum();
  }

  @Override
  protected synchronized void setDelegationTokenSeqNum(int seqNum) {
    federationFacade.setDelegationTokenSeqNum(seqNum);
  }

  @Override
  protected synchronized int getCurrentKeyId() {
    return federationFacade.getCurrentKeyId();
  }

  @Override
  protected synchronized int incrementCurrentKeyId() {
    return federationFacade.incrementCurrentKeyId();
  }
}
