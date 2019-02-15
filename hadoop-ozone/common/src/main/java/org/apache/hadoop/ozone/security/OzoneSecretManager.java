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

package org.apache.hadoop.ozone.security;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SecretManager for Ozone Master. Responsible for signing identifiers with
 * private key,
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class OzoneSecretManager<T extends TokenIdentifier>
    extends SecretManager<T> {

  private final Logger logger;
  /**
   * The name of the Private/Public Key based hashing algorithm.
   */
  private final SecurityConfig securityConfig;
  private final long tokenMaxLifetime;
  private final long tokenRenewInterval;
  private final Text service;
  private volatile boolean running;
  private OzoneSecretKey currentKey;
  private AtomicInteger currentKeyId;
  private AtomicInteger tokenSequenceNumber;
  @SuppressWarnings("visibilitymodifier")
  protected final Map<Integer, OzoneSecretKey> allKeys;

  /**
   * Create a secret manager.
   *
   * @param secureConf configuration.
   * @param tokenMaxLifetime the maximum lifetime of the delegation tokens in
   * milliseconds
   * @param tokenRenewInterval how often the tokens must be renewed in
   * milliseconds
   * @param service name of service
   */
  public OzoneSecretManager(SecurityConfig secureConf, long tokenMaxLifetime,
      long tokenRenewInterval, Text service, Logger logger) {
    this.securityConfig = secureConf;
    this.tokenMaxLifetime = tokenMaxLifetime;
    this.tokenRenewInterval = tokenRenewInterval;
    currentKeyId = new AtomicInteger();
    tokenSequenceNumber = new AtomicInteger();
    allKeys = new ConcurrentHashMap<>();
    this.service = service;
    this.logger = logger;
  }


  /**
   * Compute HMAC of the identifier using the private key and return the output
   * as password.
   *
   * @param identifier
   * @param privateKey
   * @return byte[] signed byte array
   */
  public byte[] createPassword(byte[] identifier, PrivateKey privateKey)
      throws OzoneSecurityException {
    try {
      Signature rsaSignature = Signature.getInstance(
          getDefaultSignatureAlgorithm());
      rsaSignature.initSign(privateKey);
      rsaSignature.update(identifier);
      return rsaSignature.sign();
    } catch (InvalidKeyException | NoSuchAlgorithmException |
        SignatureException ex) {
      throw new OzoneSecurityException("Error while creating HMAC hash for " +
          "token.", ex, OzoneSecurityException.ResultCodes
          .SECRET_MANAGER_HMAC_ERROR);
    }
  }

  @Override
  public byte[] createPassword(T identifier) {
    logger.debug("Creating password for identifier: {}, currentKey: {}",
        formatTokenId(identifier), currentKey.getKeyId());
    byte[] password = null;
    try {
      password = createPassword(identifier.getBytes(),
          currentKey.getPrivateKey());
    } catch (IOException ioe) {
      logger.error("Could not store token {}!!", formatTokenId(identifier),
          ioe);
    }
    return password;
  }

  /**
   * Default implementation for Ozone. Verifies if hash in token is legit.
   * */
  @Override
  public byte[] retrievePassword(T identifier) throws InvalidToken {
    byte[] password = createPassword(identifier);
    // TODO: Revisit this when key/certificate rotation is implemented.
    // i.e Try all valid keys instead of current key only.
    if (!verifySignature(identifier, password)) {
      throw new InvalidToken("Tampared/Inavalid token.");
    }
    return password;
  }

  /**
   * Renew a delegation token.
   *
   * @param token the token to renew
   * @param renewer the full principal name of the user doing the renewal
   * @return the new expiration time
   * @throws InvalidToken           if the token is invalid
   * @throws AccessControlException if the user can't renew token
   */
  public abstract long renewToken(Token<T> token, String renewer)
      throws IOException;
  /**
   * Cancel a token by removing it from store and cache.
   *
   * @return Identifier of the canceled token
   * @throws InvalidToken           for invalid token
   * @throws AccessControlException if the user isn't allowed to cancel
   */
  public abstract T cancelToken(Token<T> token, String canceller)
      throws IOException;

  public int incrementCurrentKeyId() {
    return currentKeyId.incrementAndGet();
  }

  public int getDelegationTokenSeqNum() {
    return tokenSequenceNumber.get();
  }

  public void setDelegationTokenSeqNum(int seqNum) {
    tokenSequenceNumber.set(seqNum);
  }

  public int incrementDelegationTokenSeqNum() {
    return tokenSequenceNumber.incrementAndGet();
  }

  /**
   * Update the current master key. This is called once by start method before
   * tokenRemoverThread is created,
   */
  private OzoneSecretKey updateCurrentKey(KeyPair keyPair) throws IOException {
    logger.info("Updating the current master key for generating tokens");

    // TODO: fix me based on the certificate expire time to set the key
    // expire time.
    int newCurrentId = incrementCurrentKeyId();
    OzoneSecretKey newKey = new OzoneSecretKey(newCurrentId, -1,
        keyPair);
    currentKey = newKey;
    return currentKey;
  }

  /**
   * Validates if given hash is valid.
   *
   * @param identifier
   * @param password
   */
  public boolean verifySignature(T identifier, byte[] password) {
    try {
      Signature rsaSignature =
          Signature.getInstance(getDefaultSignatureAlgorithm());
      rsaSignature.initVerify(currentKey.getPublicKey());
      rsaSignature.update(identifier.getBytes());
      return rsaSignature.verify(password);
    } catch (NoSuchAlgorithmException | SignatureException |
        InvalidKeyException e) {
      return false;
    }
  }

  public String formatTokenId(T id) {
    return "(" + id + ")";
  }

  /**
   * Should be called before this object is used.
   *
   * @param keyPair
   * @throws IOException
   */
  public synchronized void start(KeyPair keyPair) throws IOException {
    Preconditions.checkState(!isRunning());
    updateCurrentKey(keyPair);
    setIsRunning(true);
  }

  /**
   * Stops the OzoneDelegationTokenSecretManager.
   *
   * @throws IOException
   */
  public synchronized void stop() throws IOException {
    setIsRunning(false);
  }

  public String getDefaultSignatureAlgorithm() {
    return securityConfig.getSignatureAlgo();
  }

  public long getTokenMaxLifetime() {
    return tokenMaxLifetime;
  }

  public long getTokenRenewInterval() {
    return tokenRenewInterval;
  }

  public Text getService() {
    return service;
  }

  /**
   * Is Secret Manager running.
   *
   * @return true if secret mgr is running
   */
  public synchronized boolean isRunning() {
    return running;
  }

  public void setIsRunning(boolean val) {
    running = val;
  }

  public OzoneSecretKey getCurrentKey() {
    return currentKey;
  }

  public AtomicInteger getCurrentKeyId() {
    return currentKeyId;
  }

  public AtomicInteger getTokenSequenceNumber() {
    return tokenSequenceNumber;
  }
}

