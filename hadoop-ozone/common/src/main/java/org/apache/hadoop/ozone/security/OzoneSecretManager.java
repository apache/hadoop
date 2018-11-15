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
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.security.OzoneSecretStore.OzoneManagerSecretState;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier.TokenInfo;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SecretManager for Ozone Master. Responsible for signing identifiers with
 * private key,
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OzoneSecretManager<T extends OzoneTokenIdentifier>
    extends SecretManager<T> {

  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneSecretManager.class);
  /**
   * The name of the Private/Public Key based hashing algorithm.
   */
  private static final String DEFAULT_SIGNATURE_ALGORITHM = "SHA256withRSA";
  private final long tokenMaxLifetime;
  private final long tokenRenewInterval;
  private final long tokenRemoverScanInterval;
  private final Text service;
  private final Map<Integer, OzoneSecretKey> allKeys;
  private final Map<T, TokenInfo> currentTokens;
  private final OzoneSecretStore store;
  private Thread tokenRemoverThread;
  private volatile boolean running;
  private AtomicInteger tokenSequenceNumber;
  private OzoneSecretKey currentKey;
  private AtomicInteger currentKeyId;
  /**
   * If the delegation token update thread holds this lock, it will not get
   * interrupted.
   */
  private Object noInterruptsLock = new Object();
  private int maxKeyLength;

  /**
   * Create a secret manager.
   *
   * @param conf configuration.
   * @param tokenMaxLifetime the maximum lifetime of the delegation tokens in
   * milliseconds
   * @param tokenRenewInterval how often the tokens must be renewed in
   * milliseconds
   * @param dtRemoverScanInterval how often the tokens are scanned for expired
   * tokens in milliseconds
   */
  public OzoneSecretManager(OzoneConfiguration conf, long tokenMaxLifetime,
      long tokenRenewInterval, long dtRemoverScanInterval, Text service)
      throws IOException {
    this.tokenMaxLifetime = tokenMaxLifetime;
    this.tokenRenewInterval = tokenRenewInterval;
    this.tokenRemoverScanInterval = dtRemoverScanInterval;

    currentTokens = new ConcurrentHashMap();
    allKeys = new ConcurrentHashMap<>();
    currentKeyId = new AtomicInteger();
    tokenSequenceNumber = new AtomicInteger();
    this.store = new OzoneSecretStore(conf);
    loadTokenSecretState(store.loadState());
    this.service = service;
    this.maxKeyLength = conf.getInt(OzoneConfigKeys.OZONE_MAX_KEY_LEN,
        OzoneConfigKeys.OZONE_MAX_KEY_LEN_DEFAULT);
  }

  @Override
  public T createIdentifier() {
    return (T) T.newInstance();
  }

  /**
   * Create new Identifier with given,owner,renwer and realUser.
   *
   * @return T
   */
  public T createIdentifier(Text owner, Text renewer, Text realUser) {
    return (T) T.newInstance(owner, renewer, realUser);
  }

  /**
   * Returns {@link Token} for given identifier.
   *
   * @param owner
   * @param renewer
   * @param realUser
   * @return Token
   * @throws IOException to allow future exceptions to be added without breaking
   *                     compatibility
   */
  public Token<T> createToken(Text owner, Text renewer, Text realUser)
      throws IOException {
    T identifier = createIdentifier(owner, renewer, realUser);
    updateIdentifierDetails(identifier);

    byte[] password = createPassword(identifier.getBytes(),
        currentKey.getPrivateKey());
    addToTokenStore(identifier, password);
    Token<T> token = new Token<>(identifier.getBytes(), password,
        identifier.getKind(), service);
    if (LOG.isTraceEnabled()) {
      long expiryTime = identifier.getIssueDate() + tokenRenewInterval;
      String tokenId = identifier.toStringStable();
      LOG.trace("Issued delegation token -> expiryTime:{},tokenId:{}",
          expiryTime, tokenId);
    }

    return token;
  }

  /**
   * Stores given identifier in token store.
   *
   * @param identifier
   * @param password
   * @throws IOException
   */
  private void addToTokenStore(T identifier, byte[] password)
      throws IOException {
    TokenInfo tokenInfo = new TokenInfo(identifier.getIssueDate()
        + tokenRenewInterval, password, identifier.getTrackingId());
    currentTokens.put(identifier, tokenInfo);
    store.storeToken(identifier, tokenInfo.getRenewDate());
  }

  /**
   * Updates issue date, master key id and sequence number for identifier.
   *
   * @param identifier the identifier to validate
   */
  private void updateIdentifierDetails(T identifier) {
    int sequenceNum;
    long now = Time.monotonicNow();
    sequenceNum = incrementDelegationTokenSeqNum();
    identifier.setIssueDate(now);
    identifier.setMasterKeyId(currentKey.getKeyId());
    identifier.setSequenceNumber(sequenceNum);
    identifier.setMaxDate(Time.monotonicNow() + tokenMaxLifetime);
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
          DEFAULT_SIGNATURE_ALGORITHM);
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
    LOG.debug("Creating password for identifier: {}, currentKey: {}",
        formatTokenId(identifier), currentKey.getKeyId());
    byte[] password = null;
    try {
      password = createPassword(identifier.getBytes(),
          currentKey.getPrivateKey());
    } catch (IOException ioe) {
      LOG.error("Could not store token {}!!", formatTokenId(identifier),
          ioe);
    }
    return password;
  }

  @Override
  public byte[] retrievePassword(T identifier) throws InvalidToken {
    return checkToken(identifier).getPassword();
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
  public synchronized long renewToken(Token<T> token, String renewer)
      throws IOException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    T id = (T) T.readProtoBuf(in);
    LOG.debug("Token renewal for identifier: {}, total currentTokens: {}",
        formatTokenId(id), currentTokens.size());

    long now = Time.monotonicNow();
    if (id.getMaxDate() < now) {
      throw new InvalidToken(renewer + " tried to renew an expired token "
          + formatTokenId(id) + " max expiration date: "
          + Time.formatTime(id.getMaxDate())
          + " currentTime: " + Time.formatTime(now));
    }
    checkToken(id);
    if ((id.getRenewer() == null) || (id.getRenewer().toString().isEmpty())) {
      throw new AccessControlException(renewer +
          " tried to renew a token " + formatTokenId(id)
          + " without a renewer");
    }
    if (!id.getRenewer().toString().equals(renewer)) {
      throw new AccessControlException(renewer
          + " tries to renew a token " + formatTokenId(id)
          + " with non-matching renewer " + id.getRenewer());
    }
    OzoneSecretKey key = allKeys.get(id.getMasterKeyId());
    if (key == null) {
      throw new InvalidToken("Unable to find master key for keyId="
          + id.getMasterKeyId()
          + " from cache. Failed to renew an unexpired token "
          + formatTokenId(id) + " with sequenceNumber="
          + id.getSequenceNumber());
    }
    byte[] password = createPassword(token.getIdentifier(),
        key.getPrivateKey());

    long renewTime = Math.min(id.getMaxDate(), now + tokenRenewInterval);
    try {
      addToTokenStore(id, password);
    } catch (IOException e) {
      LOG.error("Unable to update token " + id.getSequenceNumber(), e);
    }
    return renewTime;
  }

  /**
   * Cancel a token by removing it from store and cache.
   *
   * @return Identifier of the canceled token
   * @throws InvalidToken           for invalid token
   * @throws AccessControlException if the user isn't allowed to cancel
   */
  public T cancelToken(Token<T> token, String canceller) throws IOException {
    T id = (T) T.readProtoBuf(token.getIdentifier());
    LOG.debug("Token cancellation requested for identifier: {}",
        formatTokenId(id));

    if (id.getUser() == null) {
      throw new InvalidToken("Token with no owner " + formatTokenId(id));
    }
    String owner = id.getUser().getUserName();
    Text renewer = id.getRenewer();
    HadoopKerberosName cancelerKrbName = new HadoopKerberosName(canceller);
    String cancelerShortName = cancelerKrbName.getShortName();
    if (!canceller.equals(owner)
        && (renewer == null || renewer.toString().isEmpty()
        || !cancelerShortName
        .equals(renewer.toString()))) {
      throw new AccessControlException(canceller
          + " is not authorized to cancel the token " + formatTokenId(id));
    }
    try {
      store.removeToken(id);
    } catch (IOException e) {
      LOG.error("Unable to remove token " + id.getSequenceNumber(), e);
    }
    TokenInfo info = currentTokens.remove(id);
    if (info == null) {
      throw new InvalidToken("Token not found " + formatTokenId(id));
    }
    return id;
  }

  public int getCurrentKeyId() {
    return currentKeyId.get();
  }

  public void setCurrentKeyId(int keyId) {
    currentKeyId.set(keyId);
  }

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
   * Validates if given token is valid.
   *
   * @param identifier
   * @param password
   */
  private boolean validateToken(T identifier, byte[] password) {
    try {
      Signature rsaSignature = Signature.getInstance("SHA256withRSA");
      rsaSignature.initVerify(currentKey.getPublicKey());
      rsaSignature.update(identifier.getBytes());
      return rsaSignature.verify(password);
    } catch (NoSuchAlgorithmException | SignatureException |
        InvalidKeyException e) {
      return false;
    }
  }

  /**
   * Checks if TokenInfo for the given identifier exists in database and if the
   * token is expired.
   */
  public TokenInfo checkToken(T identifier) throws InvalidToken {
    TokenInfo info = currentTokens.get(identifier);
    if (info == null) {
      throw new InvalidToken("token " + formatTokenId(identifier)
          + " can't be found in cache");
    }
    long now = Time.monotonicNow();
    if (info.getRenewDate() < now) {
      throw new InvalidToken("token " + formatTokenId(identifier) + " is " +
          "expired, current time: " + Time.formatTime(now) +
          " expected renewal time: " + Time.formatTime(info.getRenewDate()));
    }
    if (!validateToken(identifier, info.getPassword())) {
      throw new InvalidToken("Tampared/Inavalid token.");
    }
    return info;
  }

  // TODO: handle roll private key/certificate
  private synchronized void removeExpiredKeys() {
    long now = Time.monotonicNow();
    for (Iterator<Map.Entry<Integer, OzoneSecretKey>> it = allKeys.entrySet()
        .iterator(); it.hasNext();) {
      Map.Entry<Integer, OzoneSecretKey> e = it.next();
      OzoneSecretKey key = e.getValue();
      if (key.getExpiryDate() < now && key.getExpiryDate() != -1) {
        if (!key.equals(currentKey)) {
          it.remove();
          try {
            store.removeTokenMasterKey(key);
          } catch (IOException ex) {
            LOG.error("Unable to remove master key " + key.getKeyId(), ex);
          }
        }
      }
    }
  }

  private void loadTokenSecretState(OzoneManagerSecretState<T> state)
      throws IOException {
    LOG.info("Loading token state into token manager.");
    for (OzoneSecretKey key : state.ozoneManagerSecretState()) {
      allKeys.putIfAbsent(key.getKeyId(), key);
    }
    for (Map.Entry<T, Long> entry : state.getTokenState().entrySet()) {
      addPersistedDelegationToken(entry.getKey(), entry.getValue());
    }
  }

  private String formatTokenId(T id) {
    return "(" + id + ")";
  }

  private void addPersistedDelegationToken(
      T identifier, long renewDate)
      throws IOException {
    if (running) {
      // a safety check
      throw new IOException(
          "Can't add persisted delegation token to a running SecretManager.");
    }
    int keyId = identifier.getMasterKeyId();
    OzoneSecretKey dKey = allKeys.get(keyId);
    if (dKey == null) {
      LOG.warn("No KEY found for persisted identifier "
          + formatTokenId(identifier));
      return;
    }

    PrivateKey privateKey = dKey.getPrivateKey();
    byte[] password = createPassword(identifier.getBytes(), privateKey);
    if (identifier.getSequenceNumber() > getDelegationTokenSeqNum()) {
      setDelegationTokenSeqNum(identifier.getSequenceNumber());
    }
    if (currentTokens.get(identifier) == null) {
      currentTokens.put(identifier, new TokenInfo(renewDate,
          password, identifier.getTrackingId()));
    } else {
      throw new IOException("Same delegation token being added twice: "
          + formatTokenId(identifier));
    }
  }

  /**
   * Should be called before this object is used.
   */
  public void startThreads(KeyPair keyPair) throws IOException {
    Preconditions.checkState(!running);
    updateCurrentKey(keyPair);
    removeExpiredKeys();
    synchronized (this) {
      running = true;
      tokenRemoverThread = new Daemon(new ExpiredTokenRemover());
      tokenRemoverThread.start();
    }
  }

  public void stopThreads() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping expired delegation token remover thread");
    }
    running = false;

    if (tokenRemoverThread != null) {
      synchronized (noInterruptsLock) {
        tokenRemoverThread.interrupt();
      }
      try {
        tokenRemoverThread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(
            "Unable to join on token removal thread", e);
      }
    }
  }

  /**
   * Stops the OzoneSecretManager.
   *
   * @throws IOException
   */
  public void stop() throws IOException {
    stopThreads();
    if (this.store != null) {
      this.store.close();
    }
  }

  /**
   * Update the current master key. This is called once by startThreads before
   * tokenRemoverThread is created,
   */
  private void updateCurrentKey(KeyPair keyPair) throws IOException {
    LOG.info("Updating the current master key for generating tokens");

    // TODO: fix me based on the certificate expire time to set the key
    // expire time.
    int newCurrentId = incrementCurrentKeyId();
    OzoneSecretKey newKey = new OzoneSecretKey(newCurrentId, -1,
        keyPair, maxKeyLength);

    store.storeTokenMasterKey(newKey);
    if (!allKeys.containsKey(newKey.getKeyId())) {
      allKeys.put(newKey.getKeyId(), newKey);
    }

    synchronized (this) {
      currentKey = newKey;
    }
  }

  /**
   * Remove expired delegation tokens from cache and persisted store.
   */
  private void removeExpiredToken() throws IOException {
    long now = Time.monotonicNow();
    synchronized (this) {
      Iterator<Map.Entry<T,
          TokenInfo>> i = currentTokens.entrySet().iterator();
      while (i.hasNext()) {
        Map.Entry<T,
            TokenInfo> entry = i.next();
        long renewDate = entry.getValue().getRenewDate();
        if (renewDate < now) {
          i.remove();
          store.removeToken(entry.getKey());
        }
      }
    }
  }

  /**
   * Is Secret Manager running.
   *
   * @return true if secret mgr is running
   */
  public synchronized boolean isRunning() {
    return running;
  }

  /**
   * Returns expiry time of a token given its identifier.
   *
   * @param dtId DelegationTokenIdentifier of a token
   * @return Expiry time of the token
   * @throws IOException
   */
  public long getTokenExpiryTime(T dtId)
      throws IOException {
    TokenInfo info = currentTokens.get(dtId);
    if (info != null) {
      return info.getRenewDate();
    } else {
      throw new IOException("No delegation token found for this identifier");
    }
  }

  private class ExpiredTokenRemover extends Thread {
    private long lastTokenCacheCleanup;

    @Override
    public void run() {
      LOG.info("Starting expired delegation token remover thread, "
          + "tokenRemoverScanInterval=" + tokenRemoverScanInterval
          / (60 * 1000) + " min(s)");
      try {
        while (running) {
          long now = Time.monotonicNow();
          if (lastTokenCacheCleanup + tokenRemoverScanInterval
              < now) {
            removeExpiredToken();
            lastTokenCacheCleanup = now;
          }
          try {
            Thread.sleep(Math.min(5000,
                tokenRemoverScanInterval)); // 5 seconds
          } catch (InterruptedException ie) {
            LOG.error("ExpiredTokenRemover received " + ie);
          }
        }
      } catch (Throwable t) {
        LOG.error("ExpiredTokenRemover thread received unexpected exception",
            t);
        Runtime.getRuntime().exit(-1);
      }
    }
  }
}

