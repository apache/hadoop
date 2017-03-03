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

package org.apache.hadoop.security.token.delegation;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract 
class AbstractDelegationTokenSecretManager<TokenIdent 
extends AbstractDelegationTokenIdentifier> 
   extends SecretManager<TokenIdent> {
  private static final Log LOG = LogFactory
      .getLog(AbstractDelegationTokenSecretManager.class);

  private String formatTokenId(TokenIdent id) {
    return "(" + id + ")";
  }

  /** 
   * Cache of currently valid tokens, mapping from DelegationTokenIdentifier 
   * to DelegationTokenInformation. Protected by this object lock.
   */
  protected final Map<TokenIdent, DelegationTokenInformation> currentTokens 
      = new HashMap<TokenIdent, DelegationTokenInformation>();
  
  /**
   * Sequence number to create DelegationTokenIdentifier.
   * Protected by this object lock.
   */
  protected int delegationTokenSequenceNumber = 0;
  
  /**
   * Access to allKeys is protected by this object lock
   */
  protected final Map<Integer, DelegationKey> allKeys 
      = new HashMap<Integer, DelegationKey>();
  
  /**
   * Access to currentId is protected by this object lock.
   */
  protected int currentId = 0;
  /**
   * Access to currentKey is protected by this object lock
   */
  private DelegationKey currentKey;
  
  private long keyUpdateInterval;
  private long tokenMaxLifetime;
  private long tokenRemoverScanInterval;
  private long tokenRenewInterval;
  /**
   * Whether to store a token's tracking ID in its TokenInformation.
   * Can be overridden by a subclass.
   */
  protected boolean storeTokenTrackingId;
  private Thread tokenRemoverThread;
  protected volatile boolean running;

  /**
   * If the delegation token update thread holds this lock, it will
   * not get interrupted.
   */
  protected Object noInterruptsLock = new Object();

  /**
   * Create a secret manager
   * @param delegationKeyUpdateInterval the number of milliseconds for rolling
   *        new secret keys.
   * @param delegationTokenMaxLifetime the maximum lifetime of the delegation
   *        tokens in milliseconds
   * @param delegationTokenRenewInterval how often the tokens must be renewed
   *        in milliseconds
   * @param delegationTokenRemoverScanInterval how often the tokens are scanned
   *        for expired tokens in milliseconds
   */
  public AbstractDelegationTokenSecretManager(long delegationKeyUpdateInterval,
      long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
      long delegationTokenRemoverScanInterval) {
    this.keyUpdateInterval = delegationKeyUpdateInterval;
    this.tokenMaxLifetime = delegationTokenMaxLifetime;
    this.tokenRenewInterval = delegationTokenRenewInterval;
    this.tokenRemoverScanInterval = delegationTokenRemoverScanInterval;
    this.storeTokenTrackingId = false;
  }

  /** should be called before this object is used */
  public void startThreads() throws IOException {
    Preconditions.checkState(!running);
    updateCurrentKey();
    synchronized (this) {
      running = true;
      tokenRemoverThread = new Daemon(new ExpiredTokenRemover());
      tokenRemoverThread.start();
    }
  }
  
  /**
   * Reset all data structures and mutable state.
   */
  public synchronized void reset() {
    setCurrentKeyId(0);
    allKeys.clear();
    setDelegationTokenSeqNum(0);
    currentTokens.clear();
  }
  
  /** 
   * Add a previously used master key to cache (when NN restarts), 
   * should be called before activate(). 
   * */
  public synchronized void addKey(DelegationKey key) throws IOException {
    if (running) // a safety check
      throw new IOException("Can't add delegation key to a running SecretManager.");
    if (key.getKeyId() > getCurrentKeyId()) {
      setCurrentKeyId(key.getKeyId());
    }
    allKeys.put(key.getKeyId(), key);
  }

  public synchronized DelegationKey[] getAllKeys() {
    return allKeys.values().toArray(new DelegationKey[0]);
  }

  // HDFS
  protected void logUpdateMasterKey(DelegationKey key) throws IOException {
    return;
  }

  // HDFS
  protected void logExpireToken(TokenIdent ident) throws IOException {
    return;
  }

  // RM
  protected void storeNewMasterKey(DelegationKey key) throws IOException {
    return;
  }

  // RM
  protected void removeStoredMasterKey(DelegationKey key) {
    return;
  }

  // RM
  protected void storeNewToken(TokenIdent ident, long renewDate) throws IOException{
    return;
  }

  // RM
  protected void removeStoredToken(TokenIdent ident) throws IOException {

  }
  // RM
  protected void updateStoredToken(TokenIdent ident, long renewDate) throws IOException {
    return;
  }

  /**
   * For subclasses externalizing the storage, for example Zookeeper
   * based implementations
   */
  protected synchronized int getCurrentKeyId() {
    return currentId;
  }

  /**
   * For subclasses externalizing the storage, for example Zookeeper
   * based implementations
   */
  protected synchronized int incrementCurrentKeyId() {
    return ++currentId;
  }

  /**
   * For subclasses externalizing the storage, for example Zookeeper
   * based implementations
   */
  protected synchronized void setCurrentKeyId(int keyId) {
    currentId = keyId;
  }

  /**
   * For subclasses externalizing the storage, for example Zookeeper
   * based implementations
   */
  protected synchronized int getDelegationTokenSeqNum() {
    return delegationTokenSequenceNumber;
  }

  /**
   * For subclasses externalizing the storage, for example Zookeeper
   * based implementations
   */
  protected synchronized int incrementDelegationTokenSeqNum() {
    return ++delegationTokenSequenceNumber;
  }

  /**
   * For subclasses externalizing the storage, for example Zookeeper
   * based implementations
   */
  protected synchronized void setDelegationTokenSeqNum(int seqNum) {
    delegationTokenSequenceNumber = seqNum;
  }

  /**
   * For subclasses externalizing the storage, for example Zookeeper
   * based implementations
   */
  protected DelegationKey getDelegationKey(int keyId) {
    return allKeys.get(keyId);
  }

  /**
   * For subclasses externalizing the storage, for example Zookeeper
   * based implementations
   */
  protected void storeDelegationKey(DelegationKey key) throws IOException {
    allKeys.put(key.getKeyId(), key);
    storeNewMasterKey(key);
  }

  /**
   * For subclasses externalizing the storage, for example Zookeeper
   * based implementations
   */
  protected void updateDelegationKey(DelegationKey key) throws IOException {
    allKeys.put(key.getKeyId(), key);
  }

  /**
   * For subclasses externalizing the storage, for example Zookeeper
   * based implementations
   */
  protected DelegationTokenInformation getTokenInfo(TokenIdent ident) {
    return currentTokens.get(ident);
  }

  /**
   * For subclasses externalizing the storage, for example Zookeeper
   * based implementations
   */
  protected void storeToken(TokenIdent ident,
      DelegationTokenInformation tokenInfo) throws IOException {
    currentTokens.put(ident, tokenInfo);
    storeNewToken(ident, tokenInfo.getRenewDate());
  }

  /**
   * For subclasses externalizing the storage, for example Zookeeper
   * based implementations
   */
  protected void updateToken(TokenIdent ident,
      DelegationTokenInformation tokenInfo) throws IOException {
    currentTokens.put(ident, tokenInfo);
    updateStoredToken(ident, tokenInfo.getRenewDate());
  }

  /**
   * This method is intended to be used for recovering persisted delegation
   * tokens
   * This method must be called before this secret manager is activated (before
   * startThreads() is called)
   * @param identifier identifier read from persistent storage
   * @param renewDate token renew time
   * @throws IOException
   */
  public synchronized void addPersistedDelegationToken(
      TokenIdent identifier, long renewDate) throws IOException {
    if (running) {
      // a safety check
      throw new IOException(
          "Can't add persisted delegation token to a running SecretManager.");
    }
    int keyId = identifier.getMasterKeyId();
    DelegationKey dKey = allKeys.get(keyId);
    if (dKey == null) {
      LOG.warn("No KEY found for persisted identifier "
          + formatTokenId(identifier));
      return;
    }
    byte[] password = createPassword(identifier.getBytes(), dKey.getKey());
    if (identifier.getSequenceNumber() > getDelegationTokenSeqNum()) {
      setDelegationTokenSeqNum(identifier.getSequenceNumber());
    }
    if (getTokenInfo(identifier) == null) {
      currentTokens.put(identifier, new DelegationTokenInformation(renewDate,
          password, getTrackingIdIfEnabled(identifier)));
    } else {
      throw new IOException("Same delegation token being added twice: "
          + formatTokenId(identifier));
    }
  }

  /** 
   * Update the current master key 
   * This is called once by startThreads before tokenRemoverThread is created, 
   * and only by tokenRemoverThread afterwards.
   */
  private void updateCurrentKey() throws IOException {
    LOG.info("Updating the current master key for generating delegation tokens");
    /* Create a new currentKey with an estimated expiry date. */
    int newCurrentId;
    synchronized (this) {
      newCurrentId = incrementCurrentKeyId();
    }
    DelegationKey newKey = new DelegationKey(newCurrentId, System
        .currentTimeMillis()
        + keyUpdateInterval + tokenMaxLifetime, generateSecret());
    //Log must be invoked outside the lock on 'this'
    logUpdateMasterKey(newKey);
    synchronized (this) {
      currentKey = newKey;
      storeDelegationKey(currentKey);
    }
  }
  
  /** 
   * Update the current master key for generating delegation tokens 
   * It should be called only by tokenRemoverThread.
   */
  void rollMasterKey() throws IOException {
    synchronized (this) {
      removeExpiredKeys();
      /* set final expiry date for retiring currentKey */
      currentKey.setExpiryDate(Time.now() + tokenMaxLifetime);
      /*
       * currentKey might have been removed by removeExpiredKeys(), if
       * updateMasterKey() isn't called at expected interval. Add it back to
       * allKeys just in case.
       */
      updateDelegationKey(currentKey);
    }
    updateCurrentKey();
  }

  private synchronized void removeExpiredKeys() {
    long now = Time.now();
    for (Iterator<Map.Entry<Integer, DelegationKey>> it = allKeys.entrySet()
        .iterator(); it.hasNext();) {
      Map.Entry<Integer, DelegationKey> e = it.next();
      if (e.getValue().getExpiryDate() < now) {
        it.remove();
        // ensure the tokens generated by this current key can be recovered
        // with this current key after this current key is rolled
        if(!e.getValue().equals(currentKey))
          removeStoredMasterKey(e.getValue());
      }
    }
  }
  
  @Override
  protected synchronized byte[] createPassword(TokenIdent identifier) {
    int sequenceNum;
    long now = Time.now();
    sequenceNum = incrementDelegationTokenSeqNum();
    identifier.setIssueDate(now);
    identifier.setMaxDate(now + tokenMaxLifetime);
    identifier.setMasterKeyId(currentKey.getKeyId());
    identifier.setSequenceNumber(sequenceNum);
    LOG.info("Creating password for identifier: " + formatTokenId(identifier)
        + ", currentKey: " + currentKey.getKeyId());
    byte[] password = createPassword(identifier.getBytes(), currentKey.getKey());
    DelegationTokenInformation tokenInfo = new DelegationTokenInformation(now
        + tokenRenewInterval, password, getTrackingIdIfEnabled(identifier));
    try {
      storeToken(identifier, tokenInfo);
    } catch (IOException ioe) {
      LOG.error("Could not store token " + formatTokenId(identifier) + "!!",
          ioe);
    }
    return password;
  }
  


  /**
   * Find the DelegationTokenInformation for the given token id, and verify that
   * if the token is expired. Note that this method should be called with 
   * acquiring the secret manager's monitor.
   */
  protected DelegationTokenInformation checkToken(TokenIdent identifier)
      throws InvalidToken {
    assert Thread.holdsLock(this);
    DelegationTokenInformation info = getTokenInfo(identifier);
    if (info == null) {
      throw new InvalidToken("token " + formatTokenId(identifier)
          + " can't be found in cache");
    }
    long now = Time.now();
    if (info.getRenewDate() < now) {
      throw new InvalidToken("token " + formatTokenId(identifier) + " is " +
          "expired, current time: " + Time.formatTime(now) +
          " expected renewal time: " + Time.formatTime(info.getRenewDate()));
    }
    return info;
  }
  
  @Override
  public synchronized byte[] retrievePassword(TokenIdent identifier)
      throws InvalidToken {
    return checkToken(identifier).getPassword();
  }

  protected String getTrackingIdIfEnabled(TokenIdent ident) {
    if (storeTokenTrackingId) {
      return ident.getTrackingId();
    }
    return null;
  }

  public synchronized String getTokenTrackingId(TokenIdent identifier) {
    DelegationTokenInformation info = getTokenInfo(identifier);
    if (info == null) {
      return null;
    }
    return info.getTrackingId();
  }

  /**
   * Verifies that the given identifier and password are valid and match.
   * @param identifier Token identifier.
   * @param password Password in the token.
   * @throws InvalidToken
   */
  public synchronized void verifyToken(TokenIdent identifier, byte[] password)
      throws InvalidToken {
    byte[] storedPassword = retrievePassword(identifier);
    if (!MessageDigest.isEqual(password, storedPassword)) {
      throw new InvalidToken("token " + formatTokenId(identifier)
          + " is invalid, password doesn't match");
    }
  }
  
  /**
   * Renew a delegation token.
   * @param token the token to renew
   * @param renewer the full principal name of the user doing the renewal
   * @return the new expiration time
   * @throws InvalidToken if the token is invalid
   * @throws AccessControlException if the user can't renew token
   */
  public synchronized long renewToken(Token<TokenIdent> token,
                         String renewer) throws InvalidToken, IOException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    TokenIdent id = createIdentifier();
    id.readFields(in);
    LOG.info("Token renewal for identifier: " + formatTokenId(id)
        + "; total currentTokens " +  currentTokens.size());

    long now = Time.now();
    if (id.getMaxDate() < now) {
      throw new InvalidToken(renewer + " tried to renew an expired token "
          + formatTokenId(id) + " max expiration date: "
          + Time.formatTime(id.getMaxDate())
          + " currentTime: " + Time.formatTime(now));
    }
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
    DelegationKey key = getDelegationKey(id.getMasterKeyId());
    if (key == null) {
      throw new InvalidToken("Unable to find master key for keyId="
          + id.getMasterKeyId()
          + " from cache. Failed to renew an unexpired token "
          + formatTokenId(id) + " with sequenceNumber="
          + id.getSequenceNumber());
    }
    byte[] password = createPassword(token.getIdentifier(), key.getKey());
    if (!MessageDigest.isEqual(password, token.getPassword())) {
      throw new AccessControlException(renewer
          + " is trying to renew a token "
          + formatTokenId(id) + " with wrong password");
    }
    long renewTime = Math.min(id.getMaxDate(), now + tokenRenewInterval);
    String trackingId = getTrackingIdIfEnabled(id);
    DelegationTokenInformation info = new DelegationTokenInformation(renewTime,
        password, trackingId);

    if (getTokenInfo(id) == null) {
      throw new InvalidToken("Renewal request for unknown token "
          + formatTokenId(id));
    }
    updateToken(id, info);
    return renewTime;
  }
  
  /**
   * Cancel a token by removing it from cache.
   * @return Identifier of the canceled token
   * @throws InvalidToken for invalid token
   * @throws AccessControlException if the user isn't allowed to cancel
   */
  public synchronized TokenIdent cancelToken(Token<TokenIdent> token,
      String canceller) throws IOException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    TokenIdent id = createIdentifier();
    id.readFields(in);
    LOG.info("Token cancellation requested for identifier: "
        + formatTokenId(id));
    
    if (id.getUser() == null) {
      throw new InvalidToken("Token with no owner " + formatTokenId(id));
    }
    String owner = id.getUser().getUserName();
    Text renewer = id.getRenewer();
    HadoopKerberosName cancelerKrbName = new HadoopKerberosName(canceller);
    String cancelerShortName = cancelerKrbName.getShortName();
    if (!canceller.equals(owner)
        && (renewer == null || renewer.toString().isEmpty() || !cancelerShortName
            .equals(renewer.toString()))) {
      throw new AccessControlException(canceller
          + " is not authorized to cancel the token " + formatTokenId(id));
    }
    DelegationTokenInformation info = currentTokens.remove(id);
    if (info == null) {
      throw new InvalidToken("Token not found " + formatTokenId(id));
    }
    removeStoredToken(id);
    return id;
  }
  
  /**
   * Convert the byte[] to a secret key
   * @param key the byte[] to create the secret key from
   * @return the secret key
   */
  public static SecretKey createSecretKey(byte[] key) {
    return SecretManager.createSecretKey(key);
  }

  /** Class to encapsulate a token's renew date and password. */
  @InterfaceStability.Evolving
  public static class DelegationTokenInformation {
    long renewDate;
    byte[] password;
    String trackingId;

    public DelegationTokenInformation(long renewDate, byte[] password) {
      this(renewDate, password, null);
    }

    public DelegationTokenInformation(long renewDate, byte[] password,
        String trackingId) {
      this.renewDate = renewDate;
      this.password = password;
      this.trackingId = trackingId;
    }
    /** returns renew date */
    public long getRenewDate() {
      return renewDate;
    }
    /** returns password */
    byte[] getPassword() {
      return password;
    }
    /** returns tracking id */
    public String getTrackingId() {
      return trackingId;
    }
  }
  
  /** Remove expired delegation tokens from cache */
  private void removeExpiredToken() throws IOException {
    long now = Time.now();
    Set<TokenIdent> expiredTokens = new HashSet<TokenIdent>();
    synchronized (this) {
      Iterator<Map.Entry<TokenIdent, DelegationTokenInformation>> i =
          currentTokens.entrySet().iterator();
      while (i.hasNext()) {
        Map.Entry<TokenIdent, DelegationTokenInformation> entry = i.next();
        long renewDate = entry.getValue().getRenewDate();
        if (renewDate < now) {
          expiredTokens.add(entry.getKey());
          i.remove();
        }
      }
    }
    // don't hold lock on 'this' to avoid edit log updates blocking token ops
    for (TokenIdent ident : expiredTokens) {
      logExpireToken(ident);
      removeStoredToken(ident);
    }
  }

  public void stopThreads() {
    if (LOG.isDebugEnabled())
      LOG.debug("Stopping expired delegation token remover thread");
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
   * is secretMgr running
   * @return true if secret mgr is running
   */
  public synchronized boolean isRunning() {
    return running;
  }
  
  private class ExpiredTokenRemover extends Thread {
    private long lastMasterKeyUpdate;
    private long lastTokenCacheCleanup;

    @Override
    public void run() {
      LOG.info("Starting expired delegation token remover thread, "
          + "tokenRemoverScanInterval=" + tokenRemoverScanInterval
          / (60 * 1000) + " min(s)");
      try {
        while (running) {
          long now = Time.now();
          if (lastMasterKeyUpdate + keyUpdateInterval < now) {
            try {
              rollMasterKey();
              lastMasterKeyUpdate = now;
            } catch (IOException e) {
              LOG.error("Master key updating failed: ", e);
            }
          }
          if (lastTokenCacheCleanup + tokenRemoverScanInterval < now) {
            removeExpiredToken();
            lastTokenCacheCleanup = now;
          }
          try {
            Thread.sleep(Math.min(5000, keyUpdateInterval)); // 5 seconds
          } catch (InterruptedException ie) {
            LOG.error("ExpiredTokenRemover received " + ie);
          }
        }
      } catch (Throwable t) {
        LOG.error("ExpiredTokenRemover thread received unexpected exception", t);
        Runtime.getRuntime().exit(-1);
      }
    }
  }

  /**
   * Decode the token identifier. The subclass can customize the way to decode
   * the token identifier.
   * 
   * @param token the token where to extract the identifier
   * @return the delegation token identifier
   * @throws IOException
   */
  public TokenIdent decodeTokenIdentifier(Token<TokenIdent> token) throws IOException {
    return token.decodeIdentifier();
  }

}
