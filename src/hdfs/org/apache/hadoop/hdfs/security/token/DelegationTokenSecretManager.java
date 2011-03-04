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

package org.apache.hadoop.hdfs.security.token;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;

public class DelegationTokenSecretManager 
   extends SecretManager<DelegationTokenIdentifier> {
  private static final Log LOG = LogFactory
      .getLog(DelegationTokenSecretManager.class);

  /** 
   * Cache of currently valid tokens, mapping from DelegationTokenIdentifier 
   * to DelegationTokenInformation. Protected by its own lock.
   */
  private final Map<DelegationTokenIdentifier, DelegationTokenInformation> currentTokens 
      = new HashMap<DelegationTokenIdentifier, DelegationTokenInformation>();
  
  /**
   * Sequence number to create DelegationTokenIdentifier
   */
  private int delegationTokenSequenceNumber = 0;
  
  private final Map<Integer, DelegationKey> allKeys 
      = new HashMap<Integer, DelegationKey>();
  
  /**
   * Access to currentId and currentKey is protected by this object lock.
   */
  private int currentId = 0;
  private DelegationKey currentKey;
  
  private long keyUpdateInterval;
  private long tokenMaxLifetime;
  private long tokenRemoverScanInterval;
  private long tokenRenewInterval;
  private Thread tokenRemoverThread;
  private volatile boolean running;
  

  public DelegationTokenSecretManager(long delegationKeyUpdateInterval,
      long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
      long delegationTokenRemoverScanInterval) {
    this.keyUpdateInterval = delegationKeyUpdateInterval;
    this.tokenMaxLifetime = delegationTokenMaxLifetime;
    this.tokenRenewInterval = delegationTokenRenewInterval;
    this.tokenRemoverScanInterval = delegationTokenRemoverScanInterval;
  }
  
  /** should be called before this object is used */
  public synchronized void startThreads() throws IOException {
    updateCurrentKey();
    running = true;
    tokenRemoverThread = new Daemon(new ExpiredTokenRemover());
    tokenRemoverThread.start();
  }
  
  /** 
   * Add a previously used master key to cache (when NN restarts), 
   * should be called before activate().
   * */
  public synchronized void addKey(DelegationKey key) throws IOException {
    if (running) // a safety check
      throw new IOException("Can't add delegation key to a running SecretManager.");
    if (key.getKeyId() > currentId) {
      currentId = key.getKeyId();
    }
    allKeys.put(key.getKeyId(), key);
  }

  public synchronized DelegationKey[] getAllKeys() {
    return allKeys.values().toArray(new DelegationKey[0]);
  }
  
  /** Update the current master key */
  private synchronized void updateCurrentKey() throws IOException {
    LOG.info("Updating the current master key for generating delegation tokens");
    /* Create a new currentKey with an estimated expiry date. */
    currentId++;
    currentKey = new DelegationKey(currentId, System.currentTimeMillis()
        + keyUpdateInterval + tokenMaxLifetime, generateSecret());
    allKeys.put(currentKey.getKeyId(), currentKey);
  }
  
  /** Update the current master key for generating delegation tokens */
  public synchronized void rollMasterKey() throws IOException {
    removeExpiredKeys();
    /* set final expiry date for retiring currentKey */
    currentKey.setExpiryDate(System.currentTimeMillis() + tokenMaxLifetime);
    /*
     * currentKey might have been removed by removeExpiredKeys(), if
     * updateMasterKey() isn't called at expected interval. Add it back to
     * allKeys just in case.
     */
    allKeys.put(currentKey.getKeyId(), currentKey);
    updateCurrentKey();
  }

  private synchronized void removeExpiredKeys() {
    long now = System.currentTimeMillis();
    for (Iterator<Map.Entry<Integer, DelegationKey>> it = allKeys.entrySet()
        .iterator(); it.hasNext();) {
      Map.Entry<Integer, DelegationKey> e = it.next();
      if (e.getValue().getExpiryDate() < now) {
        it.remove();
      }
    }
  }
  
  @Override
  protected byte[] createPassword(DelegationTokenIdentifier identifier) {
    int sequenceNum;
    int id;
    DelegationKey key;
    long now = System.currentTimeMillis();    
    synchronized (this) {
      id = currentId;
      key = currentKey;
      sequenceNum = ++delegationTokenSequenceNumber;
    }
    identifier.setIssueDate(now);
    identifier.setMaxDate(now + tokenMaxLifetime);
    identifier.setMasterKeyId(id);
    identifier.setSequenceNumber(sequenceNum);
    byte[] password = createPassword(identifier.getBytes(), key.getKey());
    synchronized (currentTokens) {
      currentTokens.put(identifier, new DelegationTokenInformation(now
          + tokenRenewInterval, password));
    }
    return password;
  }

  @Override
  public byte[] retrievePassword(DelegationTokenIdentifier identifier
                                 ) throws InvalidToken {
    DelegationTokenInformation info = null;
    synchronized (currentTokens) {
      info = currentTokens.get(identifier);
    }
    if (info == null) {
      throw new InvalidToken("token is expired or doesn't exist");
    }
    long now = System.currentTimeMillis();
    if (info.getRenewDate() < now) {
      throw new InvalidToken("token is expired");
    }
    return info.getPassword();
  }

  /**
   * Renew a delegation token. Canceled tokens are not renewed. Return true if
   * the token is successfully renewed; false otherwise.
   */
  public Boolean renewToken(Token<DelegationTokenIdentifier> token,
      String renewer) throws InvalidToken, IOException {
    long now = System.currentTimeMillis();
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    DelegationTokenIdentifier id = new DelegationTokenIdentifier();
    id.readFields(in);
    synchronized (currentTokens) {
      if (currentTokens.get(id) == null) {
        LOG.warn("Renewal request for unknown token");
        return false;
      }
    }
    if (id.getMaxDate() < now) {
      LOG.warn("Client " + renewer + " tries to renew an expired token");
      return false;
    }
    if (id.getRenewer() == null || !id.getRenewer().toString().equals(renewer)) {
      LOG.warn("Client " + renewer + " tries to renew a token with "
          + "renewer specified as " + id.getRenewer());
      return false;
    }
    DelegationKey key = null;
    synchronized (this) {
      key = allKeys.get(id.getMasterKeyId());
    }
    if (key == null) {
      LOG.warn("Unable to find master key for keyId=" + id.getMasterKeyId() 
          + " from cache. Failed to renew an unexpired token with sequenceNumber=" 
          + id.getSequenceNumber() + ", issued by this key");
      return false;
    }
    byte[] password = createPassword(token.getIdentifier(), key.getKey());
    if (!Arrays.equals(password, token.getPassword())) {
      LOG.warn("Client " + renewer + " is trying to renew a token with wrong password");
      return false;
    }
    DelegationTokenInformation info = new DelegationTokenInformation(
        Math.min(id.getMaxDate(), now + tokenRenewInterval), password);
    synchronized (currentTokens) {
      currentTokens.put(id, info);
    }
    return true;
  }
  
  /**
   * Cancel a token by removing it from cache. Return true if 
   * token exists in cache; false otherwise.
   */
  public Boolean cancelToken(Token<DelegationTokenIdentifier> token,
      String canceller) throws IOException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    DelegationTokenIdentifier id = new DelegationTokenIdentifier();
    id.readFields(in);
    if (id.getRenewer() == null) {
      LOG.warn("Renewer is null: Invalid Identifier");
      return false;
    }
    if (id.getUsername() == null) {
      LOG.warn("owner is null: Invalid Identifier");
      return false;
    }
    String owner = id.getUsername().toString();
    String renewer = id.getRenewer().toString();
    if (!canceller.equals(owner) && !canceller.equals(renewer)) {
      LOG.warn(canceller + " is not authorized to cancel the token");
      return false;
    }
    DelegationTokenInformation info = null;
    synchronized (currentTokens) {
      info = currentTokens.remove(id);
    }
    return info != null;
  }
  
  /**
   * Convert the byte[] to a secret key
   * @param key the byte[] to create the secret key from
   * @return the secret key
   */
  public static SecretKey createSecretKey(byte[] key) {
    return SecretManager.createSecretKey(key);
  }


  /** Utility class to encapsulate a token's renew date and password. */
  private static class DelegationTokenInformation {
    long renewDate;
    byte[] password;
    DelegationTokenInformation(long renewDate, byte[] password) {
      this.renewDate = renewDate;
      this.password = password;
    }
    /** returns renew date */
    long getRenewDate() {
      return renewDate;
    }
    /** returns password */
    byte[] getPassword() {
      return password;
    }
  }
  
  /** Remove expired delegation tokens from cache */
  private void removeExpiredToken() {
    long now = System.currentTimeMillis();
    synchronized (currentTokens) {
      Iterator<DelegationTokenInformation> i = currentTokens.values().iterator();
      while (i.hasNext()) {
        long renewDate = i.next().getRenewDate();
        if (now > renewDate) {
          i.remove();
        }
      }
    }
  }

  public synchronized void stopThreads() {
    if (LOG.isDebugEnabled())
      LOG.debug("Stopping expired delegation token remover thread");
    running = false;
    tokenRemoverThread.interrupt();
    try {
      tokenRemoverThread.join();
    } catch (InterruptedException e) {
    }
  }
  
  private class ExpiredTokenRemover extends Thread {
    private long lastMasterKeyUpdate;
    private long lastTokenCacheCleanup;

    public void run() {
      LOG.info("Starting expired delegation token remover thread, "
          + "tokenRemoverScanInterval=" + tokenRemoverScanInterval
          / (60 * 1000) + " min(s)");
      try {
        while (running) {
          long now = System.currentTimeMillis();
          if (lastMasterKeyUpdate + keyUpdateInterval < now) {
            try {
              rollMasterKey();
              lastMasterKeyUpdate = now;
            } catch (IOException e) {
              LOG.error("Master key updating failed. "
                  + StringUtils.stringifyException(e));
            }
          }
          if (lastTokenCacheCleanup + tokenRemoverScanInterval < now) {
            removeExpiredToken();
            lastTokenCacheCleanup = now;
          }
          Thread.sleep(5000); // 5 seconds
        }
      } catch (InterruptedException ie) {
        LOG
            .error("InterruptedExcpetion recieved for ExpiredTokenRemover thread "
                + ie);
      } catch (Throwable t) {
        LOG.error("ExpiredTokenRemover thread received unexpected exception. "
            + t);
        Runtime.getRuntime().exit(-1);
      }
    }
  }
}
