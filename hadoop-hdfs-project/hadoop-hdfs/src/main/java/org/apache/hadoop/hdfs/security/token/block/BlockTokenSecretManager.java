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

package org.apache.hadoop.hdfs.security.token.block;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * BlockTokenSecretManager can be instantiated in 2 modes, master mode and slave
 * mode. Master can generate new block keys and export block keys to slaves,
 * while slaves can only import and use block keys received from master. Both
 * master and slave can generate and verify block tokens. Typically, master mode
 * is used by NN and slave mode is used by DN.
 */
@InterfaceAudience.Private
public class BlockTokenSecretManager extends
    SecretManager<BlockTokenIdentifier> {
  public static final Log LOG = LogFactory
      .getLog(BlockTokenSecretManager.class);
  
  // We use these in an HA setup to ensure that the pair of NNs produce block
  // token serial numbers that are in different ranges.
  private static final int LOW_MASK  = ~(1 << 31);
  
  public static final Token<BlockTokenIdentifier> DUMMY_TOKEN = new Token<BlockTokenIdentifier>();

  private final boolean isMaster;
  private int nnIndex;
  
  /**
   * keyUpdateInterval is the interval that NN updates its block keys. It should
   * be set long enough so that all live DN's and Balancer should have sync'ed
   * their block keys with NN at least once during each interval.
   */
  private long keyUpdateInterval;
  private volatile long tokenLifetime;
  private int serialNo;
  private BlockKey currentKey;
  private BlockKey nextKey;
  private final Map<Integer, BlockKey> allKeys;
  private String blockPoolId;
  private final String encryptionAlgorithm;
  
  private final SecureRandom nonceGenerator = new SecureRandom();

  public static enum AccessMode {
    READ, WRITE, COPY, REPLACE
  };
  
  /**
   * Constructor for slaves.
   * 
   * @param keyUpdateInterval how often a new key will be generated
   * @param tokenLifetime how long an individual token is valid
   */
  public BlockTokenSecretManager(long keyUpdateInterval,
      long tokenLifetime, String blockPoolId, String encryptionAlgorithm) {
    this(false, keyUpdateInterval, tokenLifetime, blockPoolId,
        encryptionAlgorithm);
  }
  
  /**
   * Constructor for masters.
   * 
   * @param keyUpdateInterval how often a new key will be generated
   * @param tokenLifetime how long an individual token is valid
   * @param nnIndex namenode index
   * @param blockPoolId block pool ID
   * @param encryptionAlgorithm encryption algorithm to use
   */
  public BlockTokenSecretManager(long keyUpdateInterval,
      long tokenLifetime, int nnIndex, String blockPoolId,
      String encryptionAlgorithm) {
    this(true, keyUpdateInterval, tokenLifetime, blockPoolId,
        encryptionAlgorithm);
    Preconditions.checkArgument(nnIndex == 0 || nnIndex == 1);
    this.nnIndex = nnIndex;
    setSerialNo(new SecureRandom().nextInt());
    generateKeys();
  }
  
  private BlockTokenSecretManager(boolean isMaster, long keyUpdateInterval,
      long tokenLifetime, String blockPoolId, String encryptionAlgorithm) {
    this.isMaster = isMaster;
    this.keyUpdateInterval = keyUpdateInterval;
    this.tokenLifetime = tokenLifetime;
    this.allKeys = new HashMap<Integer, BlockKey>();
    this.blockPoolId = blockPoolId;
    this.encryptionAlgorithm = encryptionAlgorithm;
    generateKeys();
  }
  
  @VisibleForTesting
  public synchronized void setSerialNo(int serialNo) {
    this.serialNo = (serialNo & LOW_MASK) | (nnIndex << 31);
  }
  
  public void setBlockPoolId(String blockPoolId) {
    this.blockPoolId = blockPoolId;
  }

  /** Initialize block keys */
  private synchronized void generateKeys() {
    if (!isMaster)
      return;
    /*
     * Need to set estimated expiry dates for currentKey and nextKey so that if
     * NN crashes, DN can still expire those keys. NN will stop using the newly
     * generated currentKey after the first keyUpdateInterval, however it may
     * still be used by DN and Balancer to generate new tokens before they get a
     * chance to sync their keys with NN. Since we require keyUpdInterval to be
     * long enough so that all live DN's and Balancer will sync their keys with
     * NN at least once during the period, the estimated expiry date for
     * currentKey is set to now() + 2 * keyUpdateInterval + tokenLifetime.
     * Similarly, the estimated expiry date for nextKey is one keyUpdateInterval
     * more.
     */
    setSerialNo(serialNo + 1);
    currentKey = new BlockKey(serialNo, Time.now() + 2
        * keyUpdateInterval + tokenLifetime, generateSecret());
    setSerialNo(serialNo + 1);
    nextKey = new BlockKey(serialNo, Time.now() + 3
        * keyUpdateInterval + tokenLifetime, generateSecret());
    allKeys.put(currentKey.getKeyId(), currentKey);
    allKeys.put(nextKey.getKeyId(), nextKey);
  }

  /** Export block keys, only to be used in master mode */
  public synchronized ExportedBlockKeys exportKeys() {
    if (!isMaster)
      return null;
    if (LOG.isDebugEnabled())
      LOG.debug("Exporting access keys");
    return new ExportedBlockKeys(true, keyUpdateInterval, tokenLifetime,
        currentKey, allKeys.values().toArray(new BlockKey[0]));
  }

  private synchronized void removeExpiredKeys() {
    long now = Time.now();
    for (Iterator<Map.Entry<Integer, BlockKey>> it = allKeys.entrySet()
        .iterator(); it.hasNext();) {
      Map.Entry<Integer, BlockKey> e = it.next();
      if (e.getValue().getExpiryDate() < now) {
        it.remove();
      }
    }
  }

  /**
   * Set block keys, only to be used in slave mode
   */
  public synchronized void addKeys(ExportedBlockKeys exportedKeys)
      throws IOException {
    if (isMaster || exportedKeys == null)
      return;
    LOG.info("Setting block keys");
    removeExpiredKeys();
    this.currentKey = exportedKeys.getCurrentKey();
    BlockKey[] receivedKeys = exportedKeys.getAllKeys();
    for (int i = 0; i < receivedKeys.length; i++) {
      if (receivedKeys[i] == null)
        continue;
      this.allKeys.put(receivedKeys[i].getKeyId(), receivedKeys[i]);
    }
  }

  /**
   * Update block keys if update time > update interval.
   * @return true if the keys are updated.
   */
  public synchronized boolean updateKeys(final long updateTime) throws IOException {
    if (updateTime > keyUpdateInterval) {
      return updateKeys();
    }
    return false;
  }

  /**
   * Update block keys, only to be used in master mode
   */
  synchronized boolean updateKeys() throws IOException {
    if (!isMaster)
      return false;

    LOG.info("Updating block keys");
    removeExpiredKeys();
    // set final expiry date of retiring currentKey
    allKeys.put(currentKey.getKeyId(), new BlockKey(currentKey.getKeyId(),
        Time.now() + keyUpdateInterval + tokenLifetime,
        currentKey.getKey()));
    // update the estimated expiry date of new currentKey
    currentKey = new BlockKey(nextKey.getKeyId(), Time.now()
        + 2 * keyUpdateInterval + tokenLifetime, nextKey.getKey());
    allKeys.put(currentKey.getKeyId(), currentKey);
    // generate a new nextKey
    setSerialNo(serialNo + 1);
    nextKey = new BlockKey(serialNo, Time.now() + 3
        * keyUpdateInterval + tokenLifetime, generateSecret());
    allKeys.put(nextKey.getKeyId(), nextKey);
    return true;
  }

  /** Generate an block token for current user */
  public Token<BlockTokenIdentifier> generateToken(ExtendedBlock block,
      EnumSet<AccessMode> modes) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String userID = (ugi == null ? null : ugi.getShortUserName());
    return generateToken(userID, block, modes);
  }

  /** Generate a block token for a specified user */
  public Token<BlockTokenIdentifier> generateToken(String userId,
      ExtendedBlock block, EnumSet<AccessMode> modes) throws IOException {
    BlockTokenIdentifier id = new BlockTokenIdentifier(userId, block
        .getBlockPoolId(), block.getBlockId(), modes);
    return new Token<BlockTokenIdentifier>(id, this);
  }

  /**
   * Check if access should be allowed. userID is not checked if null. This
   * method doesn't check if token password is correct. It should be used only
   * when token password has already been verified (e.g., in the RPC layer).
   */
  public void checkAccess(BlockTokenIdentifier id, String userId,
      ExtendedBlock block, AccessMode mode) throws InvalidToken {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking access for user=" + userId + ", block=" + block
          + ", access mode=" + mode + " using " + id.toString());
    }
    if (userId != null && !userId.equals(id.getUserId())) {
      throw new InvalidToken("Block token with " + id.toString()
          + " doesn't belong to user " + userId);
    }
    if (!id.getBlockPoolId().equals(block.getBlockPoolId())) {
      throw new InvalidToken("Block token with " + id.toString()
          + " doesn't apply to block " + block);
    }
    if (id.getBlockId() != block.getBlockId()) {
      throw new InvalidToken("Block token with " + id.toString()
          + " doesn't apply to block " + block);
    }
    if (isExpired(id.getExpiryDate())) {
      throw new InvalidToken("Block token with " + id.toString()
          + " is expired.");
    }
    if (!id.getAccessModes().contains(mode)) {
      throw new InvalidToken("Block token with " + id.toString()
          + " doesn't have " + mode + " permission");
    }
  }

  /** Check if access should be allowed. userID is not checked if null */
  public void checkAccess(Token<BlockTokenIdentifier> token, String userId,
      ExtendedBlock block, AccessMode mode) throws InvalidToken {
    BlockTokenIdentifier id = new BlockTokenIdentifier();
    try {
      id.readFields(new DataInputStream(new ByteArrayInputStream(token
          .getIdentifier())));
    } catch (IOException e) {
      throw new InvalidToken(
          "Unable to de-serialize block token identifier for user=" + userId
              + ", block=" + block + ", access mode=" + mode);
    }
    checkAccess(id, userId, block, mode);
    if (!Arrays.equals(retrievePassword(id), token.getPassword())) {
      throw new InvalidToken("Block token with " + id.toString()
          + " doesn't have the correct token password");
    }
  }

  private static boolean isExpired(long expiryDate) {
    return Time.now() > expiryDate;
  }

  /**
   * check if a token is expired. for unit test only. return true when token is
   * expired, false otherwise
   */
  static boolean isTokenExpired(Token<BlockTokenIdentifier> token)
      throws IOException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    long expiryDate = WritableUtils.readVLong(in);
    return isExpired(expiryDate);
  }

  /** set token lifetime. */
  public void setTokenLifetime(long tokenLifetime) {
    this.tokenLifetime = tokenLifetime;
  }

  /**
   * Create an empty block token identifier
   * 
   * @return a newly created empty block token identifier
   */
  @Override
  public BlockTokenIdentifier createIdentifier() {
    return new BlockTokenIdentifier();
  }

  /**
   * Create a new password/secret for the given block token identifier.
   * 
   * @param identifier
   *          the block token identifier
   * @return token password/secret
   */
  @Override
  protected byte[] createPassword(BlockTokenIdentifier identifier) {
    BlockKey key = null;
    synchronized (this) {
      key = currentKey;
    }
    if (key == null)
      throw new IllegalStateException("currentKey hasn't been initialized.");
    identifier.setExpiryDate(Time.now() + tokenLifetime);
    identifier.setKeyId(key.getKeyId());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Generating block token for " + identifier.toString());
    }
    return createPassword(identifier.getBytes(), key.getKey());
  }

  /**
   * Look up the token password/secret for the given block token identifier.
   * 
   * @param identifier
   *          the block token identifier to look up
   * @return token password/secret as byte[]
   * @throws InvalidToken
   */
  @Override
  public byte[] retrievePassword(BlockTokenIdentifier identifier)
      throws InvalidToken {
    if (isExpired(identifier.getExpiryDate())) {
      throw new InvalidToken("Block token with " + identifier.toString()
          + " is expired.");
    }
    BlockKey key = null;
    synchronized (this) {
      key = allKeys.get(identifier.getKeyId());
    }
    if (key == null) {
      throw new InvalidToken("Can't re-compute password for "
          + identifier.toString() + ", since the required block key (keyID="
          + identifier.getKeyId() + ") doesn't exist.");
    }
    return createPassword(identifier.getBytes(), key.getKey());
  }
  
  /**
   * Generate a data encryption key for this block pool, using the current
   * BlockKey.
   * 
   * @return a data encryption key which may be used to encrypt traffic
   *         over the DataTransferProtocol
   */
  public DataEncryptionKey generateDataEncryptionKey() {
    byte[] nonce = new byte[8];
    nonceGenerator.nextBytes(nonce);
    BlockKey key = null;
    synchronized (this) {
      key = currentKey;
    }
    byte[] encryptionKey = createPassword(nonce, key.getKey());
    return new DataEncryptionKey(key.getKeyId(), blockPoolId, nonce,
        encryptionKey, Time.now() + tokenLifetime,
        encryptionAlgorithm);
  }
  
  /**
   * Recreate an encryption key based on the given key id and nonce.
   * 
   * @param keyId identifier of the secret key used to generate the encryption key.
   * @param nonce random value used to create the encryption key
   * @return the encryption key which corresponds to this (keyId, blockPoolId, nonce)
   * @throws InvalidEncryptionKeyException
   */
  public byte[] retrieveDataEncryptionKey(int keyId, byte[] nonce)
      throws InvalidEncryptionKeyException {
    BlockKey key = null;
    synchronized (this) {
      key = allKeys.get(keyId);
      if (key == null) {
        throw new InvalidEncryptionKeyException("Can't re-compute encryption key"
            + " for nonce, since the required block key (keyID=" + keyId
            + ") doesn't exist. Current key: " + currentKey.getKeyId());
      }
    }
    return createPassword(nonce, key.getKey());
  }
  
  @VisibleForTesting
  public synchronized void setKeyUpdateIntervalForTesting(long millis) {
    this.keyUpdateInterval = millis;
  }

  @VisibleForTesting
  public void clearAllKeysForTesting() {
    allKeys.clear();
  }
  
  @VisibleForTesting
  public synchronized int getSerialNoForTesting() {
    return serialNo;
  }
  
}
