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

import com.google.common.base.Charsets;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Timer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * BlockTokenSecretManager can be instantiated in 2 modes, master mode
 * and worker mode. Master can generate new block keys and export block
 * keys to workers, while workers can only import and use block keys
 * received from master. Both master and worker can generate and verify
 * block tokens. Typically, master mode is used by NN and worker mode
 * is used by DN.
 */
@InterfaceAudience.Private
public class BlockTokenSecretManager extends
    SecretManager<BlockTokenIdentifier> {
  public static final Log LOG = LogFactory.getLog(BlockTokenSecretManager.class);

  public static final Token<BlockTokenIdentifier> DUMMY_TOKEN = new Token<BlockTokenIdentifier>();

  /**
   * In order to prevent serial No. of different NameNode from overlapping,
   * Using 6 bits (identify 64=2^6 namenodes, and presuppose that no scenario
   * where deploy more than 64 namenodes (include ANN, SBN, Observers, etc.)
   * in one namespace) to identify index of NameNode, and the remainder 26 bits
   * auto-incr to change the serial No.
   */
  @VisibleForTesting
  public static final int NUM_VALID_BITS = 26;
  private static final int LOW_MASK = (1 << NUM_VALID_BITS) - 1;

  private final boolean isMaster;

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

  private final int nnIndex;

  private final boolean useProto;

  private final boolean shouldWrapQOP;

  private final SecureRandom nonceGenerator = new SecureRandom();

  /**
   * Timer object for querying the current time. Separated out for
   * unit testing.
   */
  private Timer timer;
  /**
   * Constructor for workers.
   *
   * @param keyUpdateInterval how often a new key will be generated
   * @param tokenLifetime how long an individual token is valid
   * @param useProto should we use new protobuf style tokens
   */
  public BlockTokenSecretManager(long keyUpdateInterval,
      long tokenLifetime, String blockPoolId, String encryptionAlgorithm,
      boolean useProto) {
    this(false, keyUpdateInterval, tokenLifetime, blockPoolId,
        encryptionAlgorithm, 0, 1, useProto, false);
  }

  public BlockTokenSecretManager(long keyUpdateInterval,
      long tokenLifetime, int nnIndex, int numNNs, String blockPoolId,
      String encryptionAlgorithm, boolean useProto) {
    this(keyUpdateInterval, tokenLifetime, nnIndex, numNNs,
        blockPoolId, encryptionAlgorithm, useProto, false);
  }

  public BlockTokenSecretManager(long keyUpdateInterval,
      long tokenLifetime, int nnIndex, int numNNs,  String blockPoolId,
      String encryptionAlgorithm, boolean useProto, boolean shouldWrapQOP) {
    this(true, keyUpdateInterval, tokenLifetime, blockPoolId,
        encryptionAlgorithm, nnIndex, numNNs, useProto, shouldWrapQOP);
    Preconditions.checkArgument(nnIndex >= 0);
    Preconditions.checkArgument(numNNs > 0);
    setSerialNo(new SecureRandom().nextInt());
    generateKeys();
  }

  /**
   * Constructor for masters.
   *
   * @param keyUpdateInterval how often a new key will be generated
   * @param tokenLifetime how long an individual token is valid
   * @param nnIndex namenode index of the namenode for which we are creating the manager
   * @param blockPoolId block pool ID
   * @param encryptionAlgorithm encryption algorithm to use
   * @param numNNs number of namenodes possible
   * @param useProto should we use new protobuf style tokens
   * @param shouldWrapQOP should wrap QOP in the block access token
   */
  private BlockTokenSecretManager(boolean isMaster, long keyUpdateInterval,
      long tokenLifetime, String blockPoolId, String encryptionAlgorithm,
      int nnIndex, int numNNs, boolean useProto, boolean shouldWrapQOP) {
    this.nnIndex = nnIndex;
    this.isMaster = isMaster;
    this.keyUpdateInterval = keyUpdateInterval;
    this.tokenLifetime = tokenLifetime;
    this.allKeys = new HashMap<Integer, BlockKey>();
    this.blockPoolId = blockPoolId;
    this.encryptionAlgorithm = encryptionAlgorithm;
    this.useProto = useProto;
    this.shouldWrapQOP = shouldWrapQOP;
    this.timer = new Timer();
    generateKeys();
  }

  @VisibleForTesting
  public synchronized void setSerialNo(int serialNo) {
    this.serialNo = (serialNo & LOW_MASK) | (nnIndex << NUM_VALID_BITS);
  }

  public void setBlockPoolId(String blockPoolId) {
    this.blockPoolId = blockPoolId;
  }

  /** Initialize block keys */
  private synchronized void generateKeys() {
    if (!isMaster) {
      return;
    }
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
    currentKey = new BlockKey(serialNo, timer.now() + 2
        * keyUpdateInterval + tokenLifetime, generateSecret());
    setSerialNo(serialNo + 1);
    nextKey = new BlockKey(serialNo, timer.now() + 3
        * keyUpdateInterval + tokenLifetime, generateSecret());
    allKeys.put(currentKey.getKeyId(), currentKey);
    allKeys.put(nextKey.getKeyId(), nextKey);
  }

  /** Export block keys, only to be used in master mode */
  public synchronized ExportedBlockKeys exportKeys() {
    if (!isMaster) {
      return null;
    }
    LOG.debug("Exporting access keys");
    return new ExportedBlockKeys(true, keyUpdateInterval, tokenLifetime,
        currentKey, allKeys.values().toArray(new BlockKey[0]));
  }

  private synchronized void removeExpiredKeys() {
    long now = timer.now();
    for (Iterator<Map.Entry<Integer, BlockKey>> it = allKeys.entrySet()
        .iterator(); it.hasNext();) {
      Map.Entry<Integer, BlockKey> e = it.next();
      if (e.getValue().getExpiryDate() < now) {
        it.remove();
      }
    }
  }

  /**
   * Set block keys, only to be used in worker mode
   */
  public synchronized void addKeys(ExportedBlockKeys exportedKeys)
      throws IOException {
    if (isMaster || exportedKeys == null) {
      return;
    }
    LOG.info("Setting block keys");
    removeExpiredKeys();
    this.currentKey = exportedKeys.getCurrentKey();
    BlockKey[] receivedKeys = exportedKeys.getAllKeys();
    for (int i = 0; i < receivedKeys.length; i++) {
      if (receivedKeys[i] != null) {
        this.allKeys.put(receivedKeys[i].getKeyId(), receivedKeys[i]);
      }
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
    if (!isMaster) {
      return false;
    }

    LOG.info("Updating block keys");
    removeExpiredKeys();
    // set final expiry date of retiring currentKey
    allKeys.put(currentKey.getKeyId(), new BlockKey(currentKey.getKeyId(),
        timer.now() + keyUpdateInterval + tokenLifetime,
        currentKey.getKey()));
    // update the estimated expiry date of new currentKey
    currentKey = new BlockKey(nextKey.getKeyId(), timer.now()
        + 2 * keyUpdateInterval + tokenLifetime, nextKey.getKey());
    allKeys.put(currentKey.getKeyId(), currentKey);
    // generate a new nextKey
    setSerialNo(serialNo + 1);
    nextKey = new BlockKey(serialNo, timer.now() + 3
        * keyUpdateInterval + tokenLifetime, generateSecret());
    allKeys.put(nextKey.getKeyId(), nextKey);
    return true;
  }

  /** Generate an block token for current user */
  public Token<BlockTokenIdentifier> generateToken(ExtendedBlock block,
      EnumSet<BlockTokenIdentifier.AccessMode> modes,
      StorageType[] storageTypes, String[] storageIds) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String userID = (ugi == null ? null : ugi.getShortUserName());
    return generateToken(userID, block, modes, storageTypes, storageIds);
  }

  /** Generate a block token for a specified user */
  public Token<BlockTokenIdentifier> generateToken(String userId,
      ExtendedBlock block, EnumSet<BlockTokenIdentifier.AccessMode> modes,
      StorageType[] storageTypes, String[] storageIds) {
    BlockTokenIdentifier id = new BlockTokenIdentifier(userId, block
        .getBlockPoolId(), block.getBlockId(), modes, storageTypes,
        storageIds, useProto);
    if (shouldWrapQOP) {
      String qop = Server.getEstablishedQOP();
      if (qop != null) {
        id.setHandshakeMsg(qop.getBytes(Charsets.UTF_8));
      }
    }
    return new Token<BlockTokenIdentifier>(id, this);
  }

  /**
   * Check if access should be allowed. userID is not checked if null. This
   * method doesn't check if token password is correct. It should be used only
   * when token password has already been verified (e.g., in the RPC layer).
   *
   * Some places need to check the access using StorageTypes and for other
   * places the StorageTypes is not relevant.
   */
  public void checkAccess(BlockTokenIdentifier id, String userId,
      ExtendedBlock block, BlockTokenIdentifier.AccessMode mode,
      StorageType[] storageTypes, String[] storageIds) throws InvalidToken {
    checkAccess(id, userId, block, mode);
    if (ArrayUtils.isNotEmpty(storageTypes)) {
      checkAccess(id.getStorageTypes(), storageTypes, "StorageTypes");
    }
    if (ArrayUtils.isNotEmpty(storageIds)) {
      checkAccess(id.getStorageIds(), storageIds, "StorageIDs");
    }
  }

  /**
   * Check if access should be allowed. userID is not checked if null. This
   * method doesn't check if token password is correct. It should be used only
   * when token password has already been verified (e.g., in the RPC layer).
   *
   * Some places need to check the access using StorageTypes and for other
   * places the StorageTypes is not relevant.
   */
  public void checkAccess(BlockTokenIdentifier id, String userId,
      ExtendedBlock block, BlockTokenIdentifier.AccessMode mode,
      StorageType[] storageTypes) throws InvalidToken {
    checkAccess(id, userId, block, mode);
    if (ArrayUtils.isNotEmpty(storageTypes)) {
      checkAccess(id.getStorageTypes(), storageTypes, "StorageTypes");
    }
  }

  public void checkAccess(BlockTokenIdentifier id, String userId,
      ExtendedBlock block, BlockTokenIdentifier.AccessMode mode)
      throws InvalidToken {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking access for user=" + userId + ", block=" + block
          + ", access mode=" + mode + " using " + id);
    }
    if (userId != null && !userId.equals(id.getUserId())) {
      throw new InvalidToken("Block token with " + id
          + " doesn't belong to user " + userId);
    }
    if (!id.getBlockPoolId().equals(block.getBlockPoolId())) {
      throw new InvalidToken("Block token with " + id
          + " doesn't apply to block " + block);
    }
    if (id.getBlockId() != block.getBlockId()) {
      throw new InvalidToken("Block token with " + id
          + " doesn't apply to block " + block);
    }
    if (isExpired(id.getExpiryDate())) {
      throw new InvalidToken("Block token with " + id
          + " is expired.");
    }
    if (!id.getAccessModes().contains(mode)) {
      throw new InvalidToken("Block token with " + id
          + " doesn't have " + mode + " permission");
    }
  }

  /**
   * Check if the requested values can be satisfied with the values in the
   * BlockToken. This is intended for use with StorageTypes and StorageIDs.
   *
   * The current node can only verify that one of the storage [Type|ID] is
   * available. The rest will be on different nodes.
   */
  public static <T> void checkAccess(T[] candidates, T[] requested, String msg)
      throws InvalidToken {
    if (ArrayUtils.isEmpty(requested)) {
      throw new InvalidToken("The request has no " + msg + ". "
          + "This is probably a configuration error.");
    }
    if (ArrayUtils.isEmpty(candidates)) {
      return;
    }

    Multiset<T> c = HashMultiset.create(Arrays.asList(candidates));

    for (T req : requested) {
      if (!c.remove(req)) {
        throw new InvalidToken("Block token with " + msg + " "
            + Arrays.toString(candidates)
            + " not valid for access with " + msg + " "
            + Arrays.toString(requested));
      }
    }
  }

  /** Check if access should be allowed. userID is not checked if null */
  public void checkAccess(Token<BlockTokenIdentifier> token, String userId,
      ExtendedBlock block, BlockTokenIdentifier.AccessMode mode,
      StorageType[] storageTypes, String[] storageIds) throws InvalidToken {
    BlockTokenIdentifier id = new BlockTokenIdentifier();
    try {
      id.readFields(new DataInputStream(new ByteArrayInputStream(token
          .getIdentifier())));
    } catch (IOException e) {
      throw new InvalidToken(
          "Unable to de-serialize block token identifier for user=" + userId
              + ", block=" + block + ", access mode=" + mode);
    }
    checkAccess(id, userId, block, mode, storageTypes, storageIds);
    if (!Arrays.equals(retrievePassword(id), token.getPassword())) {
      throw new InvalidToken("Block token with " + id
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
    if (key == null) {
      throw new IllegalStateException("currentKey hasn't been initialized.");
    }
    identifier.setExpiryDate(timer.now() + tokenLifetime);
    identifier.setKeyId(key.getKeyId());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Generating block token for " + identifier);
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
      throw new InvalidToken("Block token with " + identifier
          + " is expired.");
    }
    BlockKey key = null;
    synchronized (this) {
      key = allKeys.get(identifier.getKeyId());
    }
    if (key == null) {
      throw new InvalidToken("Can't re-compute password for "
          + identifier + ", since the required block key (keyID="
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
        encryptionKey, timer.now() + tokenLifetime,
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

  public BlockKey getCurrentKey() {
    return currentKey;
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
  public synchronized boolean hasKey(int keyId) {
    BlockKey key = allKeys.get(keyId);
    return key != null;
  }

  @VisibleForTesting
  public synchronized int getSerialNoForTesting() {
    return serialNo;
  }

}
