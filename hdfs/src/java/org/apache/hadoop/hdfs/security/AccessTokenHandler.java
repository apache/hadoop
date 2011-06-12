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

package org.apache.hadoop.hdfs.security;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * AccessTokenHandler can be instantiated in 2 modes, master mode and slave
 * mode. Master can generate new access keys and export access keys to slaves,
 * while slaves can only import and use access keys received from master. Both
 * master and slave can generate and verify access tokens. Typically, master
 * mode is used by NN and slave mode is used by DN.
 */

@InterfaceAudience.Private
public class AccessTokenHandler {
  private static final Log LOG = LogFactory.getLog(AccessTokenHandler.class);

  private final boolean isMaster;
  /*
   * keyUpdateInterval is the interval that NN updates its access keys. It
   * should be set long enough so that all live DN's and Balancer should have
   * sync'ed their access keys with NN at least once during each interval.
   */
  private final long keyUpdateInterval;
  private long tokenLifetime;
  private long serialNo = new SecureRandom().nextLong();
  private KeyGenerator keyGen;
  private BlockAccessKey currentKey;
  private BlockAccessKey nextKey;
  private Map<Long, BlockAccessKey> allKeys;

  public static enum AccessMode {
    READ, WRITE, COPY, REPLACE
  };

  /**
   * Constructor
   * 
   * @param isMaster
   * @param keyUpdateInterval
   * @param tokenLifetime
   * @throws IOException
   */
  public AccessTokenHandler(boolean isMaster, long keyUpdateInterval,
      long tokenLifetime) throws IOException {
    this.isMaster = isMaster;
    this.keyUpdateInterval = keyUpdateInterval;
    this.tokenLifetime = tokenLifetime;
    this.allKeys = new HashMap<Long, BlockAccessKey>();
    if (isMaster) {
      try {
        generateKeys();
        initMac(currentKey);
      } catch (GeneralSecurityException e) {
        throw (IOException) new IOException(
            "Failed to create AccessTokenHandler").initCause(e);
      }
    }
  }

  /** Initialize access keys */
  private synchronized void generateKeys() throws NoSuchAlgorithmException {
    keyGen = KeyGenerator.getInstance("HmacSHA1");
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
    serialNo++;
    currentKey = new BlockAccessKey(serialNo, new Text(keyGen.generateKey()
        .getEncoded()), System.currentTimeMillis() + 2 * keyUpdateInterval
        + tokenLifetime);
    serialNo++;
    nextKey = new BlockAccessKey(serialNo, new Text(keyGen.generateKey()
        .getEncoded()), System.currentTimeMillis() + 3 * keyUpdateInterval
        + tokenLifetime);
    allKeys.put(currentKey.getKeyID(), currentKey);
    allKeys.put(nextKey.getKeyID(), nextKey);
  }

  /** Initialize Mac function */
  private synchronized void initMac(BlockAccessKey key) throws IOException {
    try {
      Mac mac = Mac.getInstance("HmacSHA1");
      mac.init(new SecretKeySpec(key.getKey().getBytes(), "HmacSHA1"));
      key.setMac(mac);
    } catch (GeneralSecurityException e) {
      throw (IOException) new IOException(
          "Failed to initialize Mac for access key, keyID=" + key.getKeyID())
          .initCause(e);
    }
  }

  /** Export access keys, only to be used in master mode */
  public synchronized ExportedAccessKeys exportKeys() {
    if (!isMaster)
      return null;
    if (LOG.isDebugEnabled())
      LOG.debug("Exporting access keys");
    return new ExportedAccessKeys(true, keyUpdateInterval, tokenLifetime,
        currentKey, allKeys.values().toArray(new BlockAccessKey[0]));
  }

  private synchronized void removeExpiredKeys() {
    long now = System.currentTimeMillis();
    for (Iterator<Map.Entry<Long, BlockAccessKey>> it = allKeys.entrySet()
        .iterator(); it.hasNext();) {
      Map.Entry<Long, BlockAccessKey> e = it.next();
      if (e.getValue().getExpiryDate() < now) {
        it.remove();
      }
    }
  }

  /**
   * Set access keys, only to be used in slave mode
   */
  public synchronized void setKeys(ExportedAccessKeys exportedKeys)
      throws IOException {
    if (isMaster || exportedKeys == null)
      return;
    LOG.info("Setting access keys");
    removeExpiredKeys();
    this.currentKey = exportedKeys.getCurrentKey();
    initMac(currentKey);
    BlockAccessKey[] receivedKeys = exportedKeys.getAllKeys();
    for (int i = 0; i < receivedKeys.length; i++) {
      if (receivedKeys[i] == null)
        continue;
      this.allKeys.put(receivedKeys[i].getKeyID(), receivedKeys[i]);
    }
  }

  /**
   * Update access keys, only to be used in master mode
   */
  public synchronized void updateKeys() throws IOException {
    if (!isMaster)
      return;
    LOG.info("Updating access keys");
    removeExpiredKeys();
    // set final expiry date of retiring currentKey
    allKeys.put(currentKey.getKeyID(), new BlockAccessKey(currentKey.getKeyID(),
        currentKey.getKey(), System.currentTimeMillis() + keyUpdateInterval
            + tokenLifetime));
    // update the estimated expiry date of new currentKey
    currentKey = new BlockAccessKey(nextKey.getKeyID(), nextKey.getKey(), System
        .currentTimeMillis()
        + 2 * keyUpdateInterval + tokenLifetime);
    initMac(currentKey);
    allKeys.put(currentKey.getKeyID(), currentKey);
    // generate a new nextKey
    serialNo++;
    nextKey = new BlockAccessKey(serialNo, new Text(keyGen.generateKey()
        .getEncoded()), System.currentTimeMillis() + 3 * keyUpdateInterval
        + tokenLifetime);
    allKeys.put(nextKey.getKeyID(), nextKey);
  }

  /** Check if token is well formed */
  private synchronized boolean verifyToken(long keyID, BlockAccessToken token)
      throws IOException {
    BlockAccessKey key = allKeys.get(keyID);
    if (key == null) {
      LOG.warn("Access key for keyID=" + keyID + " doesn't exist.");
      return false;
    }
    if (key.getMac() == null) {
      initMac(key);
    }
    Text tokenID = token.getTokenID();
    Text authenticator = new Text(key.getMac().doFinal(tokenID.getBytes()));
    return authenticator.equals(token.getTokenAuthenticator());
  }

  /** Generate an access token for current user */
  public BlockAccessToken generateToken(long blockID, EnumSet<AccessMode> modes)
      throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String userID = (ugi == null ? null : ugi.getShortUserName());
    return generateToken(userID, blockID, modes);
  }

  /** Generate an access token for a specified user */
  public synchronized BlockAccessToken generateToken(String userID, long blockID,
      EnumSet<AccessMode> modes) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Generating access token for user=" + userID + ", blockID="
          + blockID + ", access modes=" + modes + ", keyID="
          + currentKey.getKeyID());
    }
    if (modes == null || modes.isEmpty())
      throw new IOException("access modes can't be null or empty");
    ByteArrayOutputStream buf = new ByteArrayOutputStream(4096);
    DataOutputStream out = new DataOutputStream(buf);
    WritableUtils.writeVLong(out, System.currentTimeMillis() + tokenLifetime);
    WritableUtils.writeVLong(out, currentKey.getKeyID());
    WritableUtils.writeString(out, userID);
    WritableUtils.writeVLong(out, blockID);
    WritableUtils.writeVInt(out, modes.size());
    for (AccessMode aMode : modes) {
      WritableUtils.writeEnum(out, aMode);
    }
    Text tokenID = new Text(buf.toByteArray());
    return new BlockAccessToken(tokenID, new Text(currentKey.getMac().doFinal(
        tokenID.getBytes())));
  }

  /** Check if access should be allowed. userID is not checked if null */
  public boolean checkAccess(BlockAccessToken token, String userID, long blockID,
      AccessMode mode) throws IOException {
    long oExpiry = 0;
    long oKeyID = 0;
    String oUserID = null;
    long oBlockID = 0;
    EnumSet<AccessMode> oModes = EnumSet.noneOf(AccessMode.class);

    try {
      ByteArrayInputStream buf = new ByteArrayInputStream(token.getTokenID()
          .getBytes());
      DataInputStream in = new DataInputStream(buf);
      oExpiry = WritableUtils.readVLong(in);
      oKeyID = WritableUtils.readVLong(in);
      oUserID = WritableUtils.readString(in);
      oBlockID = WritableUtils.readVLong(in);
      int length = WritableUtils.readVInt(in);
      for (int i = 0; i < length; ++i) {
        oModes.add(WritableUtils.readEnum(in, AccessMode.class));
      }
    } catch (IOException e) {
      throw (IOException) new IOException(
          "Unable to parse access token for user=" + userID + ", blockID="
              + blockID + ", access mode=" + mode).initCause(e);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Verifying access token for user=" + userID + ", blockID="
          + blockID + ", access mode=" + mode + ", keyID=" + oKeyID);
    }
    return (userID == null || userID.equals(oUserID)) && oBlockID == blockID
        && !isExpired(oExpiry) && oModes.contains(mode)
        && verifyToken(oKeyID, token);
  }

  private static boolean isExpired(long expiryDate) {
    return System.currentTimeMillis() > expiryDate;
  }

  /** check if a token is expired. for unit test only.
   *  return true when token is expired, false otherwise */
  static boolean isTokenExpired(BlockAccessToken token) throws IOException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getTokenID()
        .getBytes());
    DataInputStream in = new DataInputStream(buf);
    long expiryDate = WritableUtils.readVLong(in);
    return isExpired(expiryDate);
  }

  /** set token lifetime. for unit test only */
  synchronized void setTokenLifetime(long tokenLifetime) {
    this.tokenLifetime = tokenLifetime;
  }
}
