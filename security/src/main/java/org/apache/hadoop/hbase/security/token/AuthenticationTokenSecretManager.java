/*
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

package org.apache.hadoop.hbase.security.token;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKLeaderManager;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.zookeeper.KeeperException;

/**
 * Manages an internal list of secret keys used to sign new authentication
 * tokens as they are generated, and to valid existing tokens used for
 * authentication.
 *
 * <p>
 * A single instance of {@code AuthenticationTokenSecretManager} will be
 * running as the "leader" in a given HBase cluster.  The leader is responsible
 * for periodically generating new secret keys, which are then distributed to
 * followers via ZooKeeper, and for expiring previously used secret keys that
 * are no longer needed (as any tokens using them have expired).
 * </p>
 */
public class AuthenticationTokenSecretManager
    extends SecretManager<AuthenticationTokenIdentifier> {

  static final String NAME_PREFIX = "SecretManager-";

  private static Log LOG = LogFactory.getLog(
      AuthenticationTokenSecretManager.class);

  private long lastKeyUpdate;
  private long keyUpdateInterval;
  private long tokenMaxLifetime;
  private ZKSecretWatcher zkWatcher;
  private LeaderElector leaderElector;
  private ClusterId clusterId;

  private Map<Integer,AuthenticationKey> allKeys =
      new ConcurrentHashMap<Integer, AuthenticationKey>();
  private AuthenticationKey currentKey;

  private int idSeq;
  private AtomicLong tokenSeq = new AtomicLong();
  private String name;

  /**
   * Create a new secret manager instance for generating keys.
   * @param conf Configuration to use
   * @param zk Connection to zookeeper for handling leader elections
   * @param keyUpdateInterval Time (in milliseconds) between rolling a new master key for token signing
   * @param tokenMaxLifetime Maximum age (in milliseconds) before a token expires and is no longer valid
   */
  /* TODO: Restrict access to this constructor to make rogues instances more difficult.
   * For the moment this class is instantiated from
   * org.apache.hadoop.hbase.ipc.SecureServer so public access is needed.
   */
  public AuthenticationTokenSecretManager(Configuration conf,
      ZooKeeperWatcher zk, String serverName,
      long keyUpdateInterval, long tokenMaxLifetime) {
    this.zkWatcher = new ZKSecretWatcher(conf, zk, this);
    this.keyUpdateInterval = keyUpdateInterval;
    this.tokenMaxLifetime = tokenMaxLifetime;
    this.leaderElector = new LeaderElector(zk, serverName);
    this.name = NAME_PREFIX+serverName;
    this.clusterId = new ClusterId(zk, zk);
  }

  public void start() {
    try {
      // populate any existing keys
      this.zkWatcher.start();
      // try to become leader
      this.leaderElector.start();
    } catch (KeeperException ke) {
      LOG.error("Zookeeper initialization failed", ke);
    }
  }

  public void stop() {
    this.leaderElector.stop("SecretManager stopping");
  }

  public boolean isMaster() {
    return leaderElector.isMaster();
  }

  public String getName() {
    return name;
  }

  @Override
  protected byte[] createPassword(AuthenticationTokenIdentifier identifier) {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    AuthenticationKey secretKey = currentKey;
    identifier.setKeyId(secretKey.getKeyId());
    identifier.setIssueDate(now);
    identifier.setExpirationDate(now + tokenMaxLifetime);
    identifier.setSequenceNumber(tokenSeq.getAndIncrement());
    return createPassword(WritableUtils.toByteArray(identifier),
        secretKey.getKey());
  }

  @Override
  public byte[] retrievePassword(AuthenticationTokenIdentifier identifier)
      throws InvalidToken {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    if (identifier.getExpirationDate() < now) {
      throw new InvalidToken("Token has expired");
    }
    AuthenticationKey masterKey = allKeys.get(identifier.getKeyId());
    if (masterKey == null) {
      throw new InvalidToken("Unknown master key for token (id="+
          identifier.getKeyId()+")");
    }
    // regenerate the password
    return createPassword(WritableUtils.toByteArray(identifier),
        masterKey.getKey());
  }

  @Override
  public AuthenticationTokenIdentifier createIdentifier() {
    return new AuthenticationTokenIdentifier();
  }

  public Token<AuthenticationTokenIdentifier> generateToken(String username) {
    AuthenticationTokenIdentifier ident =
        new AuthenticationTokenIdentifier(username);
    Token<AuthenticationTokenIdentifier> token =
        new Token<AuthenticationTokenIdentifier>(ident, this);
    if (clusterId.hasId()) {
      token.setService(new Text(clusterId.getId()));
    }
    return token;
  }

  public synchronized void addKey(AuthenticationKey key) throws IOException {
    // ignore zk changes when running as master
    if (leaderElector.isMaster()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Running as master, ignoring new key "+key.getKeyId());
      }
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding key "+key.getKeyId());
    }

    allKeys.put(key.getKeyId(), key);
    if (currentKey == null || key.getKeyId() > currentKey.getKeyId()) {
      currentKey = key;
    }
    // update current sequence
    if (key.getKeyId() > idSeq) {
      idSeq = key.getKeyId();
    }
  }

  synchronized void removeKey(Integer keyId) {
    // ignore zk changes when running as master
    if (leaderElector.isMaster()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Running as master, ignoring removed key "+keyId);
      }
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing key "+keyId);
    }

    allKeys.remove(keyId);
  }

  AuthenticationKey getCurrentKey() {
    return currentKey;
  }

  AuthenticationKey getKey(int keyId) {
    return allKeys.get(keyId);
  }

  synchronized void removeExpiredKeys() {
    if (!leaderElector.isMaster()) {
      LOG.info("Skipping removeExpiredKeys() because not running as master.");
      return;
    }

    long now = EnvironmentEdgeManager.currentTimeMillis();
    Iterator<AuthenticationKey> iter = allKeys.values().iterator();
    while (iter.hasNext()) {
      AuthenticationKey key = iter.next();
      if (key.getExpiration() < now) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Removing expired key "+key.getKeyId());
        }
        iter.remove();
        zkWatcher.removeKeyFromZK(key);
      }
    }
  }

  synchronized void rollCurrentKey() {
    if (!leaderElector.isMaster()) {
      LOG.info("Skipping rollCurrentKey() because not running as master.");
      return;
    }

    long now = EnvironmentEdgeManager.currentTimeMillis();
    AuthenticationKey prev = currentKey;
    AuthenticationKey newKey = new AuthenticationKey(++idSeq,
        Long.MAX_VALUE, // don't allow to expire until it's replaced by a new key
        generateSecret());
    allKeys.put(newKey.getKeyId(), newKey);
    currentKey = newKey;
    zkWatcher.addKeyToZK(newKey);
    lastKeyUpdate = now;

    if (prev != null) {
      // make sure previous key is still stored
      prev.setExpiration(now + tokenMaxLifetime);
      allKeys.put(prev.getKeyId(), prev);
      zkWatcher.updateKeyInZK(prev);
    }
  }

  public static SecretKey createSecretKey(byte[] raw) {
    return SecretManager.createSecretKey(raw);
  }

  private class LeaderElector extends Thread implements Stoppable {
    private boolean stopped = false;
    /** Flag indicating whether we're in charge of rolling/expiring keys */
    private boolean isMaster = false;
    private ZKLeaderManager zkLeader;

    public LeaderElector(ZooKeeperWatcher watcher, String serverName) {
      setDaemon(true);
      setName("ZKSecretWatcher-leaderElector");
      zkLeader = new ZKLeaderManager(watcher,
          ZKUtil.joinZNode(zkWatcher.getRootKeyZNode(), "keymaster"),
          Bytes.toBytes(serverName), this);
    }

    public boolean isMaster() {
      return isMaster;
    }

    @Override
    public boolean isStopped() {
      return stopped;
    }

    @Override
    public void stop(String reason) {
      stopped = true;
      // prevent further key generation when stopping
      if (isMaster) {
        zkLeader.stepDownAsLeader();
      }
      isMaster = false;
      LOG.info("Stopping leader election, because: "+reason);
      interrupt();
    }

    public void run() {
      zkLeader.start();
      zkLeader.waitToBecomeLeader();
      isMaster = true;

      while (!stopped) {
        long now = EnvironmentEdgeManager.currentTimeMillis();

        // clear any expired
        removeExpiredKeys();

        if (lastKeyUpdate + keyUpdateInterval < now) {
          // roll a new master key
          rollCurrentKey();
        }

        try {
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Interrupted waiting for next update", ie);
          }
        }
      }
    }
  }
}
