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
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheBridge;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.SecurityUtil.TruststoreKeystore;
import org.apache.hadoop.security.authentication.util.ZookeeperClient;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenManager;

import static org.apache.hadoop.security.SecurityUtil.getServerPrincipal;
import static org.apache.hadoop.util.Time.now;

import org.apache.hadoop.util.curator.ZKCuratorManager.HadoopZookeeperFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * An implementation of {@link AbstractDelegationTokenSecretManager} that
 * persists TokenIdentifiers and DelegationKeys in Zookeeper. This class can
 * be used by HA (Highly available) services that consists of multiple nodes.
 * This class ensures that Identifiers and Keys are replicated to all nodes of
 * the service.
 */
@InterfaceAudience.Private
public abstract class ZKDelegationTokenSecretManager<TokenIdent extends AbstractDelegationTokenIdentifier>
    extends AbstractDelegationTokenSecretManager<TokenIdent> {

  public static final String ZK_CONF_PREFIX = "zk-dt-secret-manager.";
  public static final String ZK_DTSM_ZK_NUM_RETRIES = ZK_CONF_PREFIX
      + "zkNumRetries";
  public static final String ZK_DTSM_ZK_SESSION_TIMEOUT = ZK_CONF_PREFIX
      + "zkSessionTimeout";
  public static final String ZK_DTSM_ZK_CONNECTION_TIMEOUT = ZK_CONF_PREFIX
      + "zkConnectionTimeout";
  public static final String ZK_DTSM_ZK_SHUTDOWN_TIMEOUT = ZK_CONF_PREFIX
      + "zkShutdownTimeout";
  public static final String ZK_DTSM_ZNODE_WORKING_PATH = ZK_CONF_PREFIX
      + "znodeWorkingPath";
  public static final String ZK_DTSM_ZK_AUTH_TYPE = ZK_CONF_PREFIX
      + "zkAuthType";
  public static final String ZK_DTSM_ZK_CONNECTION_STRING = ZK_CONF_PREFIX
      + "zkConnectionString";
  public static final String ZK_DTSM_ZK_KERBEROS_KEYTAB = ZK_CONF_PREFIX
      + "kerberos.keytab";
  public static final String ZK_DTSM_ZK_KERBEROS_PRINCIPAL = ZK_CONF_PREFIX
      + "kerberos.principal";
  public static final String ZK_DTSM_ZK_KERBEROS_SERVER_PRINCIPAL = ZK_CONF_PREFIX
      + "kerberos.server.principal";
  public static final String ZK_DTSM_TOKEN_SEQNUM_BATCH_SIZE = ZK_CONF_PREFIX
      + "token.seqnum.batch.size";
  public static final String ZK_DTSM_TOKEN_WATCHER_ENABLED = ZK_CONF_PREFIX
      + "token.watcher.enabled";
  public static final boolean ZK_DTSM_TOKEN_WATCHER_ENABLED_DEFAULT = true;

  public static final String ZK_DTSM_ZK_SSL_ENABLED = ZK_CONF_PREFIX + "ssl.enabled";
  public static final String ZK_DTSM_ZK_SSL_KEYSTORE_LOCATION =
      ZK_CONF_PREFIX + "ssl.keystore.location";
  public static final String ZK_DTSM_ZK_SSL_KEYSTORE_PASSWORD =
      ZK_CONF_PREFIX + "ssl.keystore.password";
  public static final String ZK_DTSM_ZK_SSL_TRUSTSTORE_LOCATION =
      ZK_CONF_PREFIX + "ssl.truststore.location";
  public static final String ZK_DTSM_ZK_SSL_TRUSTSTORE_PASSWORD =
      ZK_CONF_PREFIX + "ssl.truststore.password";

  public static final int ZK_DTSM_ZK_NUM_RETRIES_DEFAULT = 3;
  public static final int ZK_DTSM_ZK_SESSION_TIMEOUT_DEFAULT = 10000;
  public static final int ZK_DTSM_ZK_CONNECTION_TIMEOUT_DEFAULT = 10000;
  public static final int ZK_DTSM_ZK_SHUTDOWN_TIMEOUT_DEFAULT = 10000;
  public static final String ZK_DTSM_ZNODE_WORKING_PATH_DEAFULT = "zkdtsm";
  // By default, increase seq number by 100 each time to reduce overflow
  // speed of znode dataVersion which is 32-integer now.
  public static final int ZK_DTSM_TOKEN_SEQNUM_BATCH_SIZE_DEFAULT = 100;

  private static final Logger LOG = LoggerFactory
      .getLogger(ZKDelegationTokenSecretManager.class);

  private static final String JAAS_LOGIN_ENTRY_NAME =
      "ZKDelegationTokenSecretManagerClient";

  private static final String ZK_DTSM_NAMESPACE = "ZKDTSMRoot";
  private static final String ZK_DTSM_SEQNUM_ROOT = "/ZKDTSMSeqNumRoot";
  private static final String ZK_DTSM_KEYID_ROOT = "/ZKDTSMKeyIdRoot";
  protected static final String ZK_DTSM_TOKENS_ROOT = "/ZKDTSMTokensRoot";
  private static final String ZK_DTSM_MASTER_KEY_ROOT = "/ZKDTSMMasterKeyRoot";

  private static final String DELEGATION_KEY_PREFIX = "DK_";
  private static final String DELEGATION_TOKEN_PREFIX = "DT_";

  private static final ThreadLocal<CuratorFramework> CURATOR_TL =
      new ThreadLocal<CuratorFramework>();

  public static void setCurator(CuratorFramework curator) {
    CURATOR_TL.set(curator);
  }

  @VisibleForTesting
  protected static CuratorFramework getCurator() {
    return CURATOR_TL.get();
  }

  private final boolean isExternalClient;
  protected final CuratorFramework zkClient;
  private SharedCount delTokSeqCounter;
  private SharedCount keyIdSeqCounter;
  private CuratorCacheBridge keyCache;
  private CuratorCacheBridge tokenCache;
  private final int seqNumBatchSize;
  // Bounded local coordination; different stripes can mutate concurrently.
  private final Lock[] tokenMutationLocks = createTokenMutationLocks();

  private final Object sequenceNumberLock = new Object();
  private int currentSeqNum;
  private int currentMaxSeqNum;

  private final boolean isTokenWatcherEnabled;

  public ZKDelegationTokenSecretManager(Configuration conf) {
    super(conf.getLong(DelegationTokenManager.UPDATE_INTERVAL,
        DelegationTokenManager.UPDATE_INTERVAL_DEFAULT) * 1000,
        conf.getLong(DelegationTokenManager.MAX_LIFETIME,
            DelegationTokenManager.MAX_LIFETIME_DEFAULT) * 1000,
        conf.getLong(DelegationTokenManager.RENEW_INTERVAL,
            DelegationTokenManager.RENEW_INTERVAL_DEFAULT) * 1000,
        conf.getLong(DelegationTokenManager.REMOVAL_SCAN_INTERVAL,
            DelegationTokenManager.REMOVAL_SCAN_INTERVAL_DEFAULT) * 1000);
    seqNumBatchSize = conf.getInt(ZK_DTSM_TOKEN_SEQNUM_BATCH_SIZE,
        ZK_DTSM_TOKEN_SEQNUM_BATCH_SIZE_DEFAULT);
    isTokenWatcherEnabled = conf.getBoolean(ZK_DTSM_TOKEN_WATCHER_ENABLED,
        ZK_DTSM_TOKEN_WATCHER_ENABLED_DEFAULT);
    
    String workPath = conf.get(ZK_DTSM_ZNODE_WORKING_PATH, ZK_DTSM_ZNODE_WORKING_PATH_DEAFULT);
    String nameSpace = workPath + "/" + ZK_DTSM_NAMESPACE;
    if (CURATOR_TL.get() != null) {
      zkClient = CURATOR_TL.get().usingNamespace(nameSpace);
      isExternalClient = true;
    } else {
      zkClient = createCuratorClient(conf, nameSpace);
      isExternalClient = false;
    }
  }

  @VisibleForTesting
  static CuratorFramework createCuratorClient(Configuration conf, String namespace) {
    try {
      String connString = conf.get(ZK_DTSM_ZK_CONNECTION_STRING);
      String authType = conf.get(ZK_DTSM_ZK_AUTH_TYPE);
      String keytab = conf.get(ZK_DTSM_ZK_KERBEROS_KEYTAB, "").trim();
      String principal = getServerPrincipal(conf.get(ZK_DTSM_ZK_KERBEROS_PRINCIPAL, "").trim(), "");
      int sessionTimeout =
          conf.getInt(ZK_DTSM_ZK_SESSION_TIMEOUT, ZK_DTSM_ZK_SESSION_TIMEOUT_DEFAULT);
      int connectionTimeout =
          conf.getInt(ZK_DTSM_ZK_CONNECTION_TIMEOUT, ZK_DTSM_ZK_CONNECTION_TIMEOUT_DEFAULT);
      int retryCount =
          conf.getInt(ZK_DTSM_ZK_NUM_RETRIES, ZK_DTSM_ZK_NUM_RETRIES_DEFAULT);
      RetryPolicy retryPolicy =
          new RetryNTimes(retryCount, retryCount == 0 ? 0 : sessionTimeout / retryCount);

      boolean isSSLEnabled =
          conf.getBoolean(CommonConfigurationKeys.ZK_CLIENT_SSL_ENABLED,
              conf.getBoolean(ZK_DTSM_ZK_SSL_ENABLED, false));
      String keystoreLocation = conf.get(ZK_DTSM_ZK_SSL_KEYSTORE_LOCATION,
          conf.get(CommonConfigurationKeys.ZK_SSL_KEYSTORE_LOCATION, ""));
      String keystorePassword = conf.get(ZK_DTSM_ZK_SSL_KEYSTORE_PASSWORD,
          conf.get(CommonConfigurationKeys.ZK_SSL_KEYSTORE_PASSWORD, ""));
      String truststoreLocation = conf.get(ZK_DTSM_ZK_SSL_TRUSTSTORE_LOCATION,
          conf.get(CommonConfigurationKeys.ZK_SSL_TRUSTSTORE_LOCATION, ""));
      String truststorePassword = conf.get(ZK_DTSM_ZK_SSL_TRUSTSTORE_PASSWORD,
          conf.get(CommonConfigurationKeys.ZK_SSL_TRUSTSTORE_PASSWORD, ""));

      ZookeeperFactory zkFactory = new HadoopZookeeperFactory(
          conf.get(ZK_DTSM_ZK_KERBEROS_SERVER_PRINCIPAL),
          conf.get(ZK_DTSM_ZK_KERBEROS_PRINCIPAL),
          conf.get(ZK_DTSM_ZK_KERBEROS_KEYTAB),
          isSSLEnabled,
          new TruststoreKeystore(conf));


      return ZookeeperClient.configure()
          .withConnectionString(connString)
          .withNamespace(namespace)
          .withZookeeperFactory(zkFactory)
          .withAuthType(authType)
          .withKeytab(keytab)
          .withPrincipal(principal)
          .withJaasLoginEntryName(JAAS_LOGIN_ENTRY_NAME)
          .withRetryPolicy(retryPolicy)
          .withSessionTimeout(sessionTimeout)
          .withConnectionTimeout(connectionTimeout)
          .enableSSL(isSSLEnabled)
          .withKeystore(keystoreLocation)
          .withKeystorePassword(keystorePassword)
          .withTruststore(truststoreLocation)
          .withTruststorePassword(truststorePassword)
          .create();
    } catch (Exception ex) {
      throw new RuntimeException("Could not Load ZK acls or auth: " + ex, ex);
    }
  }

  @Override
  public void startThreads() throws IOException {
    if (!isExternalClient) {
      try {
        zkClient.start();
      } catch (Exception e) {
        throw new IOException("Could not start Curator Framework", e);
      }
    } else {
      // If namespace parents are implicitly created, they won't have ACLs.
      // So, let's explicitly create them.
      CuratorFramework nullNsFw = zkClient.usingNamespace(null);
      try {
        String nameSpace = "/" + zkClient.getNamespace();
        nullNsFw.create().creatingParentContainersIfNeeded().forPath(nameSpace);
      } catch (KeeperException.NodeExistsException ignore) {
        // We don't care if the znode already exists
      } catch (Exception e) {
        throw new IOException("Could not create namespace", e);
      }
    }
    try {
      delTokSeqCounter = new SharedCount(zkClient, ZK_DTSM_SEQNUM_ROOT, 0);
      if (delTokSeqCounter != null) {
        delTokSeqCounter.start();
      }
      // the first batch range should be allocated during this starting window
      // by calling the incrSharedCount
      synchronized (sequenceNumberLock) {
        currentSeqNum = incrSharedCount(delTokSeqCounter, seqNumBatchSize);
        currentMaxSeqNum = currentSeqNum + seqNumBatchSize;
        LOG.info("Fetched initial range of seq num, from {} to {} ",
            currentSeqNum + 1, currentMaxSeqNum);
      }
    } catch (Exception e) {
      throw new IOException("Could not start Sequence Counter", e);
    }
    try {
      keyIdSeqCounter = new SharedCount(zkClient, ZK_DTSM_KEYID_ROOT, 0);
      if (keyIdSeqCounter != null) {
        keyIdSeqCounter.start();
      }
    } catch (Exception e) {
      throw new IOException("Could not start KeyId Counter", e);
    }
    try {
      createPersistentNode(ZK_DTSM_MASTER_KEY_ROOT);
      createPersistentNode(ZK_DTSM_TOKENS_ROOT);
    } catch (Exception e) {
      throw new RuntimeException("Could not create ZK paths");
    }
    try {
      keyCache = CuratorCache.bridgeBuilder(zkClient, ZK_DTSM_MASTER_KEY_ROOT)
          .build();
      CuratorCacheListener keyCacheListener = CuratorCacheListener.builder()
          .forCreatesAndChanges((oldNode, node) -> {
            try {
              processKeyAddOrUpdate(node.getData());
            } catch (IOException e) {
              LOG.error("Error while processing Curator keyCacheListener "
                  + "NODE_CREATED / NODE_CHANGED event");
              throw new UncheckedIOException(e);
            }
          })
          .forDeletes(childData -> processKeyRemoved(childData.getPath()))
          .build();
      keyCache.listenable().addListener(keyCacheListener);
      keyCache.start();
      loadFromZKCache(false);
    } catch (Exception e) {
      throw new IOException("Could not start Curator keyCacheListener for keys",
          e);
    }
    if (isTokenWatcherEnabled) {
      LOG.info("TokenCache is enabled");
      try {
        tokenCache = CuratorCache.bridgeBuilder(zkClient, ZK_DTSM_TOKENS_ROOT)
            .build();
        CuratorCacheListener tokenCacheListener = CuratorCacheListener.builder()
            .forCreatesAndChanges((oldNode, node) -> {
              try {
                processTokenAddOrUpdate(node.getData());
              } catch (IOException e) {
                LOG.error("Error while processing Curator tokenCacheListener "
                    + "NODE_CREATED / NODE_CHANGED event");
                throw new UncheckedIOException(e);
              }
            })
            .forDeletes(childData -> {
              try {
                processTokenRemoved(childData);
              } catch (IOException e) {
                LOG.error("Error while processing Curator tokenCacheListener "
                    + "NODE_DELETED event");
                throw new UncheckedIOException(e);
              }
            })
            .build();
        tokenCache.listenable().addListener(tokenCacheListener);
        tokenCache.start();
        loadFromZKCache(true);
      } catch (Exception e) {
        throw new IOException(
            "Could not start Curator tokenCacheListener for tokens", e);
      }
    }
    super.startThreads();
  }

  /**
   * Load the CuratorCache into the in-memory map. Possible caches to be
   * loaded are keyCache and tokenCache.
   *
   * @param isTokenCache true if loading tokenCache, false if loading keyCache.
   */
  private void loadFromZKCache(final boolean isTokenCache) {
    final String cacheName = isTokenCache ? "token" : "key";
    LOG.info("Starting to load {} cache.", cacheName);
    final Stream<ChildData> children;
    if (isTokenCache) {
      children = tokenCache.stream();
    } else {
      children = keyCache.stream();
    }

    final AtomicInteger count = new AtomicInteger(0);
    children.forEach(childData -> {
      try {
        if (isTokenCache) {
          processTokenAddOrUpdate(childData.getData());
        } else {
          processKeyAddOrUpdate(childData.getData());
        }
      } catch (Exception e) {
        LOG.info("Ignoring node {} because it failed to load.",
            childData.getPath());
        LOG.debug("Failure exception:", e);
        count.getAndIncrement();
      }
    });
    if (isTokenCache) {
      syncTokenOwnerStats();
    }
    if (count.get() > 0) {
      LOG.warn("Ignored {} nodes while loading {} cache.", count.get(),
          cacheName);
    }
    LOG.info("Loaded {} cache.", cacheName);
  }

  private void processKeyAddOrUpdate(byte[] data) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream(data);
    DataInputStream din = new DataInputStream(bin);
    DelegationKey key = new DelegationKey();
    key.readFields(din);
    allKeys.put(key.getKeyId(), key);
  }

  private void processKeyRemoved(String path) {
    int i = path.lastIndexOf('/');
    if (i > 0) {
      String tokSeg = path.substring(i + 1);
      int j = tokSeg.indexOf('_');
      if (j > 0) {
        int keyId = Integer.parseInt(tokSeg.substring(j + 1));
        allKeys.remove(keyId);
      }
    }
  }

  protected TokenIdent processTokenAddOrUpdate(byte[] data) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream(data);
    DataInputStream din = new DataInputStream(bin);
    TokenIdent ident = createIdentifier();
    ident.readFields(din);
    long renewDate = din.readLong();
    int pwdLen = din.readInt();
    byte[] password = new byte[pwdLen];
    int numRead = din.read(password, 0, pwdLen);
    if (numRead > -1) {
      DelegationTokenInformation tokenInfo =
          new DelegationTokenInformation(renewDate, password);
      currentTokens.put(ident, tokenInfo);
      return ident;
    }
    return null;
  }

  private void processTokenRemoved(ChildData data) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream(data.getData());
    DataInputStream din = new DataInputStream(bin);
    TokenIdent ident = createIdentifier();
    ident.readFields(din);
    currentTokens.remove(ident);
  }

  @Override
  public void stopThreads() {
    super.stopThreads();
    try {
      if (tokenCache != null) {
        tokenCache.close();
      }
    } catch (Exception e) {
      LOG.error("Could not stop Delegation Token Cache", e);
    }
    try {
      if (delTokSeqCounter != null) {
        delTokSeqCounter.close();
      }
    } catch (Exception e) {
      LOG.error("Could not stop Delegation Token Counter", e);
    }
    try {
      if (keyIdSeqCounter != null) {
        keyIdSeqCounter.close();
      }
    } catch (Exception e) {
      LOG.error("Could not stop Key Id Counter", e);
    }
    try {
      if (keyCache != null) {
        keyCache.close();
      }
    } catch (Exception e) {
      LOG.error("Could not stop KeyCache", e);
    }
    try {
      if (!isExternalClient && (zkClient != null)) {
        zkClient.close();
      }
    } catch (Exception e) {
      LOG.error("Could not stop Curator Framework", e);
    }
  }

  private void createPersistentNode(String nodePath) throws Exception {
    try {
      zkClient.create().withMode(CreateMode.PERSISTENT).forPath(nodePath);
    } catch (KeeperException.NodeExistsException ne) {
      LOG.debug(nodePath + " znode already exists !!");
    } catch (Exception e) {
      throw new IOException(nodePath + " znode could not be created !!", e);
    }
  }

  private static Lock[] createTokenMutationLocks() {
    Lock[] locks = new Lock[256];
    for (int i = 0; i < locks.length; i++) {
      locks[i] = new ReentrantLock();
    }
    return locks;
  }

  @Override
  protected Lock getTokenMutationLock(TokenIdent identifier) {
    return tokenMutationLocks[identifier.getSequenceNumber() & 255];
  }

  @Override
  protected boolean persistMasterKeysOutsideApiLock() {
    return true;
  }

  @Override
  protected boolean useTokenMutationLocksForCleanup() {
    return true;
  }

  @Override
  protected void removeExpiredStoredToken(TokenIdent identifier) throws IOException {
    removeStoredToken(identifier, true);
  }

  @Override
  protected int getDelegationTokenSeqNum() {
    return delTokSeqCounter.getCount();
  }

  private int incrSharedCount(SharedCount sharedCount, int batchSize)
      throws Exception {
    while (true) {
      // Loop until we successfully increment the counter
      VersionedValue<Integer> versionedValue = sharedCount.getVersionedValue();
      if (sharedCount.trySetCount(
          versionedValue, versionedValue.getValue() + batchSize)) {
        return versionedValue.getValue();
      }
    }
  }

  @Override
  protected int incrementDelegationTokenSeqNum() {
    synchronized (sequenceNumberLock) {
      // A range belongs exclusively to this manager. Never consume a number
      // beyond the range unless its replacement was successfully reserved.
      if (currentSeqNum >= currentMaxSeqNum) {
        try {
          int start = incrSharedCount(delTokSeqCounter, seqNumBatchSize);
          currentSeqNum = start;
          currentMaxSeqNum = start + seqNumBatchSize;
          LOG.info("Fetched new range of seq num, from {} to {}",
              currentSeqNum + 1, currentMaxSeqNum);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Interrupted reserving token sequence range", e);
        } catch (Exception e) {
          throw new IllegalStateException("Could not increment shared counter", e);
        }
      }
      return ++currentSeqNum;
    }
  }

  @Override
  protected byte[] createPassword(TokenIdent identifier) {
    // Neither range reservation nor token persistence may hold apiLock.
    DelegationTokenInformation info = prepareToken(identifier,
        incrementDelegationTokenSeqNum());
    try {
      getMetrics().trackStoreToken(() -> storeToken(identifier, info));
    } catch (IOException e) {
      throw new IllegalStateException("Could not store delegation token", e);
    }
    return info.getPassword();
  }

  @Override
  protected void setDelegationTokenSeqNum(int seqNum) {
    try {
      delTokSeqCounter.setCount(seqNum);
    } catch (Exception e) {
      throw new RuntimeException("Could not set shared counter !!", e);
    }
  }

  @Override
  protected int getCurrentKeyId() {
    return keyIdSeqCounter.getCount();
  }

  @Override
  protected int incrementCurrentKeyId() {
    try {
      return incrSharedCount(keyIdSeqCounter, 1) + 1;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted reserving master key ID", e);
    } catch (Exception e) {
      throw new IllegalStateException("Could not increment shared keyId counter", e);
    }
  }

  @Override
  protected DelegationKey getDelegationKey(int keyId) {
    // First check if its I already have this key
    DelegationKey key = allKeys.get(keyId);
    // Then query ZK
    if (key == null) {
      try {
        key = getKeyFromZK(keyId);
        if (key != null) {
          allKeys.put(keyId, key);
        }
      } catch (IOException e) {
        LOG.error("Error retrieving key [" + keyId + "] from ZK", e);
      }
    }
    return key;
  }

  private DelegationKey getKeyFromZK(int keyId) throws IOException {
    String nodePath =
        getNodePath(ZK_DTSM_MASTER_KEY_ROOT, DELEGATION_KEY_PREFIX + keyId);
    try {
      byte[] data = zkClient.getData().forPath(nodePath);
      if ((data == null) || (data.length == 0)) {
        return null;
      }
      ByteArrayInputStream bin = new ByteArrayInputStream(data);
      DataInputStream din = new DataInputStream(bin);
      DelegationKey key = new DelegationKey();
      key.readFields(din);
      return key;
    } catch (KeeperException.NoNodeException e) {
      LOG.error("No node in path [" + nodePath + "]");
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    return null;
  }

  @Override
  protected DelegationTokenInformation getTokenInfo(TokenIdent ident) {
    DelegationTokenInformation tokenInfo = currentTokens.get(ident);
    if (tokenInfo != null) {
      return tokenInfo;
    }
    Lock mutationLock = getTokenMutationLock(ident);
    mutationLock.lock();
    try {
      tokenInfo = currentTokens.get(ident);
      if (tokenInfo == null) {
        tokenInfo = getTokenInfoFromZK(ident);
        if (tokenInfo != null) {
          currentTokens.put(ident, tokenInfo);
        }
      }
      return tokenInfo;
    } catch (IOException e) {
      LOG.error("Error retrieving tokenInfo [{}] from ZK", ident.getSequenceNumber(), e);
      return null;
    } finally {
      mutationLock.unlock();
    }
  }

  /**
   * This method synchronizes the state of a delegation token information in
   * local cache with its actual value in Zookeeper.
   *
   * @param ident Identifier of the token
   */
  protected void syncLocalCacheWithZk(TokenIdent ident) {
    try {
      DelegationTokenInformation tokenInfo = getTokenInfoFromZK(ident);
      if (tokenInfo != null && !currentTokens.containsKey(ident)) {
        currentTokens.put(ident, tokenInfo);
      } else if (tokenInfo == null && currentTokens.containsKey(ident)) {
        currentTokens.remove(ident);
      }
    } catch (IOException e) {
      LOG.error("Error retrieving tokenInfo [" + ident.getSequenceNumber()
          + "] from ZK", e);
    }
  }

  protected DelegationTokenInformation getTokenInfoFromZK(TokenIdent ident)
      throws IOException {
    return getTokenInfoFromZK(ident, false);
  }

  protected DelegationTokenInformation getTokenInfoFromZK(TokenIdent ident,
      boolean quiet) throws IOException {
    String nodePath =
        getNodePath(ZK_DTSM_TOKENS_ROOT,
            DELEGATION_TOKEN_PREFIX + ident.getSequenceNumber());
    return getTokenInfoFromZK(nodePath, quiet);
  }

  protected DelegationTokenInformation getTokenInfoFromZK(String nodePath,
      boolean quiet) throws IOException {
    return getTokenInfoFromZK(nodePath, quiet, new Stat());
  }

  private DelegationTokenInformation getTokenInfoFromZK(String nodePath,
      boolean quiet, Stat stat) throws IOException {
    try {
      byte[] data = zkClient.getData().storingStatIn(stat).forPath(nodePath);
      if ((data == null) || (data.length == 0)) {
        return null;
      }
      ByteArrayInputStream bin = new ByteArrayInputStream(data);
      DataInputStream din = new DataInputStream(bin);
      createIdentifier().readFields(din);
      long renewDate = din.readLong();
      int pwdLen = din.readInt();
      byte[] password = new byte[pwdLen];
      int numRead = din.read(password, 0, pwdLen);
      if (numRead > -1) {
        DelegationTokenInformation tokenInfo =
            new DelegationTokenInformation(renewDate, password);
        return tokenInfo;
      }
    } catch (KeeperException.NoNodeException e) {
      if (!quiet) {
        LOG.error("No node in path [" + nodePath + "]");
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    return null;
  }

  @Override
  protected void storeDelegationKey(DelegationKey key) throws IOException {
    addOrUpdateDelegationKey(key, false);
  }

  @Override
  protected void updateDelegationKey(DelegationKey key) throws IOException {
    addOrUpdateDelegationKey(key, true);
  }

  private void addOrUpdateDelegationKey(DelegationKey key, boolean isUpdate)
      throws IOException {
    String nodeCreatePath =
        getNodePath(ZK_DTSM_MASTER_KEY_ROOT,
            DELEGATION_KEY_PREFIX + key.getKeyId());
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream fsOut = new DataOutputStream(os);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing ZKDTSMDelegationKey_" + key.getKeyId());
    }
    key.write(fsOut);
    try {
      if (zkClient.checkExists().forPath(nodeCreatePath) != null) {
        zkClient.setData().forPath(nodeCreatePath, os.toByteArray())
            .setVersion(-1);
        if (!isUpdate) {
          LOG.debug("Key with path [" + nodeCreatePath
              + "] already exists.. Updating !!");
        }
      } else {
        zkClient.create().withMode(CreateMode.PERSISTENT)
            .forPath(nodeCreatePath, os.toByteArray());
        if (isUpdate) {
          LOG.debug("Updating non existent Key path [" + nodeCreatePath
              + "].. Adding new !!");
        }
      }
    } catch (KeeperException.NodeExistsException ne) {
      LOG.debug(nodeCreatePath + " znode already exists !!");
    } catch (Exception ex) {
      throw new IOException(ex);
    } finally {
      os.close();
    }
  }

  @Override
  protected void removeStoredMasterKey(DelegationKey key) {
    String nodeRemovePath =
        getNodePath(ZK_DTSM_MASTER_KEY_ROOT,
            DELEGATION_KEY_PREFIX + key.getKeyId());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing ZKDTSMDelegationKey_" + key.getKeyId());
    }
    try {
      while (true) {
        Stat stat = new Stat();
        byte[] data = zkClient.getData().storingStatIn(stat).forPath(nodeRemovePath);
        DelegationKey stored = new DelegationKey();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
          stored.readFields(in);
        }
        if (stored.getExpiryDate() >= now()) {
          allKeys.putIfAbsent(stored.getKeyId(), stored);
          return;
        }
        try {
          zkClient.delete().withVersion(stat.getVersion()).forPath(nodeRemovePath);
          return;
        } catch (KeeperException.BadVersionException e) {
          // A peer extended this key's lifetime; check the new expiry.
        }
      }
    } catch (NoNodeException e) {
      LOG.debug("Master key already removed: {}", nodeRemovePath);
    } catch (Exception e) {
      // Keep the candidate so a later rotation retries a failed deletion.
      allKeys.putIfAbsent(key.getKeyId(), key);
      LOG.warn("Could not remove master key {}", nodeRemovePath, e);
    }
  }

  @Override
  protected void storeToken(TokenIdent ident,
      DelegationTokenInformation tokenInfo) throws IOException {
    try {
      addOrUpdateToken(ident, tokenInfo, false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void updateToken(TokenIdent ident,
      DelegationTokenInformation tokenInfo) throws IOException {
    try {
      addOrUpdateToken(ident, tokenInfo, true);
    } catch (NoNodeException e) {
      currentTokens.remove(ident);
      throw new InvalidToken("Cannot renew a cancelled token " + ident);
    } catch (Exception e) {
      throw new IOException("Could not update stored token " + ident, e);
    }
  }

  @Override
  protected void removeStoredToken(TokenIdent ident)
      throws IOException {
    removeStoredToken(ident, false);
  }

  protected void removeStoredToken(TokenIdent ident,
      boolean checkAgainstZkBeforeDeletion) throws IOException {
    String nodeRemovePath =
        getNodePath(ZK_DTSM_TOKENS_ROOT, DELEGATION_TOKEN_PREFIX
            + ident.getSequenceNumber());
    try {
      while (true) {
        Stat stat = new Stat();
        DelegationTokenInformation info = getTokenInfoFromZK(nodeRemovePath, true, stat);
        if (info == null) {
          return;
        }
        if (checkAgainstZkBeforeDeletion && info.getRenewDate() > now()) {
          return;
        }
        try {
          zkClient.delete().withVersion(stat.getVersion()).forPath(nodeRemovePath);
          return;
        } catch (KeeperException.BadVersionException e) {
          // Recheck expiry after a concurrent peer renewal.
        } catch (NoNodeException e) {
          return;
        }
      }
    } catch (Exception e) {
      throw new IOException("Could not remove stored token " + ident, e);
    }
  }

  @Override
  public TokenIdent cancelToken(Token<TokenIdent> token,
      String canceller) throws IOException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    TokenIdent id = createIdentifier();
    id.readFields(in);

    Lock mutationLock = getTokenMutationLock(id);
    mutationLock.lock();
    try {
      syncLocalCacheWithZk(id);
      return super.cancelToken(token, canceller);
    } finally {
      mutationLock.unlock();
    }
  }

  protected void addOrUpdateToken(TokenIdent ident,
      DelegationTokenInformation info, boolean isUpdate) throws Exception {
    String nodeCreatePath =
        getNodePath(ZK_DTSM_TOKENS_ROOT, DELEGATION_TOKEN_PREFIX
            + ident.getSequenceNumber());

    if (isUpdate) {
      DelegationTokenInformation previousLocal = currentTokens.get(ident);
      while (true) {
        Stat stat = new Stat();
        byte[] stored = zkClient.getData().storingStatIn(stat).forPath(nodeCreatePath);
        long previousRenewDate;
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(stored))) {
          createIdentifier().readFields(in);
          previousRenewDate = in.readLong();
        }
        DelegationTokenInformation renewed = new DelegationTokenInformation(
            Math.max(previousRenewDate, info.getRenewDate()), info.getPassword(),
            info.getTrackingId());
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(bytes)) {
          ident.write(out);
          out.writeLong(renewed.getRenewDate());
          out.writeInt(renewed.getPassword().length);
          out.write(renewed.getPassword());
          try {
            zkClient.setData().withVersion(stat.getVersion())
                .forPath(nodeCreatePath, bytes.toByteArray());
          } catch (KeeperException.BadVersionException e) {
            // A peer renewed the token. Re-read rather than shortening its life.
            continue;
          }
        }
        // A watcher may have removed or replaced the entry while storage was
        // pending. Never restore a removal or overwrite that newer observation.
        if (previousLocal != null) {
          currentTokens.replace(ident, previousLocal, renewed);
        }
        return;
      }
    }

    try (ByteArrayOutputStream tokenOs = new ByteArrayOutputStream();
         DataOutputStream tokenOut = new DataOutputStream(tokenOs)) {
      ident.write(tokenOut);
      tokenOut.writeLong(info.getRenewDate());
      tokenOut.writeInt(info.getPassword().length);
      tokenOut.write(info.getPassword());
      if (LOG.isDebugEnabled()) {
        LOG.debug((isUpdate ? "Updating " : "Storing ")
            + "ZKDTSMDelegationToken_" +
            ident.getSequenceNumber());
      }
      zkClient.create().withMode(CreateMode.PERSISTENT)
          .forPath(nodeCreatePath, tokenOs.toByteArray());
    }
  }

  public boolean isTokenWatcherEnabled() {
    return isTokenWatcherEnabled;
  }

  @VisibleForTesting
  @Private
  @Unstable
  static String getNodePath(String root, String nodeName) {
    return (root + "/" + nodeName);
  }

  @VisibleForTesting
  DelegationTokenInformation getTokenInfoFromMemory(TokenIdent ident) {
    return currentTokens.get(ident);
  }
}
