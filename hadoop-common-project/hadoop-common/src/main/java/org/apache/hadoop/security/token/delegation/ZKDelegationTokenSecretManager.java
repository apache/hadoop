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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheBridge;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.util.JaasConfiguration;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenManager;
import static org.apache.hadoop.util.Time.now;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

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
  private int currentSeqNum;
  private int currentMaxSeqNum;
  private final ReentrantLock currentSeqNumLock;
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
    this.currentSeqNumLock = new ReentrantLock(true);
    if (CURATOR_TL.get() != null) {
      zkClient =
          CURATOR_TL.get().usingNamespace(
              conf.get(ZK_DTSM_ZNODE_WORKING_PATH,
                  ZK_DTSM_ZNODE_WORKING_PATH_DEAFULT)
                  + "/" + ZK_DTSM_NAMESPACE);
      isExternalClient = true;
    } else {
      String connString = conf.get(ZK_DTSM_ZK_CONNECTION_STRING);
      Preconditions.checkNotNull(connString,
          "Zookeeper connection string cannot be null");
      String authType = conf.get(ZK_DTSM_ZK_AUTH_TYPE);

      // AuthType has to be explicitly set to 'none' or 'sasl'
      Preconditions.checkNotNull(authType, "Zookeeper authType cannot be null !!");
      Preconditions.checkArgument(
          authType.equals("sasl") || authType.equals("none"),
          "Zookeeper authType must be one of [none, sasl]");

      Builder builder = null;
      try {
        ACLProvider aclProvider = null;
        if (authType.equals("sasl")) {
          LOG.info("Connecting to ZooKeeper with SASL/Kerberos"
              + "and using 'sasl' ACLs");
          String principal = setJaasConfiguration(conf);
          System.setProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY,
                             JAAS_LOGIN_ENTRY_NAME);
          System.setProperty("zookeeper.authProvider.1",
              "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
          aclProvider = new SASLOwnerACLProvider(principal);
        } else { // "none"
          LOG.info("Connecting to ZooKeeper without authentication");
          aclProvider = new DefaultACLProvider(); // open to everyone
        }
        int sessionT =
            conf.getInt(ZK_DTSM_ZK_SESSION_TIMEOUT,
                ZK_DTSM_ZK_SESSION_TIMEOUT_DEFAULT);
        int numRetries =
            conf.getInt(ZK_DTSM_ZK_NUM_RETRIES, ZK_DTSM_ZK_NUM_RETRIES_DEFAULT);
        builder =
            CuratorFrameworkFactory
                .builder()
                .zookeeperFactory(new ZKCuratorManager.HadoopZookeeperFactory(
                    conf.get(ZK_DTSM_ZK_KERBEROS_SERVER_PRINCIPAL)))
                .aclProvider(aclProvider)
                .namespace(
                    conf.get(ZK_DTSM_ZNODE_WORKING_PATH,
                        ZK_DTSM_ZNODE_WORKING_PATH_DEAFULT)
                        + "/"
                        + ZK_DTSM_NAMESPACE
                )
                .sessionTimeoutMs(sessionT)
                .connectionTimeoutMs(
                    conf.getInt(ZK_DTSM_ZK_CONNECTION_TIMEOUT,
                        ZK_DTSM_ZK_CONNECTION_TIMEOUT_DEFAULT)
                )
                .retryPolicy(
                    new RetryNTimes(numRetries, numRetries == 0 ? 0 : sessionT / numRetries));
      } catch (Exception ex) {
        throw new RuntimeException("Could not Load ZK acls or auth: " + ex, ex);
      }
      zkClient = builder.ensembleProvider(new FixedEnsembleProvider(connString))
          .build();
      isExternalClient = false;
    }
  }

  private String setJaasConfiguration(Configuration config) throws Exception {
    String keytabFile =
        config.get(ZK_DTSM_ZK_KERBEROS_KEYTAB, "").trim();
    if (keytabFile == null || keytabFile.length() == 0) {
      throw new IllegalArgumentException(ZK_DTSM_ZK_KERBEROS_KEYTAB
          + " must be specified");
    }
    String principal =
        config.get(ZK_DTSM_ZK_KERBEROS_PRINCIPAL, "").trim();
    principal = SecurityUtil.getServerPrincipal(principal, "");
    if (principal == null || principal.length() == 0) {
      throw new IllegalArgumentException(ZK_DTSM_ZK_KERBEROS_PRINCIPAL
          + " must be specified");
    }

    JaasConfiguration jConf =
        new JaasConfiguration(JAAS_LOGIN_ENTRY_NAME, principal, keytabFile);
    javax.security.auth.login.Configuration.setConfiguration(jConf);
    return principal.split("[/@]")[0];
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
      currentSeqNum = incrSharedCount(delTokSeqCounter, seqNumBatchSize);
      currentMaxSeqNum = currentSeqNum + seqNumBatchSize;
      LOG.info("Fetched initial range of seq num, from {} to {} ",
          currentSeqNum+1, currentMaxSeqNum);
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
    // The secret manager will keep a local range of seq num which won't be
    // seen by peers, so only when the range is exhausted it will ask zk for
    // another range again
    try {
      this.currentSeqNumLock.lock();
      if (currentSeqNum >= currentMaxSeqNum) {
        try {
          // after a successful batch request, we can get the range starting point
          currentSeqNum = incrSharedCount(delTokSeqCounter, seqNumBatchSize);
          currentMaxSeqNum = currentSeqNum + seqNumBatchSize;
          LOG.info("Fetched new range of seq num, from {} to {} ",
              currentSeqNum+1, currentMaxSeqNum);
        } catch (InterruptedException e) {
          // The ExpirationThread is just finishing.. so dont do anything..
          LOG.debug(
                  "Thread interrupted while performing token counter increment", e);
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          throw new RuntimeException("Could not increment shared counter !!", e);
        }
      }
      return ++currentSeqNum;
    } finally {
      this.currentSeqNumLock.unlock();
    }
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
      incrSharedCount(keyIdSeqCounter, 1);
    } catch (InterruptedException e) {
      // The ExpirationThread is just finishing.. so dont do anything..
      LOG.debug("Thread interrupted while performing keyId increment", e);
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      throw new RuntimeException("Could not increment shared keyId counter !!", e);
    }
    return keyIdSeqCounter.getCount();
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
    // First check if I have this..
    DelegationTokenInformation tokenInfo = currentTokens.get(ident);
    // Then query ZK
    if (tokenInfo == null) {
      try {
        tokenInfo = getTokenInfoFromZK(ident);
        if (tokenInfo != null) {
          currentTokens.put(ident, tokenInfo);
        }
      } catch (IOException e) {
        LOG.error("Error retrieving tokenInfo [" + ident.getSequenceNumber()
            + "] from ZK", e);
      }
    }
    return tokenInfo;
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
    try {
      byte[] data = zkClient.getData().forPath(nodePath);
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
      if (zkClient.checkExists().forPath(nodeRemovePath) != null) {
        while(zkClient.checkExists().forPath(nodeRemovePath) != null){
          try {
            zkClient.delete().guaranteed().forPath(nodeRemovePath);
          } catch (NoNodeException nne) {
            // It is possible that the node might be deleted between the
            // check and the actual delete.. which might lead to an
            // exception that can bring down the daemon running this
            // SecretManager
            LOG.debug("Node already deleted by peer " + nodeRemovePath);
          }
        }
      } else {
        LOG.debug("Attempted to delete a non-existing znode " + nodeRemovePath);
      }
    } catch (Exception e) {
      LOG.debug(nodeRemovePath + " znode could not be removed!!");
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
    String nodeRemovePath =
        getNodePath(ZK_DTSM_TOKENS_ROOT, DELEGATION_TOKEN_PREFIX
            + ident.getSequenceNumber());
    try {
      if (zkClient.checkExists().forPath(nodeRemovePath) != null) {
        addOrUpdateToken(ident, tokenInfo, true);
      } else {
        addOrUpdateToken(ident, tokenInfo, false);
        LOG.debug("Attempted to update a non-existing znode " + nodeRemovePath);
      }
    } catch (Exception e) {
      throw new RuntimeException("Could not update Stored Token ZKDTSMDelegationToken_"
          + ident.getSequenceNumber(), e);
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
      DelegationTokenInformation dtInfo = getTokenInfoFromZK(ident, true);
      if (dtInfo != null) {
        // For the case there is no sync or watch miss, it is possible that the
        // local storage has expired tokens which have been renewed by peer
        // so double check again to avoid accidental delete
        if (checkAgainstZkBeforeDeletion
            && dtInfo.getRenewDate() > now()) {
          LOG.info("Node already renewed by peer " + nodeRemovePath +
              " so this token should not be deleted");
          return;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Removing ZKDTSMDelegationToken_"
              + ident.getSequenceNumber());
        }
        while(zkClient.checkExists().forPath(nodeRemovePath) != null){
          try {
            zkClient.delete().guaranteed().forPath(nodeRemovePath);
          } catch (NoNodeException nne) {
            // It is possible that the node might be deleted between the
            // check and the actual delete.. which might lead to an
            // exception that can bring down the daemon running this
            // SecretManager
            LOG.debug("Node already deleted by peer " + nodeRemovePath);
          }
        }
      } else {
        LOG.debug("Attempted to remove a non-existing znode " + nodeRemovePath);
      }
    } catch (Exception e) {
      throw new RuntimeException(
          "Could not remove Stored Token ZKDTSMDelegationToken_"
          + ident.getSequenceNumber(), e);
    }
  }

  @Override
  public TokenIdent cancelToken(Token<TokenIdent> token,
      String canceller) throws IOException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    TokenIdent id = createIdentifier();
    id.readFields(in);

    syncLocalCacheWithZk(id);
    return super.cancelToken(token, canceller);
  }

  protected void addOrUpdateToken(TokenIdent ident,
      DelegationTokenInformation info, boolean isUpdate) throws Exception {
    String nodeCreatePath =
        getNodePath(ZK_DTSM_TOKENS_ROOT, DELEGATION_TOKEN_PREFIX
            + ident.getSequenceNumber());

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
      if (isUpdate) {
        zkClient.setData().forPath(nodeCreatePath, tokenOs.toByteArray())
            .setVersion(-1);
      } else {
        zkClient.create().withMode(CreateMode.PERSISTENT)
            .forPath(nodeCreatePath, tokenOs.toByteArray());
      }
    }
  }

  public boolean isTokenWatcherEnabled() {
    return isTokenWatcherEnabled;
  }

  /**
   * Simple implementation of an {@link ACLProvider} that simply returns an ACL
   * that gives all permissions only to a single principal.
   */
  private static class SASLOwnerACLProvider implements ACLProvider {

    private final List<ACL> saslACL;

    private SASLOwnerACLProvider(String principal) {
      this.saslACL = Collections.singletonList(
          new ACL(Perms.ALL, new Id("sasl", principal)));
    }

    @Override
    public List<ACL> getDefaultAcl() {
      return saslACL;
    }

    @Override
    public List<ACL> getAclForPath(String path) {
      return saslACL;
    }
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
