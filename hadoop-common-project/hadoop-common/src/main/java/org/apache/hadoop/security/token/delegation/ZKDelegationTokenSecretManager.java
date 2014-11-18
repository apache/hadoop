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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.security.auth.login.AppConfigurationEntry;

import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenManager;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

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

  private static final String ZK_CONF_PREFIX = "zk-dt-secret-manager.";
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

  public static final int ZK_DTSM_ZK_NUM_RETRIES_DEFAULT = 3;
  public static final int ZK_DTSM_ZK_SESSION_TIMEOUT_DEFAULT = 10000;
  public static final int ZK_DTSM_ZK_CONNECTION_TIMEOUT_DEFAULT = 10000;
  public static final int ZK_DTSM_ZK_SHUTDOWN_TIMEOUT_DEFAULT = 10000;
  public static final String ZK_DTSM_ZNODE_WORKING_PATH_DEAFULT = "zkdtsm";

  private static Logger LOG = LoggerFactory
      .getLogger(ZKDelegationTokenSecretManager.class);

  private static final String JAAS_LOGIN_ENTRY_NAME =
      "ZKDelegationTokenSecretManagerClient";

  private static final String ZK_DTSM_NAMESPACE = "ZKDTSMRoot";
  private static final String ZK_DTSM_SEQNUM_ROOT = "ZKDTSMSeqNumRoot";
  private static final String ZK_DTSM_KEYID_ROOT = "ZKDTSMKeyIdRoot";
  private static final String ZK_DTSM_TOKENS_ROOT = "ZKDTSMTokensRoot";
  private static final String ZK_DTSM_MASTER_KEY_ROOT = "ZKDTSMMasterKeyRoot";

  private static final String DELEGATION_KEY_PREFIX = "DK_";
  private static final String DELEGATION_TOKEN_PREFIX = "DT_";

  private static final ThreadLocal<CuratorFramework> CURATOR_TL =
      new ThreadLocal<CuratorFramework>();

  public static void setCurator(CuratorFramework curator) {
    CURATOR_TL.set(curator);
  }

  private final boolean isExternalClient;
  private final CuratorFramework zkClient;
  private SharedCount delTokSeqCounter;
  private SharedCount keyIdSeqCounter;
  private PathChildrenCache keyCache;
  private PathChildrenCache tokenCache;
  private ExecutorService listenerThreadPool;
  private final long shutdownTimeout;

  public ZKDelegationTokenSecretManager(Configuration conf) {
    super(conf.getLong(DelegationTokenManager.UPDATE_INTERVAL,
        DelegationTokenManager.UPDATE_INTERVAL_DEFAULT) * 1000,
        conf.getLong(DelegationTokenManager.MAX_LIFETIME,
            DelegationTokenManager.MAX_LIFETIME_DEFAULT) * 1000,
        conf.getLong(DelegationTokenManager.RENEW_INTERVAL,
            DelegationTokenManager.RENEW_INTERVAL_DEFAULT * 1000),
        conf.getLong(DelegationTokenManager.REMOVAL_SCAN_INTERVAL,
            DelegationTokenManager.REMOVAL_SCAN_INTERVAL_DEFAULT) * 1000);
    shutdownTimeout = conf.getLong(ZK_DTSM_ZK_SHUTDOWN_TIMEOUT,
        ZK_DTSM_ZK_SHUTDOWN_TIMEOUT_DEFAULT);
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
          System.setProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY,
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
                    new RetryNTimes(numRetries, sessionT / numRetries));
      } catch (Exception ex) {
        throw new RuntimeException("Could not Load ZK acls or auth");
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
    if (principal == null || principal.length() == 0) {
      throw new IllegalArgumentException(ZK_DTSM_ZK_KERBEROS_PRINCIPAL
          + " must be specified");
    }

    JaasConfiguration jConf =
        new JaasConfiguration(JAAS_LOGIN_ENTRY_NAME, principal, keytabFile);
    javax.security.auth.login.Configuration.setConfiguration(jConf);
    return principal.split("[/@]")[0];
  }

  /**
   * Creates a programmatic version of a jaas.conf file. This can be used
   * instead of writing a jaas.conf file and setting the system property,
   * "java.security.auth.login.config", to point to that file. It is meant to be
   * used for connecting to ZooKeeper.
   */
  @InterfaceAudience.Private
  public static class JaasConfiguration extends
      javax.security.auth.login.Configuration {

    private static AppConfigurationEntry[] entry;
    private String entryName;

    /**
     * Add an entry to the jaas configuration with the passed in name,
     * principal, and keytab. The other necessary options will be set for you.
     *
     * @param entryName
     *          The name of the entry (e.g. "Client")
     * @param principal
     *          The principal of the user
     * @param keytab
     *          The location of the keytab
     */
    public JaasConfiguration(String entryName, String principal, String keytab) {
      this.entryName = entryName;
      Map<String, String> options = new HashMap<String, String>();
      options.put("keyTab", keytab);
      options.put("principal", principal);
      options.put("useKeyTab", "true");
      options.put("storeKey", "true");
      options.put("useTicketCache", "false");
      options.put("refreshKrb5Config", "true");
      String jaasEnvVar = System.getenv("HADOOP_JAAS_DEBUG");
      if (jaasEnvVar != null && "true".equalsIgnoreCase(jaasEnvVar)) {
        options.put("debug", "true");
      }
      entry = new AppConfigurationEntry[] {
          new AppConfigurationEntry(getKrb5LoginModuleName(),
              AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
              options) };
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      return (entryName.equals(name)) ? entry : null;
    }

    private String getKrb5LoginModuleName() {
      String krb5LoginModuleName;
      if (System.getProperty("java.vendor").contains("IBM")) {
        krb5LoginModuleName = "com.ibm.security.auth.module.Krb5LoginModule";
      } else {
        krb5LoginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
      }
      return krb5LoginModuleName;
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
    }
    listenerThreadPool = Executors.newSingleThreadExecutor();
    try {
      delTokSeqCounter = new SharedCount(zkClient, ZK_DTSM_SEQNUM_ROOT, 0);
      if (delTokSeqCounter != null) {
        delTokSeqCounter.start();
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
      keyCache = new PathChildrenCache(zkClient, ZK_DTSM_MASTER_KEY_ROOT, true);
      if (keyCache != null) {
        keyCache.start(StartMode.BUILD_INITIAL_CACHE);
        keyCache.getListenable().addListener(new PathChildrenCacheListener() {
          @Override
          public void childEvent(CuratorFramework client,
              PathChildrenCacheEvent event)
              throws Exception {
            switch (event.getType()) {
            case CHILD_ADDED:
              processKeyAddOrUpdate(event.getData().getData());
              break;
            case CHILD_UPDATED:
              processKeyAddOrUpdate(event.getData().getData());
              break;
            case CHILD_REMOVED:
              processKeyRemoved(event.getData().getPath());
              break;
            default:
              break;
            }
          }
        }, listenerThreadPool);
      }
    } catch (Exception e) {
      throw new IOException("Could not start PathChildrenCache for keys", e);
    }
    try {
      tokenCache = new PathChildrenCache(zkClient, ZK_DTSM_TOKENS_ROOT, true);
      if (tokenCache != null) {
        tokenCache.start(StartMode.BUILD_INITIAL_CACHE);
        tokenCache.getListenable().addListener(new PathChildrenCacheListener() {

          @Override
          public void childEvent(CuratorFramework client,
              PathChildrenCacheEvent event) throws Exception {
            switch (event.getType()) {
            case CHILD_ADDED:
              processTokenAddOrUpdate(event.getData());
              break;
            case CHILD_UPDATED:
              processTokenAddOrUpdate(event.getData());
              break;
            case CHILD_REMOVED:
              processTokenRemoved(event.getData());
              break;
            default:
              break;
            }
          }
        }, listenerThreadPool);
      }
    } catch (Exception e) {
      throw new IOException("Could not start PathChildrenCache for tokens", e);
    }
    super.startThreads();
  }

  private void processKeyAddOrUpdate(byte[] data) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream(data);
    DataInputStream din = new DataInputStream(bin);
    DelegationKey key = new DelegationKey();
    key.readFields(din);
    synchronized (this) {
      allKeys.put(key.getKeyId(), key);
    }
  }

  private void processKeyRemoved(String path) {
    int i = path.lastIndexOf('/');
    if (i > 0) {
      String tokSeg = path.substring(i + 1);
      int j = tokSeg.indexOf('_');
      if (j > 0) {
        int keyId = Integer.parseInt(tokSeg.substring(j + 1));
        synchronized (this) {
          allKeys.remove(keyId);
        }
      }
    }
  }

  private void processTokenAddOrUpdate(ChildData data) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream(data.getData());
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
      synchronized (this) {
        currentTokens.put(ident, tokenInfo);
        // The cancel task might be waiting
        notifyAll();
      }
    }
  }

  private void processTokenRemoved(ChildData data) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream(data.getData());
    DataInputStream din = new DataInputStream(bin);
    TokenIdent ident = createIdentifier();
    ident.readFields(din);
    synchronized (this) {
      currentTokens.remove(ident);
      // The cancel task might be waiting
      notifyAll();
    }
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
    if (listenerThreadPool != null) {
      listenerThreadPool.shutdown();
      try {
        // wait for existing tasks to terminate
        if (!listenerThreadPool.awaitTermination(shutdownTimeout,
            TimeUnit.MILLISECONDS)) {
          LOG.error("Forcing Listener threadPool to shutdown !!");
          listenerThreadPool.shutdownNow();
        }
      } catch (InterruptedException ie) {
        listenerThreadPool.shutdownNow();
        Thread.currentThread().interrupt();
      }
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

  @Override
  protected int incrementDelegationTokenSeqNum() {
    try {
      while (!delTokSeqCounter.trySetCount(delTokSeqCounter.getCount() + 1)) {
      }
    } catch (InterruptedException e) {
      // The ExpirationThread is just finishing.. so dont do anything..
      LOG.debug("Thread interrupted while performing token counter increment", e);
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      throw new RuntimeException("Could not increment shared counter !!", e);
    }
    return delTokSeqCounter.getCount();
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
      while (!keyIdSeqCounter.trySetCount(keyIdSeqCounter.getCount() + 1)) {
      }
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

  private DelegationTokenInformation getTokenInfoFromZK(TokenIdent ident)
      throws IOException {
    return getTokenInfoFromZK(ident, false);
  }

  private DelegationTokenInformation getTokenInfoFromZK(TokenIdent ident,
      boolean quiet) throws IOException {
    String nodePath =
        getNodePath(ZK_DTSM_TOKENS_ROOT,
            DELEGATION_TOKEN_PREFIX + ident.getSequenceNumber());
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
          zkClient.delete().guaranteed().forPath(nodeRemovePath);
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
    String nodeRemovePath =
        getNodePath(ZK_DTSM_TOKENS_ROOT, DELEGATION_TOKEN_PREFIX
            + ident.getSequenceNumber());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing ZKDTSMDelegationToken_"
          + ident.getSequenceNumber());
    }
    try {
      if (zkClient.checkExists().forPath(nodeRemovePath) != null) {
        while(zkClient.checkExists().forPath(nodeRemovePath) != null){
          zkClient.delete().guaranteed().forPath(nodeRemovePath);
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
  public synchronized TokenIdent cancelToken(Token<TokenIdent> token,
      String canceller) throws IOException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    TokenIdent id = createIdentifier();
    id.readFields(in);
    try {
      if (!currentTokens.containsKey(id)) {
        // See if token can be retrieved and placed in currentTokens
        getTokenInfo(id);
      }
      return super.cancelToken(token, canceller);
    } catch (Exception e) {
      LOG.error("Exception while checking if token exist !!", e);
      return id;
    }
  }

  private void addOrUpdateToken(TokenIdent ident,
      DelegationTokenInformation info, boolean isUpdate) throws Exception {
    String nodeCreatePath =
        getNodePath(ZK_DTSM_TOKENS_ROOT, DELEGATION_TOKEN_PREFIX
            + ident.getSequenceNumber());
    ByteArrayOutputStream tokenOs = new ByteArrayOutputStream();
    DataOutputStream tokenOut = new DataOutputStream(tokenOs);
    ByteArrayOutputStream seqOs = new ByteArrayOutputStream();

    try {
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
    } finally {
      seqOs.close();
    }
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
  public ExecutorService getListenerThreadPool() {
    return listenerThreadPool;
  }
}
