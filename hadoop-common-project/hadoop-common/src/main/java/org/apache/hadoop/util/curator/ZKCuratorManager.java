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

package org.apache.hadoop.util.curator;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.util.JaasConfiguration;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.util.Preconditions;

import javax.naming.ConfigurationException;

/**
 * Helper class that provides utility methods specific to ZK operations.
 */
@InterfaceAudience.Private
public final class ZKCuratorManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ZKCuratorManager.class);

  /** Configuration for the ZooKeeper connection. */
  private final Configuration conf;

  /** Curator for ZooKeeper. */
  private CuratorFramework curator;

  public ZKCuratorManager(Configuration config) throws IOException {
    this.conf = config;
  }

  /**
   * Get the curator framework managing the ZooKeeper connection.
   * @return Curator framework.
   */
  public CuratorFramework getCurator() {
    return curator;
  }

  /**
   * Close the connection with ZooKeeper.
   */
  public void close() {
    if (curator != null) {
      curator.close();
    }
  }

  /**
   * Utility method to fetch the ZK ACLs from the configuration.
   *
   * @param conf configuration.
   * @throws java.io.IOException if the Zookeeper ACLs configuration file
   * cannot be read
   * @return acl list.
   */
  public static List<ACL> getZKAcls(Configuration conf) throws IOException {
    // Parse authentication from configuration.
    String zkAclConf = conf.get(CommonConfigurationKeys.ZK_ACL,
        CommonConfigurationKeys.ZK_ACL_DEFAULT);
    try {
      zkAclConf = ZKUtil.resolveConfIndirection(zkAclConf);
      return ZKUtil.parseACLs(zkAclConf);
    } catch (IOException | ZKUtil.BadAclFormatException e) {
      LOG.error("Couldn't read ACLs based on {}",
          CommonConfigurationKeys.ZK_ACL);
      throw e;
    }
  }

  /**
   * Utility method to fetch ZK auth info from the configuration.
   *
   * @param conf configuration.
   * @throws java.io.IOException if the Zookeeper ACLs configuration file
   * cannot be read
   * @throws ZKUtil.BadAuthFormatException if the auth format is invalid
   * @return ZKAuthInfo List.
   */
  public static List<ZKUtil.ZKAuthInfo> getZKAuths(Configuration conf)
      throws IOException {
    return SecurityUtil.getZKAuthInfos(conf, CommonConfigurationKeys.ZK_AUTH);
  }

  /**
   * Start the connection to the ZooKeeper ensemble.
   * @throws IOException If the connection cannot be started.
   */
  public void start() throws IOException{
    this.start(new ArrayList<>());
  }

  /**
   * Start the connection to the ZooKeeper ensemble.
   * @param authInfos List of authentication keys.
   * @throws IOException If the connection cannot be started.
   */
  public void start(List<AuthInfo> authInfos) throws IOException {
    this.start(authInfos, false);
  }

  /**
   * Start the connection to the ZooKeeper ensemble.
   *
   * @param authInfos  List of authentication keys.
   * @param sslEnabled If the connection should be SSL/TLS encrypted.
   * @throws IOException            If the connection cannot be started.
   */
  public void start(List<AuthInfo> authInfos, boolean sslEnabled)
      throws IOException{

    ZKClientConfig zkClientConfig = new ZKClientConfig();

    // Connect to the ZooKeeper ensemble
    String zkHostPort = conf.get(CommonConfigurationKeys.ZK_ADDRESS);
    if (zkHostPort == null) {
      throw new IOException(
          CommonConfigurationKeys.ZK_ADDRESS + " is not configured.");
    }
    LOG.debug("Configured {} as {}", CommonConfigurationKeys.ZK_ADDRESS, zkHostPort);

    int numRetries = conf.getInt(CommonConfigurationKeys.ZK_NUM_RETRIES,
        CommonConfigurationKeys.ZK_NUM_RETRIES_DEFAULT);
    int zkSessionTimeout = conf.getInt(CommonConfigurationKeys.ZK_TIMEOUT_MS,
        CommonConfigurationKeys.ZK_TIMEOUT_MS_DEFAULT);
    int zkRetryInterval = conf.getInt(
        CommonConfigurationKeys.ZK_RETRY_INTERVAL_MS,
        CommonConfigurationKeys.ZK_RETRY_INTERVAL_MS_DEFAULT);
    RetryNTimes retryPolicy = new RetryNTimes(numRetries, zkRetryInterval);

    // Set up ZK auths
    List<ZKUtil.ZKAuthInfo> zkAuths = getZKAuths(conf);
    if (authInfos == null) {
      authInfos = new ArrayList<>();
    }
    for (ZKUtil.ZKAuthInfo zkAuth : zkAuths) {
      authInfos.add(new AuthInfo(zkAuth.getScheme(), zkAuth.getAuth()));
    }
    if (sslEnabled) {
      validateSslConfiguration(conf);
    }
    CuratorFramework client = CuratorFrameworkFactory.builder().connectString(zkHostPort)
        .zookeeperFactory(
            new HadoopZookeeperFactory(conf.get(CommonConfigurationKeys.ZK_SERVER_PRINCIPAL),
                conf.get(CommonConfigurationKeys.ZK_KERBEROS_PRINCIPAL),
                conf.get(CommonConfigurationKeys.ZK_KERBEROS_KEYTAB), sslEnabled,
                new TruststoreKeystore(conf))).zkClientConfig(zkClientConfig)
        .sessionTimeoutMs(zkSessionTimeout).retryPolicy(retryPolicy)
        .authorization(authInfos).build();
    client.start();

    this.curator = client;
  }
  /* Check on SSL/TLS client connection requirements to emit the name of the
   configuration missing. It improves supportability. */
  private void validateSslConfiguration(Configuration config) throws IOException {
    if (StringUtils.isEmpty(config.get(CommonConfigurationKeys.ZK_SSL_KEYSTORE_LOCATION))) {
      throw new IOException(
          "The SSL encryption is enabled for the component's ZooKeeper client connection, "
              + "however the " + CommonConfigurationKeys.ZK_SSL_KEYSTORE_LOCATION + " " +
              "parameter is empty.");
    }
    if (StringUtils.isEmpty(config.get(CommonConfigurationKeys.ZK_SSL_KEYSTORE_PASSWORD))) {
      throw new IOException(
          "The SSL encryption is enabled for the component's " + "ZooKeeper client connection, "
              + "however the " + CommonConfigurationKeys.ZK_SSL_KEYSTORE_PASSWORD + " " +
              "parameter is empty.");
    }
    if (StringUtils.isEmpty(config.get(CommonConfigurationKeys.ZK_SSL_TRUSTSTORE_LOCATION))) {
      throw new IOException(
          "The SSL encryption is enabled for the component's ZooKeeper client connection, "
              + "however the " + CommonConfigurationKeys.ZK_SSL_TRUSTSTORE_LOCATION + " " +
              "parameter is empty.");
    }
    if (StringUtils.isEmpty(config.get(CommonConfigurationKeys.ZK_SSL_TRUSTSTORE_PASSWORD))) {
      throw new IOException(
          "The SSL encryption is enabled for the component's ZooKeeper client connection, "
              + "however the " + CommonConfigurationKeys.ZK_SSL_TRUSTSTORE_PASSWORD + "  " +
              "parameter is empty.");
    }
  }

  /**
   * Get ACLs for a ZNode.
   * @param path Path of the ZNode.
   * @return The list of ACLs.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public List<ACL> getACL(final String path) throws Exception {
    return curator.getACL().forPath(path);
  }

  /**
   * Get the data in a ZNode.
   * @param path Path of the ZNode.
   * @return The data in the ZNode.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public byte[] getData(final String path) throws Exception {
    return curator.getData().forPath(path);
  }

  /**
   * Get the data in a ZNode.
   * @param path Path of the ZNode.
   * @param stat stat.
   * @return The data in the ZNode.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public byte[] getData(final String path, Stat stat) throws Exception {
    return curator.getData().storingStatIn(stat).forPath(path);
  }

  /**
   * Get the data in a ZNode.
   * @param path Path of the ZNode.
   * @return The data in the ZNode.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public String getStringData(final String path) throws Exception {
    byte[] bytes = getData(path);
    if (bytes != null) {
      return new String(bytes, Charset.forName("UTF-8"));
    }
    return null;
  }

  /**
   * Get the data in a ZNode.
   * @param path Path of the ZNode.
   * @param stat Output statistics of the ZNode.
   * @return The data in the ZNode.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public String getStringData(final String path, Stat stat) throws Exception {
    byte[] bytes = getData(path, stat);
    if (bytes != null) {
      return new String(bytes, Charset.forName("UTF-8"));
    }
    return null;
  }

  /**
   * Set data into a ZNode.
   * @param path Path of the ZNode.
   * @param data Data to set.
   * @param version Version of the data to store.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public void setData(String path, byte[] data, int version) throws Exception {
    curator.setData().withVersion(version).forPath(path, data);
  }

  /**
   * Set data into a ZNode.
   * @param path Path of the ZNode.
   * @param data Data to set as String.
   * @param version Version of the data to store.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public void setData(String path, String data, int version) throws Exception {
    byte[] bytes = data.getBytes(Charset.forName("UTF-8"));
    setData(path, bytes, version);
  }

  /**
   * Get children of a ZNode.
   * @param path Path of the ZNode.
   * @return The list of children.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public List<String> getChildren(final String path) throws Exception {
    return curator.getChildren().forPath(path);
  }

  /**
   * Check if a ZNode exists.
   * @param path Path of the ZNode.
   * @return If the ZNode exists.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public boolean exists(final String path) throws Exception {
    return curator.checkExists().forPath(path) != null;
  }

  /**
   * Create a ZNode.
   * @param path Path of the ZNode.
   * @return If the ZNode was created.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public boolean create(final String path) throws Exception {
    return create(path, null);
  }

  /**
   * Create a ZNode.
   * @param path Path of the ZNode.
   * @param zkAcl ACL for the node.
   * @return If the ZNode was created.
   * @throws Exception If it cannot contact Zookeeper.
   */
  public boolean create(final String path, List<ACL> zkAcl) throws Exception {
    boolean created = false;
    if (!exists(path)) {
      curator.create()
          .withMode(CreateMode.PERSISTENT)
          .withACL(zkAcl)
          .forPath(path, null);
      created = true;
    }
    return created;
  }

  /**
   * Utility function to ensure that the configured base znode exists.
   * This recursively creates the znode as well as all of its parents.
   * @param path Path of the znode to create.
   * @throws Exception If it cannot create the file.
   */
  public void createRootDirRecursively(String path) throws Exception {
    createRootDirRecursively(path, null);
  }

  /**
   * Utility function to ensure that the configured base znode exists.
   * This recursively creates the znode as well as all of its parents.
   * @param path Path of the znode to create.
   * @param zkAcl ACLs for ZooKeeper.
   * @throws Exception If it cannot create the file.
   */
  public void createRootDirRecursively(String path, List<ACL> zkAcl)
      throws Exception {
    String[] pathParts = path.split("/");
    Preconditions.checkArgument(
        pathParts.length >= 1 && pathParts[0].isEmpty(),
        "Invalid path: %s", path);
    StringBuilder sb = new StringBuilder();

    for (int i = 1; i < pathParts.length; i++) {
      sb.append("/").append(pathParts[i]);
      create(sb.toString(), zkAcl);
    }
  }

  /**
   * Delete a ZNode.
   * @param path Path of the ZNode.
   * @return If the znode was deleted.
   * @throws Exception If it cannot contact ZooKeeper.
   */
  public boolean delete(final String path) throws Exception {
    if (exists(path)) {
      curator.delete().deletingChildrenIfNeeded().forPath(path);
      return true;
    }
    return false;
  }

  /**
   * Get the path for a ZNode.
   * @param root Root of the ZNode.
   * @param nodeName Name of the ZNode.
   * @return Path for the ZNode.
   */
  public static String getNodePath(String root, String nodeName) {
    return root + "/" + nodeName;
  }

  public void safeCreate(String path, byte[] data, List<ACL> acl,
      CreateMode mode, List<ACL> fencingACL, String fencingNodePath)
      throws Exception {
    if (!exists(path)) {
      SafeTransaction transaction = createTransaction(fencingACL,
          fencingNodePath);
      transaction.create(path, data, acl, mode);
      transaction.commit();
    }
  }

  /**
   * Deletes the path. Checks for existence of path as well.
   *
   * @param path Path to be deleted.
   * @param fencingNodePath fencingNodePath.
   * @param fencingACL fencingACL.
   * @throws Exception if any problem occurs while performing deletion.
   */
  public void safeDelete(final String path, List<ACL> fencingACL,
      String fencingNodePath) throws Exception {
    if (exists(path)) {
      SafeTransaction transaction = createTransaction(fencingACL,
          fencingNodePath);
      transaction.delete(path);
      transaction.commit();
    }
  }

  public void safeSetData(String path, byte[] data, int version,
      List<ACL> fencingACL, String fencingNodePath)
      throws Exception {
    SafeTransaction transaction = createTransaction(fencingACL,
        fencingNodePath);
    transaction.setData(path, data, version);
    transaction.commit();
  }

  public SafeTransaction createTransaction(List<ACL> fencingACL,
      String fencingNodePath) throws Exception {
    return new SafeTransaction(fencingACL, fencingNodePath);
  }

  /**
   * Use curator transactions to ensure zk-operations are performed in an all
   * or nothing fashion. This is equivalent to using ZooKeeper#multi.
   */
  public class SafeTransaction {
    private String fencingNodePath;
    private List<CuratorOp> curatorOperations = new LinkedList<>();

    SafeTransaction(List<ACL> fencingACL, String fencingNodePath)
        throws Exception {
      this.fencingNodePath = fencingNodePath;
      curatorOperations.add(curator.transactionOp().create()
          .withMode(CreateMode.PERSISTENT)
          .withACL(fencingACL)
          .forPath(fencingNodePath, new byte[0]));
    }

    public void commit() throws Exception {
      curatorOperations.add(curator.transactionOp().delete()
          .forPath(fencingNodePath));
      curator.transaction().forOperations(curatorOperations);
      curatorOperations.clear();
    }

    public void create(String path, byte[] data, List<ACL> acl, CreateMode mode)
        throws Exception {
      curatorOperations.add(curator.transactionOp().create()
          .withMode(mode)
          .withACL(acl)
          .forPath(path, data));
    }

    public void delete(String path) throws Exception {
      curatorOperations.add(curator.transactionOp().delete()
          .forPath(path));
    }

    public void setData(String path, byte[] data, int version)
        throws Exception {
      curatorOperations.add(curator.transactionOp().setData()
          .withVersion(version)
          .forPath(path, data));
    }
  }

  public static class HadoopZookeeperFactory implements ZookeeperFactory {
    public final static String JAAS_CLIENT_ENTRY = "HadoopZookeeperClient";
    private final String zkPrincipal;
    private final String kerberosPrincipal;
    private final String kerberosKeytab;
    private final Boolean sslEnabled;
    private final TruststoreKeystore truststoreKeystore;

    /**
     * Constructor for the helper class to configure the ZooKeeper client connection.
     * @param zkPrincipal Optional.
     */
    public HadoopZookeeperFactory(String zkPrincipal) {
      this(zkPrincipal, null, null);
    }

    /**
     * Constructor for the helper class to configure the ZooKeeper client connection.
     * @param zkPrincipal Optional.
     * @param kerberosPrincipal Optional. Use along with kerberosKeytab.
     * @param kerberosKeytab Optional. Use along with kerberosPrincipal.
     */
    public HadoopZookeeperFactory(String zkPrincipal, String kerberosPrincipal,
        String kerberosKeytab) {
      this(zkPrincipal, kerberosPrincipal, kerberosKeytab, false,
          new TruststoreKeystore(new Configuration()));
    }

    /**
     * Constructor for the helper class to configure the ZooKeeper client connection.
     *
     * @param zkPrincipal        Optional.
     * @param kerberosPrincipal  Optional. Use along with kerberosKeytab.
     * @param kerberosKeytab     Optional. Use along with kerberosPrincipal.
     * @param sslEnabled         Flag to enable SSL/TLS ZK client connection for each component
     *                           independently.
     * @param truststoreKeystore TruststoreKeystore object containing the keystoreLocation,
     *                           keystorePassword, truststoreLocation, truststorePassword for
     *                           SSL/TLS connection when sslEnabled is set to true.
     */
    public HadoopZookeeperFactory(String zkPrincipal, String kerberosPrincipal,
        String kerberosKeytab, boolean sslEnabled, TruststoreKeystore truststoreKeystore) {
      this.zkPrincipal = zkPrincipal;
      this.kerberosPrincipal = kerberosPrincipal;
      this.kerberosKeytab = kerberosKeytab;
      this.sslEnabled = sslEnabled;
      this.truststoreKeystore = truststoreKeystore;
    }

    @Override
    public ZooKeeper newZooKeeper(String connectString, int sessionTimeout,
        Watcher watcher, boolean canBeReadOnly
    ) throws Exception {
      ZKClientConfig zkClientConfig = new ZKClientConfig();
      if (zkPrincipal != null) {
        LOG.info("Configuring zookeeper to use {} as the server principal",
            zkPrincipal);
        zkClientConfig.setProperty(ZKClientConfig.ZK_SASL_CLIENT_USERNAME,
            zkPrincipal);
      }
      if (zkClientConfig.isSaslClientEnabled() && !isJaasConfigurationSet(zkClientConfig)) {
        setJaasConfiguration(zkClientConfig);
      }
      if (sslEnabled) {
        setSslConfiguration(zkClientConfig);
      }
      return new ZooKeeper(connectString, sessionTimeout, watcher,
          canBeReadOnly, zkClientConfig);
    }

    /**
     * Configure ZooKeeper Client with SSL/TLS connection.
     * @param zkClientConfig ZooKeeper Client configuration
     */
    private void setSslConfiguration(ZKClientConfig zkClientConfig) throws ConfigurationException {
      this.setSslConfiguration(zkClientConfig, new ClientX509Util());
    }

    private void setSslConfiguration(ZKClientConfig zkClientConfig, ClientX509Util x509Util)
        throws ConfigurationException {
      validateSslConfiguration();
      LOG.info("Configuring the ZooKeeper client to use SSL/TLS encryption for connecting to the "
          + "ZooKeeper server.");
      LOG.debug("Configuring the ZooKeeper client with {} location: {}.",
          this.truststoreKeystore.keystoreLocation,
          CommonConfigurationKeys.ZK_SSL_KEYSTORE_LOCATION);
      LOG.debug("Configuring the ZooKeeper client with {} location: {}.",
          this.truststoreKeystore.truststoreLocation,
          CommonConfigurationKeys.ZK_SSL_TRUSTSTORE_LOCATION);

      zkClientConfig.setProperty(ZKClientConfig.SECURE_CLIENT, "true");
      zkClientConfig.setProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET,
          "org.apache.zookeeper.ClientCnxnSocketNetty");
      zkClientConfig.setProperty(x509Util.getSslKeystoreLocationProperty(),
          this.truststoreKeystore.keystoreLocation);
      zkClientConfig.setProperty(x509Util.getSslKeystorePasswdProperty(),
          this.truststoreKeystore.keystorePassword);
      zkClientConfig.setProperty(x509Util.getSslTruststoreLocationProperty(),
          this.truststoreKeystore.truststoreLocation);
      zkClientConfig.setProperty(x509Util.getSslTruststorePasswdProperty(),
          this.truststoreKeystore.truststorePassword);
    }

    private void validateSslConfiguration() throws ConfigurationException {
      if (StringUtils.isEmpty(this.truststoreKeystore.keystoreLocation)) {
        throw new ConfigurationException(
            "The keystore location parameter is empty for the ZooKeeper client connection.");
      }
      if (StringUtils.isEmpty(this.truststoreKeystore.keystorePassword)) {
        throw new ConfigurationException(
            "The keystore password parameter is empty for the ZooKeeper client connection.");
      }
      if (StringUtils.isEmpty(this.truststoreKeystore.truststoreLocation)) {
        throw new ConfigurationException(
            "The truststore location parameter is empty for the ZooKeeper client connection.");
      }
      if (StringUtils.isEmpty(this.truststoreKeystore.truststorePassword)) {
        throw new ConfigurationException(
            "The truststore password parameter is empty for the ZooKeeper client connection.");
      }
    }

    private boolean isJaasConfigurationSet(ZKClientConfig zkClientConfig) {
      String clientConfig = zkClientConfig.getProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY,
          ZKClientConfig.LOGIN_CONTEXT_NAME_KEY_DEFAULT);
      return javax.security.auth.login.Configuration.getConfiguration()
          .getAppConfigurationEntry(clientConfig) != null;
    }

    private void setJaasConfiguration(ZKClientConfig zkClientConfig) throws IOException {
      if (kerberosPrincipal == null || kerberosKeytab == null) {
        LOG.warn("JaasConfiguration has not been set since kerberos principal "
            + "or keytab is not specified");
        return;
      }

      String principal = SecurityUtil.getServerPrincipal(kerberosPrincipal, "");
      JaasConfiguration jconf = new JaasConfiguration(JAAS_CLIENT_ENTRY, principal,
          kerberosKeytab);
      javax.security.auth.login.Configuration.setConfiguration(jconf);
      zkClientConfig.setProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY, JAAS_CLIENT_ENTRY);
    }
  }

  /**
   * Helper class to contain the Truststore/Keystore paths for the ZK client connection over
   * SSL/TLS.
   */
  public static class TruststoreKeystore {
    private final String keystoreLocation;
    private final String keystorePassword;
    private final String truststoreLocation;
    private final String truststorePassword;

    /**
     * Configuration for the ZooKeeper connection when SSL/TLS is enabled.
     * When a value is not configured, ensure that empty string is set instead of null.
     *
     * @param conf ZooKeeper Client configuration
     */
    public TruststoreKeystore(Configuration conf) {
      keystoreLocation = conf.get(CommonConfigurationKeys.ZK_SSL_KEYSTORE_LOCATION, "");
      keystorePassword = conf.get(CommonConfigurationKeys.ZK_SSL_KEYSTORE_PASSWORD, "");
      truststoreLocation = conf.get(CommonConfigurationKeys.ZK_SSL_TRUSTSTORE_LOCATION, "");
      truststorePassword = conf.get(CommonConfigurationKeys.ZK_SSL_TRUSTSTORE_PASSWORD, "");
    }

    public String getKeystoreLocation() {
      return keystoreLocation;
    }

    public String getKeystorePassword() {
      return keystorePassword;
    }

    public String getTruststoreLocation() {
      return truststoreLocation;
    }

    public String getTruststorePassword() {
      return truststorePassword;
    }
  }
}