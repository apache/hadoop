/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.security.authentication.util;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ConfigurableZookeeperFactory;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;
import java.util.Collections;
import java.util.List;

/**
 * Utility class to create a CuratorFramework object that can be used to connect to Zookeeper
 * based on configuration values that can be supplied from different configuration properties.
 * It is used from ZKDelegationTokenSecretManager in hadoop-common, and from
 * {@link ZKSignerSecretProvider}.
 *
 * The class implements a fluid API to set up all the different properties. A very basic setup
 * would seem like:
 * <pre>
 *   ZookeeperClient.configure()
 *     .withConnectionString(&lt;connectionString&gt;)
 *     .create();
 * </pre>
 *
 * Mandatory parameters to be set:
 * <ul>
 *   <li>connectionString: A Zookeeper connection string.</li>
 *   <li>if authentication type is set to 'sasl':
 *   <ul>
 *     <li>keytab: the location of the keytab to be used for Kerberos authentication</li>
 *     <li>principal: the Kerberos principal to be used from the supplied Kerberos keytab file.</li>
 *     <li>jaasLoginEntryName: the login entry name in the JAAS configuration that is created for
 *                           the KerberosLoginModule to be used by the Zookeeper client code.</li>
 *   </ul>
 *   </li>
 *   <li>if SSL is enabled:
 *   <ul>
 *     <li>the location of the Truststore file to be used</li>
 *     <li>the location of the Keystore file to be used</li>
 *     <li>if the Truststore is protected by a password, then the password of the Truststore</li>
 *     <li>if the Keystore is protected by a password, then the password if the Keystore</li>
 *   </ul>
 *   </li>
 * </ul>
 *
 * When using 'sasl' authentication type, the JAAS configuration to be used by the Zookeeper client
 * withing CuratorFramework is set to use the supplied keytab and principal for Kerberos login,
 * moreover an ACL provider is set to provide a default ACL that requires SASL auth and the same
 * principal to have access to the used paths.
 *
 * When using SSL/TLS, the Zookeeper client will set to use the secure channel towards Zookeeper,
 * with the specified Keystore and Truststore.
 *
 * Default values:
 * <ul>
 *   <li>authentication type: 'none'</li>
 *   <li>sessionTimeout: either the system property curator-default-session-timeout, or 60
 *                     seconds</li>
 *   <li>connectionTimeout: either the system property curator-default-connection-timeout, or 15
 *                        seconds</li>
 *   <li>retryPolicy: an ExponentialBackoffRetry, with a starting interval of 1 seconds and 3
 *                  retries</li>
 *   <li>zkFactory: a ConfigurableZookeeperFactory instance, to allow SSL setup via
 *                ZKClientConfig</li>
 * </ul>
 *
 * @see ZKSignerSecretProvider
 */
public class ZookeeperClient {

  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperClient.class);

  private String connectionString;
  private String namespace;

  private String authenticationType = "none";
  private String keytab;
  private String principal;
  private String jaasLoginEntryName;

  private int sessionTimeout =
      Integer.getInteger("curator-default-session-timeout", 60 * 1000);
  private int connectionTimeout =
      Integer.getInteger("curator-default-connection-timeout", 15 * 1000);

  private RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

  private ZookeeperFactory zkFactory = new ConfigurableZookeeperFactory();

  private boolean isSSLEnabled;
  private String keystoreLocation;
  private String keystorePassword;
  private String truststoreLocation;
  private String truststorePassword;

  public static ZookeeperClient configure() {
    return new ZookeeperClient();
  }

  public ZookeeperClient withConnectionString(String conn) {
    connectionString = conn;
    return this;
  }

  public ZookeeperClient withNamespace(String ns) {
    this.namespace = ns;
    return this;
  }

  public ZookeeperClient withAuthType(String authType) {
    this.authenticationType = authType;
    return this;
  }

  public ZookeeperClient withKeytab(String keytabPath) {
    this.keytab = keytabPath;
    return this;
  }

  public ZookeeperClient withPrincipal(String princ) {
    this.principal = princ;
    return this;
  }

  public ZookeeperClient withJaasLoginEntryName(String entryName) {
    this.jaasLoginEntryName = entryName;
    return this;
  }

  public ZookeeperClient withSessionTimeout(int timeoutMS) {
    this.sessionTimeout = timeoutMS;
    return this;
  }

  public ZookeeperClient withConnectionTimeout(int timeoutMS) {
    this.connectionTimeout = timeoutMS;
    return this;
  }

  public ZookeeperClient withRetryPolicy(RetryPolicy policy) {
    this.retryPolicy = policy;
    return this;
  }

  public ZookeeperClient withZookeeperFactory(ZookeeperFactory factory) {
    this.zkFactory = factory;
    return this;
  }

  public ZookeeperClient enableSSL(boolean enable) {
    this.isSSLEnabled = enable;
    return this;
  }

  public ZookeeperClient withKeystore(String keystorePath) {
    this.keystoreLocation = keystorePath;
    return this;
  }

  public ZookeeperClient withKeystorePassword(String keystorePass) {
    this.keystorePassword = keystorePass;
    return this;
  }

  public ZookeeperClient withTruststore(String truststorePath) {
    this.truststoreLocation = truststorePath;
    return this;
  }

  public ZookeeperClient withTruststorePassword(String truststorePass) {
    this.truststorePassword = truststorePass;
    return this;
  }

  public CuratorFramework create() {
    checkNotNull(connectionString, "Zookeeper connection string cannot be null!");
    checkNotNull(retryPolicy, "Zookeeper connection retry policy cannot be null!");

    return createFrameworkFactoryBuilder()
        .connectString(connectionString)
        .zookeeperFactory(zkFactory)
        .namespace(namespace)
        .sessionTimeoutMs(sessionTimeout)
        .connectionTimeoutMs(connectionTimeout)
        .retryPolicy(retryPolicy)
        .aclProvider(aclProvider())
        .zkClientConfig(zkClientConfig())
        .build();
  }

  @VisibleForTesting
  CuratorFrameworkFactory.Builder createFrameworkFactoryBuilder() {
    return CuratorFrameworkFactory.builder();
  }

  private ACLProvider aclProvider() {
    // AuthType has to be explicitly set to 'none' or 'sasl'
    checkNotNull(authenticationType, "Zookeeper authType cannot be null!");
    checkArgument(authenticationType.equals("sasl") || authenticationType.equals("none"),
        "Zookeeper authType must be one of [none, sasl]!");

    ACLProvider aclProvider;
    if (authenticationType.equals("sasl")) {
      LOG.info("Connecting to ZooKeeper with SASL/Kerberos and using 'sasl' ACLs.");

      checkArgument(!isEmpty(keytab), "Zookeeper client's Kerberos Keytab must be specified!");
      checkArgument(!isEmpty(principal),
          "Zookeeper client's Kerberos Principal must be specified!");
      checkArgument(!isEmpty(jaasLoginEntryName), "JAAS Login Entry name must be specified!");

      JaasConfiguration jConf = new JaasConfiguration(jaasLoginEntryName, principal, keytab);
      Configuration.setConfiguration(jConf);
      System.setProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY, jaasLoginEntryName);
      System.setProperty("zookeeper.authProvider.1",
          "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
      aclProvider = new SASLOwnerACLProvider(principal.split("[/@]")[0]);
    } else { // "none"
      LOG.info("Connecting to ZooKeeper without authentication.");
      aclProvider = new DefaultACLProvider(); // open to everyone
    }
    return aclProvider;
  }

  private ZKClientConfig zkClientConfig() {
    ZKClientConfig zkClientConfig = new ZKClientConfig();
    if (isSSLEnabled){
      LOG.info("Zookeeper client will use SSL connection. (keystore = {}; truststore = {};)",
          keystoreLocation, truststoreLocation);
      checkArgument(!isEmpty(keystoreLocation),
            "The keystore location parameter is empty for the ZooKeeper client connection.");
      checkArgument(!isEmpty(truststoreLocation),
            "The truststore location parameter is empty for the ZooKeeper client connection.");

      try (ClientX509Util sslOpts = new ClientX509Util()) {
        zkClientConfig.setProperty(ZKClientConfig.SECURE_CLIENT, "true");
        zkClientConfig.setProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET,
            "org.apache.zookeeper.ClientCnxnSocketNetty");
        zkClientConfig.setProperty(sslOpts.getSslKeystoreLocationProperty(), keystoreLocation);
        zkClientConfig.setProperty(sslOpts.getSslKeystorePasswdProperty(), keystorePassword);
        zkClientConfig.setProperty(sslOpts.getSslTruststoreLocationProperty(), truststoreLocation);
        zkClientConfig.setProperty(sslOpts.getSslTruststorePasswdProperty(), truststorePassword);
      }
    } else {
      LOG.info("Zookeeper client will use Plain connection.");
    }
    return zkClientConfig;
  }

  /**
   * Simple implementation of an {@link ACLProvider} that simply returns an ACL
   * that gives all permissions only to a single principal.
   */
  @VisibleForTesting
  static final class SASLOwnerACLProvider implements ACLProvider {

    private final List<ACL> saslACL;

    private SASLOwnerACLProvider(String principal) {
      this.saslACL = Collections.singletonList(
          new ACL(ZooDefs.Perms.ALL, new Id("sasl", principal)));
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

  private boolean isEmpty(String str) {
    return str == null || str.length() == 0;
  }

  //Preconditions allowed to be imported from hadoop-common, but that results
  //  in a circular dependency
  private void checkNotNull(Object reference, String errorMessage) {
    if (reference == null) {
      throw new NullPointerException(errorMessage);
    }
  }

  private void checkArgument(boolean expression, String errorMessage) {
    if (!expression) {
      throw new IllegalArgumentException(errorMessage);
    }
  }
}
