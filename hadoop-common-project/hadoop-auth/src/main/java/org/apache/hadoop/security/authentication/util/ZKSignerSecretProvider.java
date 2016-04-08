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

import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.servlet.ServletContext;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SignerSecretProvider that synchronizes a rolling random secret between
 * multiple servers using ZooKeeper.
 * <p>
 * It works by storing the secrets and next rollover time in a ZooKeeper znode.
 * All ZKSignerSecretProviders looking at that znode will use those
 * secrets and next rollover time to ensure they are synchronized.  There is no
 * "leader" -- any of the ZKSignerSecretProviders can choose the next secret;
 * which one is indeterminate.  Kerberos-based ACLs can also be enforced to
 * prevent a malicious third-party from getting or setting the secrets.  It uses
 * its own CuratorFramework client for talking to ZooKeeper.  If you want to use
 * your own Curator client, you can pass it to ZKSignerSecretProvider; see
 * {@link org.apache.hadoop.security.authentication.server.AuthenticationFilter}
 * for more details.
 * <p>
 * Details of the configurations are listed on <a href="../../../../../../../Configuration.html">Configuration Page</a>
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public class ZKSignerSecretProvider extends RolloverSignerSecretProvider {

  private static final String CONFIG_PREFIX =
          "signer.secret.provider.zookeeper.";

  /**
   * Constant for the property that specifies the ZooKeeper connection string.
   */
  public static final String ZOOKEEPER_CONNECTION_STRING =
          CONFIG_PREFIX + "connection.string";

  /**
   * Constant for the property that specifies the ZooKeeper path.
   */
  public static final String ZOOKEEPER_PATH = CONFIG_PREFIX + "path";

  /**
   * Constant for the property that specifies the auth type to use.  Supported
   * values are "none" and "sasl".  The default value is "none".
   */
  public static final String ZOOKEEPER_AUTH_TYPE = CONFIG_PREFIX + "auth.type";

  /**
   * Constant for the property that specifies the Kerberos keytab file.
   */
  public static final String ZOOKEEPER_KERBEROS_KEYTAB =
          CONFIG_PREFIX + "kerberos.keytab";

  /**
   * Constant for the property that specifies the Kerberos principal.
   */
  public static final String ZOOKEEPER_KERBEROS_PRINCIPAL =
          CONFIG_PREFIX + "kerberos.principal";

  /**
   * Constant for the property that specifies whether or not the Curator client
   * should disconnect from ZooKeeper on shutdown.  The default is "true".  Only
   * set this to "false" if a custom Curator client is being provided and the
   * disconnection is being handled elsewhere.
   */
  public static final String DISCONNECT_FROM_ZOOKEEPER_ON_SHUTDOWN =
          CONFIG_PREFIX + "disconnect.on.shutdown";

  /**
   * Constant for the ServletContext attribute that can be used for providing a
   * custom CuratorFramework client. If set ZKSignerSecretProvider will use this
   * Curator client instead of creating a new one. The providing class is
   * responsible for creating and configuring the Curator client (including
   * security and ACLs) in this case.
   */
  public static final String
      ZOOKEEPER_SIGNER_SECRET_PROVIDER_CURATOR_CLIENT_ATTRIBUTE =
      CONFIG_PREFIX + "curator.client";

  private static final String JAAS_LOGIN_ENTRY_NAME =
          "ZKSignerSecretProviderClient";

  private static Logger LOG = LoggerFactory.getLogger(
          ZKSignerSecretProvider.class);
  private String path;
  /**
   * Stores the next secret that will be used after the current one rolls over.
   * We do this to help with rollover performance by actually deciding the next
   * secret at the previous rollover.  This allows us to switch to the next
   * secret very quickly.  Afterwards, we have plenty of time to decide on the
   * next secret.
   */
  private volatile byte[] nextSecret;
  private final Random rand;
  /**
   * Stores the current version of the znode.
   */
  private int zkVersion;
  /**
   * Stores the next date that the rollover will occur.  This is only used
   * for allowing new servers joining later to synchronize their rollover
   * with everyone else.
   */
  private long nextRolloverDate;
  private long tokenValidity;
  private CuratorFramework client;
  private boolean shouldDisconnect;
  private static int INT_BYTES = Integer.SIZE / Byte.SIZE;
  private static int LONG_BYTES = Long.SIZE / Byte.SIZE;
  private static int DATA_VERSION = 0;

  public ZKSignerSecretProvider() {
    super();
    rand = new Random();
  }

  /**
   * This constructor lets you set the seed of the Random Number Generator and
   * is meant for testing.
   * @param seed the seed for the random number generator
   */
  @VisibleForTesting
  public ZKSignerSecretProvider(long seed) {
    super();
    rand = new Random(seed);
  }

  @Override
  public void init(Properties config, ServletContext servletContext,
          long tokenValidity) throws Exception {
    Object curatorClientObj = servletContext.getAttribute(
            ZOOKEEPER_SIGNER_SECRET_PROVIDER_CURATOR_CLIENT_ATTRIBUTE);
    if (curatorClientObj != null
            && curatorClientObj instanceof CuratorFramework) {
      client = (CuratorFramework) curatorClientObj;
    } else {
      client = createCuratorClient(config);
      servletContext.setAttribute(
          ZOOKEEPER_SIGNER_SECRET_PROVIDER_CURATOR_CLIENT_ATTRIBUTE, client);
    }
    this.tokenValidity = tokenValidity;
    shouldDisconnect = Boolean.parseBoolean(
            config.getProperty(DISCONNECT_FROM_ZOOKEEPER_ON_SHUTDOWN, "true"));
    path = config.getProperty(ZOOKEEPER_PATH);
    if (path == null) {
      throw new IllegalArgumentException(ZOOKEEPER_PATH
              + " must be specified");
    }
    try {
      nextRolloverDate = System.currentTimeMillis() + tokenValidity;
      // everyone tries to do this, only one will succeed and only when the
      // znode doesn't already exist.  Everyone else will synchronize on the
      // data from the znode
      client.create().creatingParentsIfNeeded()
              .forPath(path, generateZKData(generateRandomSecret(),
              generateRandomSecret(), null));
      zkVersion = 0;
      LOG.info("Creating secret znode");
    } catch (KeeperException.NodeExistsException nee) {
      LOG.info("The secret znode already exists, retrieving data");
    }
    // Synchronize on the data from the znode
    // passing true tells it to parse out all the data for initing
    pullFromZK(true);
    long initialDelay = nextRolloverDate - System.currentTimeMillis();
    // If it's in the past, try to find the next interval that we should
    // be using
    if (initialDelay < 1l) {
      int i = 1;
      while (initialDelay < 1l) {
        initialDelay = nextRolloverDate + tokenValidity * i
                - System.currentTimeMillis();
        i++;
      }
    }
    super.startScheduler(initialDelay, tokenValidity);
  }

  /**
   * Disconnects from ZooKeeper unless told not to.
   */
  @Override
  public void destroy() {
    if (shouldDisconnect && client != null) {
      client.close();
    }
    super.destroy();
  }

  @Override
  protected synchronized void rollSecret() {
    super.rollSecret();
    // Try to push the information to ZooKeeper with a potential next secret.
    nextRolloverDate += tokenValidity;
    byte[][] secrets = super.getAllSecrets();
    pushToZK(generateRandomSecret(), secrets[0], secrets[1]);
    // Pull info from ZooKeeper to get the decided next secret
    // passing false tells it that we don't care about most of the data
    pullFromZK(false);
  }

  @Override
  protected byte[] generateNewSecret() {
    // We simply return nextSecret because it's already been decided on
    return nextSecret;
  }

  /**
   * Pushes proposed data to ZooKeeper.  If a different server pushes its data
   * first, it gives up.
   * @param newSecret The new secret to use
   * @param currentSecret The current secret
   * @param previousSecret  The previous secret
   */
  private synchronized void pushToZK(byte[] newSecret, byte[] currentSecret,
          byte[] previousSecret) {
    byte[] bytes = generateZKData(newSecret, currentSecret, previousSecret);
    try {
      client.setData().withVersion(zkVersion).forPath(path, bytes);
    } catch (KeeperException.BadVersionException bve) {
      LOG.debug("Unable to push to znode; another server already did it");
    } catch (Exception ex) {
      LOG.error("An unexpected exception occured pushing data to ZooKeeper",
              ex);
    }
  }

  /**
   * Serialize the data to attempt to push into ZooKeeper.  The format is this:
   * <p>
   * [DATA_VERSION, newSecretLength, newSecret, currentSecretLength, currentSecret, previousSecretLength, previousSecret, nextRolloverDate]
   * <p>
   * Only previousSecret can be null, in which case the format looks like this:
   * <p>
   * [DATA_VERSION, newSecretLength, newSecret, currentSecretLength, currentSecret, 0, nextRolloverDate]
   * <p>
   * @param newSecret The new secret to use
   * @param currentSecret The current secret
   * @param previousSecret The previous secret
   * @return The serialized data for ZooKeeper
   */
  private synchronized byte[] generateZKData(byte[] newSecret,
          byte[] currentSecret, byte[] previousSecret) {
    int newSecretLength = newSecret.length;
    int currentSecretLength = currentSecret.length;
    int previousSecretLength = 0;
    if (previousSecret != null) {
      previousSecretLength = previousSecret.length;
    }
    ByteBuffer bb = ByteBuffer.allocate(INT_BYTES + INT_BYTES + newSecretLength
        + INT_BYTES + currentSecretLength + INT_BYTES + previousSecretLength
        + LONG_BYTES);
    bb.putInt(DATA_VERSION);
    bb.putInt(newSecretLength);
    bb.put(newSecret);
    bb.putInt(currentSecretLength);
    bb.put(currentSecret);
    bb.putInt(previousSecretLength);
    if (previousSecretLength > 0) {
      bb.put(previousSecret);
    }
    bb.putLong(nextRolloverDate);
    return bb.array();
  }

  /**
   * Pulls data from ZooKeeper.  If isInit is false, it will only parse the
   * next secret and version.  If isInit is true, it will also parse the current
   * and previous secrets, and the next rollover date; it will also init the
   * secrets.  Hence, isInit should only be true on startup.
   * @param isInit  see description above
   */
  private synchronized void pullFromZK(boolean isInit) {
    try {
      Stat stat = new Stat();
      byte[] bytes = client.getData().storingStatIn(stat).forPath(path);
      ByteBuffer bb = ByteBuffer.wrap(bytes);
      int dataVersion = bb.getInt();
      if (dataVersion > DATA_VERSION) {
        throw new IllegalStateException("Cannot load data from ZooKeeper; it"
                + "was written with a newer version");
      }
      int nextSecretLength = bb.getInt();
      byte[] nextSecret = new byte[nextSecretLength];
      bb.get(nextSecret);
      this.nextSecret = nextSecret;
      zkVersion = stat.getVersion();
      if (isInit) {
        int currentSecretLength = bb.getInt();
        byte[] currentSecret = new byte[currentSecretLength];
        bb.get(currentSecret);
        int previousSecretLength = bb.getInt();
        byte[] previousSecret = null;
        if (previousSecretLength > 0) {
          previousSecret = new byte[previousSecretLength];
          bb.get(previousSecret);
        }
        super.initSecrets(currentSecret, previousSecret);
        nextRolloverDate = bb.getLong();
      }
    } catch (Exception ex) {
      LOG.error("An unexpected exception occurred while pulling data from"
              + "ZooKeeper", ex);
    }
  }

  private byte[] generateRandomSecret() {
    return Long.toString(rand.nextLong()).getBytes(Charset.forName("UTF-8"));
  }

  /**
   * This method creates the Curator client and connects to ZooKeeper.
   * @param config configuration properties
   * @return A Curator client
   * @throws Exception thrown if an error occurred
   */
  protected CuratorFramework createCuratorClient(Properties config)
          throws Exception {
    String connectionString = config.getProperty(
            ZOOKEEPER_CONNECTION_STRING, "localhost:2181");

    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    ACLProvider aclProvider;
    String authType = config.getProperty(ZOOKEEPER_AUTH_TYPE, "none");
    if (authType.equals("sasl")) {
      LOG.info("Connecting to ZooKeeper with SASL/Kerberos"
              + "and using 'sasl' ACLs");
      String principal = setJaasConfiguration(config);
      System.setProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY,
              JAAS_LOGIN_ENTRY_NAME);
      System.setProperty("zookeeper.authProvider.1",
              "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
      aclProvider = new SASLOwnerACLProvider(principal);
    } else {  // "none"
      LOG.info("Connecting to ZooKeeper without authentication");
      aclProvider = new DefaultACLProvider();     // open to everyone
    }
    CuratorFramework cf = CuratorFrameworkFactory.builder()
            .connectString(connectionString)
            .retryPolicy(retryPolicy)
            .aclProvider(aclProvider)
            .build();
    cf.start();
    return cf;
  }

  private String setJaasConfiguration(Properties config) throws Exception {
    String keytabFile = config.getProperty(ZOOKEEPER_KERBEROS_KEYTAB).trim();
    if (keytabFile == null || keytabFile.length() == 0) {
      throw new IllegalArgumentException(ZOOKEEPER_KERBEROS_KEYTAB
              + " must be specified");
    }
    String principal = config.getProperty(ZOOKEEPER_KERBEROS_PRINCIPAL)
            .trim();
    if (principal == null || principal.length() == 0) {
      throw new IllegalArgumentException(ZOOKEEPER_KERBEROS_PRINCIPAL
              + " must be specified");
    }

    // This is equivalent to writing a jaas.conf file and setting the system
    // property, "java.security.auth.login.config", to point to it
    JaasConfiguration jConf =
            new JaasConfiguration(JAAS_LOGIN_ENTRY_NAME, principal, keytabFile);
    Configuration.setConfiguration(jConf);
    return principal.split("[/@]")[0];
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

  /**
   * Creates a programmatic version of a jaas.conf file. This can be used
   * instead of writing a jaas.conf file and setting the system property,
   * "java.security.auth.login.config", to point to that file. It is meant to be
   * used for connecting to ZooKeeper.
   */
  @InterfaceAudience.Private
  public static class JaasConfiguration extends Configuration {

    private static AppConfigurationEntry[] entry;
    private String entryName;

    /**
     * Add an entry to the jaas configuration with the passed in name,
     * principal, and keytab. The other necessary options will be set for you.
     *
     * @param entryName The name of the entry (e.g. "Client")
     * @param principal The principal of the user
     * @param keytab The location of the keytab
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
      entry = new AppConfigurationEntry[]{
                  new AppConfigurationEntry(getKrb5LoginModuleName(),
                  AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                  options)};
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
}
