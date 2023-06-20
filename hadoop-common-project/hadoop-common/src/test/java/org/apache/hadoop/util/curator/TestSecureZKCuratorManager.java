/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.util.curator;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.zookeeper.ClientCnxnSocketNetty;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.server.NettyServerCnxnFactory;

import static org.apache.hadoop.fs.FileContext.LOG;
import static org.junit.Assert.assertEquals;

/**
 * Test the manager for ZooKeeper Curator when SSL/TLS is enabled for the ZK server-client
 * connection.
 */
public class TestSecureZKCuratorManager {

  public static final boolean DELETE_DATA_DIRECTORY_ON_CLOSE = true;
  private TestingServer server;
  private ZKCuratorManager curator;
  private Configuration hadoopConf;
  static final int SECURE_CLIENT_PORT = 2281;
  static final int JUTE_MAXBUFFER = 400000000;
  static final File ZK_DATA_DIR = new File("testZkSSLClientConnectionDataDir");
  private static final int SERVER_ID = 1;
  private static final int TICK_TIME = 100;
  private static final int MAX_CLIENT_CNXNS = 10;
  public static final int ELECTION_PORT = -1;
  public static final int QUORUM_PORT = -1;

  @Before
  public void setup() throws Exception {
    // inject values to the ZK configuration file for secure connection
    Map<String, Object> customConfiguration = new HashMap<>();
    customConfiguration.put("secureClientPort", String.valueOf(SECURE_CLIENT_PORT));
    customConfiguration.put("audit.enable", true);
    this.hadoopConf = setUpSecureConfig();
    InstanceSpec spec =
        new InstanceSpec(ZK_DATA_DIR, SECURE_CLIENT_PORT, ELECTION_PORT, QUORUM_PORT,
            DELETE_DATA_DIRECTORY_ON_CLOSE, SERVER_ID, TICK_TIME, MAX_CLIENT_CNXNS,
            customConfiguration);
    this.server = new TestingServer(spec, true);
    this.hadoopConf.set(CommonConfigurationKeys.ZK_ADDRESS, this.server.getConnectString());
    this.curator = new ZKCuratorManager(this.hadoopConf);
    this.curator.start(new ArrayList<>(), true);
  }

  /**
   * A static method to configure the test ZK server to accept secure client connection.
   * The self-signed certificates were generated for testing purposes as described below.
   * For the ZK client to connect with the ZK server, the ZK server's keystore and truststore
   * should be used.
   * For testing purposes the keystore and truststore were generated using default values.
   * 1. to generate the keystore.jks file:
   * # keytool -genkey -alias mockcert -keyalg RSA -keystore keystore.jks -keysize 2048
   * 2. generate the ca-cert and the ca-key:
   * # openssl req -new -x509 -keyout ca-key -out ca-cert
   * 3. to generate the certificate signing request (cert-file):
   * # keytool -keystore keystore.jks -alias mockcert -certreq -file certificate-request
   * 4. to generate the ca-cert.srl file and make the cert valid for 10 years:
   * # openssl x509 -req -CA ca-cert -CAkey ca-key -in certificate-request -out cert-signed
   * -days 3650 -CAcreateserial -passin pass:password
   * 5. add the ca-cert to the keystore.jks:
   * # keytool -keystore keystore.jks -alias mockca -import -file ca-cert
   * 6. install the signed certificate to the keystore:
   * # keytool -keystore keystore.jks -alias mockcert -import -file cert-signed
   * 7. add the certificate to the truststore:
   * # keytool -keystore truststore.jks -alias mockcert -import -file ca-cert
   * For our purpose, we only need the end result of this process: the keystore.jks and the
   * truststore.jks files.
   *
   * @return conf The method returns the updated Configuration.
   */
  public static Configuration setUpSecureConfig() {
    return setUpSecureConfig(new Configuration(),
        "src/test/java/org/apache/hadoop/util/curator" + "/resources/data");
  }

  public static Configuration setUpSecureConfig(Configuration conf, String testDataPath) {
    System.setProperty("zookeeper.serverCnxnFactory",
        NettyServerCnxnFactory.class.getCanonicalName());

    System.setProperty("zookeeper.ssl.keyStore.location", testDataPath + "keystore.jks");
    System.setProperty("zookeeper.ssl.keyStore.password", "password");
    System.setProperty("zookeeper.ssl.trustStore.location", testDataPath + "truststore.jks");
    System.setProperty("zookeeper.ssl.trustStore.password", "password");
    System.setProperty("zookeeper.request.timeout", "12345");

    System.setProperty("jute.maxbuffer", String.valueOf(JUTE_MAXBUFFER));

    System.setProperty("javax.net.debug", "ssl");
    System.setProperty("zookeeper.authProvider.x509",
        "org.apache.zookeeper.server.auth.X509AuthenticationProvider");

    conf.set(CommonConfigurationKeys.ZK_SSL_KEYSTORE_LOCATION,
        testDataPath + "/ssl/keystore.jks");
    conf.set(CommonConfigurationKeys.ZK_SSL_KEYSTORE_PASSWORD, "password");
    conf.set(CommonConfigurationKeys.ZK_SSL_TRUSTSTORE_LOCATION,
        testDataPath + "/ssl/truststore.jks");
    conf.set(CommonConfigurationKeys.ZK_SSL_TRUSTSTORE_PASSWORD, "password");
    return conf;
  }

  @After
  public void teardown() throws Exception {
    this.curator.close();
    if (this.server != null) {
      this.server.close();
      this.server = null;
    }
  }

  @Test
  public void testSecureZKConfiguration() throws Exception {
    LOG.info("Entered to the testSecureZKConfiguration test case.");
    // Validate that HadoopZooKeeperFactory will set ZKConfig with given principals
    ZKCuratorManager.HadoopZookeeperFactory factory =
        new ZKCuratorManager.HadoopZookeeperFactory(null, null, null, true,
            new ZKCuratorManager.TruststoreKeystore(hadoopConf));
    ZooKeeper zk = factory.newZooKeeper(this.server.getConnectString(), 1000, null, false);
    validateSSLConfiguration(this.hadoopConf.get(CommonConfigurationKeys.ZK_SSL_KEYSTORE_LOCATION),
        this.hadoopConf.get(CommonConfigurationKeys.ZK_SSL_KEYSTORE_PASSWORD),
        this.hadoopConf.get(CommonConfigurationKeys.ZK_SSL_TRUSTSTORE_LOCATION),
        this.hadoopConf.get(CommonConfigurationKeys.ZK_SSL_TRUSTSTORE_PASSWORD), zk);
  }

  private void validateSSLConfiguration(String keystoreLocation, String keystorePassword,
      String truststoreLocation, String truststorePassword, ZooKeeper zk) {
    try (ClientX509Util x509Util = new ClientX509Util()) {
      //testing if custom values are set properly
      assertEquals("Validate that expected clientConfig is set in ZK config", keystoreLocation,
          zk.getClientConfig().getProperty(x509Util.getSslKeystoreLocationProperty()));
      assertEquals("Validate that expected clientConfig is set in ZK config", keystorePassword,
          zk.getClientConfig().getProperty(x509Util.getSslKeystorePasswdProperty()));
      assertEquals("Validate that expected clientConfig is set in ZK config", truststoreLocation,
          zk.getClientConfig().getProperty(x509Util.getSslTruststoreLocationProperty()));
      assertEquals("Validate that expected clientConfig is set in ZK config", truststorePassword,
          zk.getClientConfig().getProperty(x509Util.getSslTruststorePasswdProperty()));
    }
    //testing if constant values hardcoded into the code are set properly
    assertEquals("Validate that expected clientConfig is set in ZK config", Boolean.TRUE.toString(),
        zk.getClientConfig().getProperty(ZKClientConfig.SECURE_CLIENT));
    assertEquals("Validate that expected clientConfig is set in ZK config",
        ClientCnxnSocketNetty.class.getCanonicalName(),
        zk.getClientConfig().getProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET));
  }

  @Test
  public void testTruststoreKeystoreConfiguration() {
    LOG.info("Entered to the testTruststoreKeystoreConfiguration test case.");
    /*
      By default the truststore/keystore configurations are not set, hence the values are null.
      Validate that the null values are converted into empty strings by the class.
     */
    Configuration conf = new Configuration();
    ZKCuratorManager.TruststoreKeystore truststoreKeystore =
        new ZKCuratorManager.TruststoreKeystore(conf);

    assertEquals("Validate that null value is converted to empty string.", "",
        truststoreKeystore.getKeystoreLocation());
    assertEquals("Validate that null value is converted to empty string.", "",
        truststoreKeystore.getKeystorePassword());
    assertEquals("Validate that null value is converted to empty string.", "",
        truststoreKeystore.getTruststoreLocation());
    assertEquals("Validate that null value is converted to empty string.", "",
        truststoreKeystore.getTruststorePassword());

    //Validate that non-null values will remain intact
    conf.set(CommonConfigurationKeys.ZK_SSL_KEYSTORE_LOCATION, "/keystore.jks");
    conf.set(CommonConfigurationKeys.ZK_SSL_KEYSTORE_PASSWORD, "keystorePassword");
    conf.set(CommonConfigurationKeys.ZK_SSL_TRUSTSTORE_LOCATION, "/truststore.jks");
    conf.set(CommonConfigurationKeys.ZK_SSL_TRUSTSTORE_PASSWORD, "truststorePassword");
    ZKCuratorManager.TruststoreKeystore truststoreKeystore1 =
        new ZKCuratorManager.TruststoreKeystore(conf);
    assertEquals("Validate that non-null value kept intact.", "/keystore.jks",
        truststoreKeystore1.getKeystoreLocation());
    assertEquals("Validate that null value is converted to empty string.", "keystorePassword",
        truststoreKeystore1.getKeystorePassword());
    assertEquals("Validate that null value is converted to empty string.", "/truststore.jks",
        truststoreKeystore1.getTruststoreLocation());
    assertEquals("Validate that null value is converted to empty string.", "truststorePassword",
        truststoreKeystore1.getTruststorePassword());
  }
}
