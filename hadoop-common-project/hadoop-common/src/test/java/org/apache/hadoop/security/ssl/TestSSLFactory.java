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
package org.apache.hadoop.security.ssl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.net.ssl.HttpsURLConnection;
import java.io.File;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Map;

public class TestSSLFactory {

  private static final String BASEDIR =
    System.getProperty("test.build.dir", "target/test-dir") + "/" +
    TestSSLFactory.class.getSimpleName();
  private static final String KEYSTORES_DIR =
    new File(BASEDIR).getAbsolutePath();
  private String sslConfsDir;

  @BeforeClass
  public static void setUp() throws Exception {
    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
  }

  private Configuration createConfiguration(boolean clientCert,
      boolean trustStore)
    throws Exception {
    Configuration conf = new Configuration();
    KeyStoreTestUtil.setupSSLConfig(KEYSTORES_DIR, sslConfsDir, conf,
      clientCert, trustStore);
    return conf;
  }

  @After
  @Before
  public void cleanUp() throws Exception {
    sslConfsDir = KeyStoreTestUtil.getClasspathDir(TestSSLFactory.class);
    KeyStoreTestUtil.cleanupSSLConfig(KEYSTORES_DIR, sslConfsDir);
  }

  @Test(expected = IllegalStateException.class)
  public void clientMode() throws Exception {
    Configuration conf = createConfiguration(false, true);
    SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    try {
      sslFactory.init();
      Assert.assertNotNull(sslFactory.createSSLSocketFactory());
      Assert.assertNotNull(sslFactory.getHostnameVerifier());
      sslFactory.createSSLServerSocketFactory();
    } finally {
      sslFactory.destroy();
    }
  }

  private void serverMode(boolean clientCert, boolean socket) throws Exception {
    Configuration conf = createConfiguration(clientCert, true);
    SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.SERVER, conf);
    try {
      sslFactory.init();
      Assert.assertNotNull(sslFactory.createSSLServerSocketFactory());
      Assert.assertEquals(clientCert, sslFactory.isClientCertRequired());
      if (socket) {
        sslFactory.createSSLSocketFactory();
      } else {
        sslFactory.getHostnameVerifier();
      }
    } finally {
      sslFactory.destroy();
    }
  }


  @Test(expected = IllegalStateException.class)
  public void serverModeWithoutClientCertsSocket() throws Exception {
    serverMode(false, true);
  }

  @Test(expected = IllegalStateException.class)
  public void serverModeWithClientCertsSocket() throws Exception {
    serverMode(true, true);
  }

  @Test(expected = IllegalStateException.class)
  public void serverModeWithoutClientCertsVerifier() throws Exception {
    serverMode(false, false);
  }

  @Test(expected = IllegalStateException.class)
  public void serverModeWithClientCertsVerifier() throws Exception {
    serverMode(true, false);
  }

  @Test
  public void validHostnameVerifier() throws Exception {
    Configuration conf = createConfiguration(false, true);
    conf.unset(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY);
    SSLFactory sslFactory = new
      SSLFactory(SSLFactory.Mode.CLIENT, conf);
    sslFactory.init();
    Assert.assertEquals("DEFAULT", sslFactory.getHostnameVerifier().toString());
    sslFactory.destroy();

    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "ALLOW_ALL");
    sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    sslFactory.init();
    Assert.assertEquals("ALLOW_ALL",
                        sslFactory.getHostnameVerifier().toString());
    sslFactory.destroy();

    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "DEFAULT_AND_LOCALHOST");
    sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    sslFactory.init();
    Assert.assertEquals("DEFAULT_AND_LOCALHOST",
                        sslFactory.getHostnameVerifier().toString());
    sslFactory.destroy();

    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "STRICT");
    sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    sslFactory.init();
    Assert.assertEquals("STRICT", sslFactory.getHostnameVerifier().toString());
    sslFactory.destroy();

    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "STRICT_IE6");
    sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    sslFactory.init();
    Assert.assertEquals("STRICT_IE6",
                        sslFactory.getHostnameVerifier().toString());
    sslFactory.destroy();
  }

  @Test(expected = GeneralSecurityException.class)
  public void invalidHostnameVerifier() throws Exception {
    Configuration conf = createConfiguration(false, true);
    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "foo");
    SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    try {
      sslFactory.init();
    } finally {
      sslFactory.destroy();
    }
  }

  @Test
  public void testConnectionConfigurator() throws Exception {
    Configuration conf = createConfiguration(false, true);
    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "STRICT_IE6");
    SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    try {
      sslFactory.init();
      HttpsURLConnection sslConn =
          (HttpsURLConnection) new URL("https://foo").openConnection();
      Assert.assertNotSame("STRICT_IE6",
                           sslConn.getHostnameVerifier().toString());
      sslFactory.configure(sslConn);
      Assert.assertEquals("STRICT_IE6",
                          sslConn.getHostnameVerifier().toString());
    } finally {
      sslFactory.destroy();
    }
  }

  @Test
  public void testServerDifferentPasswordAndKeyPassword() throws Exception {
    checkSSLFactoryInitWithPasswords(SSLFactory.Mode.SERVER, "password",
      "keyPassword", "password", "keyPassword");
  }

  @Test
  public void testServerKeyPasswordDefaultsToPassword() throws Exception {
    checkSSLFactoryInitWithPasswords(SSLFactory.Mode.SERVER, "password",
      "password", "password", null);
  }

  @Test
  public void testClientDifferentPasswordAndKeyPassword() throws Exception {
    checkSSLFactoryInitWithPasswords(SSLFactory.Mode.CLIENT, "password",
      "keyPassword", "password", "keyPassword");
  }

  @Test
  public void testClientKeyPasswordDefaultsToPassword() throws Exception {
    checkSSLFactoryInitWithPasswords(SSLFactory.Mode.CLIENT, "password",
      "password", "password", null);
  }

  @Test
  public void testServerCredProviderPasswords() throws Exception {
    KeyStoreTestUtil.provisionPasswordsToCredentialProvider();
    checkSSLFactoryInitWithPasswords(SSLFactory.Mode.SERVER,
        "storepass", "keypass", null, null, true);
  }

  /**
   * Checks that SSLFactory initialization is successful with the given
   * arguments.  This is a helper method for writing test cases that cover
   * different combinations of settings for the store password and key password.
   * It takes care of bootstrapping a keystore, a truststore, and SSL client or
   * server configuration.  Then, it initializes an SSLFactory.  If no exception
   * is thrown, then initialization was successful.
   *
   * @param mode SSLFactory.Mode mode to test
   * @param password String store password to set on keystore
   * @param keyPassword String key password to set on keystore
   * @param confPassword String store password to set in SSL config file, or null
   *   to avoid setting in SSL config file
   * @param confKeyPassword String key password to set in SSL config file, or
   *   null to avoid setting in SSL config file
   * @throws Exception for any error
   */
  private void checkSSLFactoryInitWithPasswords(SSLFactory.Mode mode,
      String password, String keyPassword, String confPassword,
      String confKeyPassword) throws Exception {
    checkSSLFactoryInitWithPasswords(mode, password, keyPassword,
        confPassword, confKeyPassword, false);
  }

 /**
   * Checks that SSLFactory initialization is successful with the given
   * arguments.  This is a helper method for writing test cases that cover
   * different combinations of settings for the store password and key password.
   * It takes care of bootstrapping a keystore, a truststore, and SSL client or
   * server configuration.  Then, it initializes an SSLFactory.  If no exception
   * is thrown, then initialization was successful.
   *
   * @param mode SSLFactory.Mode mode to test
   * @param password String store password to set on keystore
   * @param keyPassword String key password to set on keystore
   * @param confPassword String store password to set in SSL config file, or null
   *   to avoid setting in SSL config file
   * @param confKeyPassword String key password to set in SSL config file, or
   *   null to avoid setting in SSL config file
   * @param useCredProvider boolean to indicate whether passwords should be set
   * into the config or not. When set to true nulls are set and aliases are
   * expected to be resolved through credential provider API through the
   * Configuration.getPassword method
   * @throws Exception for any error
   */
  private void checkSSLFactoryInitWithPasswords(SSLFactory.Mode mode,
      String password, String keyPassword, String confPassword,
      String confKeyPassword, boolean useCredProvider) throws Exception {
    String keystore = new File(KEYSTORES_DIR, "keystore.jks").getAbsolutePath();
    String truststore = new File(KEYSTORES_DIR, "truststore.jks")
      .getAbsolutePath();
    String trustPassword = "trustP";

    // Create keys, certs, keystore, and truststore.
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    X509Certificate cert = KeyStoreTestUtil.generateCertificate("CN=Test",
      keyPair, 30, "SHA1withRSA");
    KeyStoreTestUtil.createKeyStore(keystore, password, keyPassword, "Test",
      keyPair.getPrivate(), cert);
    Map<String, X509Certificate> certs = Collections.singletonMap("server",
      cert);
    KeyStoreTestUtil.createTrustStore(truststore, trustPassword, certs);

    // Create SSL configuration file, for either server or client.
    final String sslConfFileName;
    final Configuration sslConf;

    // if the passwords are provisioned in a cred provider then don't set them
    // in the configuration properly - expect them to be resolved through the
    // provider
    if (useCredProvider) {
      confPassword = null;
      confKeyPassword = null;
    }
    if (mode == SSLFactory.Mode.SERVER) {
      sslConfFileName = "ssl-server.xml";
      sslConf = KeyStoreTestUtil.createServerSSLConfig(keystore, confPassword,
        confKeyPassword, truststore);
      if (useCredProvider) {
        File testDir = new File(System.getProperty("test.build.data",
            "target/test-dir"));
        final Path jksPath = new Path(testDir.toString(), "test.jks");
        final String ourUrl =
            JavaKeyStoreProvider.SCHEME_NAME + "://file" + jksPath.toUri();
        sslConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, ourUrl);
      }
    } else {
      sslConfFileName = "ssl-client.xml";
      sslConf = KeyStoreTestUtil.createClientSSLConfig(keystore, confPassword,
        confKeyPassword, truststore);
    }
    KeyStoreTestUtil.saveConfig(new File(sslConfsDir, sslConfFileName), sslConf);

    // Create the master configuration for use by the SSLFactory, which by
    // default refers to the ssl-server.xml or ssl-client.xml created above.
    Configuration conf = new Configuration();
    conf.setBoolean(SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY, true);

    // Try initializing an SSLFactory.
    SSLFactory sslFactory = new SSLFactory(mode, conf);
    try {
      sslFactory.init();
    } finally {
      sslFactory.destroy();
    }
  }

  @Test
  public void testNoClientCertsInitialization() throws Exception {
    Configuration conf = createConfiguration(false, true);
    conf.unset(SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY);
    SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    try {
      sslFactory.init();
    } finally {
      sslFactory.destroy();
    }
  }

  @Test
  public void testNoTrustStore() throws Exception {
    Configuration conf = createConfiguration(false, false);
    conf.unset(SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY);
    SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.SERVER, conf);
    try {
      sslFactory.init();
    } finally {
      sslFactory.destroy();
    }
  }

}
