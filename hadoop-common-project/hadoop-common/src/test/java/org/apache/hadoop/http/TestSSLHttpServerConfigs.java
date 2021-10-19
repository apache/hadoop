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

package org.apache.hadoop.http;

import java.util.function.Supplier;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.http.TestSSLHttpServer.EXCLUDED_CIPHERS;
import static org.apache.hadoop.http.TestSSLHttpServer.INCLUDED_PROTOCOLS;
import static org.apache.hadoop.http.TestSSLHttpServer.SSL_SERVER_KEYSTORE_PROP_PREFIX;
import static org.apache.hadoop.http.TestSSLHttpServer.SSL_SERVER_TRUSTSTORE_PROP_PREFIX;
import static org.apache.hadoop.security.ssl.KeyStoreTestUtil.CLIENT_KEY_STORE_PASSWORD_DEFAULT;
import static org.apache.hadoop.security.ssl.KeyStoreTestUtil.SERVER_KEY_STORE_PASSWORD_DEFAULT;
import static org.apache.hadoop.security.ssl.KeyStoreTestUtil.TRUST_STORE_PASSWORD_DEFAULT;

/**
 * Test suit for testing KeyStore and TrustStore password settings.
 */
public class TestSSLHttpServerConfigs {

  private static final String BASEDIR =
      GenericTestUtils.getTempPath(TestSSLHttpServer.class.getSimpleName());

  private static Configuration conf;
  private static Configuration sslConf;
  private static String keystoreDir;
  private static String sslConfDir;
  private static final String SERVER_PWD = SERVER_KEY_STORE_PASSWORD_DEFAULT;
  private static final String CLIENT_PWD = CLIENT_KEY_STORE_PASSWORD_DEFAULT;
  private static final String TRUST_STORE_PWD = TRUST_STORE_PASSWORD_DEFAULT;

  @Before
  public void start() throws Exception {
    TestSSLHttpServer.turnOnSSLDebugLogging();
    TestSSLHttpServer.storeHttpsCipherSuites();

    conf = new Configuration();
    conf.setInt(HttpServer2.HTTP_MAX_THREADS_KEY, 10);

    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    keystoreDir = new File(BASEDIR).getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestSSLHttpServer.class);
  }

  @After
  public void shutdown() throws Exception {
    FileUtil.fullyDelete(new File(BASEDIR));
    KeyStoreTestUtil.cleanupSSLConfig(keystoreDir, sslConfDir);
    TestSSLHttpServer.restoreHttpsCipherSuites();
    TestSSLHttpServer.restoreSSLDebugLogging();
  }

  /**
   * Setup KeyStore and TrustStore with given passwords.
   */
  private void setupKeyStores(String serverPassword,
      String clientPassword, String trustStorePassword) throws Exception {

    KeyStoreTestUtil.setupSSLConfig(keystoreDir, sslConfDir, conf, false, true,
        EXCLUDED_CIPHERS, serverPassword, clientPassword, trustStorePassword);

    sslConf = KeyStoreTestUtil.getSslConfig();
    sslConf.set(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, INCLUDED_PROTOCOLS);
    conf.set(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, INCLUDED_PROTOCOLS);
  }

  /**
   * Build HttpServer2 using the given passwords to access KeyStore/ TrustStore.
   */
  private HttpServer2 setupServer(String keyStoreKeyPassword,
      String keyStorePassword, String trustStorePassword) throws Exception {

    HttpServer2 server = new HttpServer2.Builder().setName("test")
        .addEndpoint(new URI("https://localhost")).setConf(conf)
        .keyPassword(keyStoreKeyPassword)
        .keyStore(sslConf.get(SSL_SERVER_KEYSTORE_PROP_PREFIX + ".location"),
            keyStorePassword,
            sslConf.get(SSL_SERVER_KEYSTORE_PROP_PREFIX + ".type", "jks"))
        .trustStore(
            sslConf.get(SSL_SERVER_TRUSTSTORE_PROP_PREFIX + ".location"),
            trustStorePassword,
            sslConf.get(SSL_SERVER_TRUSTSTORE_PROP_PREFIX + ".type", "jks"))
        .excludeCiphers(sslConf.get("ssl.server.exclude.cipher.list")).build();

    return server;
  }

  /**
   * Test if HttpServer2 start succeeds in validating KeyStore/ TrustStore
   * using the given passowords.
   */
  private void testServerStart(String keyStoreKeyPassword,
      String keyStorePassword, String trustStorePassword) throws Exception {
    HttpServer2 server = setupServer(keyStoreKeyPassword, keyStorePassword,
        trustStorePassword);
    try {
      server.start();

      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return server.isAlive();
        }
      }, 200, 100000);
    } finally {
      server.stop();
    }
  }

  @Test(timeout=120000)
  public void testServerSetup() throws Exception {
    setupKeyStores(SERVER_PWD, CLIENT_PWD, TRUST_STORE_PWD);
    testServerStart(SERVER_PWD, SERVER_PWD, TRUST_STORE_PWD);
  }

  @Test(timeout=120000)
  public void testServerSetupWithoutTrustPassword() throws Exception {
    setupKeyStores(SERVER_PWD, CLIENT_PWD, TRUST_STORE_PWD);
    testServerStart(SERVER_PWD, SERVER_PWD, null);
  }

  @Test(timeout=120000)
  public void testServerSetupWithoutKeyStorePassword() throws Exception {
    setupKeyStores(SERVER_PWD, CLIENT_PWD, TRUST_STORE_PWD);
    testServerStart(SERVER_PWD, null, null);
  }

  @Test(timeout=120000)
  public void testServerSetupWithoutKeyStoreKeyPassword() throws Exception {
    setupKeyStores(SERVER_PWD, CLIENT_PWD, TRUST_STORE_PWD);
    testServerStart(null, SERVER_PWD, null);
  }

  @Test(timeout=120000)
  public void testServerSetupWithNoKeyStorePassword() throws Exception {
    setupKeyStores(SERVER_PWD, CLIENT_PWD, TRUST_STORE_PWD);
    // Accessing KeyStore without either of KeyStore.KeyPassword or KeyStore
    // .password should fail.
    try {
      testServerStart(null, null, null);
      Assert.fail("Server should have failed to start without any " +
          "KeyStore password.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Problem starting http server",
          e);
    }
  }

  @Test(timeout=120000)
  public void testServerSetupWithWrongKeyStorePassword() throws Exception {
    setupKeyStores(SERVER_PWD, CLIENT_PWD, TRUST_STORE_PWD);

    // Accessing KeyStore with wrong keyStore password/ keyPassword should fail.
    try {
      testServerStart(SERVER_PWD, "wrongPassword", null);
      Assert.fail("Server should have failed to start with wrong " +
          "KeyStore password.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Keystore was tampered with, " +
          "or password was incorrect", e);
    }

    try {
      testServerStart("wrongPassword", SERVER_PWD, null);
      Assert.fail("Server should have failed to start with wrong " +
          "KeyStore password.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Problem starting http server",
          e);
      GenericTestUtils.assertExceptionContains("Cannot recover key",
          e.getCause());
    }
  }

  @Test(timeout=120000)
  public void testKeyStoreSetupWithoutTrustStorePassword() throws Exception {
    // Setup TrustStore without TrustStore password
    setupKeyStores(SERVER_PWD, CLIENT_PWD, "");

    // Accessing TrustStore without password (null password) should succeed
    testServerStart(SERVER_PWD, SERVER_PWD, null);

    // Accessing TrustStore with wrong password (even if password is not
    // set) should fail.
    try {
      testServerStart(SERVER_PWD, SERVER_PWD, "wrongPassword");
      Assert.fail("Server should have failed to start with wrong " +
          "TrustStore password.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Keystore was tampered with, " +
          "or password was incorrect", e);
    }
  }

  @Test(timeout=120000)
  public void testKeyStoreSetupWithoutKeyStorePassword() throws Exception {
    // Setup KeyStore without KeyStore password
    setupKeyStores(SERVER_PWD, "", TRUST_STORE_PWD);

    // Accessing KeyStore without password (null password) should succeed
    testServerStart(SERVER_PWD, null, TRUST_STORE_PWD);

    // Accessing KeyStore with wrong password (even if password is not
    // set) should fail.
    try {
      testServerStart(SERVER_PWD, "wrongPassword", TRUST_STORE_PWD);
      Assert.fail("Server should have failed to start with wrong " +
          "KeyStore password.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Keystore was tampered with, " +
          "or password was incorrect", e);
    }
  }

  @Test(timeout=120000)
  public void testKeyStoreSetupWithoutPassword() throws Exception {
    // Setup KeyStore without any password
    setupKeyStores("", "", "");

    // Accessing KeyStore with either one of KeyStore.Password or KeyStore
    // .KeyPassword as empty string should pass. If the password is null, it
    // is not set in SSLContextFactory while setting up the server.
    testServerStart("", null, null);
    testServerStart(null, "", null);

    try {
      testServerStart(null, null, null);
      Assert.fail("Server should have failed to start without " +
          "KeyStore password.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Problem starting http server",
          e);
      GenericTestUtils.assertExceptionContains("Password must not be null",
          e.getCause());
    }
  }
}
