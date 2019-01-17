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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.GeneralSecurityException;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.http.TestHttpServer.EchoServlet;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This testcase issues SSL certificates configures the HttpServer to serve
 * HTTPS using the created certificates and calls an echo servlet using the
 * corresponding HTTPS URL.
 */
public class TestSSLHttpServer extends HttpServerFunctionalTest {

  private static final String BASEDIR =
      GenericTestUtils.getTempPath(TestSSLHttpServer.class.getSimpleName());

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSSLHttpServer.class);
  private static final String HTTPS_CIPHER_SUITES_KEY = "https.cipherSuites";
  private static final String JAVAX_NET_DEBUG_KEY = "javax.net.debug";
  private static final String SSL_SERVER_KEYSTORE_PROP_PREFIX = "ssl.server" +
          ".keystore";
  private static final String SSL_SERVER_TRUSTSTORE_PROP_PREFIX = "ssl.server" +
          ".truststore";

  private static final String SERVLET_NAME_LONGHEADER = "longheader";
  private static final String SERVLET_PATH_LONGHEADER =
      "/" + SERVLET_NAME_LONGHEADER;
  private static final String SERVLET_NAME_ECHO = "echo";
  private static final String SERVLET_PATH_ECHO = "/" + SERVLET_NAME_ECHO;

  private static HttpServer2 server;
  private static String keystoreDir;
  private static String sslConfDir;
  private static SSLFactory clientSslFactory;
  private static String cipherSuitesPropertyValue;
  private static String sslDebugPropertyValue;
  private static final String EXCLUDED_CIPHERS =
          "TLS_ECDHE_RSA_WITH_RC4_128_SHA,"
      + "SSL_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA, \n"
      + "SSL_RSA_WITH_DES_CBC_SHA,"
      + "SSL_DHE_RSA_WITH_DES_CBC_SHA,  "
      + "SSL_RSA_EXPORT_WITH_RC4_40_MD5,\t \n"
      + "SSL_RSA_EXPORT_WITH_DES40_CBC_SHA,"
      + "SSL_RSA_WITH_RC4_128_MD5 \t";
  private static final String ONE_ENABLED_CIPHERS = EXCLUDED_CIPHERS
      + ",TLS_RSA_WITH_AES_128_CBC_SHA";
  private static final String EXCLUSIVE_ENABLED_CIPHERS
      = "\tTLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA, \n"
      + "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,"
      + "TLS_RSA_WITH_AES_128_CBC_SHA,"
      + "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA,  "
      + "TLS_ECDH_RSA_WITH_AES_128_CBC_SHA,"
      + "TLS_DHE_RSA_WITH_AES_128_CBC_SHA,\t\n "
      + "TLS_DHE_DSS_WITH_AES_128_CBC_SHA";

  @BeforeClass
  public static void setup() throws Exception {
    turnOnSSLDebugLogging();
    storeHttpsCipherSuites();

    Configuration conf = new Configuration();
    conf.setInt(HttpServer2.HTTP_MAX_THREADS_KEY, 10);

    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    keystoreDir = new File(BASEDIR).getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestSSLHttpServer.class);

    KeyStoreTestUtil.setupSSLConfig(keystoreDir, sslConfDir, conf, false, true,
        EXCLUDED_CIPHERS);

    Configuration sslConf = KeyStoreTestUtil.getSslConfig();

    clientSslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, sslConf);
    clientSslFactory.init();

    setupServer(conf, sslConf);
    baseUrl = new URL("https://"
        + NetUtils.getHostPortString(server.getConnectorAddress(0)));
    LOG.info("HTTP server started: " + baseUrl);
  }

  private static void setupServer(Configuration conf, Configuration sslConf)
      throws IOException, URISyntaxException {
    server = new HttpServer2.Builder().setName("test")
        .addEndpoint(new URI("https://localhost")).setConf(conf)
        .keyPassword(
            sslConf.get(SSL_SERVER_KEYSTORE_PROP_PREFIX + ".keypassword"))
        .keyStore(sslConf.get(SSL_SERVER_KEYSTORE_PROP_PREFIX + ".location"),
            sslConf.get(SSL_SERVER_KEYSTORE_PROP_PREFIX + ".password"),
            sslConf.get(SSL_SERVER_KEYSTORE_PROP_PREFIX + ".type", "jks"))
        .trustStore(
            sslConf.get(SSL_SERVER_TRUSTSTORE_PROP_PREFIX + ".location"),
            sslConf.get(SSL_SERVER_TRUSTSTORE_PROP_PREFIX + ".password"),
            sslConf.get(SSL_SERVER_TRUSTSTORE_PROP_PREFIX + ".type", "jks"))
        .excludeCiphers(sslConf.get("ssl.server.exclude.cipher.list")).build();
    server.addServlet(SERVLET_NAME_ECHO, SERVLET_PATH_ECHO, EchoServlet.class);
    server.addServlet(SERVLET_NAME_LONGHEADER, SERVLET_PATH_LONGHEADER,
        LongHeaderServlet.class);
    server.start();
  }

  @AfterClass
  public static void cleanup() throws Exception {
    server.stop();
    FileUtil.fullyDelete(new File(BASEDIR));
    KeyStoreTestUtil.cleanupSSLConfig(keystoreDir, sslConfDir);
    clientSslFactory.destroy();
    restoreHttpsCipherSuites();
    restoreSSLDebugLogging();
  }

  /**
   * Stores the JVM property value of https.cipherSuites and sets its
   * value to an empty string.
   * This ensures that the value https.cipherSuites does
   * not affect the result of tests.
   */
  private static void storeHttpsCipherSuites() {
    String cipherSuites = System.getProperty(HTTPS_CIPHER_SUITES_KEY);
    if (cipherSuites != null) {
      LOG.info(
          "Found value for property {}: {}", HTTPS_CIPHER_SUITES_KEY,
          cipherSuites);
      cipherSuitesPropertyValue = cipherSuites;
    }
    System.clearProperty(HTTPS_CIPHER_SUITES_KEY);
  }

  private static void restoreHttpsCipherSuites() {
    if (cipherSuitesPropertyValue != null) {
      LOG.info("Restoring property {} to value: {}", HTTPS_CIPHER_SUITES_KEY,
          cipherSuitesPropertyValue);
      System.setProperty(HTTPS_CIPHER_SUITES_KEY, cipherSuitesPropertyValue);
      cipherSuitesPropertyValue = null;
    }
  }

  private static void turnOnSSLDebugLogging() {
    String sslDebug = System.getProperty(JAVAX_NET_DEBUG_KEY);
    if (sslDebug != null) {
      sslDebugPropertyValue = sslDebug;
    }
    System.setProperty(JAVAX_NET_DEBUG_KEY, "all");
  }

  private static void restoreSSLDebugLogging() {
    if (sslDebugPropertyValue != null) {
      System.setProperty(JAVAX_NET_DEBUG_KEY, sslDebugPropertyValue);
      sslDebugPropertyValue = null;
    } else {
      System.clearProperty(JAVAX_NET_DEBUG_KEY);
    }
  }

  private HttpsURLConnection getConnectionWithSSLSocketFactory(URL url,
          String ciphers) throws IOException, GeneralSecurityException {
    HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
    SSLSocketFactory sslSocketFactory = clientSslFactory
            .createSSLSocketFactory();
    LOG.info("Creating " +
        PreferredCipherSSLSocketFactory.class.getCanonicalName() +
        " with ciphers: " + ciphers);
    PreferredCipherSSLSocketFactory cipherSSLSocketFactory
            = new PreferredCipherSSLSocketFactory(sslSocketFactory,
            StringUtils.getTrimmedStrings(ciphers));
    conn.setSSLSocketFactory(cipherSSLSocketFactory);
    return conn;
  }

  @Test
  public void testEcho() throws Exception {
    assertEquals("a:b\nc:d\n",
        readFromURL(new URL(baseUrl, SERVLET_PATH_ECHO + "?a=b&c=d")));
    assertEquals("a:b\nc&lt;:d\ne:&gt;\n",
        readFromURL(new URL(baseUrl, SERVLET_PATH_ECHO + "?a=b&c<=d&e=>")));
  }

  /**
   * Test that verifies headers can be up to 64K long. The test adds a 63K
   * header leaving 1K for other headers. This is because the header buffer
   * setting is for ALL headers, names and values included.
   */
  @Test
  public void testLongHeader() throws Exception {
    URL url = new URL(baseUrl, SERVLET_PATH_LONGHEADER);
    HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
    conn.setSSLSocketFactory(clientSslFactory.createSSLSocketFactory());
    testLongHeader(conn);
  }

  private static String readFromURL(URL url) throws Exception {
    HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
    conn.setSSLSocketFactory(clientSslFactory.createSSLSocketFactory());
    return readFromConnection(conn);
  }

  private static String readFromConnection(HttpsURLConnection conn)
      throws IOException {
    InputStream in = conn.getInputStream();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copyBytes(in, out, 1024);
    return out.toString();
  }

  /**
   * Test that verifies that excluded ciphers (SSL_RSA_WITH_RC4_128_SHA,
   * TLS_ECDH_ECDSA_WITH_RC4_128_SHA,TLS_ECDH_RSA_WITH_RC4_128_SHA,
   * TLS_ECDHE_ECDSA_WITH_RC4_128_SHA,TLS_ECDHE_RSA_WITH_RC4_128_SHA) are not
   * available for negotiation during SSL connection.
   */
  @Test
  public void testExcludedCiphers() throws Exception {
    URL url = new URL(baseUrl, SERVLET_PATH_ECHO + "?a=b&c=d");
    HttpsURLConnection conn = getConnectionWithSSLSocketFactory(url,
        EXCLUDED_CIPHERS);
    assertFalse("excludedCipher list is empty", EXCLUDED_CIPHERS.isEmpty());
    try {
      readFromConnection(conn);
      fail("No Ciphers in common, SSLHandshake must fail.");
    } catch (SSLHandshakeException ex) {
      LOG.info("No Ciphers in common, expected successful test result.", ex);
    }
  }

  /** Test that verified that additionally included cipher
   * TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA is only available cipher for working
   * TLS connection from client to server disabled for all other common ciphers.
   */
  @Test
  public void testOneEnabledCiphers() throws Exception {
    testEnabledCiphers(ONE_ENABLED_CIPHERS);
  }

  /** Test verifies that mutually exclusive server's disabled cipher suites and
   * client's enabled cipher suites can successfully establish TLS connection.
   */
  @Test
  public void testExclusiveEnabledCiphers() throws Exception {
    testEnabledCiphers(EXCLUSIVE_ENABLED_CIPHERS);
  }

  private void testEnabledCiphers(String ciphers) throws
      IOException, GeneralSecurityException {
    URL url = new URL(baseUrl, SERVLET_PATH_ECHO + "?a=b&c=d");
    HttpsURLConnection conn = getConnectionWithSSLSocketFactory(url, ciphers);
    assertFalse("excludedCipher list is empty", ciphers.isEmpty());
    String out = readFromConnection(conn);
    assertEquals(out, "a:b\nc:d\n");
    LOG.info("At least one additional enabled cipher than excluded ciphers,"
        + " expected successful test result.");
  }

  private class PreferredCipherSSLSocketFactory extends SSLSocketFactory {
    private final SSLSocketFactory delegateSocketFactory;
    private final String[] enabledCipherSuites;

    PreferredCipherSSLSocketFactory(SSLSocketFactory sslSocketFactory,
        String[] pEnabledCipherSuites) {
      delegateSocketFactory = sslSocketFactory;
      if (null != pEnabledCipherSuites && pEnabledCipherSuites.length > 0) {
        enabledCipherSuites = pEnabledCipherSuites;
      } else {
        enabledCipherSuites = null;
      }
    }

    @Override
    public String[] getDefaultCipherSuites() {
      return delegateSocketFactory.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return delegateSocketFactory.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket(Socket socket, String string, int i, boolean bln)
        throws IOException {
      SSLSocket sslSocket = (SSLSocket) delegateSocketFactory.createSocket(
          socket, string, i, bln);
      setEnabledCipherSuites(sslSocket);
      return sslSocket;
    }

    @Override
    public Socket createSocket(String string, int i) throws IOException {
      SSLSocket sslSocket = (SSLSocket) delegateSocketFactory.createSocket(
          string, i);
      setEnabledCipherSuites(sslSocket);
      return sslSocket;
    }

    @Override
    public Socket createSocket(String string, int i, InetAddress ia, int i1)
        throws IOException {
      SSLSocket sslSocket = (SSLSocket) delegateSocketFactory.createSocket(
          string, i, ia, i1);
      setEnabledCipherSuites(sslSocket);
      return sslSocket;
    }

    @Override
    public Socket createSocket(InetAddress ia, int i) throws IOException {
      SSLSocket sslSocket = (SSLSocket) delegateSocketFactory.createSocket(ia,
          i);
      setEnabledCipherSuites(sslSocket);
      return sslSocket;
    }

    @Override
    public Socket createSocket(InetAddress ia, int i, InetAddress ia1, int i1)
        throws IOException {
      SSLSocket sslSocket = (SSLSocket) delegateSocketFactory.createSocket(ia,
          i, ia1, i1);
      setEnabledCipherSuites(sslSocket);
      return sslSocket;
    }

    private void setEnabledCipherSuites(SSLSocket sslSocket) {
      if (null != enabledCipherSuites) {
        sslSocket.setEnabledCipherSuites(enabledCipherSuites);
      }
    }
  }
}
