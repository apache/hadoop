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
import java.net.URL;
import java.net.UnknownHostException;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This testcase issues SSL certificates configures the HttpServer to serve
 * HTTPS using the created certficates and calls an echo servlet using the
 * corresponding HTTPS URL.
 */
public class TestSSLHttpServer extends HttpServerFunctionalTest {

  private static final String BASEDIR =
      GenericTestUtils.getTempPath(TestSSLHttpServer.class.getSimpleName());

  private static final Log LOG = LogFactory.getLog(TestSSLHttpServer.class);
  private static Configuration conf;
  private static HttpServer2 server;
  private static String keystoresDir;
  private static String sslConfDir;
  private static SSLFactory clientSslFactory;
  private static final String excludeCiphers = "TLS_ECDHE_RSA_WITH_RC4_128_SHA,"
      + "SSL_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA,"
      + "SSL_RSA_WITH_DES_CBC_SHA,"
      + "SSL_DHE_RSA_WITH_DES_CBC_SHA,"
      + "SSL_RSA_EXPORT_WITH_RC4_40_MD5,"
      + "SSL_RSA_EXPORT_WITH_DES40_CBC_SHA,"
      + "SSL_RSA_WITH_RC4_128_MD5";
  private static final String oneEnabledCiphers = excludeCiphers
      + ",TLS_RSA_WITH_AES_128_CBC_SHA";
  private static final String exclusiveEnabledCiphers
      = "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,"
      + "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,"
      + "TLS_RSA_WITH_AES_128_CBC_SHA,"
      + "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA,"
      + "TLS_ECDH_RSA_WITH_AES_128_CBC_SHA,"
      + "TLS_DHE_RSA_WITH_AES_128_CBC_SHA,"
      + "TLS_DHE_DSS_WITH_AES_128_CBC_SHA";

  @BeforeClass
  public static void setup() throws Exception {
    conf = new Configuration();
    conf.setInt(HttpServer2.HTTP_MAX_THREADS_KEY, 10);

    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    keystoresDir = new File(BASEDIR).getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestSSLHttpServer.class);

    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false, true,
        excludeCiphers);

    Configuration sslConf = KeyStoreTestUtil.getSslConfig();

    clientSslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, sslConf);
    clientSslFactory.init();

    server = new HttpServer2.Builder()
        .setName("test")
        .addEndpoint(new URI("https://localhost"))
        .setConf(conf)
        .keyPassword(sslConf.get("ssl.server.keystore.keypassword"))
        .keyStore(sslConf.get("ssl.server.keystore.location"),
            sslConf.get("ssl.server.keystore.password"),
            sslConf.get("ssl.server.keystore.type", "jks"))
        .trustStore(sslConf.get("ssl.server.truststore.location"),
            sslConf.get("ssl.server.truststore.password"),
            sslConf.get("ssl.server.truststore.type", "jks"))
        .excludeCiphers(
            sslConf.get("ssl.server.exclude.cipher.list")).build();
    server.addServlet("echo", "/echo", TestHttpServer.EchoServlet.class);
    server.addServlet("longheader", "/longheader", LongHeaderServlet.class);
    server.start();
    baseUrl = new URL("https://"
        + NetUtils.getHostPortString(server.getConnectorAddress(0)));
    LOG.info("HTTP server started: " + baseUrl);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    server.stop();
    FileUtil.fullyDelete(new File(BASEDIR));
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
    clientSslFactory.destroy();
  }

  @Test
  public void testEcho() throws Exception {
    assertEquals("a:b\nc:d\n", readOut(new URL(baseUrl, "/echo?a=b&c=d")));
    assertEquals("a:b\nc&lt;:d\ne:&gt;\n", readOut(new URL(baseUrl,
        "/echo?a=b&c<=d&e=>")));
  }

  /**
   * Test that verifies headers can be up to 64K long. The test adds a 63K
   * header leaving 1K for other headers. This is because the header buffer
   * setting is for ALL headers, names and values included.
   */
  @Test
  public void testLongHeader() throws Exception {
    URL url = new URL(baseUrl, "/longheader");
    HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
    conn.setSSLSocketFactory(clientSslFactory.createSSLSocketFactory());
    testLongHeader(conn);
  }

  private static String readOut(URL url) throws Exception {
    HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
    conn.setSSLSocketFactory(clientSslFactory.createSSLSocketFactory());
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
    URL url = new URL(baseUrl, "/echo?a=b&c=d");
    HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
    SSLSocketFactory sslSocketF = clientSslFactory.createSSLSocketFactory();
    PrefferedCipherSSLSocketFactory testPreferredCipherSSLSocketF
        = new PrefferedCipherSSLSocketFactory(sslSocketF,
            excludeCiphers.split(","));
    conn.setSSLSocketFactory(testPreferredCipherSSLSocketF);
    assertFalse("excludedCipher list is empty", excludeCiphers.isEmpty());
    try {
      InputStream in = conn.getInputStream();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copyBytes(in, out, 1024);
      fail("No Ciphers in common, SSLHandshake must fail.");
    } catch (SSLHandshakeException ex) {
      LOG.info("No Ciphers in common, expected succesful test result.", ex);
    }
  }

  /** Test that verified that additionally included cipher
   * TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA is only available cipher for working
   * TLS connection from client to server disabled for all other common ciphers.
   */
  @Test
  public void testOneEnabledCiphers() throws Exception {
    URL url = new URL(baseUrl, "/echo?a=b&c=d");
    HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
    SSLSocketFactory sslSocketF = clientSslFactory.createSSLSocketFactory();
    PrefferedCipherSSLSocketFactory testPreferredCipherSSLSocketF
        = new PrefferedCipherSSLSocketFactory(sslSocketF,
            oneEnabledCiphers.split(","));
    conn.setSSLSocketFactory(testPreferredCipherSSLSocketF);
    assertFalse("excludedCipher list is empty", oneEnabledCiphers.isEmpty());
    try {
      InputStream in = conn.getInputStream();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copyBytes(in, out, 1024);
      assertEquals(out.toString(), "a:b\nc:d\n");
      LOG.info("Atleast one additional enabled cipher than excluded ciphers,"
          + " expected successful test result.");
    } catch (SSLHandshakeException ex) {
      fail("Atleast one additional cipher available for successful handshake."
          + " Unexpected test failure: " + ex);
    }
  }

  /** Test verifies that mutually exclusive server's disabled cipher suites and
   * client's enabled cipher suites can successfully establish TLS connection.
   */
  @Test
  public void testExclusiveEnabledCiphers() throws Exception {
    URL url = new URL(baseUrl, "/echo?a=b&c=d");
    HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
    SSLSocketFactory sslSocketF = clientSslFactory.createSSLSocketFactory();
    PrefferedCipherSSLSocketFactory testPreferredCipherSSLSocketF
        = new PrefferedCipherSSLSocketFactory(sslSocketF,
            exclusiveEnabledCiphers.split(","));
    conn.setSSLSocketFactory(testPreferredCipherSSLSocketF);
    assertFalse("excludedCipher list is empty",
        exclusiveEnabledCiphers.isEmpty());
    try {
      InputStream in = conn.getInputStream();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copyBytes(in, out, 1024);
      assertEquals(out.toString(), "a:b\nc:d\n");
      LOG.info("Atleast one additional enabled cipher than excluded ciphers,"
          + " expected successful test result.");
    } catch (SSLHandshakeException ex) {
      fail("Atleast one additional cipher available for successful handshake."
          + " Unexpected test failure: " + ex);
    }
  }

  private class PrefferedCipherSSLSocketFactory extends SSLSocketFactory {
    private final SSLSocketFactory delegateSocketFactory;
    private final String[] enabledCipherSuites;

    public PrefferedCipherSSLSocketFactory(SSLSocketFactory sslSocketFactory,
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
      if (null != enabledCipherSuites) {
        sslSocket.setEnabledCipherSuites(enabledCipherSuites);
      }
      return sslSocket;
    }

    @Override
    public Socket createSocket(String string, int i) throws IOException,
        UnknownHostException {
      SSLSocket sslSocket = (SSLSocket) delegateSocketFactory.createSocket(
          string, i);
      if (null != enabledCipherSuites) {
        sslSocket.setEnabledCipherSuites(enabledCipherSuites);
      }
      return sslSocket;
    }

    @Override
    public Socket createSocket(String string, int i, InetAddress ia, int i1)
        throws IOException, UnknownHostException {
      SSLSocket sslSocket = (SSLSocket) delegateSocketFactory.createSocket(
          string, i, ia, i1);
      if (null != enabledCipherSuites) {
        sslSocket.setEnabledCipherSuites(enabledCipherSuites);
      }
      return sslSocket;
    }

    @Override
    public Socket createSocket(InetAddress ia, int i) throws IOException {
      SSLSocket sslSocket = (SSLSocket) delegateSocketFactory.createSocket(ia,
          i);
      if (null != enabledCipherSuites) {
        sslSocket.setEnabledCipherSuites(enabledCipherSuites);
      }
      return sslSocket;
    }

    @Override
    public Socket createSocket(InetAddress ia, int i, InetAddress ia1, int i1)
        throws IOException {
      SSLSocket sslSocket = (SSLSocket) delegateSocketFactory.createSocket(ia,
          i, ia1, i1);
      if (null != enabledCipherSuites) {
        sslSocket.setEnabledCipherSuites(enabledCipherSuites);
      }
      return sslSocket;
    }
  }
}
