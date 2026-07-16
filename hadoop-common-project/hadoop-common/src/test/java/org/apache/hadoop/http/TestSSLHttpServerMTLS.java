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

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.security.KeyPair;
import java.security.cert.X509Certificate;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLHandshakeException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.http.TestHttpServer.EchoServlet;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that HttpServer2 enforces mutual TLS (mTLS) when
 * ssl.server.need.client.auth is set in ssl-server.xml.
 * Verifies that a trusted client is accepted and an untrusted client is
 * rejected.
 */
public class TestSSLHttpServerMTLS extends HttpServerFunctionalTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSSLHttpServerMTLS.class);

  private static final String BASEDIR =
      GenericTestUtils.getTempPath(TestSSLHttpServerMTLS.class.getSimpleName());

  private static HttpServer2 server;
  private static SSLFactory clientSslFactory;
  private static KeyPair untrustedKeyPair;
  private static X509Certificate untrustedCert;

  @BeforeAll
  public static void setup() throws Exception {
    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();

    String keystoreDir = base.getAbsolutePath();
    String sslConfDir = KeyStoreTestUtil.getClasspathDir(
        TestSSLHttpServerMTLS.class);

    Configuration conf = new Configuration();
    conf.setInt(HttpServer2.HTTP_MAX_THREADS_KEY, 10);

    // useClientCert=true: generates clientKS.jks, adds client cert to trustKS
    KeyStoreTestUtil.setupSSLConfig(keystoreDir, sslConfDir, conf, true, true,
        "");

    // Enable mTLS on the server side (this is the key under test)
    conf.setBoolean(SSLFactory.SSL_SERVER_NEED_CLIENT_AUTH, true);

    Configuration sslConf = KeyStoreTestUtil.getSslConfig();

    // Trusted client SSL factory (uses clientKS.jks which is in trustKS)
    clientSslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    clientSslFactory.init();

    // Build the server with mTLS enforced
    server = new HttpServer2.Builder().setName("test")
        .addEndpoint(new URI("https://localhost"))
        .setConf(conf)
        .keyPassword(sslConf.get("ssl.server.keystore.keypassword"))
        .keyStore(sslConf.get("ssl.server.keystore.location"),
            sslConf.get("ssl.server.keystore.password"),
            sslConf.get("ssl.server.keystore.type", "jks"))
        .trustStore(sslConf.get("ssl.server.truststore.location"),
            sslConf.get("ssl.server.truststore.password"),
            sslConf.get("ssl.server.truststore.type", "jks"))
        .needsClientAuth(true)
        .build();
    server.addServlet("echo", "/echo", EchoServlet.class);
    server.start();

    baseUrl = new URL("https://"
        + NetUtils.getHostPortString(server.getConnectorAddress(0)));

    // Generate a keypair/cert that is NOT in the server's truststore
    untrustedKeyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    untrustedCert = KeyStoreTestUtil.generateCertificate(
        "CN=localhost, O=untrusted", untrustedKeyPair, 30, "SHA1withRSA");

    LOG.info("mTLS test server started: {}", baseUrl);
  }

  @AfterAll
  public static void cleanup() throws Exception {
    if (server != null) {
      server.stop();
    }
    if (clientSslFactory != null) {
      clientSslFactory.destroy();
    }
    FileUtil.fullyDelete(new File(BASEDIR));
    KeyStoreTestUtil.cleanupSSLConfig(
        new File(BASEDIR).getAbsolutePath(),
        KeyStoreTestUtil.getClasspathDir(TestSSLHttpServerMTLS.class));
  }

  @Test
  public void testTrustedClientCanConnect() throws Exception {
    URL url = new URL(baseUrl, "/echo?a=b");
    HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
    conn.setSSLSocketFactory(clientSslFactory.createSSLSocketFactory());
    conn.setHostnameVerifier((h, s) -> true);
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    conn.disconnect();
  }

  @Test
  public void testUntrustedClientIsRejected() throws Exception {
    URL url = new URL(baseUrl, "/echo?a=b");
    HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
    // presents untrustedCert; server cert is trusted via no-op TrustManager
    KeyStoreTestUtil.setAllowAllSSL(conn, untrustedCert, untrustedKeyPair);
    assertThrows(SSLHandshakeException.class, () -> conn.getInputStream());
  }
}
