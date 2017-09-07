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

package org.apache.hadoop.yarn.server.webproxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.HttpCookie;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the WebAppProxyServlet and WebAppProxy. For back end use simple web
 * server.
 */
public class TestWebAppProxyServletWithSSL {
  private static final String BASEDIR = System.getProperty("test.build.dir",
      "target/test-dir") + "/" + TestWebAppProxyServletWithSSL.class.getSimpleName();
  private static final Logger LOG = LoggerFactory.getLogger(
      TestWebAppProxyServletWithSSL.class);

  private static String keystoresDir;
  private static HttpServer2 server;
  private static String sslConfDir;
  private static int originalPort = 0;
  private static final Configuration configuration = new Configuration();

  /**
   * Simple https server. Server should send answer with status 200
   */
  @BeforeClass
  public static void start() throws Exception {
    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    keystoresDir = new File(BASEDIR).getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestWebAppProxyServletWithSSL.class);

    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, configuration, false);
    Configuration sslConf = new Configuration(false);
    sslConf.addResource("ssl-server.xml");
    sslConf.addResource("ssl-client.xml");

    server = new HttpServer2.Builder()
        .setName("test")
        .addEndpoint(new URI("https://localhost"))
        .setConf(configuration)
        .keyPassword(sslConf.get("ssl.server.keystore.keypassword"))
        .keyStore(sslConf.get("ssl.server.keystore.location"),
                sslConf.get("ssl.server.keystore.password"),
                sslConf.get("ssl.server.keystore.type", "jks"))
        .trustStore(sslConf.get("ssl.server.truststore.location"),
                sslConf.get("ssl.server.truststore.password"),
                sslConf.get("ssl.server.truststore.type", "jks")).build();
    server.addServlet("foobar" , "/foo/bar", TestWebAppProxyServlet.TestServlet.class);
    server.start();
    URL baseUrl = new URL("https://"
            + NetUtils.getHostPortString(server.getConnectorAddress(0)));
    originalPort = baseUrl.getPort();
    LOG.info("Running embedded HTTPS servlet container at: " + baseUrl);
  }

  @Test(timeout=500000)
  public void testWebAppProxyServlet() throws Exception {

    configuration.set(YarnConfiguration.PROXY_ADDRESS, "localhost:9090");
    // overriding num of web server threads, see HttpServer.HTTP_MAXTHREADS
    configuration.setInt("hadoop.http.max.threads", 5);
    WebAppProxyServerForTest proxy = new WebAppProxyServerForTest(originalPort, "https");
    proxy.init(configuration);
    proxy.start();

    int proxyPort = proxy.getProxyPort();

    try {
      // set true Application ID in url
      URL url = new URL("http://localhost:" + proxyPort + "/proxy/application_00_0");
      HttpURLConnection proxyConn = (HttpURLConnection) url.openConnection();
      // set cookie
      proxyConn.setRequestProperty("Cookie", "checked_application_0_0000=true");
      proxyConn.connect();
      assertEquals(HttpURLConnection.HTTP_OK, proxyConn.getResponseCode());
      assertTrue(WebAppProxyServerForTest.isResponseCookiePresent(
          proxyConn, "checked_application_0_0000", "true"));
    } finally {
      proxy.close();
    }
  }

 @AfterClass
  public static void stop() {
    try {
      server.stop();
    } catch (Exception e) {
    }
  }

}
