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
package org.apache.hadoop.yarn.server.webapp;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.http.TestHttpServer.EchoServlet;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class TestWebServiceClient {

  private static final String BASEDIR = System.getProperty("test.build.dir",
      "target/test-dir") + "/" + TestWebServiceClient.class.getSimpleName();
  static final String SSL_SERVER_KEYSTORE_PROP_PREFIX = "ssl.server.keystore";
  static final String SSL_SERVER_TRUSTSTORE_PROP_PREFIX =
      "ssl.server.truststore";
  static final String SERVLET_NAME_ECHO = "echo";
  static final String SERVLET_PATH_ECHO = "/" + SERVLET_NAME_ECHO;

  @Test
  public void testGetWebServiceClient() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.YARN_HTTP_POLICY_KEY, "HTTPS_ONLY");
    WebServiceClient.initialize(conf);
    WebServiceClient client = WebServiceClient.getWebServiceClient();
    Assert.assertNotNull(client.getSSLFactory());
    WebServiceClient.destroy();
  }

  @Test
  public void testCreateClient() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.YARN_HTTP_POLICY_KEY, "HTTPS_ONLY");
    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    String keystoresDir = new File(BASEDIR).getAbsolutePath();
    String sslConfDir = KeyStoreTestUtil
        .getClasspathDir(TestWebServiceClient.class);

    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false,
        true);

    Configuration sslConf = KeyStoreTestUtil.getSslConfig();
    sslConf.set(YarnConfiguration.YARN_HTTP_POLICY_KEY, "HTTPS_ONLY");

    HttpServer2 server = new HttpServer2.Builder().setName("test")
        .addEndpoint(new URI("https://localhost")).setConf(sslConf)
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
    server.start();

    final URL baseUrl = new URL(
        "https://" + NetUtils.getHostPortString(server.getConnectorAddress(0)));
    URL u = new URL(baseUrl, SERVLET_PATH_ECHO + "?a=b&c=d");
    WebServiceClient.initialize(sslConf);
    WebServiceClient client = WebServiceClient.getWebServiceClient();
    HttpURLConnection conn = client.getHttpURLConnectionFactory()
        .getHttpURLConnection(u);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    WebServiceClient.destroy();
    server.stop();
    FileUtil.fullyDelete(new File(BASEDIR));
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }

}