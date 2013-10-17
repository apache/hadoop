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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.HttpsURLConnection;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.Writer;
import java.net.URL;

/**
 * This testcase issues SSL certificates configures the HttpServer to serve
 * HTTPS using the created certficates and calls an echo servlet using the
 * corresponding HTTPS URL.
 */
public class TestSSLHttpServer extends HttpServerFunctionalTest {
  private static final String CONFIG_SITE_XML = "sslhttpserver-site.xml";

  private static final String BASEDIR =
      System.getProperty("test.build.dir", "target/test-dir") + "/" +
      TestSSLHttpServer.class.getSimpleName();

  static final Log LOG = LogFactory.getLog(TestSSLHttpServer.class);
  private static HttpServer server;
  private static URL baseUrl;


  @Before
  public void setup() throws Exception {
    HttpConfig.setPolicy(HttpConfig.Policy.HTTPS_ONLY);
    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    String classpathDir =
        KeyStoreTestUtil.getClasspathDir(TestSSLHttpServer.class);
    Configuration conf = new Configuration();
    String keystoresDir = new File(BASEDIR).getAbsolutePath();
    String sslConfsDir =
        KeyStoreTestUtil.getClasspathDir(TestSSLHttpServer.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfsDir, conf, false);
    conf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SSL_ENABLED_KEY, true);

    //we do this trick because the MR AppMaster is started in another VM and
    //the HttpServer configuration is not loaded from the job.xml but from the
    //site.xml files in the classpath
    Writer writer = new FileWriter(new File(classpathDir, CONFIG_SITE_XML));
    conf.writeXml(writer);
    writer.close();

    conf.setInt(HttpServer.HTTP_MAX_THREADS, 10);
    conf.addResource(CONFIG_SITE_XML);
    server = createServer("test", conf);
    server.addServlet("echo", "/echo", TestHttpServer.EchoServlet.class);
    server.start();
    baseUrl = new URL("https://localhost:" + server.getPort() + "/");
    LOG.info("HTTP server started: "+ baseUrl);
  }

  @After
  public void cleanup() throws Exception {
    server.stop();
    String classpathDir =
        KeyStoreTestUtil.getClasspathDir(TestSSLHttpServer.class);
    new File(classpathDir, CONFIG_SITE_XML).delete();
    HttpConfig.setPolicy(HttpConfig.Policy.HTTP_ONLY);
  }
  

  @Test
  public void testEcho() throws Exception {
    assertEquals("a:b\nc:d\n", 
        readOut(new URL(baseUrl, "/echo?a=b&c=d")));
    assertEquals("a:b\nc&lt;:d\ne:&gt;\n", 
        readOut(new URL(baseUrl, "/echo?a=b&c<=d&e=>")));
  }

  private static String readOut(URL url) throws Exception {
    StringBuilder out = new StringBuilder();
    HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
    Configuration conf = new Configuration();
    conf.addResource(CONFIG_SITE_XML);
    SSLFactory sslf = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    sslf.init();
    conn.setSSLSocketFactory(sslf.createSSLSocketFactory());
    InputStream in = conn.getInputStream();
    byte[] buffer = new byte[64 * 1024];
    int len = in.read(buffer);
    while (len > 0) {
      out.append(new String(buffer, 0, len));
      len = in.read(buffer);
    }
    return out.toString();
  }

}
