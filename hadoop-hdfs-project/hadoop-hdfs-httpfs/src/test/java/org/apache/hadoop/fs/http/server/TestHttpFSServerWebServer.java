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
package org.apache.hadoop.fs.http.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.MessageFormat;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.HadoopUsersConfTestHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Test {@link HttpFSServerWebServer}.
 */
public class TestHttpFSServerWebServer {

  @Rule
  public Timeout timeout = new Timeout(30000);
  private HttpFSServerWebServer webServer;

  @BeforeClass
  public static void beforeClass() throws Exception {
    File homeDir = GenericTestUtils.getTestDir();
    File confDir = new File(homeDir, "etc/hadoop");
    File logsDir = new File(homeDir, "logs");
    File tempDir = new File(homeDir, "temp");
    confDir.mkdirs();
    logsDir.mkdirs();
    tempDir.mkdirs();
    System.setProperty("hadoop.home.dir", homeDir.getAbsolutePath());
    System.setProperty("hadoop.log.dir", logsDir.getAbsolutePath());
    System.setProperty("httpfs.home.dir", homeDir.getAbsolutePath());
    System.setProperty("httpfs.log.dir", logsDir.getAbsolutePath());
    System.setProperty("httpfs.config.dir", confDir.getAbsolutePath());
    FileUtils.writeStringToFile(new File(confDir, "httpfs-signature.secret"),
        "foo", Charset.forName("UTF-8"));
  }

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.set(HttpFSServerWebServer.HTTP_HOSTNAME_KEY, "localhost");
    conf.setInt(HttpFSServerWebServer.HTTP_PORT_KEY, 0);
    conf.set(AuthenticationFilter.SIGNATURE_SECRET_FILE,
        "httpfs-signature.secret");
    Configuration sslConf = new Configuration();
    webServer = new HttpFSServerWebServer(conf, sslConf);
  }

  @Test
  public void testStartStop() throws Exception {
    webServer.start();
    String user = HadoopUsersConfTestHelper.getHadoopUsers()[0];
    URL url = new URL(webServer.getUrl(), MessageFormat.format(
        "/webhdfs/v1/?user.name={0}&op=liststatus", user));
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(conn.getInputStream()));
    reader.readLine();
    reader.close();
    webServer.stop();
  }

  @Test
  public void testJustStop() throws Exception {
    webServer.stop();
  }

  @Test
  public void testDoubleStop() throws Exception {
    webServer.start();
    webServer.stop();
    webServer.stop();
  }

  @Test
  public void testDoubleStart() throws Exception {
    webServer.start();
    webServer.start();
    webServer.stop();
  }

}
