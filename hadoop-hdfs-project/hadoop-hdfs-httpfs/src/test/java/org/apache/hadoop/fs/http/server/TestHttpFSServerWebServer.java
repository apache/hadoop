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

import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.util.FileSignerSecretProvider;
import org.apache.hadoop.security.authentication.util.RandomSignerSecretProvider;
import org.apache.hadoop.security.authentication.util.SignerSecretProvider;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.HadoopUsersConfTestHelper;
import org.apache.hadoop.util.Shell;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.apache.hadoop.security.authentication.server.AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE;

/**
 * Test {@link HttpFSServerWebServer}.
 */
public class TestHttpFSServerWebServer {

  @Rule
  public Timeout timeout = new Timeout(30000);

  private File secretFile;
  private HttpFSServerWebServer webServer;

  @Before
  public void init() throws Exception {
    File homeDir = GenericTestUtils.setupTestRootDir(TestHttpFSServerWebServer.class);
    File confDir = new File(homeDir, "etc/hadoop");
    File logsDir = new File(homeDir, "logs");
    File tempDir = new File(homeDir, "temp");
    confDir.mkdirs();
    logsDir.mkdirs();
    tempDir.mkdirs();

    if (Shell.WINDOWS) {
      File binDir = new File(homeDir, "bin");
      binDir.mkdirs();
      File winutils = Shell.getWinUtilsFile();
      if (winutils.exists()) {
        FileUtils.copyFileToDirectory(winutils, binDir);
      }
    }

    System.setProperty("hadoop.home.dir", homeDir.getAbsolutePath());
    System.setProperty("hadoop.log.dir", logsDir.getAbsolutePath());
    System.setProperty("httpfs.home.dir", homeDir.getAbsolutePath());
    System.setProperty("httpfs.log.dir", logsDir.getAbsolutePath());
    System.setProperty("httpfs.config.dir", confDir.getAbsolutePath());
    secretFile = new File(System.getProperty("httpfs.config.dir"),
        "httpfs-signature-custom.secret");
  }

  @After
  public void teardown() throws Exception {
    if (webServer != null) {
      webServer.stop();
    }
  }

  @Test
  public void testStartStop() throws Exception {
    webServer = createWebServer(createConfigurationWithRandomSecret());
    webServer.start();
    webServer.stop();
  }

  @Test
  public void testJustStop() throws Exception {
    webServer = createWebServer(createConfigurationWithRandomSecret());
    webServer.stop();
  }

  @Test
  public void testDoubleStop() throws Exception {
    webServer = createWebServer(createConfigurationWithRandomSecret());
    webServer.start();
    webServer.stop();
    webServer.stop();
  }

  @Test
  public void testDoubleStart() throws Exception {
    webServer = createWebServer(createConfigurationWithRandomSecret());
    webServer.start();
    webServer.start();
    webServer.stop();
  }

  @Test
  public void testServiceWithSecretFile() throws Exception {
    createSecretFile("foo");
    webServer = createWebServer(createConfigurationWithSecretFile());
    webServer.start();
    assertServiceRespondsWithOK(webServer.getUrl());
    assertSignerSecretProviderType(webServer.getHttpServer(),
        FileSignerSecretProvider.class);
    webServer.stop();
  }

  @Test
  public void testServiceWithSecretFileWithDeprecatedConfigOnly()
      throws Exception {
    createSecretFile("foo");
    Configuration conf = createConfiguration();
    setDeprecatedSecretFile(conf, secretFile.getAbsolutePath());
    webServer = createWebServer(conf);
    webServer.start();
    assertServiceRespondsWithOK(webServer.getUrl());
    assertSignerSecretProviderType(webServer.getHttpServer(),
        FileSignerSecretProvider.class);
    webServer.stop();
  }

  @Test
  public void testServiceWithSecretFileWithBothConfigOptions() throws Exception {
    createSecretFile("foo");
    Configuration conf = createConfigurationWithSecretFile();
    setDeprecatedSecretFile(conf, secretFile.getAbsolutePath());
    webServer = createWebServer(conf);
    webServer.start();
    assertServiceRespondsWithOK(webServer.getUrl());
    assertSignerSecretProviderType(webServer.getHttpServer(),
        FileSignerSecretProvider.class);
    webServer.stop();
  }

  @Test
  public void testServiceWithMissingSecretFile() throws Exception {
    webServer = createWebServer(createConfigurationWithSecretFile());
    webServer.start();
    assertServiceRespondsWithOK(webServer.getUrl());
    assertSignerSecretProviderType(webServer.getHttpServer(),
        RandomSignerSecretProvider.class);
    webServer.stop();
  }

  @Test
  public void testServiceWithEmptySecretFile() throws Exception {
    // The AuthenticationFilter.constructSecretProvider will do the fallback
    // to the random secrets not the HttpFSAuthenticationFilter.
    createSecretFile("");
    webServer = createWebServer(createConfigurationWithSecretFile());
    webServer.start();
    assertServiceRespondsWithOK(webServer.getUrl());
    assertSignerSecretProviderType(webServer.getHttpServer(),
        RandomSignerSecretProvider.class);
    webServer.stop();
  }

  private <T extends SignerSecretProvider> void assertSignerSecretProviderType(
      HttpServer2 server, Class<T> expected) {
    SignerSecretProvider secretProvider = (SignerSecretProvider)
        server.getWebAppContext().getServletContext()
            .getAttribute(SIGNER_SECRET_PROVIDER_ATTRIBUTE);
    Assert.assertNotNull("The secret provider must not be null", secretProvider);
    Assert.assertEquals("The secret provider must match the following", expected, secretProvider.getClass());
  }

  private void assertServiceRespondsWithOK(URL serviceURL)
      throws Exception {
    String user = HadoopUsersConfTestHelper.getHadoopUsers()[0];
    URL url = new URL(serviceURL, MessageFormat.format(
        "/webhdfs/v1/?user.name={0}&op=liststatus", user));
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(conn.getInputStream()))) {
      reader.readLine();
    }
  }

  private void setDeprecatedSecretFile(Configuration conf, String path) {
    conf.set(HttpFSAuthenticationFilter.CONF_PREFIX +
            AuthenticationFilter.SIGNATURE_SECRET_FILE,
        path);
  }

  private Configuration createConfigurationWithRandomSecret() {
    Configuration conf = createConfiguration();
    conf.set(HttpFSAuthenticationFilter.HADOOP_HTTP_CONF_PREFIX +
        AuthenticationFilter.SIGNER_SECRET_PROVIDER, "random");
    return conf;
  }

  private Configuration createConfigurationWithSecretFile() {
    Configuration conf = createConfiguration();
    conf.set(HttpFSAuthenticationFilter.HADOOP_HTTP_CONF_PREFIX +
            AuthenticationFilter.SIGNATURE_SECRET_FILE,
        secretFile.getAbsolutePath());
    return conf;
  }

  private Configuration createConfiguration() {
    Configuration conf = new Configuration(false);
    conf.set(HttpFSServerWebServer.HTTP_HOSTNAME_KEY, "localhost");
    conf.setInt(HttpFSServerWebServer.HTTP_PORT_KEY, 0);
    return conf;
  }

  private HttpFSServerWebServer createWebServer(Configuration conf)
      throws Exception {
    Configuration sslConf = new Configuration(false);

    // The configuration must be stored for the HttpFSAuthenticatorFilter, because
    // it accesses the configuration from the webapp: HttpFSServerWebApp.get().getConfig()
    try (FileOutputStream os = new FileOutputStream(
        new File(System.getProperty("httpfs.config.dir"), "httpfs-site.xml"))) {
      conf.writeXml(os);
    }

    return new HttpFSServerWebServer(conf, sslConf);
  }

  private void createSecretFile(String content) throws IOException {
    Assert.assertTrue(secretFile.createNewFile());
    FileUtils.writeStringToFile(secretFile, content, StandardCharsets.UTF_8);
  }

}
