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
package org.apache.hadoop.crypto.key.kms.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.authentication.server.ProxyUserAuthenticationFilterInitializer;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.crypto.key.kms.server.KMSConfiguration.METRICS_PROCESS_NAME_DEFAULT;
import static org.apache.hadoop.crypto.key.kms.server.KMSConfiguration.METRICS_PROCESS_NAME_KEY;
import static org.apache.hadoop.crypto.key.kms.server.KMSConfiguration.METRICS_SESSION_ID_KEY;

/**
 * The KMS web server.
 */
@InterfaceAudience.Private
public class KMSWebServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(KMSWebServer.class);

  private static final String NAME = "kms";
  private static final String SERVLET_PATH = "/kms";

  private final HttpServer2 httpServer;
  private final String scheme;
  private final String processName;
  private final String sessionId;
  private final JvmPauseMonitor pauseMonitor;

  KMSWebServer(Configuration conf, Configuration sslConf) throws Exception {
    // Override configuration with deprecated environment variables.
    deprecateEnv("KMS_TEMP", conf, HttpServer2.HTTP_TEMP_DIR_KEY,
        KMSConfiguration.KMS_SITE_XML);
    deprecateEnv("KMS_HTTP_PORT", conf,
        KMSConfiguration.HTTP_PORT_KEY, KMSConfiguration.KMS_SITE_XML);
    deprecateEnv("KMS_MAX_THREADS", conf,
        HttpServer2.HTTP_MAX_THREADS_KEY, KMSConfiguration.KMS_SITE_XML);
    deprecateEnv("KMS_MAX_HTTP_HEADER_SIZE", conf,
        HttpServer2.HTTP_MAX_REQUEST_HEADER_SIZE_KEY,
        KMSConfiguration.KMS_SITE_XML);
    deprecateEnv("KMS_MAX_HTTP_HEADER_SIZE", conf,
        HttpServer2.HTTP_MAX_RESPONSE_HEADER_SIZE_KEY,
        KMSConfiguration.KMS_SITE_XML);
    deprecateEnv("KMS_SSL_ENABLED", conf,
        KMSConfiguration.SSL_ENABLED_KEY, KMSConfiguration.KMS_SITE_XML);
    deprecateEnv("KMS_SSL_KEYSTORE_FILE", sslConf,
        SSLFactory.SSL_SERVER_KEYSTORE_LOCATION,
        SSLFactory.SSL_SERVER_CONF_DEFAULT);
    deprecateEnv("KMS_SSL_KEYSTORE_PASS", sslConf,
        SSLFactory.SSL_SERVER_KEYSTORE_PASSWORD,
        SSLFactory.SSL_SERVER_CONF_DEFAULT);

    boolean sslEnabled = conf.getBoolean(KMSConfiguration.SSL_ENABLED_KEY,
        KMSConfiguration.SSL_ENABLED_DEFAULT);
    scheme = sslEnabled ? HttpServer2.HTTPS_SCHEME : HttpServer2.HTTP_SCHEME;
    processName =
        conf.get(METRICS_PROCESS_NAME_KEY, METRICS_PROCESS_NAME_DEFAULT);
    sessionId = conf.get(METRICS_SESSION_ID_KEY);
    pauseMonitor = new JvmPauseMonitor();
    pauseMonitor.init(conf);

    String host = conf.get(KMSConfiguration.HTTP_HOST_KEY,
        KMSConfiguration.HTTP_HOST_DEFAULT);
    int port = conf.getInt(KMSConfiguration.HTTP_PORT_KEY,
        KMSConfiguration.HTTP_PORT_DEFAULT);
    URI endpoint = new URI(scheme, null, host, port, null, null, null);

    String configuredInitializers =
        conf.get(HttpServer2.FILTER_INITIALIZER_PROPERTY);
    if (configuredInitializers != null) {
      Set<String> target = new LinkedHashSet<String>();
      String[] initializers = configuredInitializers.split(",");
      for (String init : initializers) {
        if (!init.equals(AuthenticationFilterInitializer.class.getName()) &&
            !init.equals(
                ProxyUserAuthenticationFilterInitializer.class.getName())) {
          target.add(init);
        }
      }
      String actualInitializers = StringUtils.join(",", target);
      conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY, actualInitializers);
    }

    httpServer = new HttpServer2.Builder()
        .setName(NAME)
        .setConf(conf)
        .setSSLConf(sslConf)
        .authFilterConfigurationPrefix(KMSAuthenticationFilter.CONFIG_PREFIX)
        .setACL(new AccessControlList(conf.get(
            KMSConfiguration.HTTP_ADMINS_KEY, " ")))
        .addEndpoint(endpoint)
        .build();
  }

  /**
   * Load the deprecated environment variable into the configuration.
   *
   * @param varName the environment variable name
   * @param conf the configuration
   * @param propName the configuration property name
   * @param confFile the configuration file name
   */
  private static void deprecateEnv(String varName, Configuration conf,
                                   String propName, String confFile) {
    String value = System.getenv(varName);
    if (value == null) {
      return;
    }
    LOG.warn("Environment variable {} is deprecated and overriding"
        + " property {}, please set the property in {} instead.",
        varName, propName, confFile);
    conf.set(propName, value, "environment variable " + varName);
  }

  public void start() throws IOException {
    httpServer.start();

    DefaultMetricsSystem.initialize(processName);
    final JvmMetrics jm = JvmMetrics.initSingleton(processName, sessionId);
    jm.setPauseMonitor(pauseMonitor);
    pauseMonitor.start();
  }

  public boolean isRunning() {
    return httpServer.isAlive();
  }

  public void join() throws InterruptedException {
    httpServer.join();
  }

  public void stop() throws Exception {
    httpServer.stop();

    pauseMonitor.stop();
    JvmMetrics.shutdownSingleton();
    DefaultMetricsSystem.shutdown();
  }

  public URL getKMSUrl() {
    InetSocketAddress addr = httpServer.getConnectorAddress(0);
    if (null == addr) {
      return null;
    }
    try {
      return new URL(scheme, addr.getHostName(), addr.getPort(),
          SERVLET_PATH);
    } catch (MalformedURLException ex) {
      throw new RuntimeException("It should never happen: " + ex.getMessage(),
          ex);
    }
  }

  public static void main(String[] args) throws Exception {
    KMSConfiguration.initLogging();
    StringUtils.startupShutdownMessage(KMSWebServer.class, args, LOG);
    Configuration conf = KMSConfiguration.getKMSConf();
    Configuration sslConf = SSLFactory.readSSLConfiguration(conf, SSLFactory.Mode.SERVER);
    KMSWebServer kmsWebServer = new KMSWebServer(conf, sslConf);
    kmsWebServer.start();
    kmsWebServer.join();
  }
}
