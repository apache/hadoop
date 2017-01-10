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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.ConfigurationWithLogging;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  KMSWebServer(Configuration cnf) throws Exception {
    ConfigurationWithLogging conf = new ConfigurationWithLogging(cnf);

    // Add SSL configuration file
    conf.addResource(conf.get(SSLFactory.SSL_SERVER_CONF_KEY,
        SSLFactory.SSL_SERVER_CONF_DEFAULT));

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
    deprecateEnv("KMS_SSL_KEYSTORE_FILE", conf,
        SSLFactory.SSL_SERVER_KEYSTORE_LOCATION,
        SSLFactory.SSL_SERVER_CONF_DEFAULT);
    deprecateEnv("KMS_SSL_KEYSTORE_PASS", conf,
        SSLFactory.SSL_SERVER_KEYSTORE_PASSWORD,
        SSLFactory.SSL_SERVER_CONF_DEFAULT);

    boolean sslEnabled = conf.getBoolean(KMSConfiguration.SSL_ENABLED_KEY,
        KMSConfiguration.SSL_ENABLED_DEFAULT);
    scheme = sslEnabled ? HttpServer2.HTTPS_SCHEME : HttpServer2.HTTP_SCHEME;

    String host = conf.get(KMSConfiguration.HTTP_HOST_KEY,
        KMSConfiguration.HTTP_HOST_DEFAULT);
    int port = conf.getInt(KMSConfiguration.HTTP_PORT_KEY,
        KMSConfiguration.HTTP_PORT_DEFAULT);
    URI endpoint = new URI(scheme, null, host, port, null, null, null);

    httpServer = new HttpServer2.Builder()
        .setName(NAME)
        .setConf(conf)
        .setSSLConf(conf)
        .authFilterConfigurationPrefix(KMSAuthenticationFilter.CONFIG_PREFIX)
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
    String propValue = conf.get(propName);
    LOG.warn("Environment variable {} = '{}' is deprecated and overriding"
        + " property {} = '{}', please set the property in {} instead.",
        varName, value, propName, propValue, confFile);
    conf.set(propName, value, "environment variable " + varName);
  }

  public void start() throws IOException {
    httpServer.start();
  }

  public boolean isRunning() {
    return httpServer.isAlive();
  }

  public void join() throws InterruptedException {
    httpServer.join();
  }

  public void stop() throws Exception {
    httpServer.stop();
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
    StringUtils.startupShutdownMessage(KMSWebServer.class, args, LOG);
    Configuration conf = KMSConfiguration.getKMSConf();
    KMSWebServer kmsWebServer = new KMSWebServer(conf);
    kmsWebServer.start();
    kmsWebServer.join();
  }
}
