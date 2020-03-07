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

import static org.apache.hadoop.util.StringUtils.startupShutdownMessage;

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
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.authentication.server.ProxyUserAuthenticationFilterInitializer;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HttpFS web server.
 */
@InterfaceAudience.Private
public class HttpFSServerWebServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(HttpFSServerWebServer.class);

  private static final String HTTPFS_DEFAULT_XML = "httpfs-default.xml";
  private static final String HTTPFS_SITE_XML = "httpfs-site.xml";

  // HTTP properties
  static final String HTTP_PORT_KEY = "httpfs.http.port";
  private static final int HTTP_PORT_DEFAULT = 14000;
  static final String HTTP_HOSTNAME_KEY = "httpfs.http.hostname";
  private static final String HTTP_HOSTNAME_DEFAULT = "0.0.0.0";

  // SSL properties
  static final String SSL_ENABLED_KEY = "httpfs.ssl.enabled";
  private static final boolean SSL_ENABLED_DEFAULT = false;

  private static final String HTTP_ADMINS_KEY = "httpfs.http.administrators";

  private static final String NAME = "webhdfs";
  private static final String SERVLET_PATH = "/webhdfs";

  static {
    Configuration.addDefaultResource(HTTPFS_DEFAULT_XML);
    Configuration.addDefaultResource(HTTPFS_SITE_XML);
  }

  private final HttpServer2 httpServer;
  private final String scheme;

  HttpFSServerWebServer(Configuration conf, Configuration sslConf) throws
      Exception {
    // Override configuration with deprecated environment variables.
    deprecateEnv("HTTPFS_HTTP_HOSTNAME", conf, HTTP_HOSTNAME_KEY,
        HTTPFS_SITE_XML);
    deprecateEnv("HTTPFS_HTTP_PORT", conf, HTTP_PORT_KEY,
        HTTPFS_SITE_XML);
    deprecateEnv("HTTPFS_MAX_THREADS", conf,
        HttpServer2.HTTP_MAX_THREADS_KEY, HTTPFS_SITE_XML);
    deprecateEnv("HTTPFS_MAX_HTTP_HEADER_SIZE", conf,
        HttpServer2.HTTP_MAX_REQUEST_HEADER_SIZE_KEY, HTTPFS_SITE_XML);
    deprecateEnv("HTTPFS_MAX_HTTP_HEADER_SIZE", conf,
        HttpServer2.HTTP_MAX_RESPONSE_HEADER_SIZE_KEY, HTTPFS_SITE_XML);
    deprecateEnv("HTTPFS_SSL_ENABLED", conf, SSL_ENABLED_KEY,
        HTTPFS_SITE_XML);
    deprecateEnv("HTTPFS_SSL_KEYSTORE_FILE", sslConf,
        SSLFactory.SSL_SERVER_KEYSTORE_LOCATION,
        SSLFactory.SSL_SERVER_CONF_DEFAULT);
    deprecateEnv("HTTPFS_SSL_KEYSTORE_PASS", sslConf,
        SSLFactory.SSL_SERVER_KEYSTORE_PASSWORD,
        SSLFactory.SSL_SERVER_CONF_DEFAULT);

    boolean sslEnabled = conf.getBoolean(SSL_ENABLED_KEY,
        SSL_ENABLED_DEFAULT);
    scheme = sslEnabled ? HttpServer2.HTTPS_SCHEME : HttpServer2.HTTP_SCHEME;

    String host = conf.get(HTTP_HOSTNAME_KEY, HTTP_HOSTNAME_DEFAULT);
    int port = conf.getInt(HTTP_PORT_KEY, HTTP_PORT_DEFAULT);
    URI endpoint = new URI(scheme, null, host, port, null, null, null);

    // Allow the default authFilter HttpFSAuthenticationFilter
    String configuredInitializers = conf.get(HttpServer2.
        FILTER_INITIALIZER_PROPERTY);
    if (configuredInitializers != null) {
      Set<String> target = new LinkedHashSet<String>();
      String[] parts = configuredInitializers.split(",");
      for (String filterInitializer : parts) {
        if (!filterInitializer.equals(AuthenticationFilterInitializer.class.
            getName()) && !filterInitializer.equals(
            ProxyUserAuthenticationFilterInitializer.class.getName())) {
          target.add(filterInitializer);
        }
      }
      String actualInitializers =
          org.apache.commons.lang3.StringUtils.join(target, ",");
      conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY, actualInitializers);
    }

    httpServer = new HttpServer2.Builder()
        .setName(NAME)
        .setConf(conf)
        .setSSLConf(sslConf)
        .authFilterConfigurationPrefix(HttpFSAuthenticationFilter.CONF_PREFIX)
        .setACL(new AccessControlList(conf.get(HTTP_ADMINS_KEY, " ")))
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
            + " property {}', please set the property in {} instead.",
        varName, propName, confFile);
    conf.set(propName, value, "environment variable " + varName);
  }

  public void start() throws IOException {
    httpServer.start();
  }

  public void join() throws InterruptedException {
    httpServer.join();
  }

  public void stop() throws Exception {
    httpServer.stop();
  }

  public URL getUrl() {
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
    startupShutdownMessage(HttpFSServerWebServer.class, args, LOG);
    Configuration conf = new Configuration(true);
    Configuration sslConf = SSLFactory.readSSLConfiguration(conf, SSLFactory.Mode.SERVER);
    HttpFSServerWebServer webServer =
        new HttpFSServerWebServer(conf, sslConf);
    webServer.start();
    webServer.join();
  }
}
