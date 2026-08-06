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

package org.apache.hadoop.mcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.StringUtils;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal Jetty HTTP/HTTPS server that hosts an {@link McpServer} servlet on a dedicated port.
 *
 * <p>No Kerberos/SPNEGO filters are installed. Callers authenticate MCP tool calls
 * at the application layer (for example API keys).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class McpHttpServer implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(McpHttpServer.class);

  private final McpServer mcpServer;
  private final Server server;
  private final ServerConnector connector;
  private final SSLFactory sslFactory;

  private McpHttpServer(McpServer mcpServer, Server server, ServerConnector connector,
      SSLFactory sslFactory) {
    this.mcpServer = mcpServer;
    this.server = server;
    this.connector = connector;
    this.sslFactory = sslFactory;
  }

  /**
   * Starts a dedicated MCP HTTPS server.
   *
   * @param mcpServer MCP server whose servlet handles JSON-RPC requests
   * @param conf daemon configuration containing SSL keystore settings
   * @param bindAddress address to bind (port {@code 0} picks an ephemeral port)
   * @param pathSpec servlet path, for example {@code /ws/v1/mcp}
   */
  public static McpHttpServer start(McpServer mcpServer, Configuration conf,
      InetSocketAddress bindAddress, String pathSpec) throws IOException {
    return start(mcpServer, conf, bindAddress, pathSpec, true);
  }

  /**
   * Starts a dedicated MCP HTTP or HTTPS server.
   *
   * @param mcpServer MCP server whose servlet handles JSON-RPC requests
   * @param conf daemon configuration; SSL keystore settings are required when {@code useHttps}
   *     is {@code true}
   * @param bindAddress address to bind (port {@code 0} picks an ephemeral port)
   * @param pathSpec servlet path, for example {@code /ws/v1/mcp}
   * @param useHttps when {@code true}, serve HTTPS using the daemon SSL keystore; otherwise
   *     serve plain HTTP
   */
  public static McpHttpServer start(McpServer mcpServer, Configuration conf,
      InetSocketAddress bindAddress, String pathSpec, boolean useHttps) throws IOException {
    Configuration serverConf = conf == null ? new Configuration(false) : conf;
    SSLFactory sslFactory = null;
    Server server = new Server();
    ServerConnector connector;
    if (useHttps) {
      sslFactory = new SSLFactory(SSLFactory.Mode.SERVER, serverConf);
      try {
        sslFactory.init();
      } catch (GeneralSecurityException e) {
        throw new IOException("Failed to initialize SSLFactory", e);
      }
      connector = createHttpsConnector(server, serverConf, bindAddress, sslFactory);
    } else {
      connector = createHttpConnector(server, bindAddress);
    }
    server.addConnector(connector);

    ServletContextHandler context =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.setContextPath("/");
    context.addServlet(new ServletHolder(mcpServer.getServlet()), pathSpec);
    server.setHandler(context);

    try {
      server.start();
    } catch (Exception e) {
      if (sslFactory != null) {
        sslFactory.destroy();
      }
      throw new IOException("Failed to start MCP HTTP server", e);
    }

    String host = bindAddress.getHostString();
    if (host == null || host.isEmpty()) {
      host = "0.0.0.0";
    }
    String scheme = useHttps ? "https" : "http";
    LOG.info("MCP {} server listening on {}://{}:{}{}",
        useHttps ? "HTTPS" : "HTTP", scheme, host, connector.getLocalPort(), pathSpec);
    return new McpHttpServer(mcpServer, server, connector, sslFactory);
  }

  private static ServerConnector createHttpConnector(Server server,
      InetSocketAddress bindAddress) {
    HttpConfiguration httpConfig = new HttpConfiguration();
    httpConfig.setSendServerVersion(false);

    ServerConnector connector = new ServerConnector(server,
        new HttpConnectionFactory(httpConfig));
    applyBindAddress(connector, bindAddress);
    return connector;
  }

  private static ServerConnector createHttpsConnector(Server server, Configuration sslConf,
      InetSocketAddress bindAddress, SSLFactory sslFactory) throws IOException {
    HttpConfiguration httpConfig = new HttpConfiguration();
    httpConfig.setSendServerVersion(false);
    httpConfig.setSecureScheme("https");
    boolean sniHostCheckEnabled = sslConf.getBoolean(
        HttpServer2.HTTP_SNI_HOST_CHECK_ENABLED_KEY,
        HttpServer2.HTTP_SNI_HOST_CHECK_ENABLED_DEFAULT);
    httpConfig.addCustomizer(new SecureRequestCustomizer(sniHostCheckEnabled));

    ServerConnector connector = new ServerConnector(server);
    SslContextFactory.Server sslContextFactory =
        createJettySslContextFactory(sslFactory, sslConf);
    connector.addConnectionFactory(new HttpConnectionFactory(httpConfig));
    connector.addFirstConnectionFactory(new SslConnectionFactory(sslContextFactory,
        HttpVersion.HTTP_1_1.asString()));

    applyBindAddress(connector, bindAddress);
    return connector;
  }

  private static void applyBindAddress(ServerConnector connector,
      InetSocketAddress bindAddress) {
    String host = bindAddress.getHostString();
    if (host != null && !host.isEmpty() && !"0.0.0.0".equals(host)) {
      connector.setHost(host);
    }
    connector.setPort(bindAddress.getPort());
  }

  private static SslContextFactory.Server createJettySslContextFactory(
      SSLFactory sslFactory, Configuration conf) throws IOException {
    Configuration sslServerConf =
        SSLFactory.readSSLConfiguration(conf, SSLFactory.Mode.SERVER);
    String keyStore = sslServerConf.getTrimmed(SSLFactory.SSL_SERVER_KEYSTORE_LOCATION);
    if (keyStore == null || keyStore.isEmpty()) {
      throw new IOException("Property " + SSLFactory.SSL_SERVER_KEYSTORE_LOCATION
          + " not specified");
    }

    SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
    sslContextFactory.setKeyStorePath(keyStore);
    sslContextFactory.setKeyStoreType(sslServerConf.get(SSLFactory.SSL_SERVER_KEYSTORE_TYPE,
        SSLFactory.SSL_SERVER_KEYSTORE_TYPE_DEFAULT));
    String keyStorePassword = getPasswordString(sslServerConf,
        SSLFactory.SSL_SERVER_KEYSTORE_PASSWORD);
    if (keyStorePassword == null) {
      throw new IOException("Property " + SSLFactory.SSL_SERVER_KEYSTORE_PASSWORD
          + " not specified");
    }
    sslContextFactory.setKeyStorePassword(keyStorePassword);

    String keyPassword = getPasswordString(sslServerConf,
        SSLFactory.SSL_SERVER_KEYSTORE_KEYPASSWORD);
    if (keyPassword != null) {
      sslContextFactory.setKeyManagerPassword(keyPassword);
    }

    String trustStore = sslServerConf.get(SSLFactory.SSL_SERVER_TRUSTSTORE_LOCATION);
    if (trustStore != null) {
      sslContextFactory.setTrustStorePath(trustStore);
      sslContextFactory.setTrustStoreType(sslServerConf.get(
          SSLFactory.SSL_SERVER_TRUSTSTORE_TYPE,
          SSLFactory.SSL_SERVER_TRUSTSTORE_TYPE_DEFAULT));
      String trustStorePassword = getPasswordString(sslServerConf,
          SSLFactory.SSL_SERVER_TRUSTSTORE_PASSWORD);
      if (trustStorePassword != null) {
        sslContextFactory.setTrustStorePassword(trustStorePassword);
      }
    }

    sslContextFactory.setNeedClientAuth(sslFactory.isClientCertRequired());

    String[] enabledProtocols = conf.getStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY,
        SSLFactory.SSL_ENABLED_PROTOCOLS_DEFAULT);
    if (enabledProtocols != null) {
      sslContextFactory.setIncludeProtocols(enabledProtocols);
    }

    String excludeCiphers = sslServerConf.get(SSLFactory.SSL_SERVER_EXCLUDE_CIPHER_LIST);
    if (StringUtils.hasLength(excludeCiphers)) {
      sslContextFactory.setExcludeCipherSuites(StringUtils.getTrimmedStrings(excludeCiphers));
    }
    String includeCiphers = sslServerConf.get(SSLFactory.SSL_SERVER_INCLUDE_CIPHER_LIST);
    if (StringUtils.hasLength(includeCiphers)) {
      sslContextFactory.setIncludeCipherSuites(StringUtils.getTrimmedStrings(includeCiphers));
    }
    return sslContextFactory;
  }

  private static String getPasswordString(Configuration conf, String name)
      throws IOException {
    char[] passchars = conf.getPassword(name);
    if (passchars == null) {
      return null;
    }
    return new String(passchars);
  }

  /** Local port of the connector (useful when bind port was 0). */
  public int getPort() {
    return connector.getLocalPort();
  }

  public InetSocketAddress getConnectorAddress() {
    String host = connector.getHost();
    if (host == null || host.isEmpty()) {
      host = "0.0.0.0";
    }
    return new InetSocketAddress(host, connector.getLocalPort());
  }

  @Override
  public void close() throws IOException {
    try {
      server.stop();
    } catch (Exception e) {
      throw new IOException("Failed to stop MCP HTTP server", e);
    }
    if (sslFactory != null) {
      sslFactory.destroy();
    }
    mcpServer.close();
  }
}
