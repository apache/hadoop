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
package org.apache.hadoop.test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URL;
import java.net.UnknownHostException;

import org.apache.hadoop.http.JettyUtils;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

public class TestJettyHelper implements MethodRule {
  private boolean ssl;
  private String keyStoreType;
  private String keyStore;
  private String keyStorePassword;
  private Server server;

  public TestJettyHelper() {
    this.ssl = false;
  }

  public TestJettyHelper(String keyStoreType, String keyStore,
      String keyStorePassword) {
    ssl = true;
    this.keyStoreType = keyStoreType;
    this.keyStore = keyStore;
    this.keyStorePassword = keyStorePassword;
  }

  private static final ThreadLocal<TestJettyHelper> TEST_JETTY_TL =
      new InheritableThreadLocal<TestJettyHelper>();

  @Override
  public Statement apply(final Statement statement, final FrameworkMethod frameworkMethod, final Object o) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        TestJetty testJetty = frameworkMethod.getAnnotation(TestJetty.class);
        if (testJetty != null) {
          server = createJettyServer();
        }
        try {
          TEST_JETTY_TL.set(TestJettyHelper.this);
          statement.evaluate();
        } finally {
          TEST_JETTY_TL.remove();
          if (server != null && server.isRunning()) {
            try {
              server.stop();
            } catch (Exception ex) {
              throw new RuntimeException("Could not stop embedded servlet container, " + ex.getMessage(), ex);
            }
          }
        }
      }
    };
  }

  private Server createJettyServer() {
    try {
      InetAddress localhost = InetAddress.getByName("localhost");
      String host = "localhost";
      ServerSocket ss = new ServerSocket(0, 50, localhost);
      int port = ss.getLocalPort();
      ss.close();
      Server server = new Server();
      ServerConnector conn = new ServerConnector(server);
      HttpConfiguration http_config = new HttpConfiguration();
      http_config.setRequestHeaderSize(JettyUtils.HEADER_SIZE);
      http_config.setResponseHeaderSize(JettyUtils.HEADER_SIZE);
      http_config.setSecureScheme("https");
      http_config.addCustomizer(new SecureRequestCustomizer());
      ConnectionFactory connFactory = new HttpConnectionFactory(http_config);
      conn.addConnectionFactory(connFactory);
      conn.setHost(host);
      conn.setPort(port);
      if (ssl) {
        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setNeedClientAuth(false);
        sslContextFactory.setKeyStorePath(keyStore);
        sslContextFactory.setKeyStoreType(keyStoreType);
        sslContextFactory.setKeyStorePassword(keyStorePassword);
        conn.addFirstConnectionFactory(new SslConnectionFactory(sslContextFactory,
            HttpVersion.HTTP_1_1.asString()));
      }
      server.addConnector(conn);
      return server;
    } catch (Exception ex) {
      throw new RuntimeException("Could not start embedded servlet container, " + ex.getMessage(), ex);
    }
  }

  /**
   * Returns the authority (hostname & port) used by the JettyServer.
   *
   * @return an <code>InetSocketAddress</code> with the corresponding authority.
   */
  public static InetSocketAddress getAuthority() {
    Server server = getJettyServer();
    try {
      InetAddress add =
        InetAddress.getByName(((ServerConnector)server.getConnectors()[0]).getHost());
      int port = ((ServerConnector)server.getConnectors()[0]).getPort();
      return new InetSocketAddress(add, port);
    } catch (UnknownHostException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Returns a Jetty server ready to be configured and the started. This server
   * is only available when the test method has been annotated with
   * {@link TestJetty}. Refer to {@link HTestCase} header for details.
   * <p/>
   * Once configured, the Jetty server should be started. The server will be
   * automatically stopped when the test method ends.
   *
   * @return a Jetty server ready to be configured and the started.
   */
  public static Server getJettyServer() {
    TestJettyHelper helper = TEST_JETTY_TL.get();
    if (helper == null || helper.server == null) {
      throw new IllegalStateException("This test does not use @TestJetty");
    }
    return helper.server;
  }

  /**
   * Returns the base URL (SCHEMA://HOST:PORT) of the test Jetty server
   * (see {@link #getJettyServer()}) once started.
   *
   * @return the base URL (SCHEMA://HOST:PORT) of the test Jetty server.
   */
  public static URL getJettyURL() {
    TestJettyHelper helper = TEST_JETTY_TL.get();
    if (helper == null || helper.server == null) {
      throw new IllegalStateException("This test does not use @TestJetty");
    }
    try {
      String scheme = (helper.ssl) ? "https" : "http";
      return new URL(scheme + "://" +
          ((ServerConnector)helper.server.getConnectors()[0]).getHost() + ":" +
          ((ServerConnector)helper.server.getConnectors()[0]).getPort());
    } catch (MalformedURLException ex) {
      throw new RuntimeException("It should never happen, " + ex.getMessage(), ex);
    }
  }

}
