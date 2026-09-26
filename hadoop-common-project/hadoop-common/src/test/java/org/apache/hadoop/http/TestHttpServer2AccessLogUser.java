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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.Principal;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.authentication.server.JettyAuthenticationHelper;
import org.apache.hadoop.security.authentication.server.ProxyUserAuthenticationFilterInitializer;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that the HttpServer2 access log records the authenticated user.
 * A test filter wraps the request with a fake remote user and publishes it
 * via {@link JettyAuthenticationHelper} so the access log %u picks it up.
 */
public class TestHttpServer2AccessLogUser extends HttpServerFunctionalTest {

  private static final String EXPECTED_USER = "alice";

  private HttpServer2 server;
  private LogCapturer accessLogCapturer;

  @AfterEach
  public void tearDown() throws Exception {
    if (accessLogCapturer != null) {
      accessLogCapturer.stopCapturing();
    }
    if (server != null && server.isAlive()) {
      server.stop();
    }
  }

  @Test
  public void testAccessLogIncludesAuthenticatedUser() throws Exception {
    org.apache.log4j.Logger accessLogger =
        LogManager.getLogger("http.requests.test");
    accessLogger.setLevel(Level.ALL);
    accessLogCapturer = LogCapturer.captureLogs(accessLogger);

    Configuration conf = new Configuration();
    server = createTestServer(conf);
    server.addGlobalFilter("testAuth", FakeAuthFilter.class.getName(), null);
    server.start();
    baseUrl = getServerURL(server);

    HttpURLConnection conn =
        (HttpURLConnection) new URL(baseUrl, "/jmx").openConnection();
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    conn.disconnect();

    GenericTestUtils.waitFor(() -> {
      String out = accessLogCapturer.getOutput();
      return out != null && !out.isEmpty();
    }, 100, 5000);

    String captured = accessLogCapturer.getOutput();
    assertTrue(captured.contains(" " + EXPECTED_USER + " "),
        "Access log should contain user '" + EXPECTED_USER
            + "', but was: " + captured);
  }

  @Test
  public void testAccessLogIncludesUserFromAuthenticationFilter() throws Exception {
    org.apache.log4j.Logger accessLogger =
        LogManager.getLogger("http.requests.test");
    accessLogger.setLevel(Level.ALL);
    accessLogCapturer = LogCapturer.captureLogs(accessLogger);

    String authPrefix = "hadoop.http.authentication.";
    Configuration conf = new Configuration();
    conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
        AuthenticationFilterInitializer.class.getName());
    conf.set(authPrefix + "type", "simple");
    conf.set(authPrefix + PseudoAuthenticationHandler.ANONYMOUS_ALLOWED,
        "false");

    server = createTestServer(conf);
    server.start();
    baseUrl = getServerURL(server);

    HttpURLConnection conn = (HttpURLConnection) new URL(
        baseUrl, "/jmx?user.name=" + EXPECTED_USER).openConnection();
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    conn.disconnect();

    GenericTestUtils.waitFor(() -> {
      String out = accessLogCapturer.getOutput();
      return out != null && !out.isEmpty();
    }, 100, 5000);

    String captured = accessLogCapturer.getOutput();
    assertTrue(captured.contains(" " + EXPECTED_USER + " "),
        "Access log should contain user '" + EXPECTED_USER
            + "' from AuthenticationFilter, but was: " + captured);
  }

  @Test
  public void testAccessLogIncludesDoAsUserViaProxyUserFilter() throws Exception {
    org.apache.log4j.Logger accessLogger =
        LogManager.getLogger("http.requests.test");
    accessLogger.setLevel(Level.ALL);
    accessLogCapturer = LogCapturer.captureLogs(accessLogger);

    String realUser = "alice";
    String doAsUser = "bob";

    String authPrefix = "hadoop.http.authentication.";
    Configuration conf = new Configuration();
    conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
        ProxyUserAuthenticationFilterInitializer.class.getName());
    conf.set(authPrefix + "type", "simple");
    conf.set(authPrefix + PseudoAuthenticationHandler.ANONYMOUS_ALLOWED,
        "false");
    // Allow alice to impersonate any user from any host.
    conf.set("hadoop.proxyuser." + realUser + ".groups", "*");
    conf.set("hadoop.proxyuser." + realUser + ".hosts", "*");

    server = createTestServer(conf);
    server.start();
    baseUrl = getServerURL(server);

    URL url = new URL(baseUrl,
        "/jmx?user.name=" + realUser + "&doas=" + doAsUser);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    conn.disconnect();

    GenericTestUtils.waitFor(() -> {
      String out = accessLogCapturer.getOutput();
      return out != null && !out.isEmpty();
    }, 100, 5000);

    String captured = accessLogCapturer.getOutput();
    assertTrue(captured.contains(" " + doAsUser + " "),
        "Access log should contain doAs user '" + doAsUser
            + "', but was: " + captured);
    assertFalse(captured.contains(" " + realUser + " "),
        "Access log should NOT contain real user '" + realUser
            + "' when doAs is applied, but was: " + captured);
  }

  @Test
  public void testAccessLogShowsDashWhenNoUser() throws Exception {
    org.apache.log4j.Logger accessLogger =
        LogManager.getLogger("http.requests.test");
    accessLogger.setLevel(Level.ALL);
    accessLogCapturer = LogCapturer.captureLogs(accessLogger);

    Configuration conf = new Configuration();
    server = createTestServer(conf);
    server.start();
    baseUrl = getServerURL(server);

    HttpURLConnection conn =
        (HttpURLConnection) new URL(baseUrl, "/jmx").openConnection();
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    conn.disconnect();

    GenericTestUtils.waitFor(() -> {
      String out = accessLogCapturer.getOutput();
      return out != null && !out.isEmpty();
    }, 100, 5000);

    String captured = accessLogCapturer.getOutput();
    assertTrue(captured.contains(" - - "),
        "Access log should show '-' for unauthenticated user, but was: "
            + captured);
  }

  /**
   * Test-only filter that wraps the request with a fixed remote user and
   * pushes it onto the underlying Jetty request so the access log %u
   * resolves to the user.
   */
  public static class FakeAuthFilter implements Filter {
    @Override
    public void init(FilterConfig filterConfig) {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
                         FilterChain chain)
        throws IOException, ServletException {
      HttpServletRequest httpRequest = (HttpServletRequest) request;
      HttpServletRequest wrapped = new HttpServletRequestWrapper(httpRequest) {
        @Override
        public String getRemoteUser() {
          return EXPECTED_USER;
        }

        @Override
        public Principal getUserPrincipal() {
          return () -> EXPECTED_USER;
        }
      };
      JettyAuthenticationHelper.publishRemoteUser(wrapped);
      chain.doFilter(wrapped, response);
    }

    @Override
    public void destroy() {
    }
  }
}
