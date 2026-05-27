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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.Principal;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end check that {@link HttpServer2}'s auto-installed
 * {@link JettyAuthBridgeFilter} forwards the user from a request wrapper
 * (the pattern that {@code AuthenticationFilter} uses) onto the base Jetty
 * Request, so the standard servlet / Jetty API resolves the user from
 * outside the filter chain as well — most importantly,
 * {@link org.eclipse.jetty.server.handler.RequestLogHandler}.
 *
 * <p>The test relies on {@code StaticUserWebFilter}, the default filter
 * initializer in core-default.xml, to stand in for an authentication
 * filter: its job is to wrap the request so {@code getRemoteUser()}
 * returns a configured user. Because filter initializers run <em>before</em>
 * the bridge filter, this models the production order
 * AuthenticationFilter → JettyAuthBridgeFilter.
 */
public class TestHttpServer2RemoteUser extends HttpServerFunctionalTest {

  private static final String USER = "foo";
  /** HttpServer2 emits Jetty access logs to this slf4j logger. */
  private static final String ACCESS_LOG_LOGGER = "http.requests.test";

  private HttpServer2 server;
  private StringWriter accessLog;
  private WriterAppender accessLogAppender;
  private Level previousLevel;

  @BeforeEach
  public void setup() throws IOException {
    attachAccessLogCapture();

    Configuration conf = new Configuration();
    // StaticUserWebFilter (the default filter initializer) wraps the
    // request so getRemoteUser() returns this user. Bridge filter runs
    // after the initializers, so it sees the wrap — exactly the order
    // that real auth filters experience.
    conf.set(CommonConfigurationKeys.HADOOP_HTTP_STATIC_USER, USER);
    server = createTestServer(conf);
    server.addServlet("whoami", "/whoami", WhoAmIServlet.class);
    server.start();
  }

  @AfterEach
  public void teardown() throws Exception {
    detachAccessLogCapture();
    if (server != null) {
      server.stop();
    }
  }

  /**
   * Sanity check: the servlet, which sees the wrap installed by
   * StaticUserWebFilter, gets the user via the standard servlet API.
   */
  @Test
  public void testRemoteUserVisibleToServlet() throws Exception {
    String body = httpGet("/whoami");
    assertEquals(USER + "|" + USER, body.trim(),
        "Wrapped request should expose the user via the standard servlet "
            + "API to the downstream servlet");
  }

  /**
   * The interesting case: Jetty's {@code RequestLogHandler} runs outside
   * the filter chain and only sees the base {@link
   * org.eclipse.jetty.server.Request}. Without the bridge, {@code %u}
   * would be {@code -}. With the bridge, the user is logged.
   */
  @Test
  public void testAccessLogContainsRemoteUser() throws Exception {
    httpGet("/whoami");
    String log = awaitAccessLogContaining("/whoami");
    assertTrue(log.contains("/whoami"),
        "Access log should mention the request path, was:\n" + log);
    // EXTENDED_NCSA_FORMAT places %u between the dash for identd and
    // the request timestamp: "<ip> - <user> [<ts>] ..."
    assertTrue(log.contains(" - " + USER + " ["),
        "Access log should record the user resolved through the bridge "
            + "filter (expected ' - " + USER + " ['), was:\n" + log);
  }

  /**
   * {@code ?doAs=...} should produce an "<effective>/<real>" label
   * (Kerberos principal-style) so impersonation is explicit while
   * staying a single, parser-friendly token.
   */
  @Test
  public void testAccessLogProxyUserFormatting() throws Exception {
    httpGet("/whoami?doAs=alice");
    String log = awaitAccessLogContaining("/whoami?doAs=alice");
    String expected = " - alice/" + USER + " [";
    assertTrue(log.contains(expected),
        "Access log should show '<doAs>/<auth>' label (expected '"
            + expected + "'), was:\n" + log);
  }

  /**
   * Hadoop accepts the doAs parameter case-insensitively
   * (matching {@code DelegationTokenAuthenticationFilter.getDoAs}).
   */
  @Test
  public void testAccessLogProxyUserFormattingCaseInsensitive() throws Exception {
    httpGet("/whoami?doas=alice");
    String log = awaitAccessLogContaining("/whoami?doas=alice");
    assertTrue(log.contains(" - alice/" + USER + " ["),
        "Lowercase 'doas' must also produce the slash-formatted label, "
            + "was:\n" + log);
  }

  /**
   * When doAs equals the authenticated user, no spurious "X/X".
   */
  @Test
  public void testAccessLogDoesNotFormatWhenDoAsEqualsUser() throws Exception {
    httpGet("/whoami?doAs=" + USER);
    String log = awaitAccessLogContaining("/whoami?doAs=" + USER);
    assertTrue(log.contains(" - " + USER + " ["),
        "Access log should show plain user (no '/') when doAs equals "
            + "auth user, was:\n" + log);
    assertFalse(log.contains(USER + "/" + USER),
        "Should not produce 'X/X' degenerate label, was:\n" + log);
  }

  private String awaitAccessLogContaining(String needle) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 5000;
    String log;
    do {
      log = accessLog.toString();
      if (log.contains(needle)) {
        return log;
      }
      Thread.sleep(50);
    } while (System.currentTimeMillis() < deadline);
    return log;
  }

  private String httpGet(String path) throws IOException {
    URL url = new URL(
        "http://localhost:" + server.getConnectorAddress(0).getPort() + path);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.connect();
    assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
    try (BufferedReader r = new BufferedReader(
        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = r.readLine()) != null) {
        sb.append(line);
      }
      return sb.toString();
    }
  }

  private void attachAccessLogCapture() {
    accessLog = new StringWriter();
    accessLogAppender = new WriterAppender(new PatternLayout("%m%n"), accessLog);
    accessLogAppender.setName("test-access-log-capture");
    accessLogAppender.setImmediateFlush(true);

    org.apache.log4j.Logger logger =
        org.apache.log4j.Logger.getLogger(ACCESS_LOG_LOGGER);
    previousLevel = logger.getLevel();
    logger.setLevel(Level.INFO);
    logger.addAppender(accessLogAppender);
  }

  private void detachAccessLogCapture() {
    if (accessLogAppender == null) {
      return;
    }
    org.apache.log4j.Logger logger =
        org.apache.log4j.Logger.getLogger(ACCESS_LOG_LOGGER);
    logger.removeAppender(accessLogAppender);
    logger.setLevel(previousLevel);
    accessLogAppender = null;
    accessLog = null;
  }

  public static class WhoAmIServlet extends javax.servlet.http.HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws IOException {
      String remoteUser = req.getRemoteUser();
      Principal principal = req.getUserPrincipal();
      String principalName = (principal == null) ? "<null>" : principal.getName();
      resp.setContentType("text/plain");
      resp.getWriter().write(remoteUser + "|" + principalName);
    }
  }
}
