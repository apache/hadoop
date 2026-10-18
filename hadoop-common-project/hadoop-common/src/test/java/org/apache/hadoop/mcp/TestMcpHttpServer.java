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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Shell;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestMcpHttpServer {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final JacksonMcpJsonMapper JSON_MAPPER = new JacksonMcpJsonMapper();
  private static final String BASEDIR =
      GenericTestUtils.getTempPath(TestMcpHttpServer.class.getSimpleName());

  private static Configuration conf;
  private static String keystoreDir;
  private static String sslConfDir;
  private static SSLFactory clientSslFactory;

  @BeforeAll
  public static void setupSsl() throws Exception {
    // This property needs to be set otherwise CORS Headers will be dropped
    // by HttpUrlConnection
    System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
    conf = new Configuration();
    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    keystoreDir = base.getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestMcpHttpServer.class);
    KeyStoreTestUtil.setupSSLConfig(keystoreDir, sslConfDir, conf, false);
    Configuration sslConf = KeyStoreTestUtil.getSslConfig();
    clientSslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, sslConf);
    clientSslFactory.init();
    String protocols = Shell.isJavaVersionAtLeast(11)
        ? "TLSv1.3,TLSv1.2" : "TLSv1.2";
    conf.set(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, protocols);
    sslConf.set(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, protocols);
  }

  @AfterAll
  public static void cleanupSsl() throws Exception {
    if (clientSslFactory != null) {
      clientSslFactory.destroy();
      clientSslFactory = null;
    }
    FileUtil.fullyDelete(new File(BASEDIR));
    KeyStoreTestUtil.cleanupSSLConfig(keystoreDir, sslConfDir);
  }

  private static final String INITIALIZE_BODY =
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
          + "\"params\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},"
          + "\"clientInfo\":{\"name\":\"t\",\"version\":\"1\"}}}";
  private static final String INITIALIZED_BODY =
      "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}";
  private static final String TOOLS_LIST_BODY =
      "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}";

  private static McpServer buildToolServer() {
    return McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .toolCall(McpSchema.Tool.of("echo", "Echo input", JSON_MAPPER,
                "{\"type\":\"object\",\"properties\":{}}"),
            (context, args) -> McpSchema.CallToolResult.text("ok"))
        .build();
  }

  private static McpHttpServer startPlainHttpServer(McpServer server) throws IOException {
    return McpHttpServer.start(server, new Configuration(),
        new InetSocketAddress("localhost", 0), "/mcp", false);
  }

  /**
   * Sends a request over plain HTTP.
   *
   * @param headers request headers as alternating name/value pairs
   */
  private static HttpURLConnection send(McpHttpServer httpServer, String method, byte[] body,
      Map<String, String> headers) throws IOException {
    URL url = new URL("http://localhost:" + httpServer.getPort() + "/mcp");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod(method);
    if (headers != null) {
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        conn.setRequestProperty(entry.getKey(), entry.getValue());
      }
    }
    if (body != null) {
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");
      try (OutputStream out = conn.getOutputStream()) {
        out.write(body);
      }
    }
    return conn;
  }

  private static HttpURLConnection post(McpHttpServer httpServer, String body,
      Map<String, String> headers) throws IOException {
    return send(httpServer, "POST", body.getBytes(StandardCharsets.UTF_8), headers);
  }

  /** Reads the response body, using the error stream for 4xx and 5xx responses. */
  private static JsonNode readBody(HttpURLConnection conn) throws IOException {
    try (InputStream in = conn.getResponseCode() >= 400
        ? conn.getErrorStream() : conn.getInputStream()) {
      return OBJECT_MAPPER.readTree(in);
    }
  }

  @Test
  public void testPlainHttpServerRespondsToInitialize() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .build();

    try (McpHttpServer httpServer = startPlainHttpServer(server)) {
      HttpURLConnection conn = post(httpServer, INITIALIZE_BODY, null);

      assertEquals(200, conn.getResponseCode());
      JsonNode response = readBody(conn);
      assertEquals("2.0", response.get("jsonrpc").asText());
      assertEquals(McpJsonRpc.PROTOCOL_VERSION,
          response.get("result").get("protocolVersion").asText());
      assertEquals("test-server",
          response.get("result").get("serverInfo").get("name").asText());
      assertNotNull(conn.getHeaderField(McpRequestHandler.SESSION_HEADER));
    }
  }

  @Test
  public void testStandaloneServerRespondsToInitialize() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .build();

    try (McpHttpServer httpServer = McpHttpServer.start(server, conf,
        new InetSocketAddress("localhost", 0), "/mcp")) {
      assertNotNull(httpServer.getConnectorAddress());

      URL url = new URL("https://localhost:" + httpServer.getPort() + "/mcp");
      HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
      conn.setSSLSocketFactory(clientSslFactory.createSSLSocketFactory());
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");
      try (OutputStream out = conn.getOutputStream()) {
        out.write(INITIALIZE_BODY.getBytes(StandardCharsets.UTF_8));
      }

      assertEquals(200, conn.getResponseCode());
      JsonNode response = readBody(conn);
      assertEquals("2.0", response.get("jsonrpc").asText());
      assertEquals(McpJsonRpc.PROTOCOL_VERSION,
          response.get("result").get("protocolVersion").asText());
      assertEquals("test-server",
          response.get("result").get("serverInfo").get("name").asText());
      assertNotNull(conn.getHeaderField(McpRequestHandler.SESSION_HEADER));
    }
  }

  @Test
  public void testEmptyBodyReturnsParseError() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .build();

    try (McpHttpServer httpServer = startPlainHttpServer(server)) {
      HttpURLConnection conn = post(httpServer, "", null);

      assertEquals(200, conn.getResponseCode());
      JsonNode response = readBody(conn);
      assertEquals(McpJsonRpc.PARSE_ERROR, response.get("error").get("code").asInt());
      assertTrue(response.has("id"));
      assertTrue(response.get("id").isNull());
    }
  }

  @Test
  public void testMalformedJsonReturnsParseError() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .build();

    try (McpHttpServer httpServer = startPlainHttpServer(server)) {
      HttpURLConnection conn = post(httpServer, "{not-json", null);

      assertEquals(200, conn.getResponseCode());
      JsonNode response = readBody(conn);
      assertEquals(McpJsonRpc.PARSE_ERROR, response.get("error").get("code").asInt());
      assertTrue(response.has("id"));
      assertTrue(response.get("id").isNull());
    }
  }

  @Test
  public void testFullSessionLifecycleOverHttp() throws Exception {
    try (McpHttpServer httpServer = startPlainHttpServer(buildToolServer())) {
      HttpURLConnection init = post(httpServer, INITIALIZE_BODY, null);
      assertEquals(200, init.getResponseCode());
      String sessionId = init.getHeaderField(McpRequestHandler.SESSION_HEADER);
      assertNotNull(sessionId);

      Map<String, String> headers = new HashMap<>();
      headers.put(McpRequestHandler.SESSION_HEADER, sessionId);
      headers.put(McpJsonRpc.PROTOCOL_VERSION_HEADER, McpJsonRpc.PROTOCOL_VERSION);

      HttpURLConnection initialized = post(httpServer, INITIALIZED_BODY, headers);
      assertEquals(202, initialized.getResponseCode());

      HttpURLConnection list = post(httpServer, TOOLS_LIST_BODY, headers);
      assertEquals(200, list.getResponseCode());
      assertEquals("echo",
          readBody(list).get("result").get("tools").get(0).get("name").asText());

      // no session id in the request
      headers.remove(McpRequestHandler.SESSION_HEADER);
      HttpURLConnection noSession = post(httpServer, TOOLS_LIST_BODY, headers);
      assertEquals(400, noSession.getResponseCode());
      assertEquals(McpJsonRpc.SESSION_REQUIRED_MESSAGE,
          readBody(noSession).get("message").asText());

      // invalid session id in the request
      headers.put(McpRequestHandler.SESSION_HEADER, "no-such-session");
      HttpURLConnection unknownSession = post(httpServer, TOOLS_LIST_BODY, headers);
      assertEquals(404, unknownSession.getResponseCode());

      headers.put(McpRequestHandler.SESSION_HEADER, sessionId);
      HttpURLConnection delete = send(httpServer, "DELETE", null, headers);
      assertEquals(204, delete.getResponseCode());

      HttpURLConnection afterDelete = post(httpServer, TOOLS_LIST_BODY, headers);
      assertEquals(404, afterDelete.getResponseCode());
    }
  }
  
  @Test
  public void testDeleteRejectionsOverHttp() throws Exception {
    try (McpHttpServer httpServer = startPlainHttpServer(buildToolServer())) {
      Map<String, String> headers = new HashMap<>();
      headers.put(McpJsonRpc.PROTOCOL_VERSION_HEADER, McpJsonRpc.PROTOCOL_VERSION);

      headers.put(McpRequestHandler.SESSION_HEADER, "no-such-session");
      HttpURLConnection unknown = send(httpServer, "DELETE", null, headers);
      assertEquals(404, unknown.getResponseCode());

      headers.remove(McpRequestHandler.SESSION_HEADER);
      HttpURLConnection noSession = send(httpServer, "DELETE", null, headers);
      assertEquals(400, noSession.getResponseCode());
    }
  }

  @Test
  public void testOriginHeaderReturns403OverHttp() throws Exception {
    try (McpHttpServer httpServer = startPlainHttpServer(buildToolServer())) {
      Map<String, String> headers = new HashMap<>();
      headers.put(McpJsonRpc.ORIGIN_HEADER, "http://different.origin");
      HttpURLConnection conn = post(httpServer, INITIALIZE_BODY, headers);

      assertEquals(403, conn.getResponseCode());
      assertEquals(McpJsonRpc.ORIGIN_NOT_ALLOWED_MESSAGE,
          readBody(conn).get("message").asText());
      assertNull(conn.getHeaderField(McpRequestHandler.SESSION_HEADER));
    }
  }

  @Test
  public void testOversizedBodyReturns413() throws Exception {
    try (McpHttpServer httpServer = startPlainHttpServer(buildToolServer())) {
      byte[] body = new byte[McpJsonRpc.MAX_REQUEST_BODY_BYTES + 1];
      Arrays.fill(body, (byte) ' ');

      HttpURLConnection conn = send(httpServer, "POST", body, null);

      assertEquals(413, conn.getResponseCode());
      assertEquals(McpJsonRpc.REQUEST_BODY_TOO_LARGE_MESSAGE,
          readBody(conn).get("message").asText());
    }
  }
}
