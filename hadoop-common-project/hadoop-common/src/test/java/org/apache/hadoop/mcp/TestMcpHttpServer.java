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

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;

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
  private static final JacksonMcpJsonMapper JSON_MAPPER =
      new JacksonMcpJsonMapper(OBJECT_MAPPER);
  private static final String BASEDIR =
      GenericTestUtils.getTempPath(TestMcpHttpServer.class.getSimpleName());

  private static Configuration conf;
  private static String keystoreDir;
  private static String sslConfDir;
  private static SSLFactory clientSslFactory;

  @BeforeAll
  public static void setupSsl() throws Exception {
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

  @Test
  public void testPlainHttpServerRespondsToInitialize() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .build();

    try (McpHttpServer httpServer = McpHttpServer.start(server, new Configuration(),
        new InetSocketAddress("localhost", 0), "/mcp", false)) {
      URL url = new URL("http://localhost:" + httpServer.getPort() + "/mcp");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");
      byte[] body = ("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
          + "\"params\":{\"protocolVersion\":\"2024-11-05\",\"capabilities\":{},"
          + "\"clientInfo\":{\"name\":\"t\",\"version\":\"1\"}}}").getBytes(
          StandardCharsets.UTF_8);
      try (OutputStream out = conn.getOutputStream()) {
        out.write(body);
      }

      assertEquals(200, conn.getResponseCode());
      JsonNode response;
      try (InputStream in = conn.getInputStream()) {
        response = OBJECT_MAPPER.readTree(in);
      }
      assertEquals("2.0", response.get("jsonrpc").asText());
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
      byte[] body = ("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
          + "\"params\":{\"protocolVersion\":\"2024-11-05\",\"capabilities\":{},"
          + "\"clientInfo\":{\"name\":\"t\",\"version\":\"1\"}}}").getBytes(
          StandardCharsets.UTF_8);
      try (OutputStream out = conn.getOutputStream()) {
        out.write(body);
      }

      assertEquals(200, conn.getResponseCode());
      JsonNode response;
      try (InputStream in = conn.getInputStream()) {
        response = OBJECT_MAPPER.readTree(in);
      }
      assertEquals("2.0", response.get("jsonrpc").asText());
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

    try (McpHttpServer httpServer = McpHttpServer.start(server, new Configuration(),
        new InetSocketAddress("localhost", 0), "/mcp", false)) {
      URL url = new URL("http://localhost:" + httpServer.getPort() + "/mcp");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");
      conn.getOutputStream().close();

      assertEquals(200, conn.getResponseCode());
      JsonNode response;
      try (InputStream in = conn.getInputStream()) {
        response = OBJECT_MAPPER.readTree(in);
      }
      assertEquals(McpJsonRpc.PARSE_ERROR, response.get("error").get("code").asInt());
    }
  }

  @Test
  public void testMalformedJsonReturnsParseError() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .build();

    try (McpHttpServer httpServer = McpHttpServer.start(server, new Configuration(),
        new InetSocketAddress("localhost", 0), "/mcp", false)) {
      URL url = new URL("http://localhost:" + httpServer.getPort() + "/mcp");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");
      byte[] body = "{not-json".getBytes(StandardCharsets.UTF_8);
      try (OutputStream out = conn.getOutputStream()) {
        out.write(body);
      }

      assertEquals(200, conn.getResponseCode());
      JsonNode response;
      try (InputStream in = conn.getInputStream()) {
        response = OBJECT_MAPPER.readTree(in);
      }
      assertEquals(McpJsonRpc.PARSE_ERROR, response.get("error").get("code").asInt());
    }
  }
}
