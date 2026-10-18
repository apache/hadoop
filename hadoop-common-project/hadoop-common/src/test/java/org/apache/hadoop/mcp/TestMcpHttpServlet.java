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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.util.FakeTimer;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestMcpHttpServlet {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final JacksonMcpJsonMapper JSON_MAPPER =
      new JacksonMcpJsonMapper(OBJECT_MAPPER);

  private static final String INITIALIZE_REQUEST =
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
          + "\"params\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},"
          + "\"clientInfo\":{\"name\":\"test\",\"version\":\"1.0\"}}}";
  private static final String INITIALIZED_NOTIFICATION =
      "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}";
  private static final String TOOLS_LIST_REQUEST =
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}";

  private static McpCallContext createContext(String sessionId) {
    return new McpCallContext(null, sessionId, McpJsonRpc.PROTOCOL_VERSION);
  }

  private static McpHttpResponse sendRequest(McpServer server, String json, String sessionId)
      throws Exception {
    return server.getRequestHandler().handle(OBJECT_MAPPER.readTree(json), createContext(sessionId));
  }

  /** Sends {@code initialize} and returns the new session id. */
  private static String initialize(McpServer server) throws Exception {
    McpHttpResponse response = sendRequest(server, INITIALIZE_REQUEST, null);
    String sessionId = response.headers().get(McpRequestHandler.SESSION_HEADER);
    assertNotNull(sessionId, "initialize did not return a session id");
    return sessionId;
  }

  /** Runs the full handshake and returns the id of a READY session. */
  private static String handshake(McpServer server) throws Exception {
    String sessionId = initialize(server);
    assertEquals(202, sendRequest(server, INITIALIZED_NOTIFICATION, sessionId).status());
    return sessionId;
  }

  private static McpSession getSession(McpServer server, String sessionId) {
    return server.getRequestHandler().getSessionStore().get(sessionId);
  }

  @Test
  public void testToolsList() throws Exception {
    McpServer server = buildEchoServer();
    String sessionId = handshake(server);

    McpHttpResponse response = sendRequest(server, TOOLS_LIST_REQUEST, sessionId);

    assertEquals(200, response.status());
    JsonNode body = response.body();
    assertEquals("2.0", body.get("jsonrpc").asText());
    assertEquals(1, body.get("id").asInt());
    assertTrue(body.get("result").get("tools").isArray());
    assertEquals("echo", body.get("result").get("tools").get(0).get("name").asText());
  }

  @Test
  public void testToolsCall() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER, new Configuration())
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .toolCall(McpSchema.Tool.of("echo", "Echo input", JSON_MAPPER,
                "{\"type\":\"object\",\"properties\":{}}"),
            (context, args) -> McpSchema.CallToolResult.text("{\"value\":\"test\"}"))
        .build();
    String sessionId = handshake(server);

    McpHttpResponse response = sendRequest(server,
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\","
            + "\"params\":{\"name\":\"echo\",\"arguments\":{}}}", sessionId);

    JsonNode body = response.body();
    assertEquals("test", OBJECT_MAPPER.readTree(
        body.get("result").get("content").get(0).get("text").asText()).get("value").asText());
  }

  @Test
  public void testUnknownToolReturnsError() throws Exception {
    McpServer server = buildServer();
    String sessionId = handshake(server);

    McpHttpResponse response = sendRequest(server,
        "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\","
            + "\"params\":{\"name\":\"missing\",\"arguments\":{}}}", sessionId);

    // A bad tool name is a JSON-RPC error on a healthy session, not an HTTP error.
    assertEquals(200, response.status());
    JsonNode body = response.body();
    assertNotNull(body.get("error"));
    assertEquals(McpJsonRpc.INVALID_PARAMS, body.get("error").get("code").asInt());
  }

  @Test
  public void testToolHandlerExceptionReturnsErrorResult() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER, new Configuration())
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .toolCall(McpSchema.Tool.of("boom", "Throws", JSON_MAPPER,
                "{\"type\":\"object\",\"properties\":{}}"),
            (context, args) -> {
              throw new RuntimeException("boom");
            })
        .build();
    String sessionId = handshake(server);

    McpHttpResponse response = sendRequest(server,
        "{\"jsonrpc\":\"2.0\",\"id\":8,\"method\":\"tools/call\","
            + "\"params\":{\"name\":\"boom\",\"arguments\":{}}}", sessionId);

    assertEquals(200, response.status());
    JsonNode body = response.body();
    assertEquals(8, body.get("id").asInt());
    assertTrue(body.has("result"));
    assertFalse(body.has("error"));
    assertTrue(body.get("result").get("isError").asBoolean());
    assertTrue(body.get("result").get("content").get(0).get("text").asText().contains("boom"));
  }

  @Test
  public void testToolHandlerExceptionWithoutMessageUsesFallback() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER, new Configuration())
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .toolCall(McpSchema.Tool.of("fail", "Throws without message", JSON_MAPPER,
                "{\"type\":\"object\",\"properties\":{}}"),
            (context, args) -> {
              throw new RuntimeException();
            })
        .build();
    String sessionId = handshake(server);

    McpHttpResponse response = sendRequest(server,
        "{\"jsonrpc\":\"2.0\",\"id\":9,\"method\":\"tools/call\","
            + "\"params\":{\"name\":\"fail\",\"arguments\":{}}}", sessionId);

    JsonNode body = response.body();
    assertTrue(body.get("result").get("isError").asBoolean());
    assertEquals("Tool execution failed",
        body.get("result").get("content").get(0).get("text").asText());
  }

  @Test
  public void testNonObjectArgumentsReturnsInvalidParams() throws Exception {
    McpServer server = buildEchoServer();
    String sessionId = handshake(server);

    JsonNode stringArgsBody = sendRequest(server,
        "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"tools/call\","
            + "\"params\":{\"name\":\"echo\",\"arguments\":\"oops\"}}", sessionId).body();
    assertEquals(McpJsonRpc.INVALID_PARAMS, stringArgsBody.get("error").get("code").asInt());
    assertEquals("arguments must be a JSON object",
        stringArgsBody.get("error").get("message").asText());
    assertEquals(5, stringArgsBody.get("id").asInt());

    JsonNode arrayArgsBody = sendRequest(server,
        "{\"jsonrpc\":\"2.0\",\"id\":6,\"method\":\"tools/call\","
            + "\"params\":{\"name\":\"echo\",\"arguments\":[]}}", sessionId).body();
    assertEquals(McpJsonRpc.INVALID_PARAMS, arrayArgsBody.get("error").get("code").asInt());
    assertEquals("arguments must be a JSON object",
        arrayArgsBody.get("error").get("message").asText());
    assertEquals(6, arrayArgsBody.get("id").asInt());
  }

  @Test
  public void testNullArgumentsReturnsInvalidParams() throws Exception {
    McpServer server = buildEchoServer();
    String sessionId = handshake(server);

    JsonNode body = sendRequest(server,
        "{\"jsonrpc\":\"2.0\",\"id\":8,\"method\":\"tools/call\","
            + "\"params\":{\"name\":\"echo\",\"arguments\":null}}", sessionId).body();
    assertEquals(McpJsonRpc.INVALID_PARAMS, body.get("error").get("code").asInt());
    assertEquals("arguments must be a JSON object",
        body.get("error").get("message").asText());
    assertEquals(8, body.get("id").asInt());
  }

  @Test
  public void testOmittedArgumentsUsesEmptyMap() throws Exception {
    McpServer server = buildEchoServer();
    String sessionId = handshake(server);

    McpHttpResponse response = sendRequest(server,
        "{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"tools/call\","
            + "\"params\":{\"name\":\"echo\"}}", sessionId);

    assertEquals(200, response.status());
    JsonNode body = response.body();
    assertEquals(7, body.get("id").asInt());
    assertTrue(body.has("result"));
  }

  private static McpServer buildEchoServer(Configuration conf) {
    return McpServer.sync(JSON_MAPPER, conf)
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .toolCall(McpSchema.Tool.of("echo", "Echo input", JSON_MAPPER,
                "{\"type\":\"object\",\"properties\":{}}"),
            (context, args) -> McpSchema.CallToolResult.text("ok"))
        .build();
  }

  private static McpServer buildEchoServer() {
    return buildEchoServer(new Configuration());
  }

  private static final String ECHO_TOOLS_REQUEST =
      "{\"jsonrpc\":\"2.0\",\"id\":9,\"method\":\"tools/call\",\"params\":{\"name\":\"echo\"}}";

  /** A configuration allowing {@code limit} tool calls per session per minute. */
  private static Configuration createRateLimitConfiguration(int limit) {
    Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeysPublic.HADOOP_MCP_SESSION_TOOL_CALL_LIMIT_KEY, limit);
    return conf;
  }

  @Test
  public void testToolCallOverRateLimitReturnsError() throws Exception {
    McpServer server = buildEchoServer(createRateLimitConfiguration(2));
    String sessionId = handshake(server);

    assertTrue(sendRequest(server, ECHO_TOOLS_REQUEST, sessionId).body().has("result"));
    assertTrue(sendRequest(server, ECHO_TOOLS_REQUEST, sessionId).body().has("result"));
    McpHttpResponse response = sendRequest(server, ECHO_TOOLS_REQUEST, sessionId);

    // The session is still healthy, so this is a JSON-RPC error, not an HTTP one.
    assertEquals(200, response.status());
    assertEquals(9, response.body().get("id").asInt());
    assertEquals(McpJsonRpc.SERVER_ERROR_RATE_LIMITED,
        response.body().get("error").get("code").asInt());
  }

  @Test
  public void testRateLimitIsPerSession() throws Exception {
    McpServer server = buildEchoServer(createRateLimitConfiguration(1));
    String first = handshake(server);
    String second = handshake(server);

    sendRequest(server, ECHO_TOOLS_REQUEST, first);
    assertTrue(sendRequest(server, ECHO_TOOLS_REQUEST, first).body().has("error"));

    assertTrue(sendRequest(server, ECHO_TOOLS_REQUEST, second).body().has("result"));
  }

  @Test
  public void testRateLimitDoesNotApplyToOtherMethods() throws Exception {
    McpServer server = buildEchoServer(createRateLimitConfiguration(1));
    String sessionId = handshake(server);
    sendRequest(server, ECHO_TOOLS_REQUEST, sessionId);

    assertTrue(sendRequest(server, TOOLS_LIST_REQUEST, sessionId).body().has("result"));
    assertTrue(sendRequest(server,
        "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"ping\"}", sessionId).body().has("result"));
  }

  private static McpServer buildServer() {
    return McpServer.sync(JSON_MAPPER, new Configuration())
        .serverInfo("test-server", "1.0")
        .build();
  }

  @Test
  public void testNonObjectParamsReturnsInvalidParams() throws Exception {
    McpServer server = buildEchoServer();
    String sessionId = handshake(server);

    McpHttpResponse response = sendRequest(server,
        "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/list\",\"params\":[]}", sessionId);

    assertEquals(200, response.status());
    JsonNode body = response.body();
    assertEquals(McpJsonRpc.INVALID_PARAMS, body.get("error").get("code").asInt());
    assertEquals("params must be a JSON object", body.get("error").get("message").asText());
    assertEquals(4, body.get("id").asInt());
  }

  @Test
  public void testInvalidEnvelopeRejectedByHandler() throws Exception {
    McpServer server = buildServer();
    String sessionId = handshake(server);

    McpHttpResponse response = sendRequest(server, "{\"id\":1,\"method\":\"tools/list\"}", sessionId);

    JsonNode body = response.body();
    assertEquals(McpJsonRpc.INVALID_REQUEST, body.get("error").get("code").asInt());
    assertEquals(McpJsonRpc.INVALID_REQUEST_MESSAGE,
        body.get("error").get("message").asText());
    assertEquals(1, body.get("id").asInt());
  }

  @Test
  public void testInitializeReturns20250618ProtocolVersion() throws Exception {
    McpServer server = buildServer();

    McpHttpResponse response = sendRequest(server, INITIALIZE_REQUEST, null);

    assertEquals(200, response.status());
    assertEquals(McpJsonRpc.PROTOCOL_VERSION,
        response.body().get("result").get("protocolVersion").asText());
    String sessionId = response.headers().get(McpRequestHandler.SESSION_HEADER);
    assertNotNull(sessionId);
    assertEquals(McpSessionState.INITIALIZING, getSession(server, sessionId).getState());
  }

  @Test
  public void testStringIdIsAccepted() throws Exception {
    McpServer server = buildEchoServer();
    String sessionId = handshake(server);

    McpHttpResponse response = sendRequest(server,
        "{\"jsonrpc\":\"2.0\",\"id\":\"req-1\",\"method\":\"tools/list\",\"params\":{}}",
        sessionId);

    JsonNode body = response.body();
    assertEquals("req-1", body.get("id").asText());
    assertTrue(body.has("result"));
  }

  @Test
  public void testRequestMethodWithoutIdReturnsAccepted() throws Exception {
    McpServer server = buildEchoServer();
    String sessionId = handshake(server);

    McpHttpResponse response = sendRequest(server,
        "{\"jsonrpc\":\"2.0\",\"method\":\"tools/list\",\"params\":{}}",
        sessionId);

    assertEquals(202, response.status());
    assertTrue(response.body() == null);
  }

  @Test
  public void testNotificationWithoutIdReturnsAccepted() throws Exception {
    McpServer server = buildEchoServer();
    String sessionId = handshake(server);

    McpHttpResponse response = sendRequest(server,
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}",
        sessionId);

    assertEquals(202, response.status());
    assertTrue(response.body() == null);
  }

  @Test
  public void testUnknownMethodReturnsMethodNotFound() throws Exception {
    McpServer server = buildServer();
    String sessionId = handshake(server);

    JsonNode body = sendRequest(server,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"resources/list\"}", sessionId).body();

    assertEquals(McpJsonRpc.METHOD_NOT_FOUND, body.get("error").get("code").asInt());
  }

  @Test
  public void testInitializeFallsBackForUnsupportedVersion() throws Exception {
    McpServer server = buildServer();

    McpHttpResponse response = sendRequest(server,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
            + "\"params\":{\"protocolVersion\":\"1999-01-01\"}}", null);

    // Negotiation, not an error: the server answers with the version it prefers.
    assertEquals(200, response.status());
    assertFalse(response.body().has("error"));
    assertEquals(McpJsonRpc.PROTOCOL_VERSION,
        response.body().get("result").get("protocolVersion").asText());
  }

  @Test
  public void testInitializeWithNonObjectParamsReturnsInvalidParams() throws Exception {
    McpServer server = buildServer();

    McpHttpResponse response = sendRequest(server,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":[]}", null);

    assertEquals(McpJsonRpc.INVALID_PARAMS,
        response.body().get("error").get("code").asInt());
    assertNull(response.headers().get(McpRequestHandler.SESSION_HEADER));
  }

  @Test
  public void testInitializeAtCapacityLimitReturnsServerError() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeysPublic.HADOOP_MCP_SESSION_MAX_KEY, 1);
    McpServer server = McpServer.sync(JSON_MAPPER, conf).build();
    initialize(server);

    McpHttpResponse response = sendRequest(server, INITIALIZE_REQUEST, null);

    assertEquals(200, response.status());
    assertEquals(McpJsonRpc.SERVER_ERROR_CAPACITY_LIMIT,
        response.body().get("error").get("code").asInt());
    assertEquals(McpJsonRpc.CAPACITY_LIMIT_MESSAGE,
        response.body().get("error").get("message").asText());
    assertNull(response.headers().get(McpRequestHandler.SESSION_HEADER));
  }

  @Test
  public void testInitializedNotificationMakesSessionReady() throws Exception {
    McpServer server = buildServer();
    String sessionId = initialize(server);

    McpHttpResponse response = sendRequest(server, INITIALIZED_NOTIFICATION, sessionId);

    assertEquals(202, response.status());
    assertNull(response.body());
    assertEquals(McpSessionState.READY, getSession(server, sessionId).getState());
  }

  @Test
  public void testToolsListBeforeReadyReturnsError() throws Exception {
    McpServer server = buildEchoServer();
    String sessionId = initialize(server);

    McpHttpResponse response = sendRequest(server, TOOLS_LIST_REQUEST, sessionId);

    assertEquals(200, response.status());
    assertEquals(McpJsonRpc.SERVER_ERROR_SESSION_NOT_READY,
        response.body().get("error").get("code").asInt());
    assertEquals(1, response.body().get("id").asInt());
  }

  @Test
  public void testToolsCallBeforeReadyReturnsError() throws Exception {
    McpServer server = buildEchoServer();
    String sessionId = initialize(server);

    McpHttpResponse response = sendRequest(server,
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\","
            + "\"params\":{\"name\":\"echo\"}}", sessionId);

    assertEquals(McpJsonRpc.SERVER_ERROR_SESSION_NOT_READY,
        response.body().get("error").get("code").asInt());
  }

  @Test
  public void testPingAllowedBeforeReady() throws Exception {
    McpServer server = buildServer();
    String sessionId = initialize(server);

    McpHttpResponse response = sendRequest(server,
        "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"ping\"}", sessionId);

    assertEquals(200, response.status());
    assertEquals(5, response.body().get("id").asInt());
    assertTrue(response.body().get("result").isObject());
    assertEquals(0, response.body().get("result").size());
  }

  @Test
  public void testSessionExpiresAfterIdleTimeout() throws Exception {
    FakeTimer timer = new FakeTimer();
    Configuration conf = new Configuration();
    conf.setLong(CommonConfigurationKeysPublic.HADOOP_MCP_SESSION_TIMEOUT_MS_KEY, 1000);
    McpServer server = McpServer.sync(JSON_MAPPER, conf)
        .timer(timer)
        .build();
    String sessionId = handshake(server);

    timer.advance(1001);

    assertEquals(404, sendRequest(server, TOOLS_LIST_REQUEST, sessionId).status());
  }

  @Test
  public void testCloseTerminatesAllSessions() throws Exception {
    McpServer server = buildServer();
    String sessionId = handshake(server);

    server.close();

    assertNull(getSession(server, sessionId));
  }

  @Test
  public void testRequestWithoutSessionHeaderReturns400() throws Exception {
    McpServer server = buildEchoServer();

    McpHttpResponse response = sendRequest(server, TOOLS_LIST_REQUEST, null);

    assertEquals(400, response.status());
    assertEquals(McpJsonRpc.SESSION_REQUIRED_MESSAGE,
        response.body().get("message").asText());
    assertFalse(response.body().has("jsonrpc"));
  }

  @Test
  public void testEmptySessionHeaderReturns400() throws Exception {
    McpServer server = buildEchoServer();
    assertEquals(400, sendRequest(server, TOOLS_LIST_REQUEST, "").status());
  }

  @Test
  public void testUnknownSessionReturns404() throws Exception {
    McpServer server = buildEchoServer();

    McpHttpResponse response = sendRequest(server, TOOLS_LIST_REQUEST, "no-such-session");

    assertEquals(404, response.status());
    assertEquals(McpJsonRpc.SESSION_NOT_FOUND_MESSAGE,
        response.body().get("message").asText());
  }

  @Test
  public void testNotificationWithoutSessionReturns400() throws Exception {
    McpServer server = buildServer();
    assertEquals(400, sendRequest(server, INITIALIZED_NOTIFICATION, null).status());
  }

  @Test
  public void testInitializedNotificationForUnknownSessionReturns404() throws Exception {
    McpServer server = buildServer();
    assertEquals(404, sendRequest(server, INITIALIZED_NOTIFICATION, "no-such-session").status());
  }

  @Test
  public void testMissingProtocolVersionHeaderOnToolsReturns400() throws Exception {
    McpServer server = buildEchoServer();
    String sessionId = handshake(server);

    McpHttpResponse response = server.getRequestHandler().handle(
        OBJECT_MAPPER.readTree(TOOLS_LIST_REQUEST), new McpCallContext(null, sessionId, null));

    assertEquals(400, response.status());
    assertEquals(McpJsonRpc.PROTOCOL_VERSION_REQUIRED_MESSAGE,
        response.body().get("message").asText());
  }

  @Test
  public void testMissingProtocolVersionHeaderOnNotificationReturns400() throws Exception {
    McpServer server = buildServer();
    String sessionId = initialize(server);

    McpHttpResponse response = server.getRequestHandler().handle(
        OBJECT_MAPPER.readTree(INITIALIZED_NOTIFICATION),
        new McpCallContext(null, sessionId, null));

    assertEquals(400, response.status());
    // The rejected notification must not have changed the session state.
    assertEquals(McpSessionState.INITIALIZING, getSession(server, sessionId).getState());
  }

  @Test
  public void testEmptyProtocolVersionHeaderReturns400() throws Exception {
    McpServer server = buildEchoServer();
    String sessionId = handshake(server);

    McpHttpResponse response = server.getRequestHandler().handle(
        OBJECT_MAPPER.readTree(TOOLS_LIST_REQUEST), new McpCallContext(null, sessionId, ""));

    assertEquals(400, response.status());
    assertEquals(McpJsonRpc.UNSUPPORTED_PROTOCOL_VERSION_MESSAGE,
        response.body().get("message").asText());
  }

  @Test
  public void testUnsupportedProtocolVersionHeaderReturns400() throws Exception {
    McpServer server = buildEchoServer();
    String sessionId = handshake(server);

    McpHttpResponse response = server.getRequestHandler().handle(
        OBJECT_MAPPER.readTree(TOOLS_LIST_REQUEST),
        new McpCallContext(null, sessionId, "1999-01-01"));

    assertEquals(400, response.status());
    assertEquals(McpJsonRpc.UNSUPPORTED_PROTOCOL_VERSION_MESSAGE,
        response.body().get("message").asText());
  }

  private static McpHttpServlet createServlet(McpServer server) {
    return (McpHttpServlet) server.getServlet();
  }

  private static HttpServletRequest mockRequest(String origin, String sessionId) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeader(McpJsonRpc.ORIGIN_HEADER)).thenReturn(origin);
    when(request.getHeader(McpRequestHandler.SESSION_HEADER)).thenReturn(sessionId);
    when(request.getHeader(McpJsonRpc.PROTOCOL_VERSION_HEADER))
        .thenReturn(McpJsonRpc.PROTOCOL_VERSION);
    return request;
  }

  private static HttpServletResponse mockResponse() throws IOException {
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
    return response;
  }

  @Test
  public void testDeleteTerminatesSession() throws Exception {
    McpServer server = buildEchoServer();
    String sessionId = handshake(server);
    HttpServletRequest request = mockRequest(null, sessionId);
    HttpServletResponse response = mockResponse();

    createServlet(server).doDelete(request, response);

    verify(response).setStatus(204);
    assertNull(getSession(server, sessionId));
    assertEquals(404, sendRequest(server, TOOLS_LIST_REQUEST, sessionId).status());
  }

  @Test
  public void testDeleteWithoutSessionHeaderReturns400() throws Exception {
    McpServer server = buildServer();
    HttpServletRequest request = mockRequest(null, null);
    HttpServletResponse response = mockResponse();

    createServlet(server).doDelete(request, response);

    verify(response).setStatus(400);
  }

  @Test
  public void testDeleteUnknownSessionReturns404() throws Exception {
    McpServer server = buildServer();
    HttpServletRequest request = mockRequest(null, "no-such-session");
    HttpServletResponse response = mockResponse();

    createServlet(server).doDelete(request, response);

    verify(response).setStatus(404);
  }

  @Test
  public void testPostWithOriginReturns403() throws Exception {
    McpServer server = buildServer();
    HttpServletRequest request = mockRequest("http://different.origin", null);
    HttpServletResponse response = mockResponse();

    createServlet(server).doPost(request, response);

    verify(response).setStatus(403);
    // The rejected initialize must not have created a session.
    assertEquals(0, server.getRequestHandler().getSessionStore().size());
  }

  @Test
  public void testDeleteWithOriginReturns403() throws Exception {
    McpServer server = buildServer();
    String sessionId = handshake(server);
    HttpServletRequest request = mockRequest("http://different.origin", sessionId);
    HttpServletResponse response = mockResponse();

    createServlet(server).doDelete(request, response);

    verify(response).setStatus(403);
    assertNotNull(getSession(server, sessionId));
  }
}
