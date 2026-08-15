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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestMcpHttpServlet {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final JacksonMcpJsonMapper JSON_MAPPER =
      new JacksonMcpJsonMapper(OBJECT_MAPPER);

  @Test
  public void testToolsList() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .toolCall(McpSchema.Tool.of("echo", "Echo input", JSON_MAPPER,
                "{\"type\":\"object\",\"properties\":{}}"),
            (context, args) -> McpSchema.CallToolResult.text("ok"))
        .build();

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}");
    McpHttpResponse response = server.getRequestHandler().handle(request);

    assertEquals(200, response.status());
    JsonNode body = response.body();
    assertEquals("2.0", body.get("jsonrpc").asText());
    assertEquals(1, body.get("id").asInt());
    assertTrue(body.get("result").get("tools").isArray());
    assertEquals("echo", body.get("result").get("tools").get(0).get("name").asText());
  }

  @Test
  public void testToolsCall() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .toolCall(McpSchema.Tool.of("echo", "Echo input", JSON_MAPPER,
                "{\"type\":\"object\",\"properties\":{}}"),
            (context, args) -> McpSchema.CallToolResult.text("{\"value\":\"test\"}"))
        .build();

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\","
            + "\"params\":{\"name\":\"echo\",\"arguments\":{}}}");
    McpHttpResponse response = server.getRequestHandler().handle(request);

    JsonNode body = response.body();
    assertEquals("test", OBJECT_MAPPER.readTree(
        body.get("result").get("content").get(0).get("text").asText()).get("value").asText());
  }

  @Test
  public void testUnknownToolReturnsError() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .build();

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\","
            + "\"params\":{\"name\":\"missing\",\"arguments\":{}}}");
    McpHttpResponse response = server.getRequestHandler().handle(request);

    JsonNode body = response.body();
    assertNotNull(body.get("error"));
    assertEquals(McpJsonRpc.INVALID_PARAMS, body.get("error").get("code").asInt());
  }

  @Test
  public void testInvalidEnvelopeRejectedByHandler() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .build();

    JsonNode request = OBJECT_MAPPER.readTree("{\"id\":1,\"method\":\"tools/list\"}");
    McpHttpResponse response = server.getRequestHandler().handle(request);

    JsonNode body = response.body();
    assertEquals(McpJsonRpc.INVALID_REQUEST, body.get("error").get("code").asInt());
    assertEquals(McpJsonRpc.INVALID_REQUEST_MESSAGE,
        body.get("error").get("message").asText());
    assertEquals(1, body.get("id").asInt());
  }

  @Test
  public void testInitializeReturns20250618ProtocolVersion() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .build();

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
            + "\"params\":{\"protocolVersion\":\"2024-11-05\",\"capabilities\":{},"
            + "\"clientInfo\":{\"name\":\"test\",\"version\":\"1.0\"}}}");
    McpHttpResponse response = server.getRequestHandler().handle(request);

    JsonNode body = response.body();
    assertEquals(McpJsonRpc.PROTOCOL_VERSION,
        body.get("result").get("protocolVersion").asText());
  }

  @Test
  public void testStringIdIsAccepted() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .build();

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":\"req-1\",\"method\":\"tools/list\",\"params\":{}}");
    McpHttpResponse response = server.getRequestHandler().handle(request);

    JsonNode body = response.body();
    assertEquals("req-1", body.get("id").asText());
    assertTrue(body.has("result"));
  }

  @Test
  public void testNotificationWithoutIdReturnsAccepted() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .build();

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}");
    McpHttpResponse response = server.getRequestHandler().handle(request);

    assertEquals(202, response.status());
    assertTrue(response.body() == null);
  }
}
