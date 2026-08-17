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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestMcpJsonRpcValidator {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private McpJsonRpcResponses responses;

  @BeforeEach
  public void setUp() {
    responses = new McpJsonRpcResponses(OBJECT_MAPPER);
  }

  @Test
  public void testNullRequestIsInvalid() {
    assertInvalid(null, null);
  }

  @Test
  public void testNonObjectRequestIsInvalid() throws Exception {
    assertInvalid(OBJECT_MAPPER.readTree("[]"), null);
    assertInvalid(OBJECT_MAPPER.readTree("null"), null);
  }

  @Test
  public void testMissingJsonRpcIsInvalid() throws Exception {
    JsonNode request = OBJECT_MAPPER.readTree("{\"id\":1,\"method\":\"tools/list\"}");
    assertInvalid(request, 1);
  }

  @Test
  public void testWrongJsonRpcVersionIsInvalid() throws Exception {
    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"1.0\",\"id\":2,\"method\":\"tools/list\"}");
    assertInvalid(request, 2);
  }

  @Test
  public void testMissingMethodIsInvalid() throws Exception {
    JsonNode request = OBJECT_MAPPER.readTree("{\"jsonrpc\":\"2.0\",\"id\":3}");
    assertInvalid(request, 3);
  }

  @Test
  public void testValidRequestAccepted() throws Exception {
    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/list\"}");
    assertNull(McpJsonRpcValidator.validate(request, responses));
  }

  @Test
  public void testStringRequestIdAccepted() throws Exception {
    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":\"req-1\",\"method\":\"initialize\"}");
    assertNull(McpJsonRpcValidator.validate(request, responses));
  }

  @Test
  public void testMissingRequestIdIsInvalid() throws Exception {
    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"method\":\"tools/list\"}");
    assertInvalid(request, null);
  }

  @Test
  public void testNullRequestIdIsInvalid() throws Exception {
    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":null,\"method\":\"tools/list\"}");
    assertInvalid(request, null);
  }

  @Test
  public void testObjectRequestIdIsInvalid() throws Exception {
    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":{\"bad\":true},\"method\":\"tools/list\"}");
    assertInvalid(request, null);
  }

  @Test
  public void testNotificationWithoutIdAccepted() throws Exception {
    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}");
    assertNull(McpJsonRpcValidator.validate(request, responses));
  }

  @Test
  public void testNotificationWithIdIsInvalid() throws Exception {
    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"notifications/initialized\"}");
    assertInvalid(request, 5);
  }

  @Test
  public void testNotificationWithNullIdIsInvalid() throws Exception {
    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":null,\"method\":\"notifications/initialized\"}");
    assertInvalid(request, null);
  }

  @Test
  public void testErrorResponseIncludesNullIdWhenRequestIdUnknown() {
    McpHttpResponse response = responses.error(null, McpJsonRpc.PARSE_ERROR,
        McpJsonRpc.PARSE_ERROR_MESSAGE);
    JsonNode body = response.body();
    assertTrue(body.has("id"));
    assertTrue(body.get("id").isNull());
    assertEquals(McpJsonRpc.PARSE_ERROR, body.get("error").get("code").asInt());
  }

  @Test
  public void testIsNotification() {
    assertTrue(McpJsonRpcValidator.isNotification("notifications/initialized"));
    assertTrue(McpJsonRpcValidator.isNotification("notifications/tools/list_changed"));
  }

  private void assertInvalid(JsonNode request, Integer expectedId) {
    McpHttpResponse response = McpJsonRpcValidator.validate(request, responses);
    JsonNode body = response.body();
    assertEquals(McpJsonRpc.INVALID_REQUEST, body.get("error").get("code").asInt());
    if (expectedId == null) {
      assertTrue(body.has("id"));
      assertTrue(body.get("id").isNull());
    } else {
      assertEquals(expectedId.intValue(), body.get("id").asInt());
    }
  }
}
