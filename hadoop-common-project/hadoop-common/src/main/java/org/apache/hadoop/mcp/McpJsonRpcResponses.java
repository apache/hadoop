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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mcp.McpSchema.CallToolResult;
import org.apache.hadoop.mcp.McpSchema.TextContent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Builds JSON-RPC 2.0 HTTP responses for MCP.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class McpJsonRpcResponses {

  private final ObjectMapper objectMapper;

  McpJsonRpcResponses(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  McpHttpResponse success(JsonNode idNode, Object result) {
    return success(idNode, result, Collections.emptyMap());
  }

  McpHttpResponse success(JsonNode idNode, Object result, Map<String, String> headers) {
    ObjectNode response = objectMapper.createObjectNode();
    response.put("jsonrpc", McpJsonRpc.VERSION);
    if (idNode != null && !idNode.isNull()) {
      response.set("id", idNode);
    }
    response.set("result", objectMapper.valueToTree(result));
    return McpHttpResponse.ok(response, headers);
  }

  McpHttpResponse error(JsonNode idNode, int code, String message) {
    ObjectNode response = objectMapper.createObjectNode();
    response.put("jsonrpc", McpJsonRpc.VERSION);
    setErrorResponseId(response, idNode);
    ObjectNode error = objectMapper.createObjectNode();
    error.put("code", code);
    error.put("message", message);
    response.set("error", error);
    return McpHttpResponse.ok(response, Collections.emptyMap());
  }

  private void setErrorResponseId(ObjectNode response, JsonNode idNode) {
    if (idNode != null && !idNode.isNull()) {
      response.set("id", idNode);
    } else {
      response.putNull("id");
    }
  }

  static Map<String, Object> toResultMap(CallToolResult callResult) {
    List<Map<String, Object>> content = new ArrayList<>();
    for (TextContent textContent : callResult.content()) {
      Map<String, Object> item = new LinkedHashMap<>();
      item.put("type", textContent.type());
      item.put("text", textContent.text());
      content.add(item);
    }
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("content", content);
    result.put("isError", callResult.isError());
    return result;
  }
}
