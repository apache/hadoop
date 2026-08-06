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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mcp.McpSchema.CallToolResult;
import org.apache.hadoop.mcp.McpSchema.ServerCapabilities;
import org.apache.hadoop.mcp.McpSchema.TextContent;
import org.apache.hadoop.mcp.McpSchema.Tool;
import org.apache.hadoop.mcp.McpServer.RegisteredTool;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Transport-neutral MCP JSON-RPC request handler.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class McpRequestHandler {

  public static final String SESSION_HEADER = "MCP-Session-Id";

  private static final String DEFAULT_PROTOCOL_VERSION = "2025-06-18";
  private static final List<String> SUPPORTED_PROTOCOL_VERSIONS = List.of(
      "2024-11-05",
      "2025-03-26",
      DEFAULT_PROTOCOL_VERSION);

  private final ObjectMapper objectMapper;
  private final String serverName;
  private final String serverVersion;
  private final ServerCapabilities capabilities;
  private final Map<String, RegisteredTool> tools;

  McpRequestHandler(ObjectMapper objectMapper, String serverName, String serverVersion,
      ServerCapabilities capabilities, Map<String, RegisteredTool> tools) {
    this.objectMapper = objectMapper;
    this.serverName = serverName;
    this.serverVersion = serverVersion;
    this.capabilities = capabilities;
    this.tools = Collections.unmodifiableMap(new LinkedHashMap<>(tools));
  }

  public McpHttpResponse handle(JsonNode requestNode) {
    return handle(requestNode, new McpCallContext(null));
  }

  public McpHttpResponse parseErrorResponse() {
    return errorResponse(null, McpJsonRpc.PARSE_ERROR, McpJsonRpc.PARSE_ERROR_MESSAGE);
  }

  public McpHttpResponse requestBodyTooLargeResponse() {
    return errorResponse(null, McpJsonRpc.INVALID_REQUEST,
        McpJsonRpc.REQUEST_BODY_TOO_LARGE_MESSAGE);
  }

  public McpHttpResponse handle(JsonNode requestNode, McpCallContext context) {
    if (requestNode == null || requestNode.isNull() || !requestNode.isObject()) {
      return errorResponse(null, McpJsonRpc.INVALID_REQUEST, McpJsonRpc.INVALID_REQUEST_MESSAGE);
    }

    JsonNode methodNode = requestNode.get("method");
    if (methodNode == null || methodNode.isNull()) {
      return errorResponse(requestNode.get("id"), McpJsonRpc.INVALID_REQUEST,
          McpJsonRpc.INVALID_REQUEST_MESSAGE);
    }

    String method = methodNode.asText();
    if (method.startsWith("notifications/")) {
      return McpHttpResponse.notification();
    }

    JsonNode idNode = requestNode.get("id");
    JsonNode paramsNode = requestNode.get("params");
    Map<String, Object> params = paramsNode == null || paramsNode.isNull()
        ? Collections.emptyMap()
        : objectMapper.convertValue(paramsNode, Map.class);

    switch (method) {
    case "initialize":
      return initializeResponse(idNode, params);
    case "tools/list":
      return successResponse(idNode, buildToolsListResult());
    case "tools/call":
      return toolsCallResponse(idNode, params, context);
    default:
      return errorResponse(idNode, McpJsonRpc.METHOD_NOT_FOUND,
          "Method not found: " + method);
    }
  }

  private McpHttpResponse initializeResponse(JsonNode idNode, Map<String, Object> params) {
    String requestedVersion = String.valueOf(
        params.getOrDefault("protocolVersion", DEFAULT_PROTOCOL_VERSION));
    String negotiatedVersion = SUPPORTED_PROTOCOL_VERSIONS.contains(requestedVersion)
        ? requestedVersion
        : DEFAULT_PROTOCOL_VERSION;

    Map<String, Object> result = new HashMap<>();
    result.put("protocolVersion", negotiatedVersion);
    result.put("capabilities", capabilities.toMap());
    Map<String, Object> serverInfo = new HashMap<>();
    serverInfo.put("name", serverName);
    serverInfo.put("version", serverVersion);
    result.put("serverInfo", serverInfo);

    Map<String, String> headers = new HashMap<>();
    headers.put(SESSION_HEADER, UUID.randomUUID().toString());
    return successResponse(idNode, result, headers);
  }

  private Map<String, Object> buildToolsListResult() {
    List<Map<String, Object>> toolList = new ArrayList<>();
    for (RegisteredTool registeredTool : tools.values()) {
      Tool tool = registeredTool.tool();
      Map<String, Object> toolMap = new LinkedHashMap<>();
      toolMap.put("name", tool.name());
      toolMap.put("description", tool.description());
      toolMap.put("inputSchema", tool.inputSchema());
      toolList.add(toolMap);
    }
    Map<String, Object> result = new HashMap<>();
    result.put("tools", toolList);
    return result;
  }

  @SuppressWarnings("unchecked")
  private McpHttpResponse toolsCallResponse(JsonNode idNode, Map<String, Object> params,
      McpCallContext context) {
    Object nameObject = params.get("name");
    if (!(nameObject instanceof String)) {
      return errorResponse(idNode, McpJsonRpc.INVALID_PARAMS, "Missing tool name");
    }

    RegisteredTool registeredTool = tools.get(nameObject);
    if (registeredTool == null) {
      return errorResponse(idNode, McpJsonRpc.INVALID_PARAMS, "Unknown tool: " + nameObject);
    }

    Object argumentsObject = params.get("arguments");
    Map<String, Object> arguments = argumentsObject instanceof Map
        ? (Map<String, Object>) argumentsObject
        : Collections.emptyMap();

    CallToolResult callResult = registeredTool.call(context, arguments);
    return successResponse(idNode, toResultMap(callResult));
  }

  private static Map<String, Object> toResultMap(CallToolResult callResult) {
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

  private McpHttpResponse successResponse(JsonNode idNode, Object result) {
    return successResponse(idNode, result, Collections.emptyMap());
  }

  private McpHttpResponse successResponse(JsonNode idNode, Object result,
      Map<String, String> headers) {
    ObjectNode response = objectMapper.createObjectNode();
    response.put("jsonrpc", McpJsonRpc.VERSION);
    if (idNode != null && !idNode.isNull()) {
      response.set("id", idNode);
    }
    response.set("result", objectMapper.valueToTree(result));
    return McpHttpResponse.ok(response, headers);
  }

  private McpHttpResponse errorResponse(JsonNode idNode, int code, String message) {
    ObjectNode response = objectMapper.createObjectNode();
    response.put("jsonrpc", McpJsonRpc.VERSION);
    if (idNode != null && !idNode.isNull()) {
      response.set("id", idNode);
    }
    ObjectNode error = objectMapper.createObjectNode();
    error.put("code", code);
    error.put("message", message);
    response.set("error", error);
    return McpHttpResponse.ok(response, Collections.emptyMap());
  }
}
