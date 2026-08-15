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
import org.apache.hadoop.mcp.McpSchema.ServerCapabilities;
import org.apache.hadoop.mcp.McpSchema.Tool;
import org.apache.hadoop.mcp.McpServer.RegisteredTool;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Transport-neutral MCP JSON-RPC request handler.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class McpRequestHandler {

  public static final String SESSION_HEADER = "Mcp-Session-Id";

  public static final String METHOD_INITIALIZE = "initialize";
  public static final String METHOD_TOOLS_LIST = "tools/list";
  public static final String METHOD_TOOLS_CALL = "tools/call";

  private static final TypeReference<Map<String, Object>> PARAMS_TYPE = new TypeReference<>() {};

  private final ObjectMapper objectMapper;
  private final String serverName;
  private final String serverVersion;
  private final ServerCapabilities capabilities;
  private final Map<String, RegisteredTool> tools;
  private final McpJsonRpcResponses jsonRpcResponses;

  McpRequestHandler(ObjectMapper objectMapper, String serverName, String serverVersion,
      ServerCapabilities capabilities, Map<String, RegisteredTool> tools) {
    this.objectMapper = objectMapper;
    this.serverName = serverName;
    this.serverVersion = serverVersion;
    this.capabilities = capabilities;
    this.tools = Collections.unmodifiableMap(new LinkedHashMap<>(tools));
    this.jsonRpcResponses = new McpJsonRpcResponses(objectMapper);
  }

  public McpHttpResponse handle(JsonNode requestNode) {
    return handle(requestNode, new McpCallContext(null));
  }

  public McpHttpResponse parseErrorResponse() {
    return jsonRpcResponses.error(null, McpJsonRpc.PARSE_ERROR, McpJsonRpc.PARSE_ERROR_MESSAGE);
  }

  public McpHttpResponse requestBodyTooLargeResponse() {
    return jsonRpcResponses.error(null, McpJsonRpc.INVALID_REQUEST,
        McpJsonRpc.REQUEST_BODY_TOO_LARGE_MESSAGE);
  }

  public McpHttpResponse handle(JsonNode requestNode, McpCallContext context) {
    McpHttpResponse error = McpJsonRpcValidator.validate(requestNode, jsonRpcResponses);
    if (error != null) {
      return error;
    }

    String method = requestNode.get("method").asText();
    if (McpJsonRpcValidator.isNotification(method)) {
      return McpHttpResponse.notification();
    }

    JsonNode idNode = requestNode.get("id");
    JsonNode paramsNode = requestNode.get("params");
    if (paramsNode != null && !paramsNode.isNull() && !paramsNode.isObject()) {
      return jsonRpcResponses.error(idNode, McpJsonRpc.INVALID_PARAMS,
          "params must be a JSON object");
    }
    Map<String, Object> params = paramsNode == null || paramsNode.isNull()
        ? Collections.emptyMap()
        : objectMapper.convertValue(paramsNode, PARAMS_TYPE);

    switch (method) {
    case METHOD_INITIALIZE:
      return initializeResponse(idNode);
    case METHOD_TOOLS_LIST:
      return jsonRpcResponses.success(idNode, buildToolsListResult());
    case METHOD_TOOLS_CALL:
      return toolsCallResponse(idNode, params, context);
    default:
      return jsonRpcResponses.error(idNode, McpJsonRpc.METHOD_NOT_FOUND,
          "Method not found: " + method);
    }
  }

  private McpHttpResponse initializeResponse(JsonNode idNode) {
    Map<String, Object> result = new HashMap<>();
    result.put("protocolVersion", McpJsonRpc.PROTOCOL_VERSION);
    result.put("capabilities", capabilities.toMap());
    Map<String, Object> serverInfo = new HashMap<>();
    serverInfo.put("name", serverName);
    serverInfo.put("version", serverVersion);
    result.put("serverInfo", serverInfo);

    Map<String, String> headers = new HashMap<>();
    headers.put(SESSION_HEADER, UUID.randomUUID().toString());
    return jsonRpcResponses.success(idNode, result, headers);
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

  private McpHttpResponse toolsCallResponse(JsonNode idNode, Map<String, Object> params,
      McpCallContext context) {
    Object nameObject = params.get("name");
    if (!(nameObject instanceof String)) {
      return jsonRpcResponses.error(idNode, McpJsonRpc.INVALID_PARAMS, "Missing tool name");
    }

    RegisteredTool registeredTool = tools.get(nameObject);
    if (registeredTool == null) {
      return jsonRpcResponses.error(idNode, McpJsonRpc.INVALID_PARAMS,
          "Unknown tool: " + nameObject);
    }

    Object argumentsObject = params.get("arguments");
    Map<String, Object> arguments;
    if (argumentsObject == null) {
      arguments = Collections.emptyMap();
    } else if (!(argumentsObject instanceof Map)) {
      return jsonRpcResponses.error(idNode, McpJsonRpc.INVALID_PARAMS,
          "arguments must be a JSON object");
    } else {
      arguments = objectMapper.convertValue(
          objectMapper.valueToTree(argumentsObject), PARAMS_TYPE);
    }

    final McpSchema.CallToolResult callResult;
    try {
      callResult = registeredTool.call(context, arguments);
    } catch (Exception e) {
      return jsonRpcResponses.success(idNode, McpJsonRpcResponses.toResultMap(
          McpSchema.CallToolResult.error(e.getMessage())));
    }
    return jsonRpcResponses.success(idNode, McpJsonRpcResponses.toResultMap(callResult));
  }
}
