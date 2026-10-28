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

import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.mcp.McpSchema.ServerCapabilities;
import org.apache.hadoop.mcp.McpSchema.Tool;
import org.apache.hadoop.mcp.McpServer.RegisteredTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Transport-neutral MCP JSON-RPC request handler.
 *
 * <p>Owns the session lifecycle: {@code initialize} creates a session,
 * {@code notifications/initialized} makes it ready, and every other request must name a
 * live session in the {@code Mcp-Session-Id} header. Header values are read from
 * {@link McpCallContext}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class McpRequestHandler {

  private static final Logger LOG = LoggerFactory.getLogger(McpRequestHandler.class);

  public static final String SESSION_HEADER = "Mcp-Session-Id";

  public static final String METHOD_INITIALIZE = "initialize";
  public static final String METHOD_NOTIFICATIONS_INITIALIZED = "notifications/initialized";
  public static final String METHOD_PING = "ping";
  public static final String METHOD_TOOLS_LIST = "tools/list";
  public static final String METHOD_TOOLS_CALL = "tools/call";

  private static final String TOOL_EXECUTION_FAILED_MESSAGE = "Tool execution failed";

  private static final TypeReference<Map<String, Object>> PARAMS_TYPE = new TypeReference<>() {};

  private final ObjectMapper objectMapper;
  private final String serverName;
  private final String serverVersion;
  private final ServerCapabilities capabilities;
  private final Map<String, RegisteredTool> tools;
  private final McpJsonRpcResponses jsonRpcResponses;
  private final McpSessionStore sessionStore;
  private final McpOutputSanitizer outputSanitizer;

  McpRequestHandler(ObjectMapper objectMapper, String serverName, String serverVersion,
      ServerCapabilities capabilities, Map<String, RegisteredTool> tools,
      McpSessionStore sessionStore, McpOutputSanitizer outputSanitizer) {
    this.objectMapper = objectMapper;
    this.serverName = serverName;
    this.serverVersion = serverVersion;
    this.capabilities = capabilities;
    this.tools = Collections.unmodifiableMap(new LinkedHashMap<>(tools));
    this.jsonRpcResponses = new McpJsonRpcResponses(objectMapper);
    this.sessionStore = sessionStore;
    this.outputSanitizer = outputSanitizer;
  }

  public McpHttpResponse parseErrorResponse() {
    return jsonRpcResponses.error(null, McpJsonRpc.PARSE_ERROR, McpJsonRpc.PARSE_ERROR_MESSAGE);
  }

  public McpHttpResponse requestBodyTooLargeResponse() {
    return jsonRpcResponses.transportError(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE,
        McpJsonRpc.REQUEST_BODY_TOO_LARGE_MESSAGE);
  }

  private McpHttpResponse invalidParamsResponse(JsonNode idNode) {
    return jsonRpcResponses.error(idNode, McpJsonRpc.INVALID_PARAMS,
        "params must be a JSON object");
  }

  private McpHttpResponse sessionNotReadyResponse(JsonNode idNode) {
    return jsonRpcResponses.error(idNode, McpJsonRpc.SERVER_ERROR_SESSION_NOT_READY,
        McpJsonRpc.SESSION_NOT_READY_MESSAGE);
  }

  private McpHttpResponse sessionNotFoundResponse() {
    return jsonRpcResponses.transportError(HttpServletResponse.SC_NOT_FOUND,
        McpJsonRpc.SESSION_NOT_FOUND_MESSAGE);
  }

  public McpHttpResponse originNotAllowedResponse() {
    return jsonRpcResponses.transportError(HttpServletResponse.SC_FORBIDDEN,
        McpJsonRpc.ORIGIN_NOT_ALLOWED_MESSAGE);
  }

  public McpHttpResponse handle(JsonNode requestNode, McpCallContext context) {
    McpHttpResponse error = McpJsonRpcValidator.validate(requestNode, jsonRpcResponses);
    if (error != null) {
      return error;
    }
    String method = requestNode.get("method").asText();
    JsonNode idNode = requestNode.get("id");
    JsonNode paramsNode = requestNode.get("params");
    boolean isNotification = McpJsonRpcValidator.isJsonRpcNotification(requestNode);

    try {
      // initialize is the first contact, so it carries no session or protocol-version header.
      if (!isNotification && METHOD_INITIALIZE.equals(method)) {
        Map<String, Object> params = toParams(paramsNode);
        if (params == null) {
          return invalidParamsResponse(idNode);
        }
        McpSession session = createSession(params);
        if (session == null) {
          return jsonRpcResponses.error(idNode, McpJsonRpc.SERVER_ERROR_CAPACITY_LIMIT,
              McpJsonRpc.CAPACITY_LIMIT_MESSAGE);
        }
        // The initialize id is the first id used in the new session.
        session.recordRequestId(idNode);
        return initializeResponse(idNode, session);
      }

      checkProtocolVersion(context);
      McpSession session = resolveSession(context);
      if (!isNotification) {
        session.recordRequestId(idNode);
      }
      return dispatch(method, isNotification, idNode, paramsNode, context, session);
    } catch (RequestRejectedException e) {
      return jsonRpcResponses.transportError(e.getHttpStatus(), e.getErrorMessage());
    }
  }

  /** Dispatches a request or notification that belongs to a session. */
  private McpHttpResponse dispatch(String method, boolean notification, JsonNode idNode,
      JsonNode paramsNode, McpCallContext context, McpSession session) {
    if (notification) {
      if (METHOD_NOTIFICATIONS_INITIALIZED.equals(method)) {
        session.markReady();
      }
      return McpHttpResponse.notification();
    }

    Map<String, Object> params = toParams(paramsNode);
    if (params == null) {
      return invalidParamsResponse(idNode);
    }

    switch (method) {
    case METHOD_PING:
      return jsonRpcResponses.success(idNode, Collections.emptyMap());
    case METHOD_TOOLS_LIST:
      if (session.getState() != McpSessionState.READY) {
        return sessionNotReadyResponse(idNode);
      }
      return jsonRpcResponses.success(idNode, buildToolsListResult());
    case METHOD_TOOLS_CALL:
      if (session.getState() != McpSessionState.READY) {
        return sessionNotReadyResponse(idNode);
      }
      if (!session.getMcpRateLimiter().tryAcquire()) {
        return jsonRpcResponses.error(idNode, McpJsonRpc.SERVER_ERROR_RATE_LIMITED,
            McpJsonRpc.RATE_LIMITED_MESSAGE);
      }
      return toolsCallResponse(idNode, params, context);
    default:
      return jsonRpcResponses.error(idNode, McpJsonRpc.METHOD_NOT_FOUND,
          "Method not found: " + method);
    }
  }

  /**
   * Handles an HTTP {@code DELETE}, which terminates the session named in the
   * {@code Mcp-Session-Id} header.
   *
   * @return 204 if the session was terminated, otherwise a 400 or 404 rejection
   */
  McpHttpResponse handleDelete(McpCallContext context) {
    try {
      checkProtocolVersion(context);
      if (sessionStore.remove(resolveSessionId(context)) == null) {
        return sessionNotFoundResponse();
      }
      return McpHttpResponse.createResponse(HttpServletResponse.SC_NO_CONTENT,
          Collections.emptyMap());
    } catch (RequestRejectedException e) {
      return jsonRpcResponses.transportError(e.getHttpStatus(), e.getErrorMessage());
    }
  }

  void closeSessionStore() {
    sessionStore.close();
  }

  @VisibleForTesting
  McpSessionStore getSessionStore() {
    return sessionStore;
  }

  /**
   * Rejects a post-initialize request whose {@code MCP-Protocol-Version} header is missing
   * or unsupported. The specification requires clients to send it on every such request.
   */
  private void checkProtocolVersion(McpCallContext context) throws RequestRejectedException {
    String protocolVersion = context.getProtocolVersionHeader();
    if (protocolVersion == null) {
      throw new RequestRejectedException(HttpServletResponse.SC_BAD_REQUEST,
          McpJsonRpc.PROTOCOL_VERSION_REQUIRED_MESSAGE);
    }
    if (!McpJsonRpc.PROTOCOL_VERSION.equals(protocolVersion)) {
      throw new RequestRejectedException(HttpServletResponse.SC_BAD_REQUEST,
          McpJsonRpc.UNSUPPORTED_PROTOCOL_VERSION_MESSAGE);
    }
  }

  /**
   * @return the value of the {@code Mcp-Session-Id} header
   * @throws RequestRejectedException with 400 if the header is missing or empty
   */
  private String resolveSessionId(McpCallContext context) throws RequestRejectedException {
    String sessionId = context.getSessionIdHeader();
    if (sessionId == null || sessionId.isEmpty()) {
      throw new RequestRejectedException(HttpServletResponse.SC_BAD_REQUEST,
          McpJsonRpc.SESSION_REQUIRED_MESSAGE);
    }
    return sessionId;
  }

  /**
   * @return the live session named in the {@code Mcp-Session-Id} header
   * @throws RequestRejectedException with 400 if the header is missing, or 404 if the
   *     session is unknown or expired
   */
  private McpSession resolveSession(McpCallContext context) throws RequestRejectedException {
    String sessionId = resolveSessionId(context);
    McpSession session = sessionStore.get(sessionId);
    if (session == null) {
      throw new RequestRejectedException(HttpServletResponse.SC_NOT_FOUND,
          McpJsonRpc.SESSION_NOT_FOUND_MESSAGE);
    }
    return session;
  }

  /**
   * Creates a session for an {@code initialize} request. The server supports a single
   * protocol version, so that is the negotiated version whatever the client requested; a
   * client that cannot use it should disconnect.
   *
   * @return the new session, or null if the store is at capacity
   */
  private McpSession createSession(Map<String, Object> params) {
    return sessionStore.createSession(McpJsonRpc.PROTOCOL_VERSION,
        asMap(params.get("clientInfo")), asMap(params.get("capabilities")));
  }

  /** Builds the {@code initialize} result for a newly created session. */
  private McpHttpResponse initializeResponse(JsonNode idNode, McpSession session) {
    Map<String, Object> result = new HashMap<>();
    result.put("protocolVersion", session.getNegotiatedProtocolVersion());
    result.put("capabilities", capabilities.toMap());
    Map<String, Object> serverInfo = new HashMap<>();
    serverInfo.put("name", serverName);
    serverInfo.put("version", serverVersion);
    result.put("serverInfo", serverInfo);

    Map<String, String> headers = new HashMap<>();
    headers.put(SESSION_HEADER, session.getId());
    return jsonRpcResponses.success(idNode, result, headers);
  }

  /** @return the params as a map, or {@code null} if they are present but not an object */
  private Map<String, Object> toParams(JsonNode paramsNode) {
    if (paramsNode == null) {
      return Collections.emptyMap();
    }
    if (!paramsNode.isObject()) {
      return null;
    }
    return objectMapper.convertValue(paramsNode, PARAMS_TYPE);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> asMap(Object value) {
    return value instanceof Map ? (Map<String, Object>) value : Collections.emptyMap();
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
    if (!params.containsKey("arguments")) {
      arguments = Collections.emptyMap();
    } else if (argumentsObject == null) {
      return jsonRpcResponses.error(idNode, McpJsonRpc.INVALID_PARAMS,
          "arguments must be a JSON object");
    } else if (!(argumentsObject instanceof Map)) {
      return jsonRpcResponses.error(idNode, McpJsonRpc.INVALID_PARAMS,
          "arguments must be a JSON object");
    } else {
      arguments = objectMapper.convertValue(
          objectMapper.valueToTree(argumentsObject), PARAMS_TYPE);
    }

    try {
      McpInputSchemaValidator.validateArguments(registeredTool.tool().inputSchema(), arguments);
    } catch (InvalidToolArgumentsException e) {
      return jsonRpcResponses.error(idNode, McpJsonRpc.INVALID_PARAMS,
          "Invalid arguments: " + e.getMessage());
    }

    String toolName = registeredTool.tool().name();
    McpSchema.CallToolResult callResult;
    try {
      callResult = registeredTool.call(context, arguments);
    } catch (Exception e) {
      LOG.warn("Execution of {} MCP tool failed", toolName, e);
      callResult = McpSchema.CallToolResult.error(TOOL_EXECUTION_FAILED_MESSAGE);
    }
    callResult = outputSanitizer.sanitize(toolName, callResult);
    return jsonRpcResponses.success(idNode, McpJsonRpcResponses.toResultMap(callResult));
  }
}
