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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Validates JSON-RPC envelope fields required by MCP 2025-06-18.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class McpJsonRpcValidator {

  private McpJsonRpcValidator() {
  }

  /**
   * @return an error response when the envelope is invalid, otherwise {@code null}
   */
  static McpHttpResponse validate(JsonNode requestNode, McpJsonRpcResponses responses) {
    if (requestNode == null || requestNode.isNull() || !requestNode.isObject()) {
      return responses.error(null, McpJsonRpc.INVALID_REQUEST, McpJsonRpc.INVALID_REQUEST_MESSAGE);
    }

    JsonNode jsonrpcNode = requestNode.get("jsonrpc");
    if (jsonrpcNode == null || jsonrpcNode.isNull()
        || !jsonrpcNode.isTextual()
        || !McpJsonRpc.VERSION.equals(jsonrpcNode.asText())) {
      return responses.error(responseId(requestNode), McpJsonRpc.INVALID_REQUEST,
          McpJsonRpc.INVALID_REQUEST_MESSAGE);
    }

    JsonNode methodNode = requestNode.get("method");
    if (methodNode == null || methodNode.isNull() || !methodNode.isTextual()
        || methodNode.asText().isEmpty()) {
      return responses.error(responseId(requestNode), McpJsonRpc.INVALID_REQUEST,
          McpJsonRpc.INVALID_REQUEST_MESSAGE);
    }

    JsonNode paramsNode = requestNode.get("params");
    if (paramsNode != null && paramsNode.isNull()) {
      return responses.error(responseId(requestNode), McpJsonRpc.INVALID_REQUEST,
          McpJsonRpc.INVALID_REQUEST_MESSAGE);
    }

    if (isJsonRpcNotification(requestNode)) {
      return null;
    }

    if (!isValidRequestId(requestNode.get("id"))) {
      return responses.error(null, McpJsonRpc.INVALID_REQUEST, McpJsonRpc.INVALID_REQUEST_MESSAGE);
    }
    return null;
  }

  static boolean isJsonRpcNotification(JsonNode requestNode) {
    return !requestNode.has("id");
  }

  private static boolean isValidRequestId(JsonNode idNode) {
    return idNode != null && !idNode.isNull()
        && (idNode.isIntegralNumber() || idNode.isTextual());
  }

  /** Returns {@code id} when it is a valid request id, otherwise {@code null}. */
  private static JsonNode responseId(JsonNode requestNode) {
    JsonNode idNode = requestNode.get("id");
    return isValidRequestId(idNode) ? idNode : null;
  }
}
