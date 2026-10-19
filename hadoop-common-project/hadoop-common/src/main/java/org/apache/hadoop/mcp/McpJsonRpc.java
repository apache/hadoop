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

/**
 * JSON-RPC 2.0 constants used by the MCP HTTP transport.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class McpJsonRpc {

  public static final String VERSION = "2.0";

  /** MCP protocol version implemented by this server. */
  public static final String PROTOCOL_VERSION = "2025-06-18";

  public static final String PROTOCOL_VERSION_HEADER = "MCP-Protocol-Version";
  public static final String ORIGIN_HEADER = "Origin";

  /** JSON-RPC 2.0 parse error. */
  public static final int PARSE_ERROR = -32700;
  /** JSON-RPC 2.0 invalid request. */
  public static final int INVALID_REQUEST = -32600;
  /** JSON-RPC 2.0 method not found. */
  public static final int METHOD_NOT_FOUND = -32601;
  /** JSON-RPC 2.0 invalid params. */
  public static final int INVALID_PARAMS = -32602;
  /** Implementation-defined server error: the session limit has been reached. */
  public static final int SERVER_ERROR_CAPACITY_LIMIT = -32000;
  /** Implementation-defined server error: the session has not completed initialization. */
  public static final int SERVER_ERROR_SESSION_NOT_READY = -32001;
  /** Implementation-defined server error: the session's tool-call rate limit was reached. */
  public static final int SERVER_ERROR_RATE_LIMITED = -32002;

  public static final String PARSE_ERROR_MESSAGE = "Parse error";
  public static final String INVALID_REQUEST_MESSAGE = "Invalid Request";
  public static final String REQUEST_BODY_TOO_LARGE_MESSAGE = "Request body too large";
  public static final String SESSION_REQUIRED_MESSAGE =
      McpRequestHandler.SESSION_HEADER + " header is required";
  public static final String SESSION_NOT_FOUND_MESSAGE = "Unknown or expired MCP session";
  public static final String PROTOCOL_VERSION_REQUIRED_MESSAGE =
      PROTOCOL_VERSION_HEADER + " header is required";
  public static final String UNSUPPORTED_PROTOCOL_VERSION_MESSAGE =
      "Unsupported " + PROTOCOL_VERSION_HEADER;
  public static final String ORIGIN_NOT_ALLOWED_MESSAGE = "Origin not allowed";
  public static final String RATE_LIMITED_MESSAGE = "Tool call rate limit exceeded";
  public static final String DUPLICATE_REQUEST_ID_MESSAGE =
      "Request id has already been used in this session";
  public static final String REQUEST_ID_LIMIT_MESSAGE =
      "MCP session has used its maximum number of requests";
  public static final String SESSION_NOT_READY_MESSAGE =
      "Session has not completed initialization";
  public static final String CAPACITY_LIMIT_MESSAGE =
      "Server has reached the maximum number of concurrent MCP sessions";

  /** Maximum MCP JSON-RPC request body size accepted by {@link McpHttpServlet}. */
  public static final int MAX_REQUEST_BODY_BYTES = 1024 * 1024;

  private McpJsonRpc() {
  }
}
