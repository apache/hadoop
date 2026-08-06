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

  /** JSON-RPC 2.0 parse error. */
  public static final int PARSE_ERROR = -32700;
  /** JSON-RPC 2.0 invalid request. */
  public static final int INVALID_REQUEST = -32600;
  /** JSON-RPC 2.0 method not found. */
  public static final int METHOD_NOT_FOUND = -32601;
  /** JSON-RPC 2.0 invalid params. */
  public static final int INVALID_PARAMS = -32602;

  public static final String PARSE_ERROR_MESSAGE = "Parse error";
  public static final String INVALID_REQUEST_MESSAGE = "Invalid Request";
  public static final String REQUEST_BODY_TOO_LARGE_MESSAGE = "Request body too large";

  /** Maximum MCP JSON-RPC request body size accepted by {@link McpHttpServlet}. */
  public static final int MAX_REQUEST_BODY_BYTES = 1024 * 1024;

  private McpJsonRpc() {
  }
}
