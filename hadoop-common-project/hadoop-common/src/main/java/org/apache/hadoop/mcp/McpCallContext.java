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

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Request-scoped context passed to MCP tool handlers.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class McpCallContext {

  private final HttpServletRequest request;
  private final String sessionIdHeader;
  private final String protocolVersionHeader;

  /**
   * @param request the HTTP request
   * @param sessionIdHeader value of the {@code Mcp-Session-Id} header
   * @param protocolVersionHeader value of the {@code MCP-Protocol-Version} header
   */
  public McpCallContext(HttpServletRequest request, String sessionIdHeader,
      String protocolVersionHeader) {
    this.request = request;
    this.sessionIdHeader = sessionIdHeader;
    this.protocolVersionHeader = protocolVersionHeader;
  }

  public HttpServletRequest getRequest() {
    return request;
  }

  String getSessionIdHeader() {
    return sessionIdHeader;
  }

  String getProtocolVersionHeader() {
    return protocolVersionHeader;
  }
}
