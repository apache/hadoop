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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * An MCP session, created by {@code initialize} and identified by the
 * {@code Mcp-Session-Id} header.
 *
 * <p>State changes are package-private, so tool handlers that receive a session can read it
 * but cannot change it. Idle time is tracked by {@link McpSessionStore}, not here.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class McpSession {

  private final String id;
  private final String negotiatedProtocolVersion;
  private final Map<String, Object> clientInfo;
  private final Map<String, Object> clientCapabilities;
  private final long creationTimeMs;
  private final AtomicReference<McpSessionState> state =
      new AtomicReference<>(McpSessionState.INITIALIZING);
  private final McpRateLimiter mcpRateLimiter;
  private final int maxRequestIds;
  private final Set<String> usedRequestIds = new HashSet<>();

  /**
   * @param maxRequestIds number of request ids the session may use; 0 or less is unlimited
   */
  McpSession(String id, String negotiatedProtocolVersion, Map<String, Object> clientInfo,
      Map<String, Object> clientCapabilities, long creationTimeMs,
      McpRateLimiter mcpRateLimiter, int maxRequestIds) {
    this.id = id;
    this.negotiatedProtocolVersion = negotiatedProtocolVersion;
    this.clientInfo = clientInfo;
    this.clientCapabilities = clientCapabilities;
    this.creationTimeMs = creationTimeMs;
    this.mcpRateLimiter = mcpRateLimiter;
    this.maxRequestIds = maxRequestIds;
  }

  public String getId() {
    return id;
  }

  public String getNegotiatedProtocolVersion() {
    return negotiatedProtocolVersion;
  }

  /** @return the {@code clientInfo} sent with {@code initialize}; never null */
  public Map<String, Object> getClientInfo() {
    return clientInfo;
  }

  /** @return the client {@code capabilities} sent with {@code initialize}; never null */
  public Map<String, Object> getClientCapabilities() {
    return clientCapabilities;
  }

  /** @return wall-clock creation time, in milliseconds since the epoch */
  public long getCreationTimeMs() {
    return creationTimeMs;
  }

  public McpSessionState getState() {
    return state.get();
  }

  /**
   * Moves the session from {@link McpSessionState#INITIALIZING} to
   * {@link McpSessionState#READY}.
   *
   * @return true if the state changed; false if the session was already ready
   */
  boolean markReady() {
    return state.compareAndSet(McpSessionState.INITIALIZING, McpSessionState.READY);
  }

  McpRateLimiter getMcpRateLimiter() {
    return mcpRateLimiter;
  }

  /**
   * Records a request id, which the specification forbids reusing within a session.
   * {@code 1} and {@code "1"} are different ids.
   *
   * @throws RequestIdLimitReachedException if the session has used its maximum number of ids,
   *     which ends the session
   * @throws RequestRejectedException with 400 if the id was already used in this session
   */
  synchronized void recordRequestId(JsonNode idNode) throws RequestRejectedException {
    if (maxRequestIds > 0 && usedRequestIds.size() >= maxRequestIds) {
      throw new RequestIdLimitReachedException();
    }
    // Differentiating integers and numbers in string format.
    String key = idNode.isTextual()
        ? "s:" + idNode.textValue()
        : "n:" + idNode.bigIntegerValue();
    if (!usedRequestIds.add(key)) {
      throw new RequestRejectedException(HttpServletResponse.SC_BAD_REQUEST,
          McpJsonRpc.DUPLICATE_REQUEST_ID_MESSAGE);
    }
  }
}
