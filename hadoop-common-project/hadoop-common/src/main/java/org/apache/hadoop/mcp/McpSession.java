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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

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

  McpSession(String id, String negotiatedProtocolVersion, Map<String, Object> clientInfo,
      Map<String, Object> clientCapabilities, long creationTimeMs,
      McpRateLimiter mcpRateLimiter) {
    this.id = id;
    this.negotiatedProtocolVersion = negotiatedProtocolVersion;
    this.clientInfo = clientInfo;
    this.clientCapabilities = clientCapabilities;
    this.creationTimeMs = creationTimeMs;
    this.mcpRateLimiter = mcpRateLimiter;
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
}
