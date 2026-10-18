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

import java.io.Closeable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Ticker;
import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.util.Timer;

/**
 * Bounded, idle-expiring store of {@link McpSession}s keyed by session id.
 *
 * <p>A session expires after {@code sessionTimeoutMs} without being looked up. Expiry is
 * evaluated lazily, so an expired session may remain in memory until the next store
 * operation, but {@link #get(String)} never returns one.
 *
 * <p>Thread-safe.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class McpSessionStore implements Closeable {

  private final int maxSessions;
  private final Timer timer;
  private final int toolCallLimit;
  private final long toolCallWindowMs;
  private final Cache<String, McpSession> sessions;

  /** Creates a store whose sessions have no tool-call rate limit. */
  @VisibleForTesting
  McpSessionStore(long sessionTimeoutMs, int maxSessions, Timer timer) {
    this(sessionTimeoutMs, maxSessions, timer, 0, 0);
  }

  /**
   * @param toolCallLimit tool calls allowed per session within one window; 0 or less
   *     disables the limit
   * @param toolCallWindowMs length of the tool-call rate-limit window
   */
  McpSessionStore(long sessionTimeoutMs, int maxSessions, Timer timer, int toolCallLimit,
      long toolCallWindowMs) {
    if (sessionTimeoutMs <= 0) {
      throw new IllegalArgumentException("sessionTimeoutMs must be positive: " + sessionTimeoutMs);
    }
    if (maxSessions <= 0) {
      throw new IllegalArgumentException("maxSessions must be positive: " + maxSessions);
    }
    if (toolCallLimit > 0 && toolCallWindowMs <= 0) {
      throw new IllegalArgumentException("toolCallWindowMs must be positive: "
          + toolCallWindowMs);
    }
    this.maxSessions = maxSessions;
    this.timer = timer;
    this.toolCallLimit = toolCallLimit;
    this.toolCallWindowMs = toolCallWindowMs;
    this.sessions = CacheBuilder.newBuilder()
        .expireAfterAccess(sessionTimeoutMs, TimeUnit.MILLISECONDS)
        .ticker(new Ticker() {
          @Override
          public long read() {
            return timer.monotonicNowNanos();
          }
        })
        .build();
  }

  /**
   * Creates a session in the {@link McpSessionState#INITIALIZING} state.
   *
   * @return the new session, or null if the store is at capacity
   */
  McpSession create(String negotiatedProtocolVersion, Map<String, Object> clientInfo,
      Map<String, Object> clientCapabilities) {
    McpSession session = new McpSession(UUID.randomUUID().toString(),
        negotiatedProtocolVersion, clientInfo, clientCapabilities, timer.now(),
        new McpRateLimiter(toolCallLimit, toolCallWindowMs));
    synchronized (this) {
      // Drop expired sessions first so they don't count against capacity.
      sessions.cleanUp();
      if (sessions.size() >= maxSessions) {
        return null;
      }
      sessions.put(session.getId(), session);
    }
    return session;
  }

  /**
   * Looks up a session and resets its idle timer.
   *
   * @return the session, or null if it is unknown, expired or terminated
   */
  McpSession get(String sessionId) {
    if (sessionId == null) {
      return null;
    }
    return sessions.getIfPresent(sessionId);
  }

  /**
   * Removes a session.
   *
   * @return the removed session, or null if no unexpired session had this id
   */
  McpSession remove(String sessionId) {
    if (sessionId == null) {
      return null;
    }
    return sessions.asMap().remove(sessionId);
  }

  /** @return the number of sessions, which may include expired ones not yet cleaned up */
  int size() {
    return (int) sessions.size();
  }

  /** Removes expired sessions now instead of waiting for the next store operation. */
  @VisibleForTesting
  void cleanUp() {
    sessions.cleanUp();
  }

  @Override
  public void close() {
    sessions.invalidateAll();
  }
}
