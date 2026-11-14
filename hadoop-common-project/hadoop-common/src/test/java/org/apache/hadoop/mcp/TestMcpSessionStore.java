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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestMcpSessionStore {

  private static final long DEFAULT_TIMEOUT_MS = 60_000;
  /** Short idle timeout for the expiry tests. */
  private static final long SHORT_TIMEOUT_MS = 200;

  private McpSessionStore store;

  @BeforeEach
  public void setUp() {
    store = createStore(10, DEFAULT_TIMEOUT_MS);
  }

  /** A store without tool-call or request id limits, which these tests don't cover. */
  private static McpSessionStore createStore(int maxSessions, long timeoutMs) {
    return new McpSessionStore(timeoutMs, maxSessions, 0, 0, 0);
  }

  private static McpSession createSession(McpSessionStore sessionStore) {
    return sessionStore.createSession(McpJsonRpc.PROTOCOL_VERSION, Collections.emptyMap(),
        Collections.emptyMap());
  }

  @Test
  public void testCreateReturnsInitializingSession() {
    Map<String, Object> clientInfo = new HashMap<>();
    clientInfo.put("name", "test-client");
    Map<String, Object> capabilities = new HashMap<>();
    capabilities.put("roots", Collections.emptyMap());

    McpSession session = store.createSession(McpJsonRpc.PROTOCOL_VERSION, clientInfo, capabilities);

    assertNotNull(session);
    assertEquals(McpSessionState.INITIALIZING, session.getState());
    assertEquals(McpJsonRpc.PROTOCOL_VERSION, session.getNegotiatedProtocolVersion());
    assertEquals("test-client", session.getClientInfo().get("name"));
    assertTrue(session.getClientCapabilities().containsKey("roots"));
    assertSame(session, store.get(session.getId()));
  }

  @Test
  public void testSessionIdsAreUniqueVisibleAscii() {
    McpSession first = createSession(store);
    McpSession second = createSession(store);

    assertNotEquals(first.getId(), second.getId());
    // The spec requires visible ASCII (0x21-0x7E) in session ids.
    for (char c : first.getId().toCharArray()) {
      assertTrue(c >= 0x21 && c <= 0x7E, "invalid session id character: " + (int) c);
    }
  }

  @Test
  public void testGetUnknownOrNullIdReturnsNull() {
    assertNull(store.get("no-such-session"));
    assertNull(store.get(null));
  }

  @Test
  public void testRemoveSession() {
    McpSession session = createSession(store);

    assertSame(session, store.remove(session.getId()));
    assertNull(store.get(session.getId()));
    assertEquals(0, store.size());
  }

  @Test
  public void testRemoveUnknownOrNullIdReturnsNull() {
    assertNull(store.remove("no-such-session"));
    assertNull(store.remove(null));
  }

  @Test
  public void testCreateRejectedAtMaxCapacity() {
    McpSessionStore sessionStore = createStore(1, DEFAULT_TIMEOUT_MS);
    McpSession firstSession = sessionStore.createSession(McpJsonRpc.PROTOCOL_VERSION,
        Collections.emptyMap(), Collections.emptyMap());

    assertNotNull(firstSession);
    assertNull(sessionStore.createSession(McpJsonRpc.PROTOCOL_VERSION,
        Collections.emptyMap(), Collections.emptyMap()));
    // The existing session must survive the rejected attempt.
    assertSame(firstSession, sessionStore.get(firstSession.getId()));
  }

  @Test
  public void testRemoveFreesCapacity() {
    McpSessionStore sessionStore = createStore(1, DEFAULT_TIMEOUT_MS);
    McpSession firstSession = sessionStore.createSession(McpJsonRpc.PROTOCOL_VERSION,
        Collections.emptyMap(), Collections.emptyMap());

    assertSame(firstSession, sessionStore.remove(firstSession.getId()));
    assertNotNull(sessionStore.createSession(McpJsonRpc.PROTOCOL_VERSION,
        Collections.emptyMap(), Collections.emptyMap()));
  }

  @Test
  public void testIdleExpiry() throws Exception {
    McpSessionStore sessionStore = createStore(10, SHORT_TIMEOUT_MS);
    McpSession session = createSession(sessionStore);
    assertEquals(1, sessionStore.size());

    Thread.sleep(2 * SHORT_TIMEOUT_MS);

    assertNull(sessionStore.get(session.getId()));
    sessionStore.cleanUp();
    assertEquals(0, sessionStore.size());
  }

  @Test
  public void testExpiredSessionsDoNotCountAgainstCapacity() throws Exception {
    McpSessionStore sessionStore = createStore(1, SHORT_TIMEOUT_MS);
    createSession(sessionStore);

    Thread.sleep(2 * SHORT_TIMEOUT_MS);

    assertNotNull(createSession(sessionStore));
  }

  @Test
  public void testRemoveExpiredSessionReturnsNull() throws Exception {
    McpSessionStore sessionStore = createStore(10, SHORT_TIMEOUT_MS);
    McpSession session = createSession(sessionStore);

    Thread.sleep(2 * SHORT_TIMEOUT_MS);

    assertNull(sessionStore.remove(session.getId()));
  }

  @Test
  public void testCloseClearsStore() {
    McpSession session = createSession(store);

    store.close();
    assertNull(store.get(session.getId()));
    assertEquals(0, store.size());
  }

  @Test
  public void testInvalidArgumentsRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> new McpSessionStore(0, 10, 0, 0, 0));
    assertThrows(IllegalArgumentException.class,
        () -> new McpSessionStore(DEFAULT_TIMEOUT_MS, 0, 0, 0, 0));
    // A tool-call limit needs a positive window.
    assertThrows(IllegalArgumentException.class,
        () -> new McpSessionStore(DEFAULT_TIMEOUT_MS, 10, 1, 0, 0));
  }
}
