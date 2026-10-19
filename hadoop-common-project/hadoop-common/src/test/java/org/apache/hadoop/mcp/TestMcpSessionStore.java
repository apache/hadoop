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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.util.FakeTimer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestMcpSessionStore {

  private static final long TIMEOUT_MS = 60_000;

  private FakeTimer timer;
  private McpSessionStore store;

  @BeforeEach
  public void setUp() {
    timer = new FakeTimer();
    store = createStore(10);
  }

  /** A store without tool-call or request id limits, which these tests don't cover. */
  private McpSessionStore createStore(int maxSessions) {
    return new McpSessionStore(TIMEOUT_MS, maxSessions, timer, 0, 0, 0);
  }

  private McpSession create() {
    return store.create(McpJsonRpc.PROTOCOL_VERSION, Collections.emptyMap(),
        Collections.emptyMap());
  }

  @Test
  public void testCreateReturnsInitializingSession() {
    Map<String, Object> clientInfo = new HashMap<>();
    clientInfo.put("name", "test-client");
    Map<String, Object> capabilities = new HashMap<>();
    capabilities.put("roots", Collections.emptyMap());

    McpSession session = store.create(McpJsonRpc.PROTOCOL_VERSION, clientInfo, capabilities);

    assertNotNull(session);
    assertEquals(McpSessionState.INITIALIZING, session.getState());
    assertEquals(McpJsonRpc.PROTOCOL_VERSION, session.getNegotiatedProtocolVersion());
    assertEquals("test-client", session.getClientInfo().get("name"));
    assertTrue(session.getClientCapabilities().containsKey("roots"));
    assertEquals(timer.now(), session.getCreationTimeMs());
    assertSame(session, store.get(session.getId()));
  }

  @Test
  public void testSessionIdsAreUniqueVisibleAscii() {
    McpSession first = create();
    McpSession second = create();

    assertNotEquals(first.getId(), second.getId());
    // The spec requires visible ASCII (0x21-0x7E) in session ids.
    for (char c : first.getId().toCharArray()) {
      assertTrue(c >= 0x21 && c <= 0x7E, "invalid session id character: " + (int) c);
    }
  }

  @Test
  public void testMarkReadyIsIdempotent() {
    McpSession session = create();

    assertTrue(session.markReady());
    assertEquals(McpSessionState.READY, session.getState());
    assertFalse(session.markReady());
    assertEquals(McpSessionState.READY, session.getState());
  }

  @Test
  public void testGetUnknownOrNullIdReturnsNull() {
    assertNull(store.get("no-such-session"));
    assertNull(store.get(null));
  }

  @Test
  public void testRemoveReturnsRemovedSession() {
    McpSession session = create();

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
    McpSessionStore smallStore = createStore(1);
    McpSession first = smallStore.create(McpJsonRpc.PROTOCOL_VERSION,
        Collections.emptyMap(), Collections.emptyMap());

    assertNotNull(first);
    assertNull(smallStore.create(McpJsonRpc.PROTOCOL_VERSION,
        Collections.emptyMap(), Collections.emptyMap()));
    // The existing session must survive the rejected attempt.
    assertSame(first, smallStore.get(first.getId()));
  }

  @Test
  public void testRemoveFreesCapacity() {
    McpSessionStore smallStore = createStore(1);
    McpSession first = smallStore.create(McpJsonRpc.PROTOCOL_VERSION,
        Collections.emptyMap(), Collections.emptyMap());

    assertSame(first, smallStore.remove(first.getId()));
    assertNotNull(smallStore.create(McpJsonRpc.PROTOCOL_VERSION,
        Collections.emptyMap(), Collections.emptyMap()));
  }

  @Test
  public void testIdleExpiry() {
    McpSession session = create();

    timer.advance(TIMEOUT_MS - 1);
    store.cleanUp();
    assertEquals(1, store.size());

    timer.advance(2);
    assertNull(store.get(session.getId()));
    store.cleanUp();
    assertEquals(0, store.size());
  }

  @Test
  public void testAccessResetsIdleClock() {
    McpSession session = create();

    timer.advance(TIMEOUT_MS / 2);
    assertSame(session, store.get(session.getId()));

    // More than TIMEOUT_MS since creation, but less since the last access.
    timer.advance(TIMEOUT_MS - 1);
    assertSame(session, store.get(session.getId()));

    timer.advance(TIMEOUT_MS + 1);
    assertNull(store.get(session.getId()));
  }

  @Test
  public void testExpiredSessionsDoNotCountAgainstCapacity() {
    McpSessionStore smallStore = createStore(1);
    smallStore.create(McpJsonRpc.PROTOCOL_VERSION, Collections.emptyMap(),
        Collections.emptyMap());

    timer.advance(TIMEOUT_MS + 1);
    assertNotNull(smallStore.create(McpJsonRpc.PROTOCOL_VERSION,
        Collections.emptyMap(), Collections.emptyMap()));
  }

  @Test
  public void testRemoveExpiredSessionReturnsNull() {
    McpSession session = create();

    timer.advance(TIMEOUT_MS + 1);
    assertNull(store.remove(session.getId()));
  }

  @Test
  public void testCloseClearsStore() {
    McpSession session = create();

    store.close();
    assertNull(store.get(session.getId()));
    assertEquals(0, store.size());
  }

  @Test
  public void testInvalidArgumentsRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> new McpSessionStore(0, 10, timer, 0, 0, 0));
    assertThrows(IllegalArgumentException.class,
        () -> new McpSessionStore(TIMEOUT_MS, 0, timer, 0, 0, 0));
    // A tool-call limit needs a positive window.
    assertThrows(IllegalArgumentException.class,
        () -> new McpSessionStore(TIMEOUT_MS, 10, timer, 1, 0, 0));
  }
}
