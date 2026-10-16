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
    store = new McpSessionStore(TIMEOUT_MS, 10, timer);
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

    McpSession session = store.create("2025-03-26", clientInfo, capabilities);

    assertNotNull(session);
    assertEquals(McpSessionState.INITIALIZING, session.state());
    assertEquals("2025-03-26", session.negotiatedProtocolVersion());
    assertEquals("test-client", session.clientInfo().get("name"));
    assertTrue(session.clientCapabilities().containsKey("roots"));
    assertEquals(timer.now(), session.creationTimeMs());
    assertSame(session, store.get(session.id()));
  }

  @Test
  public void testSessionIdsAreUniqueVisibleAscii() {
    McpSession first = create();
    McpSession second = create();

    assertNotEquals(first.id(), second.id());
    // The spec requires visible ASCII (0x21-0x7E) in session ids.
    for (char c : first.id().toCharArray()) {
      assertTrue(c >= 0x21 && c <= 0x7E, "invalid session id character: " + (int) c);
    }
  }

  @Test
  public void testSessionCopiesClientMaps() {
    Map<String, Object> clientInfo = new HashMap<>();
    clientInfo.put("name", "before");

    McpSession session = store.create(McpJsonRpc.PROTOCOL_VERSION, clientInfo, null);
    clientInfo.put("name", "after");

    assertEquals("before", session.clientInfo().get("name"));
    assertTrue(session.clientCapabilities().isEmpty());
    assertThrows(UnsupportedOperationException.class,
        () -> session.clientInfo().put("name", "changed"));
  }

  @Test
  public void testMarkReadyIsIdempotent() {
    McpSession session = create();

    assertTrue(session.markReady());
    assertEquals(McpSessionState.READY, session.state());
    assertFalse(session.markReady());
    assertEquals(McpSessionState.READY, session.state());
  }

  @Test
  public void testGetUnknownOrNullIdReturnsNull() {
    assertNull(store.get("no-such-session"));
    assertNull(store.get(null));
  }

  @Test
  public void testTerminateRemovesSession() {
    McpSession session = create();

    assertTrue(store.terminate(session.id()));
    assertNull(store.get(session.id()));
    assertEquals(0, store.size());
  }

  @Test
  public void testTerminateUnknownOrNullIdReturnsFalse() {
    assertFalse(store.terminate("no-such-session"));
    assertFalse(store.terminate(null));
  }

  @Test
  public void testCreateRejectedAtMaxCapacity() {
    McpSessionStore smallStore = new McpSessionStore(TIMEOUT_MS, 1, timer);
    McpSession first = smallStore.create(McpJsonRpc.PROTOCOL_VERSION,
        Collections.emptyMap(), Collections.emptyMap());

    assertNotNull(first);
    assertNull(smallStore.create(McpJsonRpc.PROTOCOL_VERSION,
        Collections.emptyMap(), Collections.emptyMap()));
    // The existing session must survive the rejected attempt.
    assertSame(first, smallStore.get(first.id()));
  }

  @Test
  public void testTerminateFreesCapacity() {
    McpSessionStore smallStore = new McpSessionStore(TIMEOUT_MS, 1, timer);
    McpSession first = smallStore.create(McpJsonRpc.PROTOCOL_VERSION,
        Collections.emptyMap(), Collections.emptyMap());

    assertTrue(smallStore.terminate(first.id()));
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
    assertNull(store.get(session.id()));
    store.cleanUp();
    assertEquals(0, store.size());
  }

  @Test
  public void testAccessResetsIdleClock() {
    McpSession session = create();

    timer.advance(TIMEOUT_MS / 2);
    assertSame(session, store.get(session.id()));

    // More than TIMEOUT_MS since creation, but less since the last access.
    timer.advance(TIMEOUT_MS - 1);
    assertSame(session, store.get(session.id()));

    timer.advance(TIMEOUT_MS + 1);
    assertNull(store.get(session.id()));
  }

  @Test
  public void testExpiredSessionsDoNotCountAgainstCapacity() {
    McpSessionStore smallStore = new McpSessionStore(TIMEOUT_MS, 1, timer);
    smallStore.create(McpJsonRpc.PROTOCOL_VERSION, Collections.emptyMap(),
        Collections.emptyMap());

    timer.advance(TIMEOUT_MS + 1);
    assertNotNull(smallStore.create(McpJsonRpc.PROTOCOL_VERSION,
        Collections.emptyMap(), Collections.emptyMap()));
  }

  @Test
  public void testTerminateExpiredSessionReturnsFalse() {
    McpSession session = create();

    timer.advance(TIMEOUT_MS + 1);
    assertFalse(store.terminate(session.id()));
  }

  @Test
  public void testCloseClearsStore() {
    McpSession session = create();

    store.close();
    assertNull(store.get(session.id()));
    assertEquals(0, store.size());
  }

  @Test
  public void testInvalidArgumentsRejected() {
    assertThrows(IllegalArgumentException.class, () -> new McpSessionStore(0, 10, timer));
    assertThrows(IllegalArgumentException.class,
        () -> new McpSessionStore(TIMEOUT_MS, 0, timer));
  }
}
