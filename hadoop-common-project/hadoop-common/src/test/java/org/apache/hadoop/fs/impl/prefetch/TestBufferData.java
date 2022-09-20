/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.impl.prefetch;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;

import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class TestBufferData extends AbstractHadoopTestBase {

  @Test
  public void testArgChecks() throws Exception {
    // Should not throw.
    ByteBuffer buffer = ByteBuffer.allocate(1);
    BufferData data = new BufferData(1, buffer);

    // Verify it throws correctly.

    intercept(IllegalArgumentException.class,
        "'blockNumber' must not be negative",
        () -> new BufferData(-1, buffer));

    intercept(IllegalArgumentException.class, "'buffer' must not be null",
        () -> new BufferData(1, null));

    intercept(IllegalArgumentException.class, "'actionFuture' must not be null",
        () -> data.setPrefetch(null));

    intercept(IllegalArgumentException.class, "'actionFuture' must not be null",
        () -> data.setCaching(null));

    intercept(IllegalArgumentException.class, "'states' must not be null",
        () -> data.throwIfStateIncorrect((BufferData.State[]) null));

    intercept(IllegalStateException.class,
        "Expected buffer state to be 'READY or CACHING' but found",
        () -> data.throwIfStateIncorrect(BufferData.State.READY,
            BufferData.State.CACHING));

  }

  @Test
  public void testValidStateUpdates() {
    ByteBuffer buffer = ByteBuffer.allocate(1);
    BufferData data = new BufferData(1, buffer);

    assertEquals(BufferData.State.BLANK, data.getState());

    CompletableFuture<Void> actionFuture = new CompletableFuture<>();
    actionFuture.complete(null);
    data.setPrefetch(actionFuture);
    assertEquals(BufferData.State.PREFETCHING, data.getState());
    assertNotNull(data.getActionFuture());
    assertSame(actionFuture, data.getActionFuture());

    CompletableFuture<Void> actionFuture2 = new CompletableFuture<>();
    data.setCaching(actionFuture2);
    assertEquals(BufferData.State.CACHING, data.getState());
    assertNotNull(data.getActionFuture());
    assertSame(actionFuture2, data.getActionFuture());
    assertNotSame(actionFuture, actionFuture2);

    List<BufferData.State> states = Arrays.asList(
        BufferData.State.BLANK,
        BufferData.State.PREFETCHING,
        BufferData.State.CACHING,
        BufferData.State.READY
    );

    BufferData data2 = new BufferData(1, buffer);
    BufferData.State prevState = null;
    for (BufferData.State state : states) {
      if (prevState != null) {
        assertEquals(prevState, data2.getState());
        data2.updateState(state, prevState);
        assertEquals(state, data2.getState());
      }
      prevState = state;
    }
  }

  @Test
  public void testInvalidStateUpdates() throws Exception {
    CompletableFuture<Void> actionFuture = new CompletableFuture<>();
    actionFuture.complete(null);
    testInvalidStateUpdatesHelper(
        (d) -> d.setPrefetch(actionFuture),
        BufferData.State.BLANK,
        BufferData.State.READY);

    testInvalidStateUpdatesHelper(
        (d) -> d.setCaching(actionFuture),
        BufferData.State.PREFETCHING,
        BufferData.State.READY);
  }

  @Test
  public void testSetReady() throws Exception {
    byte[] bytes1 = new byte[5];
    initBytes(bytes1);

    ByteBuffer buffer = ByteBuffer.allocate(10);
    buffer.put(bytes1);
    buffer.limit(bytes1.length);
    BufferData data = new BufferData(1, buffer);
    assertNotEquals(BufferData.State.READY, data.getState());
    assertEquals(0, data.getChecksum());

    data.setReady(BufferData.State.BLANK);
    assertEquals(BufferData.State.READY, data.getState());
    assertNotEquals(0, data.getChecksum());

    // Verify that buffer cannot be modified once in READY state.
    ExceptionAsserts.assertThrows(
        ReadOnlyBufferException.class,
        null,
        () -> data.getBuffer().put(bytes1));

    // Verify that buffer cannot be set to READY state more than once.
    ExceptionAsserts.assertThrows(
        IllegalStateException.class,
        "Checksum cannot be changed once set",
        () -> data.setReady(BufferData.State.BLANK));

    // Verify that we detect post READY buffer modification.
    buffer.array()[2] = (byte) 42;
    ExceptionAsserts.assertThrows(
        IllegalStateException.class,
        "checksum changed after setReady()",
        () -> data.setDone());
  }

  @Test
  public void testChecksum() {
    byte[] bytes1 = new byte[5];
    byte[] bytes2 = new byte[10];

    initBytes(bytes1);
    initBytes(bytes2);

    ByteBuffer buffer1 = ByteBuffer.wrap(bytes1);
    ByteBuffer buffer2 = ByteBuffer.wrap(bytes2);
    buffer2.limit(bytes1.length);

    long checksum1 = BufferData.getChecksum(buffer1);
    long checksum2 = BufferData.getChecksum(buffer2);

    assertEquals(checksum1, checksum2);
  }

  private void initBytes(byte[] bytes) {
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) i;
    }
  }

  @FunctionalInterface
  public interface StateChanger {

    void run(BufferData data) throws Exception;
  }

  private void testInvalidStateUpdatesHelper(
      StateChanger changeState,
      BufferData.State... validFromState) throws Exception {

    ByteBuffer buffer = ByteBuffer.allocate(1);
    BufferData data = new BufferData(1, buffer);
    data.updateState(validFromState[0], BufferData.State.BLANK);
    List<BufferData.State> states = this.getStatesExcept(validFromState);
    BufferData.State prevState = validFromState[0];
    String expectedMessage =
        String.format("Expected buffer state to be '%s", validFromState[0]);
    for (BufferData.State s : states) {
      data.updateState(s, prevState);

      ExceptionAsserts.assertThrows(
          IllegalStateException.class,
          expectedMessage,
          () -> changeState.run(data));

      assertEquals(s, data.getState());
      prevState = s;
    }
  }

  static final List<BufferData.State> ALL_STATES = Arrays.asList(
      BufferData.State.UNKNOWN,
      BufferData.State.BLANK,
      BufferData.State.PREFETCHING,
      BufferData.State.CACHING,
      BufferData.State.READY
  );

  private List<BufferData.State> getStatesExcept(BufferData.State... states) {

    List<BufferData.State> result = new ArrayList<>();
    for (BufferData.State s : ALL_STATES) {
      boolean found = false;
      for (BufferData.State ss : states) {
        if (s == ss) {
          found = true;
        }
      }

      if (!found) {
        result.add(s);
      }
    }

    return result;
  }
}
