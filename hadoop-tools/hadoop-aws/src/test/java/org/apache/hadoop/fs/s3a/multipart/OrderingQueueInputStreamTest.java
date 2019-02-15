/*
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

package org.apache.hadoop.fs.s3a.multipart;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Unit tests for {@link OrderingQueueInputStream}.
 */
public final class OrderingQueueInputStreamTest {

  @Test
  public void testInputStream() throws InterruptedException, IOException {
    OrderingQueue orderingQueue = new OrderingQueue(0, 10, 10);
    Runnable closeAction = new Runnable() {
      @Override
      public void run() {
      }
    };
    try (InputStream inputStream = new OrderingQueueInputStream(
        orderingQueue, closeAction, closeAction)) {
      byte[] bytes = new byte[10];
      for (int i = 0; i < bytes.length; i++) {
        bytes[i] = (byte) i;
      }

      orderingQueue.push(0, Arrays.copyOfRange(bytes, 0, 3));
      orderingQueue.push(3, Arrays.copyOfRange(bytes, 3, 6));
      orderingQueue.push(6, Arrays.copyOfRange(bytes, 6, 9));
      orderingQueue.push(9, Arrays.copyOfRange(bytes, 9, 10));

      assertArrayEquals(bytes, IOUtils.toByteArray(inputStream));
    }
  }

  @Test
  public void testCloseActionCalled() throws IOException {
    OrderingQueue orderingQueue = new OrderingQueue(0, 10, 10);
    Runnable closeAction = mock(Runnable.class);
    Runnable abortAction = mock(Runnable.class);
    try (InputStream inputStream = new OrderingQueueInputStream(
        orderingQueue, closeAction, abortAction)) {
      verifyZeroInteractions(closeAction);
      verifyZeroInteractions(abortAction);
    }

    verify(closeAction, Mockito.times(1)).run();
    verify(abortAction, Mockito.times(0)).run();
  }

  @Test
  public void testAbortActionCalled() throws IOException {
    OrderingQueue orderingQueue = new OrderingQueue(0, 10, 10);
    Runnable closeAction = mock(Runnable.class);
    Runnable abortAction = mock(Runnable.class);
    verifyZeroInteractions(closeAction);
    OrderingQueueInputStream inputStream = new OrderingQueueInputStream(
        orderingQueue, closeAction, abortAction);
    inputStream.abort();

    verify(closeAction, Mockito.times(0)).run();
    verify(abortAction, Mockito.times(1)).run();
  }

  @Test
  public void testReadMethods() throws IOException, InterruptedException {
    OrderingQueue orderingQueue = new OrderingQueue(0, 10, 10);
    byte[] bytes = new byte[10];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) i;
    }
    orderingQueue.push(0, Arrays.copyOfRange(bytes, 0, 3));
    orderingQueue.push(3, Arrays.copyOfRange(bytes, 3, 6));
    orderingQueue.push(6, Arrays.copyOfRange(bytes, 6, 9));
    orderingQueue.push(9, Arrays.copyOfRange(bytes, 9, 10));

    Runnable closeAction = new Runnable() {
      @Override
      public void run() {
      }
    };
    try (InputStream inputStream = new OrderingQueueInputStream(
        orderingQueue, closeAction, closeAction)) {
      assertEquals(0, inputStream.read());

      byte[] bytesToRead = new byte[3];
      assertEquals(2, inputStream.read(bytesToRead));
      assertArrayEquals(
          Arrays.copyOfRange(bytes, 1, 3),
          Arrays.copyOfRange(bytesToRead, 0, 2));

      assertEquals(3, inputStream.read(bytesToRead));
      assertArrayEquals(Arrays.copyOfRange(bytes, 3, 6), bytesToRead);

      assertEquals(2, inputStream.read(bytesToRead, 1, 2));
      assertArrayEquals(Arrays.copyOfRange(bytes, 6, 8),
          Arrays.copyOfRange(bytesToRead, 1, 3));

      assertEquals(8, inputStream.read());
      assertEquals(9, inputStream.read());
      assertEquals(-1, inputStream.read());
    }
  }

  @Test
  public void testSkip() throws InterruptedException, IOException {
    OrderingQueue orderingQueue = new OrderingQueue(0, 10, 10);
    byte[] bytes = new byte[10];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) i;
    }
    orderingQueue.push(0, Arrays.copyOfRange(bytes, 0, 5));
    orderingQueue.push(5, Arrays.copyOfRange(bytes, 5, 10));

    Runnable closeAction = new Runnable() {
      @Override
      public void run() {
      }
    };
    try (InputStream inputStream = new OrderingQueueInputStream(
        orderingQueue, closeAction, closeAction)) {
      inputStream.skip(9);
      assertEquals(9, inputStream.read());
    }
  }
}
