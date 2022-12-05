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

package org.apache.hadoop.fs.s3a.select;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.exception.SdkException;

/**
 * Unit tests for {@link BlockingEnumeration}.
 */
public final class TestBlockingEnumeration extends Assert {

  @Test
  public void containsElement() {
    SdkPublisher<String> publisher = new StreamPublisher<>(Stream.of("foo"));

    BlockingEnumeration<String> enumeration =
        new BlockingEnumeration<>(publisher, 1);

    assertTrue(enumeration.hasMoreElements());
    assertEquals("foo", enumeration.nextElement());
    assertFalse(enumeration.hasMoreElements());
  }

  @Test
  public void containsInjectedElement() {
    SdkPublisher<String> publisher = new StreamPublisher<>(Stream.of("foo"));

    BlockingEnumeration<String> enumeration =
        new BlockingEnumeration<>(publisher, 1, "bar");

    assertTrue(enumeration.hasMoreElements());
    assertEquals("bar", enumeration.nextElement());
    assertTrue(enumeration.hasMoreElements());
    assertEquals("foo", enumeration.nextElement());
    assertFalse(enumeration.hasMoreElements());
  }

  @Test
  public void throwsExceptionOnFirstElement() {
    SdkPublisher<Integer> publisher = new StreamPublisher<>(
        Stream.of(0, 1)
            .map(i -> {
              throw SdkException.create("error!", null);
            }),
        Executors.newSingleThreadExecutor());

    BlockingEnumeration<Integer> enumeration =
        new BlockingEnumeration<>(publisher, 1);
    assertThrows(SdkException.class, enumeration::hasMoreElements);
  }

  @Test
  public void throwsExceptionAfterInjectedElement() {
    SdkPublisher<Integer> publisher = new StreamPublisher<>(
        Stream.of(0, 1)
            .peek(i -> {
              throw SdkException.create("error!", null);
            }),
        Executors.newSingleThreadExecutor());

    BlockingEnumeration<Integer> enumeration =
        new BlockingEnumeration<>(publisher, 1, 99);
    assertTrue(enumeration.hasMoreElements());
    assertEquals(99, enumeration.nextElement().intValue());
    assertThrows(SdkException.class, enumeration::hasMoreElements);
  }

  @Test
  public void throwsNonSdkException() {
    SdkPublisher<Integer> publisher = new StreamPublisher<>(
        Stream.of(0, 1)
            .peek(i -> {
              throw new RuntimeException("error!", null);
            }),
        Executors.newSingleThreadExecutor());

    BlockingEnumeration<Integer> enumeration =
        new BlockingEnumeration<>(publisher, 1);
    SdkException exception = Assert.assertThrows(SdkException.class, enumeration::hasMoreElements);
    assertEquals(RuntimeException.class, exception.getCause().getClass());
  }

  @Test
  public void throwsError() {
    SdkPublisher<Integer> publisher = new StreamPublisher<>(
        Stream.of(0, 1)
            .peek(i -> {
              throw new Error("error!", null);
            }),
        Executors.newSingleThreadExecutor());

    BlockingEnumeration<Integer> enumeration =
        new BlockingEnumeration<>(publisher, 1);
    assertThrows(Error.class, enumeration::hasMoreElements);
  }

  @Test
  public void throwsExceptionOnSecondElement() {
    SdkPublisher<Integer> publisher = new StreamPublisher<>(
        Stream.of(0, 1)
            .peek(i -> {
              if (i == 1) {
                throw SdkException.create("error!", null);
              }
            }),
        Executors.newSingleThreadExecutor());

    BlockingEnumeration<Integer> enumeration =
        new BlockingEnumeration<>(publisher, 1);
    assertTrue(enumeration.hasMoreElements());
    assertEquals(0, enumeration.nextElement().intValue());
    assertThrows(SdkException.class, enumeration::hasMoreElements);
  }

  @Test
  public void noMoreElementsAfterThrow() {
    SdkPublisher<Integer> publisher = new StreamPublisher<>(
        Stream.of(0, 1)
            .map(i -> {
              throw SdkException.create("error!", null);
            }),
        Executors.newSingleThreadExecutor());

    BlockingEnumeration<Integer> enumeration =
        new BlockingEnumeration<>(publisher, 1);
    assertThrows(SdkException.class, enumeration::hasMoreElements);
    assertFalse(enumeration.hasMoreElements());
  }

  @Test
  public void buffersOnSameThread() {
    verifyBuffering(10, 3, Runnable::run);
  }

  @Test
  public void publisherOnDifferentThread() {
    verifyBuffering(5, 1, Executors.newSingleThreadExecutor());
  }

  @Test
  public void publisherOnDifferentThreadWithBuffer() {
    verifyBuffering(30, 10, Executors.newSingleThreadExecutor());
  }

  private static void verifyBuffering(int length, int bufferSize, Executor executor) {
    AtomicInteger emitted = new AtomicInteger();
    SdkPublisher<Integer> publisher = new StreamPublisher<>(
        IntStream.range(0, length).boxed().peek(i -> emitted.incrementAndGet()),
        executor);

    BlockingEnumeration<Integer> enumeration =
        new BlockingEnumeration<>(publisher, bufferSize);

    int pulled = 0;
    while (true) {
      try {
        int expected = Math.min(length, pulled + bufferSize);
        if (expected != emitted.get()) {
          Thread.sleep(10);
        }
        assertEquals(expected, emitted.get());
      } catch (InterruptedException e) {
        fail("Interrupted: " + e);
      }

      if (!enumeration.hasMoreElements()) {
        break;
      }

      int i = enumeration.nextElement();
      assertEquals(pulled, i);
      pulled++;
    }
  }
}
