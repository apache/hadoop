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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponse;

/**
 * Unit tests for {@link SelectEventStreamPublisher}.
 */
@RunWith(Parameterized.class)
public final class TestSelectEventStreamPublisher extends Assert {

  @Parameterized.Parameters(name = "threading-{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"main"},
        {"background"}
    });
  }

  private final String threading;

  public TestSelectEventStreamPublisher(String threading) {
    this.threading = threading;
  }

  private Executor createExecutor() {
    if (threading.equals("main")) {
      return Runnable::run;
    } else if (threading.equals("background")) {
      return Executors.newSingleThreadExecutor();
    } else {
      throw new IllegalArgumentException("Unknown: " + threading);
    }
  }

  @Test
  public void emptyRecordsInputStream() throws IOException {
    SelectEventStreamPublisher selectEventStreamPublisher =
        createSelectPublisher(Stream.of(
            SelectObjectContentEventStream.recordsBuilder()
                .payload(SdkBytes.fromByteArray(new byte[0]))
                .build()));

    try (AbortableInputStream inputStream =
        selectEventStreamPublisher.toRecordsInputStream(e -> {})) {
      assertEquals(-1, inputStream.read());
    }
  }

  @Test
  public void multipleRecords() throws IOException {
    SelectEventStreamPublisher selectEventStreamPublisher =
        createSelectPublisher(Stream.of(
        SelectObjectContentEventStream.recordsBuilder()
            .payload(SdkBytes.fromUtf8String("foo"))
            .build(),
        SelectObjectContentEventStream.recordsBuilder()
            .payload(SdkBytes.fromUtf8String("bar"))
            .build()));

    try (AbortableInputStream inputStream =
        selectEventStreamPublisher.toRecordsInputStream(e -> {})) {
      String result = readAll(inputStream);
      assertEquals("foobar", result);
    }
  }

  @Test
  public void skipsOtherEvents() throws IOException {
    SelectEventStreamPublisher selectEventStreamPublisher =
        createSelectPublisher(Stream.of(
        SelectObjectContentEventStream.recordsBuilder()
            .payload(SdkBytes.fromUtf8String("foo"))
            .build(),
        SelectObjectContentEventStream.progressBuilder()
            .build(),
        SelectObjectContentEventStream.statsBuilder()
            .build(),
        SelectObjectContentEventStream.recordsBuilder()
            .payload(SdkBytes.fromUtf8String("bar"))
            .build(),
        SelectObjectContentEventStream.endBuilder()
            .build()));

    try (AbortableInputStream inputStream =
        selectEventStreamPublisher.toRecordsInputStream(e -> {})) {
      String result = readAll(inputStream);
      assertEquals("foobar", result);
    }
  }

  @Test
  public void callsOnEndEvent() throws IOException {
    SelectEventStreamPublisher selectEventStreamPublisher =
        createSelectPublisher(Stream.of(
        SelectObjectContentEventStream.recordsBuilder()
            .payload(SdkBytes.fromUtf8String("foo"))
            .build(),
        SelectObjectContentEventStream.endBuilder()
            .build()));

    AtomicBoolean endEvent = new AtomicBoolean(false);
    try (AbortableInputStream inputStream =
        selectEventStreamPublisher.toRecordsInputStream(e -> endEvent.set(true))) {
      String result = readAll(inputStream);
      assertEquals("foo", result);
    }

    assertTrue(endEvent.get());
  }

  @Test
  public void handlesErrors() throws IOException {
    SelectEventStreamPublisher selectEventStreamPublisher =
        createSelectPublisher(Stream.of(
        SelectObjectContentEventStream.recordsBuilder()
            .payload(SdkBytes.fromUtf8String("foo"))
            .build(),
        SelectObjectContentEventStream.recordsBuilder()
            .payload(SdkBytes.fromUtf8String("bar"))
            .build())
        .map(e -> {
          throw SdkException.create("error!", null);
        }));

    try (AbortableInputStream inputStream =
        selectEventStreamPublisher.toRecordsInputStream(e -> {})) {
      assertThrows(SdkException.class, () -> readAll(inputStream));
    }
  }

  private SelectEventStreamPublisher createSelectPublisher(
      Stream<SelectObjectContentEventStream> stream) {
    SdkPublisher<SelectObjectContentEventStream> sdkPublisher =
        new StreamPublisher<>(stream, createExecutor());
    CompletableFuture<Void> future =
        CompletableFuture.completedFuture(null);
    SelectObjectContentResponse response =
        SelectObjectContentResponse.builder().build();
    return new SelectEventStreamPublisher(future, response, sdkPublisher);
  }

  private static String readAll(InputStream inputStream) throws IOException {
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[8096];
      int read;
      while ((read = inputStream.read(buffer, 0, buffer.length)) != -1) {
        outputStream.write(buffer, 0, read);
      }
      return outputStream.toString();
    }
  }
}
