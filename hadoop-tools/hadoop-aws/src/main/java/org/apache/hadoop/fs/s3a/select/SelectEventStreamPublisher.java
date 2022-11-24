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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;

import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.model.EndEvent;
import software.amazon.awssdk.services.s3.model.RecordsEvent;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponse;
import software.amazon.awssdk.utils.ToString;

/**
 * Async publisher of {@link SelectObjectContentEventStream}s returned
 * from a SelectObjectContent call.
 */
public final class SelectEventStreamPublisher implements
    SdkPublisher<SelectObjectContentEventStream> {

  private final CompletableFuture<Void> selectOperationFuture;
  private final SelectObjectContentResponse response;
  private final SdkPublisher<SelectObjectContentEventStream> publisher;

  /**
   * Create the publisher.
   * @param selectOperationFuture SelectObjectContent future
   * @param response SelectObjectContent response
   * @param publisher SelectObjectContentEventStream publisher to wrap
   */
  public SelectEventStreamPublisher(
      CompletableFuture<Void> selectOperationFuture,
      SelectObjectContentResponse response,
      SdkPublisher<SelectObjectContentEventStream> publisher) {
    this.selectOperationFuture = selectOperationFuture;
    this.response = response;
    this.publisher = publisher;
  }

  /**
   * Retrieve an input stream to the subset of the S3 object that matched the select query.
   * This is equivalent to loading the content of all RecordsEvents into an InputStream.
   * This will lazily-load the content from S3, minimizing the amount of memory used.
   * @param onEndEvent callback on the end event
   * @return the input stream
   */
  public AbortableInputStream toRecordsInputStream(Consumer<EndEvent> onEndEvent) {
    SdkPublisher<InputStream> recordInputStreams = this.publisher
        .filter(e -> {
          if (e instanceof RecordsEvent) {
            return true;
          } else if (e instanceof EndEvent) {
            onEndEvent.accept((EndEvent) e);
          }
          return false;
        })
        .map(e -> ((RecordsEvent) e).payload().asInputStream());

    // Subscribe to the async publisher using an enumeration that will
    // buffer a single chunk (RecordsEvent's payload) at a time and
    // block until it is consumed.
    // Also inject an empty stream as the first element that
    // SequenceInputStream will request on construction.
    BlockingEnumeration enumeration =
        new BlockingEnumeration(recordInputStreams, 1, EMPTY_STREAM);
    return AbortableInputStream.create(
        new SequenceInputStream(enumeration),
        this::cancel);
  }

  /**
   * The response from the SelectObjectContent call.
   * @return the response object
   */
  public SelectObjectContentResponse response() {
    return response;
  }

  @Override
  public void subscribe(Subscriber<? super SelectObjectContentEventStream> subscriber) {
    publisher.subscribe(subscriber);
  }

  /**
   * Cancel the operation.
   */
  public void cancel() {
    selectOperationFuture.cancel(true);
  }

  @Override
  public String toString() {
    return ToString.builder("SelectObjectContentEventStream")
        .add("response", response)
        .add("publisher", publisher)
        .build();
  }

  private static final InputStream EMPTY_STREAM =
      new ByteArrayInputStream(new byte[0]);
}
