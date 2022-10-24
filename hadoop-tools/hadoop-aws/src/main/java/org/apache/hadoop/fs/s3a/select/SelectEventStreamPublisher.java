package org.apache.hadoop.fs.s3a.select;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.model.EndEvent;
import software.amazon.awssdk.services.s3.model.RecordsEvent;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponse;
import software.amazon.awssdk.utils.ToString;
import software.amazon.awssdk.utils.Validate;

public final class SelectEventStreamPublisher implements
    SdkPublisher<SelectObjectContentEventStream> {

  private final CompletableFuture<Void> future;
  private final SelectObjectContentResponse response;
  private final SdkPublisher<SelectObjectContentEventStream> publisher;

  public SelectEventStreamPublisher(
      CompletableFuture<Void> future,
      SelectObjectContentResponse response,
      SdkPublisher<SelectObjectContentEventStream> publisher) {
    this.future = Validate.paramNotNull(future, "future");
    this.response = Validate.paramNotNull(response, "response");
    this.publisher = Validate.paramNotNull(publisher, "publisher");
  }

  public void cancel() {
    future.cancel(true);
  }

  public SelectObjectContentResponse response() {
    return response;
  }

  @Override
  public void subscribe(Subscriber<? super SelectObjectContentEventStream> subscriber) {
    publisher.subscribe(subscriber);
  }

  @Override
  public String toString() {
    return ToString.builder("SelectObjectContentEventStream")
        .add("response", response)
        .add("publisher", publisher)
        .build();
  }

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

    return AbortableInputStream.create(
        new SequenceInputStream(
            new BlockingEnumeration(recordInputStreams, 1,
                EMPTY_STREAM)),
        this::cancel);
  }

  private static final InputStream EMPTY_STREAM =
      new ByteArrayInputStream(new byte[0]);
}