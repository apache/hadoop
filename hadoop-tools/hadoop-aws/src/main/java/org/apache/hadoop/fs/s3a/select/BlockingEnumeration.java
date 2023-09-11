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

import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.exception.SdkException;

/**
 * Implements the {@link Enumeration} interface by subscribing to a
 * {@link SdkPublisher} instance. The enumeration will buffer a fixed
 * number of elements and only request new ones from the publisher
 * when they are consumed. Calls to {@link #hasMoreElements()} and
 * {@link #nextElement()} may block while waiting for new elements.
 * @param <T> the type of element.
 */
public final class BlockingEnumeration<T> implements Enumeration<T> {
  private static final class Signal<T> {
    private final T element;
    private final Throwable error;

    Signal(T element) {
      this.element = element;
      this.error = null;
    }

    Signal(Throwable error) {
      this.element = null;
      this.error = error;
    }
  }

  private final Signal<T> endSignal = new Signal<>((Throwable)null);
  private final CompletableFuture<Subscription> subscription = new CompletableFuture<>();
  private final BlockingQueue<Signal<T>> signalQueue;
  private final int bufferSize;
  private Signal<T> current = null;

  /**
   * Create an enumeration with a fixed buffer size and an
   * optional injected first element.
   * @param publisher the publisher feeding the enumeration.
   * @param bufferSize the buffer size.
   * @param firstElement (optional) first element the enumeration will return.
   */
  public BlockingEnumeration(SdkPublisher<T> publisher,
      final int bufferSize,
      final T firstElement) {
    this.signalQueue = new LinkedBlockingQueue<>();
    this.bufferSize = bufferSize;
    if (firstElement != null) {
      this.current = new Signal<>(firstElement);
    }
    publisher.subscribe(new EnumerationSubscriber());
  }

  /**
   * Create an enumeration with a fixed buffer size.
   * @param publisher the publisher feeding the enumeration.
   * @param bufferSize the buffer size.
   */
  public BlockingEnumeration(SdkPublisher<T> publisher,
      final int bufferSize) {
    this(publisher, bufferSize, null);
  }

  @Override
  public boolean hasMoreElements() {
    if (current == null) {
      try {
        current = signalQueue.take();
      } catch (InterruptedException e) {
        current = new Signal<>(e);
        subscription.thenAccept(Subscription::cancel);
        Thread.currentThread().interrupt();
      }
    }
    if (current.error != null) {
      Throwable error = current.error;
      current = endSignal;
      if (error instanceof Error) {
        throw (Error)error;
      } else if (error instanceof SdkException) {
        throw (SdkException)error;
      } else {
        throw SdkException.create("Unexpected error", error);
      }
    }
    return current != endSignal;
  }

  @Override
  public T nextElement() {
    if (!hasMoreElements()) {
      throw new NoSuchElementException();
    }
    T element = current.element;
    current = null;
    subscription.thenAccept(s -> s.request(1));
    return element;
  }

  private final class EnumerationSubscriber implements Subscriber<T> {

    @Override
    public void onSubscribe(Subscription s) {
      long request = bufferSize;
      if (current != null) {
        request--;
      }
      if (request > 0) {
        s.request(request);
      }
      subscription.complete(s);
    }

    @Override
    public void onNext(T t) {
      signalQueue.add(new Signal<>(t));
    }

    @Override
    public void onError(Throwable t) {
      signalQueue.add(new Signal<>(t));
    }

    @Override
    public void onComplete() {
      signalQueue.add(endSignal);
    }
  }
}
