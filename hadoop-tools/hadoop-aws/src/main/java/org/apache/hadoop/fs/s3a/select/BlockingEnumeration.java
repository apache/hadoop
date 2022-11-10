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
import java.util.concurrent.LinkedBlockingQueue;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.exception.SdkException;

/**
 * Implements the {@link Enumeration} interface by subscribing to a
 * {@link SdkPublisher} instance. The enumeration will buffer a fixed
 * number of events received from the publisher and block until the first is
 * consumed. Calls to {@link #hasMoreElements()} and {@link #nextElement()}
 * can also block while waiting for new events.
 * @param <T> the type of element.
 */
public final class BlockingEnumeration<T> implements Enumeration<T> {
  private static final class Token<T> {
    public T element;
    public Throwable error;

    public Token() {
    }

    public Token(T element) {
      this.element = element;
    }

    public Token(Throwable error) {
      this.error = error;
    }
  }

  private final Token<T> END_TOKEN = new Token<>();
  private final BlockingQueue<Token<T>> blockingQueue;
  private Token<T> current = null;

  /**
   * Create an enumeration with a fixed buffer size.
   * @param publisher the publisher feeding the enumeration.
   * @param limit the buffer size.
   */
  public BlockingEnumeration(SdkPublisher<T> publisher, final int limit) {
    this.blockingQueue = new LinkedBlockingQueue<>(limit);
    publisher.subscribe(new Subscriber<T>() {
      private Subscription subscription;

      @Override
      public void onSubscribe(Subscription s) {
        this.subscription = s;
        this.subscription.request(limit);
      }

      @Override
      public void onNext(T t) {
        enqueue(new Token<>(t));
        subscription.request(1);
      }

      @Override
      public void onError(Throwable t) {
        enqueue(new Token<>(t));
      }

      @Override
      public void onComplete() {
        enqueue(END_TOKEN);
      }

      private void enqueue(Token<T> token) {
        try {
          blockingQueue.put(token);
        } catch (InterruptedException e) {
          // TODO: log?
        }
      }
    });
  }

  /**
   * Create an enumeration with a fixed buffer size and an
   * injected first element.
   * @param publisher the publisher feeding the enumeration.
   * @param limit the buffer size.
   * @param firstElement first element the enumeration will return.
   */
  public BlockingEnumeration(SdkPublisher<T> publisher,
      final int limit,
      T firstElement) {
    this(publisher, limit);
    this.current = new Token<>(firstElement);
  }

  @Override
  public boolean hasMoreElements() {
    if (current == null) {
      current = dequeue();
    }
    return current != END_TOKEN;
  }

  @Override
  public T nextElement() {
    if (current == null) {
      current = dequeue();
    }
    if (current == END_TOKEN) {
      throw new NoSuchElementException();
    }
    if (current.error != null) {
      if (current.error instanceof SdkException) {
        throw (SdkException)current.error;
      } else {
        throw SdkException.create("Unexpected error", current.error);
      }
    }
    T element = current.element;
    current = null;
    return element;
  }

  private Token<T> dequeue() {
    try {
      return blockingQueue.take();
    } catch (InterruptedException e) {
      // TODO: log?
      return END_TOKEN;
    }
  }
}
