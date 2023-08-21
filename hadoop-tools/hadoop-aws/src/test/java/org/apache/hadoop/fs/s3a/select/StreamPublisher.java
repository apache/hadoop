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

import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.SdkPublisher;

/**
 * Publisher used to test the handling of asynchronous responses.
 * @param <T> The type of published elements.
 */
final class StreamPublisher<T> implements SdkPublisher<T> {
  private final Executor executor;
  private final Iterator<T> iterator;
  private Boolean done = false;

  StreamPublisher(Stream<T> data, Executor executor) {
    this.iterator = data.iterator();
    this.executor = executor;
  }

  StreamPublisher(Stream<T> data) {
    this(data, Runnable::run);
  }

  @Override
  public void subscribe(Subscriber<? super T> subscriber) {
    subscriber.onSubscribe(new Subscription() {
      @Override
      public void request(long n) {
        if (done) {
          return;
        }

        if (n < 1) {
          done = true;
          executor.execute(() -> subscriber.onError(new IllegalArgumentException()));
          return;
        }

        for (long i = 0; i < n; i++) {
          final T value;
          try {
            synchronized (iterator) {
              value = iterator.hasNext() ? iterator.next() : null;
            }
          } catch (Throwable e) {
            executor.execute(() -> subscriber.onError(e));
            break;
          }

          if (value == null) {
            done = true;
            executor.execute(subscriber::onComplete);
            break;
          } else {
            executor.execute(() -> subscriber.onNext(value));
          }
        }
      }

      @Override
      public void cancel() {
        done = true;
      }
    });
  }
}
