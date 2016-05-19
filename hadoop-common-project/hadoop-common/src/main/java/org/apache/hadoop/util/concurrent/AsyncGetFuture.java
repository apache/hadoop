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
package org.apache.hadoop.util.concurrent;

import com.google.common.util.concurrent.AbstractFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/** A {@link Future} implemented using an {@link AsyncGet} object. */
public class AsyncGetFuture<T, E extends Throwable> extends AbstractFuture<T> {
  public static final Log LOG = LogFactory.getLog(AsyncGetFuture.class);

  private final AtomicBoolean called = new AtomicBoolean(false);
  private final AsyncGet<T, E> asyncGet;

  public AsyncGetFuture(AsyncGet<T, E> asyncGet) {
    this.asyncGet = asyncGet;
  }

  private void callAsyncGet(long timeout, TimeUnit unit) {
    if (!isCancelled() && called.compareAndSet(false, true)) {
      try {
        set(asyncGet.get(timeout, unit));
      } catch (TimeoutException te) {
        LOG.trace("TRACE", te);
        called.compareAndSet(true, false);
      } catch (Throwable e) {
        LOG.trace("TRACE", e);
        setException(e);
      }
    }
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    callAsyncGet(-1, TimeUnit.MILLISECONDS);
    return super.get();
  }

  @Override
  public T get(long timeout, TimeUnit unit)
      throws InterruptedException, TimeoutException, ExecutionException {
    callAsyncGet(timeout, unit);
    return super.get(0, TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean isDone() {
    callAsyncGet(0, TimeUnit.MILLISECONDS);
    return super.isDone();
  }
}
