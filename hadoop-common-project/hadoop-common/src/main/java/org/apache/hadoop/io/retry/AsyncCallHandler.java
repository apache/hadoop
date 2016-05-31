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
package org.apache.hadoop.io.retry;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/** Handle async calls. */
@InterfaceAudience.Private
public class AsyncCallHandler {
  static final Logger LOG = LoggerFactory.getLogger(AsyncCallHandler.class);

  private static final ThreadLocal<AsyncGet<?, Exception>>
      LOWER_LAYER_ASYNC_RETURN = new ThreadLocal<>();
  private static final ThreadLocal<AsyncGet<Object, Throwable>>
      ASYNC_RETURN = new ThreadLocal<>();

  /** @return the async return value from {@link AsyncCallHandler}. */
  @InterfaceStability.Unstable
  @SuppressWarnings("unchecked")
  public static <R, T extends  Throwable> AsyncGet<R, T> getAsyncReturn() {
    final AsyncGet<R, T> asyncGet = (AsyncGet<R, T>)ASYNC_RETURN.get();
    if (asyncGet != null) {
      ASYNC_RETURN.set(null);
      return asyncGet;
    } else {
      return (AsyncGet<R, T>) getLowerLayerAsyncReturn();
    }
  }

  /** For the lower rpc layers to set the async return value. */
  @InterfaceStability.Unstable
  public static void setLowerLayerAsyncReturn(
      AsyncGet<?, Exception> asyncReturn) {
    LOWER_LAYER_ASYNC_RETURN.set(asyncReturn);
  }

  private static AsyncGet<?, Exception> getLowerLayerAsyncReturn() {
    final AsyncGet<?, Exception> asyncGet = LOWER_LAYER_ASYNC_RETURN.get();
    Preconditions.checkNotNull(asyncGet);
    LOWER_LAYER_ASYNC_RETURN.set(null);
    return asyncGet;
  }

  /** A simple concurrent queue which keeping track the empty start time. */
  static class ConcurrentQueue<T> {
    private final Queue<T> queue = new LinkedList<>();
    private long emptyStartTime = Time.monotonicNow();

    synchronized int size() {
      return queue.size();
    }

    /** Is the queue empty for more than the given time in millisecond? */
    synchronized boolean isEmpty(long time) {
      return queue.isEmpty() && Time.monotonicNow() - emptyStartTime > time;
    }

    synchronized void offer(T c) {
      final boolean added = queue.offer(c);
      Preconditions.checkState(added);
    }

    synchronized T poll() {
      Preconditions.checkState(!queue.isEmpty());
      final T t = queue.poll();
      if (queue.isEmpty()) {
        emptyStartTime = Time.monotonicNow();
      }
      return t;
    }
  }

  /** A queue for handling async calls. */
  static class AsyncCallQueue {
    private final ConcurrentQueue<AsyncCall> queue = new ConcurrentQueue<>();
    private final Processor processor = new Processor();

    void addCall(AsyncCall call) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("add " + call);
      }
      queue.offer(call);
      processor.tryStart();
    }

    void checkCalls() {
      final int size = queue.size();
      for (int i = 0; i < size; i++) {
        final AsyncCall c = queue.poll();
        if (!c.isDone()) {
          queue.offer(c); // the call is not done yet, add it back.
        }
      }
    }

    /** Process the async calls in the queue. */
    private class Processor {
      static final long GRACE_PERIOD = 10*1000L;
      static final long SLEEP_PERIOD = 100L;

      private final AtomicReference<Thread> running = new AtomicReference<>();

      boolean isRunning(Daemon d) {
        return d == running.get();
      }

      void tryStart() {
        final Thread current = Thread.currentThread();
        if (running.compareAndSet(null, current)) {
          final Daemon daemon = new Daemon() {
            @Override
            public void run() {
              for (; isRunning(this);) {
                try {
                  Thread.sleep(SLEEP_PERIOD);
                } catch (InterruptedException e) {
                  kill(this);
                  return;
                }

                checkCalls();
                tryStop(this);
              }
            }
          };

          final boolean set = running.compareAndSet(current, daemon);
          Preconditions.checkState(set);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Starting AsyncCallQueue.Processor " + daemon);
          }
          daemon.start();
        }
      }

      void tryStop(Daemon d) {
        if (queue.isEmpty(GRACE_PERIOD)) {
          kill(d);
        }
      }

      void kill(Daemon d) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Killing " + d);
        }
        final boolean set = running.compareAndSet(d, null);
        Preconditions.checkState(set);
      }
    }
  }

  static class AsyncValue<V> {
    private V value;

    synchronized V waitAsyncValue(long timeout, TimeUnit unit)
        throws InterruptedException, TimeoutException {
      if (value != null) {
        return value;
      }
      AsyncGet.Util.wait(this, timeout, unit);
      if (value != null) {
        return value;
      }

      throw new TimeoutException("waitCallReturn timed out "
          + timeout + " " + unit);
    }

    synchronized void set(V v) {
      Preconditions.checkNotNull(v);
      Preconditions.checkState(value == null);
      value = v;
      notify();
    }

    synchronized boolean isDone() {
      return value != null;
    }
  }

  static class AsyncCall extends RetryInvocationHandler.Call {
    private final AsyncCallHandler asyncCallHandler;

    private final AsyncValue<CallReturn> asyncCallReturn = new AsyncValue<>();
    private AsyncGet<?, Exception> lowerLayerAsyncGet;

    AsyncCall(Method method, Object[] args, boolean isRpc, int callId,
              RetryInvocationHandler.Counters counters,
              RetryInvocationHandler<?> retryInvocationHandler,
              AsyncCallHandler asyncCallHandler) {
      super(method, args, isRpc, callId, counters, retryInvocationHandler);

      this.asyncCallHandler = asyncCallHandler;
    }

    /** @return true if the call is done; otherwise, return false. */
    boolean isDone() {
      final CallReturn r = invokeOnce();
      switch (r.getState()) {
        case RETURNED:
        case EXCEPTION:
          asyncCallReturn.set(r); // the async call is done
          return true;
        case RETRY:
          invokeOnce();
          break;
        case ASYNC_CALL_IN_PROGRESS:
        case ASYNC_INVOKED:
          // nothing to do
          break;
        default:
          Preconditions.checkState(false);
      }
      return false;
    }

    @Override
    CallReturn invoke() throws Throwable {
      LOG.debug("{}.invoke {}", getClass().getSimpleName(), this);
      if (lowerLayerAsyncGet != null) {
        // async call was submitted early, check the lower level async call
        final boolean isDone = lowerLayerAsyncGet.isDone();
        LOG.trace("invoke: lowerLayerAsyncGet.isDone()? {}", isDone);
        if (!isDone) {
          return CallReturn.ASYNC_CALL_IN_PROGRESS;
        }
        try {
          return new CallReturn(lowerLayerAsyncGet.get(0, TimeUnit.SECONDS));
        } finally {
          lowerLayerAsyncGet = null;
        }
      }

      // submit a new async call
      LOG.trace("invoke: ASYNC_INVOKED");
      final boolean mode = Client.isAsynchronousMode();
      try {
        Client.setAsynchronousMode(true);
        final Object r = invokeMethod();
        // invokeMethod should set LOWER_LAYER_ASYNC_RETURN and return null.
        Preconditions.checkState(r == null);
        lowerLayerAsyncGet = getLowerLayerAsyncReturn();

        if (counters.isZeros()) {
          // first async attempt, initialize
          LOG.trace("invoke: initAsyncCall");
          asyncCallHandler.initAsyncCall(this, asyncCallReturn);
        }
        return CallReturn.ASYNC_INVOKED;
      } finally {
        Client.setAsynchronousMode(mode);
      }
    }
  }

  private final AsyncCallQueue asyncCalls = new AsyncCallQueue();
  private volatile boolean hasSuccessfulCall = false;

  AsyncCall newAsyncCall(Method method, Object[] args, boolean isRpc,
                         int callId, RetryInvocationHandler.Counters counters,
                         RetryInvocationHandler<?> retryInvocationHandler) {
    return new AsyncCall(method, args, isRpc, callId, counters,
        retryInvocationHandler, this);
  }

  boolean hasSuccessfulCall() {
    return hasSuccessfulCall;
  }

  private void initAsyncCall(final AsyncCall asyncCall,
                             final AsyncValue<CallReturn> asyncCallReturn) {
    asyncCalls.addCall(asyncCall);

    final AsyncGet<Object, Throwable> asyncGet
        = new AsyncGet<Object, Throwable>() {
      @Override
      public Object get(long timeout, TimeUnit unit) throws Throwable {
        final CallReturn c = asyncCallReturn.waitAsyncValue(timeout, unit);
        final Object r = c.getReturnValue();
        hasSuccessfulCall = true;
        return r;
      }

      @Override
      public boolean isDone() {
        return asyncCallReturn.isDone();
      }
    };
    ASYNC_RETURN.set(asyncGet);
  }
}
