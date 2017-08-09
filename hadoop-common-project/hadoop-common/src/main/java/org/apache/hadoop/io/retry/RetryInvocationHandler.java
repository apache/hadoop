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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.retry.FailoverProxyProvider.ProxyInfo;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * A {@link RpcInvocationHandler} which supports client side retry .
 */
@InterfaceAudience.Private
public class RetryInvocationHandler<T> implements RpcInvocationHandler {
  public static final Logger LOG = LoggerFactory.getLogger(
      RetryInvocationHandler.class);

  static class Call {
    private final Method method;
    private final Object[] args;
    private final boolean isRpc;
    private final int callId;
    private final Counters counters = new Counters();

    private final RetryPolicy retryPolicy;
    private final RetryInvocationHandler<?> retryInvocationHandler;

    private RetryInfo retryInfo;

    Call(Method method, Object[] args, boolean isRpc, int callId,
         RetryInvocationHandler<?> retryInvocationHandler) {
      this.method = method;
      this.args = args;
      this.isRpc = isRpc;
      this.callId = callId;

      this.retryPolicy = retryInvocationHandler.getRetryPolicy(method);
      this.retryInvocationHandler = retryInvocationHandler;
    }

    int getCallId() {
      return callId;
    }

    Counters getCounters() {
      return counters;
    }

    synchronized Long getWaitTime(final long now) {
      return retryInfo == null? null: retryInfo.retryTime - now;
    }

    /** Invoke the call once without retrying. */
    synchronized CallReturn invokeOnce() {
      try {
        if (retryInfo != null) {
          return processWaitTimeAndRetryInfo();
        }

        // The number of times this invocation handler has ever been failed over
        // before this method invocation attempt. Used to prevent concurrent
        // failed method invocations from triggering multiple failover attempts.
        final long failoverCount = retryInvocationHandler.getFailoverCount();
        try {
          return invoke();
        } catch (Exception e) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(toString(), e);
          }
          if (Thread.currentThread().isInterrupted()) {
            // If interrupted, do not retry.
            throw e;
          }

          retryInfo = retryInvocationHandler.handleException(
              method, callId, retryPolicy, counters, failoverCount, e);
          return processWaitTimeAndRetryInfo();
        }
      } catch(Throwable t) {
        return new CallReturn(t);
      }
    }

    /**
     * It first processes the wait time, if there is any,
     * and then invokes {@link #processRetryInfo()}.
     *
     * If the wait time is positive, it either sleeps for synchronous calls
     * or immediately returns for asynchronous calls.
     *
     * @return {@link CallReturn#RETRY} if the retryInfo is processed;
     *         otherwise, return {@link CallReturn#WAIT_RETRY}.
     */
    CallReturn processWaitTimeAndRetryInfo() throws InterruptedIOException {
      final Long waitTime = getWaitTime(Time.monotonicNow());
      LOG.trace("#{} processRetryInfo: retryInfo={}, waitTime={}",
          callId, retryInfo, waitTime);
      if (waitTime != null && waitTime > 0) {
        try {
          Thread.sleep(retryInfo.delay);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Interrupted while waiting to retry", e);
          }
          InterruptedIOException intIOE = new InterruptedIOException(
              "Retry interrupted");
          intIOE.initCause(e);
          throw intIOE;
        }
      }
      processRetryInfo();
      return CallReturn.RETRY;
    }

    synchronized void processRetryInfo() {
      counters.retries++;
      if (retryInfo.isFailover()) {
        retryInvocationHandler.proxyDescriptor.failover(
            retryInfo.expectedFailoverCount, method, callId);
        counters.failovers++;
      }
      retryInfo = null;
    }

    CallReturn invoke() throws Throwable {
      return new CallReturn(invokeMethod());
    }

    Object invokeMethod() throws Throwable {
      if (isRpc) {
        Client.setCallIdAndRetryCount(callId, counters.retries,
            retryInvocationHandler.asyncCallHandler);
      }
      return retryInvocationHandler.invokeMethod(method, args);
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "#" + callId + ": "
          + method.getDeclaringClass().getSimpleName() + "." + method.getName()
          + "(" + (args == null || args.length == 0? "": Arrays.toString(args))
          +  ")";
    }
  }

  static class Counters {
    /** Counter for retries. */
    private int retries;
    /** Counter for method invocation has been failed over. */
    private int failovers;

    boolean isZeros() {
      return retries == 0 && failovers == 0;
    }
  }

  private static class ProxyDescriptor<T> {
    private final FailoverProxyProvider<T> fpp;
    /** Count the associated proxy provider has ever been failed over. */
    private long failoverCount = 0;

    private ProxyInfo<T> proxyInfo;

    ProxyDescriptor(FailoverProxyProvider<T> fpp) {
      this.fpp = fpp;
      this.proxyInfo = fpp.getProxy();
    }

    synchronized ProxyInfo<T> getProxyInfo() {
      return proxyInfo;
    }

    synchronized T getProxy() {
      return proxyInfo.proxy;
    }

    synchronized long getFailoverCount() {
      return failoverCount;
    }

    synchronized void failover(long expectedFailoverCount, Method method,
                               int callId) {
      // Make sure that concurrent failed invocations only cause a single
      // actual failover.
      if (failoverCount == expectedFailoverCount) {
        fpp.performFailover(proxyInfo.proxy);
        failoverCount++;
      } else {
        LOG.warn("A failover has occurred since the start of call #" + callId
            + " " + proxyInfo.getString(method.getName()));
      }
      proxyInfo = fpp.getProxy();
    }

    boolean idempotentOrAtMostOnce(Method method) throws NoSuchMethodException {
      final Method m = fpp.getInterface()
          .getMethod(method.getName(), method.getParameterTypes());
      return m.isAnnotationPresent(Idempotent.class)
          || m.isAnnotationPresent(AtMostOnce.class);
    }

    void close() throws IOException {
      fpp.close();
    }
  }

  private static class RetryInfo {
    private final long retryTime;
    private final long delay;
    private final RetryAction action;
    private final long expectedFailoverCount;
    private final Exception failException;

    RetryInfo(long delay, RetryAction action, long expectedFailoverCount,
        Exception failException) {
      this.delay = delay;
      this.retryTime = Time.monotonicNow() + delay;
      this.action = action;
      this.expectedFailoverCount = expectedFailoverCount;
      this.failException = failException;
    }

    boolean isFailover() {
      return action != null
          && action.action ==  RetryAction.RetryDecision.FAILOVER_AND_RETRY;
    }

    boolean isFail() {
      return action != null
          && action.action ==  RetryAction.RetryDecision.FAIL;
    }

    Exception getFailException() {
      return failException;
    }

    static RetryInfo newRetryInfo(RetryPolicy policy, Exception e,
        Counters counters, boolean idempotentOrAtMostOnce,
        long expectedFailoverCount) throws Exception {
      RetryAction max = null;
      long maxRetryDelay = 0;
      Exception ex = null;

      final Iterable<Exception> exceptions = e instanceof MultiException ?
          ((MultiException) e).getExceptions().values()
          : Collections.singletonList(e);
      for (Exception exception : exceptions) {
        final RetryAction a = policy.shouldRetry(exception,
            counters.retries, counters.failovers, idempotentOrAtMostOnce);
        if (a.action != RetryAction.RetryDecision.FAIL) {
          // must be a retry or failover
          if (a.delayMillis > maxRetryDelay) {
            maxRetryDelay = a.delayMillis;
          }
        }

        if (max == null || max.action.compareTo(a.action) < 0) {
          max = a;
          if (a.action == RetryAction.RetryDecision.FAIL) {
            ex = exception;
          }
        }
      }

      return new RetryInfo(maxRetryDelay, max, expectedFailoverCount, ex);
    }

    @Override
    public String toString() {
      return "RetryInfo{" +
              "retryTime=" + retryTime +
              ", delay=" + delay +
              ", action=" + action +
              ", expectedFailoverCount=" + expectedFailoverCount +
              ", failException=" + failException +
              '}';
    }
  }

  private final ProxyDescriptor<T> proxyDescriptor;

  private volatile boolean hasSuccessfulCall = false;

  private final RetryPolicy defaultPolicy;
  private final Map<String,RetryPolicy> methodNameToPolicyMap;

  private final AsyncCallHandler asyncCallHandler = new AsyncCallHandler();

  protected RetryInvocationHandler(FailoverProxyProvider<T> proxyProvider,
      RetryPolicy retryPolicy) {
    this(proxyProvider, retryPolicy, Collections.<String, RetryPolicy>emptyMap());
  }

  protected RetryInvocationHandler(FailoverProxyProvider<T> proxyProvider,
      RetryPolicy defaultPolicy,
      Map<String, RetryPolicy> methodNameToPolicyMap) {
    this.proxyDescriptor = new ProxyDescriptor<>(proxyProvider);
    this.defaultPolicy = defaultPolicy;
    this.methodNameToPolicyMap = methodNameToPolicyMap;
  }

  private RetryPolicy getRetryPolicy(Method method) {
    final RetryPolicy policy = methodNameToPolicyMap.get(method.getName());
    return policy != null? policy: defaultPolicy;
  }

  private long getFailoverCount() {
    return proxyDescriptor.getFailoverCount();
  }

  private Call newCall(Method method, Object[] args, boolean isRpc,
                       int callId) {
    if (Client.isAsynchronousMode()) {
      return asyncCallHandler.newAsyncCall(method, args, isRpc, callId, this);
    } else {
      return new Call(method, args, isRpc, callId, this);
    }
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
    final boolean isRpc = isRpcInvocation(proxyDescriptor.getProxy());
    final int callId = isRpc? Client.nextCallId(): RpcConstants.INVALID_CALL_ID;

    final Call call = newCall(method, args, isRpc, callId);
    while (true) {
      final CallReturn c = call.invokeOnce();
      final CallReturn.State state = c.getState();
      if (state == CallReturn.State.ASYNC_INVOKED) {
        return null; // return null for async calls
      } else if (c.getState() != CallReturn.State.RETRY) {
        return c.getReturnValue();
      }
    }
  }

  private RetryInfo handleException(final Method method, final int callId,
      final RetryPolicy policy, final Counters counters,
      final long expectFailoverCount, final Exception e) throws Exception {
    final RetryInfo retryInfo = RetryInfo.newRetryInfo(policy, e,
        counters, proxyDescriptor.idempotentOrAtMostOnce(method),
        expectFailoverCount);
    if (retryInfo.isFail()) {
      // fail.
      if (retryInfo.action.reason != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Exception while invoking call #" + callId + " "
              + proxyDescriptor.getProxyInfo().getString(method.getName())
              + ". Not retrying because " + retryInfo.action.reason, e);
        }
      }
      throw retryInfo.getFailException();
    }

    log(method, retryInfo.isFailover(), counters.failovers, retryInfo.delay, e);
    return retryInfo;
  }

  private void log(final Method method, final boolean isFailover,
      final int failovers, final long delay, final Exception ex) {
    // log info if this has made some successful calls or
    // this is not the first failover
    final boolean info = hasSuccessfulCall || failovers != 0
        || asyncCallHandler.hasSuccessfulCall();
    if (!info && !LOG.isDebugEnabled()) {
      return;
    }

    final StringBuilder b = new StringBuilder()
        .append(ex + ", while invoking ")
        .append(proxyDescriptor.getProxyInfo().getString(method.getName()));
    if (failovers > 0) {
      b.append(" after ").append(failovers).append(" failover attempts");
    }
    b.append(isFailover? ". Trying to failover ": ". Retrying ");
    b.append(delay > 0? "after sleeping for " + delay + "ms.": "immediately.");

    if (info) {
      LOG.info(b.toString());
    } else {
      LOG.debug(b.toString(), ex);
    }
  }

  protected Object invokeMethod(Method method, Object[] args) throws Throwable {
    try {
      if (!method.isAccessible()) {
        method.setAccessible(true);
      }
      final Object r = method.invoke(proxyDescriptor.getProxy(), args);
      hasSuccessfulCall = true;
      return r;
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  @VisibleForTesting
  static boolean isRpcInvocation(Object proxy) {
    if (proxy instanceof ProtocolTranslator) {
      proxy = ((ProtocolTranslator) proxy).getUnderlyingProxyObject();
    }
    if (!Proxy.isProxyClass(proxy.getClass())) {
      return false;
    }
    final InvocationHandler ih = Proxy.getInvocationHandler(proxy);
    return ih instanceof RpcInvocationHandler;
  }

  @Override
  public void close() throws IOException {
    proxyDescriptor.close();
  }

  @Override //RpcInvocationHandler
  public ConnectionId getConnectionId() {
    return RPC.getConnectionIdForProxy(proxyDescriptor.getProxy());
  }
}
