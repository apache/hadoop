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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.retry.FailoverProxyProvider.ProxyInfo;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.ipc.Client.ConnectionId;

import java.io.IOException;
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
  public static final Log LOG = LogFactory.getLog(RetryInvocationHandler.class);

  private static class Counters {
    /** Counter for retries. */
    private int retries;
    /** Counter for method invocation has been failed over. */
    private int failovers;
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

    synchronized void failover(long expectedFailoverCount, Method method) {
      // Make sure that concurrent failed invocations only cause a single
      // actual failover.
      if (failoverCount == expectedFailoverCount) {
        fpp.performFailover(proxyInfo.proxy);
        failoverCount++;
      } else {
        LOG.warn("A failover has occurred since the start of "
            + proxyInfo.getString(method.getName()));
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
    private final long delay;
    private final RetryAction failover;
    private final RetryAction fail;

    RetryInfo(long delay, RetryAction failover, RetryAction fail) {
      this.delay = delay;
      this.failover = failover;
      this.fail = fail;
    }

    static RetryInfo newRetryInfo(RetryPolicy policy, Exception e,
        Counters counters, boolean idempotentOrAtMostOnce) throws Exception {
      long maxRetryDelay = 0;
      RetryAction failover = null;
      RetryAction retry = null;
      RetryAction fail = null;

      final Iterable<Exception> exceptions = e instanceof MultiException ?
          ((MultiException) e).getExceptions().values()
          : Collections.singletonList(e);
      for (Exception exception : exceptions) {
        final RetryAction a = policy.shouldRetry(exception,
            counters.retries, counters.failovers, idempotentOrAtMostOnce);
        if (a.action == RetryAction.RetryDecision.FAIL) {
          fail = a;
        } else {
          // must be a retry or failover
          if (a.action == RetryAction.RetryDecision.FAILOVER_AND_RETRY) {
            failover = a;
          } else {
            retry = a;
          }
          if (a.delayMillis > maxRetryDelay) {
            maxRetryDelay = a.delayMillis;
          }
        }
      }

      return new RetryInfo(maxRetryDelay, failover,
          failover == null && retry == null? fail: null);
    }
  }

  private final ProxyDescriptor<T> proxyDescriptor;

  private volatile boolean hasMadeASuccessfulCall = false;
  
  private final RetryPolicy defaultPolicy;
  private final Map<String,RetryPolicy> methodNameToPolicyMap;

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

  @Override
  public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
    final boolean isRpc = isRpcInvocation(proxyDescriptor.getProxy());
    final int callId = isRpc? Client.nextCallId(): RpcConstants.INVALID_CALL_ID;
    return invoke(method, args, isRpc, callId, new Counters());
  }

  private Object invoke(final Method method, final Object[] args,
      final boolean isRpc, final int callId, final Counters counters)
      throws Throwable {
    final RetryPolicy policy = getRetryPolicy(method);

    while (true) {
      // The number of times this invocation handler has ever been failed over,
      // before this method invocation attempt. Used to prevent concurrent
      // failed method invocations from triggering multiple failover attempts.
      final long failoverCount = proxyDescriptor.getFailoverCount();

      if (isRpc) {
        Client.setCallIdAndRetryCount(callId, counters.retries);
      }
      try {
        final Object ret = invokeMethod(method, args);
        hasMadeASuccessfulCall = true;
        return ret;
      } catch (Exception ex) {
        if (Thread.currentThread().isInterrupted()) {
          // If interrupted, do not retry.
          throw ex;
        }
        handleException(method, policy, failoverCount, counters, ex);
      }
    }
  }

  private void handleException(final Method method, final RetryPolicy policy,
      final long expectedFailoverCount, final Counters counters,
      final Exception ex) throws Exception {
    final RetryInfo retryInfo = RetryInfo.newRetryInfo(policy, ex, counters,
        proxyDescriptor.idempotentOrAtMostOnce(method));
    counters.retries++;

    if (retryInfo.fail != null) {
      // fail.
      if (retryInfo.fail.reason != null) {
        LOG.warn("Exception while invoking "
            + proxyDescriptor.getProxyInfo().getString(method.getName())
            + ". Not retrying because " + retryInfo.fail.reason, ex);
      }
      throw ex;
    }

    // retry
    final boolean isFailover = retryInfo.failover != null;

    log(method, isFailover, counters.failovers, retryInfo.delay, ex);

    if (retryInfo.delay > 0) {
      Thread.sleep(retryInfo.delay);
    }

    if (isFailover) {
      proxyDescriptor.failover(expectedFailoverCount, method);
      counters.failovers++;
    }
  }

  private void log(final Method method, final boolean isFailover,
      final int failovers, final long delay, final Exception ex) {
    // log info if this has made some successful calls or
    // this is not the first failover
    final boolean info = hasMadeASuccessfulCall || failovers != 0;
    if (!info && !LOG.isDebugEnabled()) {
      return;
    }

    final StringBuilder b = new StringBuilder()
        .append("Exception while invoking ")
        .append(proxyDescriptor.getProxyInfo().getString(method.getName()));
    if (failovers > 0) {
      b.append(" after ").append(failovers).append(" failover attempts");
    }
    b.append(isFailover? ". Trying to failover ": ". Retrying ");
    b.append(delay > 0? "after sleeping for " + delay + "ms.": "immediately.");

    if (info) {
      LOG.info(b.toString(), ex);
    } else {
      LOG.debug(b.toString(), ex);
    }
  }

  protected Object invokeMethod(Method method, Object[] args) throws Throwable {
    try {
      if (!method.isAccessible()) {
        method.setAccessible(true);
      }
      return method.invoke(proxyDescriptor.getProxy(), args);
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
