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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcInvocationHandler;

class RetryInvocationHandler implements RpcInvocationHandler {
  public static final Log LOG = LogFactory.getLog(RetryInvocationHandler.class);
  private final FailoverProxyProvider proxyProvider;

  /**
   * The number of times the associated proxyProvider has ever been failed over.
   */
  private long proxyProviderFailoverCount = 0;
  private volatile boolean hasMadeASuccessfulCall = false;
  
  private final RetryPolicy defaultPolicy;
  private final Map<String,RetryPolicy> methodNameToPolicyMap;
  private Object currentProxy;
  
  public RetryInvocationHandler(FailoverProxyProvider proxyProvider,
      RetryPolicy retryPolicy) {
    this(proxyProvider, retryPolicy, Collections.<String, RetryPolicy>emptyMap());
  }

  public RetryInvocationHandler(FailoverProxyProvider proxyProvider,
      RetryPolicy defaultPolicy,
      Map<String, RetryPolicy> methodNameToPolicyMap) {
    this.proxyProvider = proxyProvider;
    this.defaultPolicy = defaultPolicy;
    this.methodNameToPolicyMap = methodNameToPolicyMap;
    this.currentProxy = proxyProvider.getProxy();
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args)
    throws Throwable {
    RetryPolicy policy = methodNameToPolicyMap.get(method.getName());
    if (policy == null) {
      policy = defaultPolicy;
    }
    
    // The number of times this method invocation has been failed over.
    int invocationFailoverCount = 0;
    int retries = 0;
    while (true) {
      // The number of times this invocation handler has ever been failed over,
      // before this method invocation attempt. Used to prevent concurrent
      // failed method invocations from triggering multiple failover attempts.
      long invocationAttemptFailoverCount;
      synchronized (proxyProvider) {
        invocationAttemptFailoverCount = proxyProviderFailoverCount;
      }
      try {
        Object ret = invokeMethod(method, args);
        hasMadeASuccessfulCall = true;
        return ret;
      } catch (Exception e) {
        boolean isMethodIdempotent = proxyProvider.getInterface()
            .getMethod(method.getName(), method.getParameterTypes())
            .isAnnotationPresent(Idempotent.class);
        RetryAction action = policy.shouldRetry(e, retries++, invocationFailoverCount,
            isMethodIdempotent);
        if (action.action == RetryAction.RetryDecision.FAIL) {
          if (action.reason != null) {
            LOG.warn("Exception while invoking " + 
                currentProxy.getClass() + "." + method.getName() +
                ". Not retrying because " + action.reason, e);
          }
          throw e;
        } else { // retry or failover
          // avoid logging the failover if this is the first call on this
          // proxy object, and we successfully achieve the failover without
          // any flip-flopping
          boolean worthLogging = 
            !(invocationFailoverCount == 0 && !hasMadeASuccessfulCall);
          worthLogging |= LOG.isDebugEnabled();
          if (action.action == RetryAction.RetryDecision.FAILOVER_AND_RETRY &&
              worthLogging) {
            String msg = "Exception while invoking " + method.getName()
              + " of class " + currentProxy.getClass().getSimpleName();
            if (invocationFailoverCount > 0) {
              msg += " after " + invocationFailoverCount + " fail over attempts"; 
            }
            msg += ". Trying to fail over " + formatSleepMessage(action.delayMillis);
            if (LOG.isDebugEnabled()) {
              LOG.debug(msg, e);
            } else {
              LOG.warn(msg);
            }
          } else {
            if(LOG.isDebugEnabled()) {
              LOG.debug("Exception while invoking " + method.getName()
                  + " of class " + currentProxy.getClass().getSimpleName() +
                  ". Retrying " + formatSleepMessage(action.delayMillis), e);
            }
          }
          
          if (action.delayMillis > 0) {
            ThreadUtil.sleepAtLeastIgnoreInterrupts(action.delayMillis);
          }
          
          if (action.action == RetryAction.RetryDecision.FAILOVER_AND_RETRY) {
            // Make sure that concurrent failed method invocations only cause a
            // single actual fail over.
            synchronized (proxyProvider) {
              if (invocationAttemptFailoverCount == proxyProviderFailoverCount) {
                proxyProvider.performFailover(currentProxy);
                proxyProviderFailoverCount++;
                currentProxy = proxyProvider.getProxy();
              } else {
                LOG.warn("A failover has occurred since the start of this method"
                    + " invocation attempt.");
              }
            }
            invocationFailoverCount++;
          }
        }
      }
    }
  }
  
  private static String formatSleepMessage(long millis) {
    if (millis > 0) {
      return "after sleeping for " + millis + "ms.";
    } else {
      return "immediately.";
    }
  }
  
  private Object invokeMethod(Method method, Object[] args) throws Throwable {
    try {
      if (!method.isAccessible()) {
        method.setAccessible(true);
      }
      return method.invoke(currentProxy, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  @Override
  public void close() throws IOException {
    proxyProvider.close();
  }

  @Override //RpcInvocationHandler
  public ConnectionId getConnectionId() {
    return RPC.getConnectionIdForProxy(currentProxy);
  }

}
