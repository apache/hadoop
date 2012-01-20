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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;

class RetryInvocationHandler implements InvocationHandler, Closeable {
  public static final Log LOG = LogFactory.getLog(RetryInvocationHandler.class);
  private FailoverProxyProvider proxyProvider;
  
  private RetryPolicy defaultPolicy;
  private Map<String,RetryPolicy> methodNameToPolicyMap;
  private Object currentProxy;
  
  public RetryInvocationHandler(FailoverProxyProvider proxyProvider,
      RetryPolicy retryPolicy) {
    this.proxyProvider = proxyProvider;
    this.defaultPolicy = retryPolicy;
    this.methodNameToPolicyMap = Collections.emptyMap();
    this.currentProxy = proxyProvider.getProxy();
  }
  
  public RetryInvocationHandler(FailoverProxyProvider proxyProvider,
      Map<String, RetryPolicy> methodNameToPolicyMap) {
    this.proxyProvider = proxyProvider;
    this.defaultPolicy = RetryPolicies.TRY_ONCE_THEN_FAIL;
    this.methodNameToPolicyMap = methodNameToPolicyMap;
    this.currentProxy = proxyProvider.getProxy();
  }

  public Object invoke(Object proxy, Method method, Object[] args)
    throws Throwable {
    RetryPolicy policy = methodNameToPolicyMap.get(method.getName());
    if (policy == null) {
      policy = defaultPolicy;
    }
    
    int failovers = 0;
    int retries = 0;
    while (true) {
      try {
        return invokeMethod(method, args);
      } catch (Exception e) {
        boolean isMethodIdempotent = proxyProvider.getInterface()
            .getMethod(method.getName(), method.getParameterTypes())
            .isAnnotationPresent(Idempotent.class);
        RetryAction action = policy.shouldRetry(e, retries++, failovers,
            isMethodIdempotent);
        if (action == RetryAction.FAIL) {
          LOG.warn("Exception while invoking " + method.getName()
                   + " of " + currentProxy.getClass() + ". Not retrying.", e);
          if (!method.getReturnType().equals(Void.TYPE)) {
            throw e; // non-void methods can't fail without an exception
          }
          return null;
        } else if (action == RetryAction.FAILOVER_AND_RETRY) {
          LOG.warn("Exception while invoking " + method.getName()
              + " of " + currentProxy.getClass()
              + ". Trying to fail over.", e);
          failovers++;
          proxyProvider.performFailover(currentProxy);
          currentProxy = proxyProvider.getProxy();
        }
        if(LOG.isDebugEnabled()) {
          LOG.debug("Exception while invoking " + method.getName()
              + " of " + currentProxy.getClass() + ". Retrying.", e);
        }
      }
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

}
