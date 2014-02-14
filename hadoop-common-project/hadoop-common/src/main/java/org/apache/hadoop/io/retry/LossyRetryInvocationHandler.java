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
package org.apache.hadoop.io.retry;

import java.lang.reflect.Method;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.RetriableException;

/**
 * A dummy invocation handler extending RetryInvocationHandler. It drops the
 * first N number of responses. This invocation handler is only used for testing.
 */
@InterfaceAudience.Private
public class LossyRetryInvocationHandler<T> extends RetryInvocationHandler<T> {
  private final int numToDrop;
  private static final ThreadLocal<Integer> RetryCount = 
      new ThreadLocal<Integer>();

  public LossyRetryInvocationHandler(int numToDrop,
      FailoverProxyProvider<T> proxyProvider, RetryPolicy retryPolicy) {
    super(proxyProvider, retryPolicy);
    this.numToDrop = numToDrop;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
    RetryCount.set(0);
    return super.invoke(proxy, method, args);
  }

  @Override
  protected Object invokeMethod(Method method, Object[] args) throws Throwable {
    Object result = super.invokeMethod(method, args);
    int retryCount = RetryCount.get();
    if (retryCount < this.numToDrop) {
      RetryCount.set(++retryCount);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Drop the response. Current retryCount == " + retryCount);
      }
      throw new RetriableException("Fake Exception");
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("retryCount == " + retryCount
          + ". It's time to normally process the response");
      }
      return result;
    }
  }
}
