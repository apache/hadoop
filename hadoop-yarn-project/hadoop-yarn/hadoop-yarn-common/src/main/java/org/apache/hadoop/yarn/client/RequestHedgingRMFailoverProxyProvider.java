/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.MultiException;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * A FailoverProxyProvider implementation that technically does not "failover"
 * per-se. It constructs a wrapper proxy that sends the request to ALL
 * underlying proxies simultaneously. Each proxy inside the wrapper proxy will
 * retry the corresponding target. It assumes the in an HA setup, there will
 * be only one Active, and the active should respond faster than any configured
 * standbys. Once it receives a response from any one of the configred proxies,
 * outstanding requests to other proxies are immediately cancelled.
 */
public class RequestHedgingRMFailoverProxyProvider<T>
    extends ConfiguredRMFailoverProxyProvider<T> {

  private static final Log LOG =
      LogFactory.getLog(RequestHedgingRMFailoverProxyProvider.class);

  private volatile String successfulProxy = null;
  private ProxyInfo<T> wrappedProxy = null;
  private Map<String, T> nonRetriableProxy = new HashMap<>();

  @Override
  @SuppressWarnings("unchecked")
  public void init(Configuration configuration, RMProxy<T> rmProxy,
      Class<T> protocol) {
    super.init(configuration, rmProxy, protocol);
    Map<String, ProxyInfo<T>> retriableProxies = new HashMap<>();

    String originalId = HAUtil.getRMHAId(conf);
    for (String rmId : rmServiceIds) {
      conf.set(YarnConfiguration.RM_HA_ID, rmId);
      nonRetriableProxy.put(rmId, super.getProxyInternal());

      T proxy = createRetriableProxy();
      ProxyInfo<T> pInfo = new ProxyInfo<T>(proxy, rmId);
      retriableProxies.put(rmId, pInfo);
    }
    conf.set(YarnConfiguration.RM_HA_ID, originalId);

    T proxyInstance = (T) Proxy.newProxyInstance(
        RMRequestHedgingInvocationHandler.class.getClassLoader(),
        new Class<?>[] {protocol},
        new RMRequestHedgingInvocationHandler(retriableProxies));
    String combinedInfo = Arrays.toString(rmServiceIds);
    wrappedProxy = new ProxyInfo<T>(proxyInstance, combinedInfo);
    LOG.info("Created wrapped proxy for " + combinedInfo);
  }

  @SuppressWarnings("unchecked")
  protected T createRetriableProxy() {
    try {
      // Create proxy that can retry exceptions properly.
      RetryPolicy retryPolicy = RMProxy.createRetryPolicy(conf, false);
      InetSocketAddress rmAddress = rmProxy.getRMAddress(conf, protocol);
      T proxy = RMProxy.<T>getProxy(conf, protocol, rmAddress);
      return (T) RetryProxy.create(protocol, proxy, retryPolicy);
    } catch (IOException ioe) {
      LOG.error("Unable to create proxy to the ResourceManager " + HAUtil
          .getRMHAId(conf), ioe);
      return null;
    }
  }

  class RMRequestHedgingInvocationHandler implements InvocationHandler {

    final private Map<String, ProxyInfo<T>> allProxies;

    public RMRequestHedgingInvocationHandler(
        Map<String, ProxyInfo<T>> allProxies) {
      this.allProxies = new HashMap<>(allProxies);
    }

    protected Object invokeMethod(Object proxy, Method method, Object[] args)
        throws Throwable {
      try {
        return method.invoke(proxy, args);
      } catch (InvocationTargetException ex) {
        throw ex.getCause();
      }
    }

    /**
     * Creates a Executor and invokes all proxies concurrently.
     */
    @Override
    public Object invoke(Object proxy, final Method method,
        final Object[] args) throws Throwable {
      if (successfulProxy != null) {
        return invokeMethod(nonRetriableProxy.get(successfulProxy), method, args);
      }

      ExecutorService executor = null;
      CompletionService<Object> completionService;
      try {
        Map<Future<Object>, ProxyInfo<T>> proxyMap = new HashMap<>();
        int numAttempts = 0;
        executor = HadoopExecutors.newFixedThreadPool(allProxies.size());
        completionService = new ExecutorCompletionService<>(executor);
        for (final ProxyInfo<T> pInfo : allProxies.values()) {
          Callable<Object> c = new Callable<Object>() {
            @Override public Object call() throws Exception {
              return method.invoke(pInfo.proxy, args);
            }
          };
          proxyMap.put(completionService.submit(c), pInfo);
          numAttempts++;
        }

        Map<String, Exception> badResults = new HashMap<>();
        while (numAttempts > 0) {
          Future<Object> callResultFuture = completionService.take();
          String pInfo = proxyMap.get(callResultFuture).proxyInfo;
          Object retVal;
          try {
            retVal = callResultFuture.get();
            successfulProxy = pInfo;
            LOG.info("Invocation successful on [" + pInfo + "]");
            return retVal;
          } catch (Exception ex) {
            LOG.warn("Invocation returned exception on " + "[" + pInfo + "]");
            badResults.put(pInfo, ex);
            numAttempts--;
          }
        }

        // At this point we should have All bad results (Exceptions)
        // Or should have returned with successful result.
        if (badResults.size() == 1) {
          throw badResults.values().iterator().next();
        } else {
          throw new MultiException(badResults);
        }
      } finally {
        if (executor != null) {
          executor.shutdownNow();
        }
      }
    }
  }

  @Override
  public ProxyInfo<T> getProxy() {
    return wrappedProxy;
  }

  @Override
  public void performFailover(T currentProxy) {
    LOG.info("Connection lost, trying to fail over.");
    successfulProxy = null;
  }
}
