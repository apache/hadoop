/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.List;

import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.ClientGSIContext;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.io.retry.AtMostOnce;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.ObserverRetryOnActiveException;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * A {@link org.apache.hadoop.io.retry.FailoverProxyProvider} implementation
 * that supports reading from observer namenode(s).
 *
 * This constructs a wrapper proxy that sends the request to observer
 * namenode(s), if observer read is enabled. In case there are multiple
 * observer namenodes, it will try them one by one in case the RPC failed. It
 * will fail back to the active namenode after it has exhausted all the
 * observer namenodes.
 *
 * Read and write requests will still be sent to active NN if reading from
 * observer is turned off.
 */
public class ObserverReadProxyProvider<T extends ClientProtocol>
    extends AbstractNNFailoverProxyProvider<T> {
  private static final Logger LOG = LoggerFactory.getLogger(
      ObserverReadProxyProvider.class);

  /** Client-side context for syncing with the NameNode server side. */
  private final AlignmentContext alignmentContext;

  /** The inner proxy provider used for active/standby failover. */
  private final AbstractNNFailoverProxyProvider<T> failoverProxy;
  /** List of all NameNode proxies. */
  private final List<NNProxyInfo<T>> nameNodeProxies;

  /** The policy used to determine if an exception is fatal or retriable. */
  private final RetryPolicy observerRetryPolicy;
  /** The combined proxy which redirects to other proxies as necessary. */
  private final ProxyInfo<T> combinedProxy;

  /**
   * Whether reading from observer is enabled. If this is false, all read
   * requests will still go to active NN.
   */
  private boolean observerReadEnabled;

  /**
   * The index into the nameNodeProxies list currently being used. Should only
   * be accessed in synchronized methods.
   */
  private int currentIndex = -1;

  /**
   * The proxy being used currently. Should only be accessed in synchronized
   * methods.
   */
  private NNProxyInfo<T> currentProxy;

  /** The last proxy that has been used. Only used for testing. */
  private volatile ProxyInfo<T> lastProxy = null;

  /**
   * By default ObserverReadProxyProvider uses
   * {@link ConfiguredFailoverProxyProvider} for failover.
   */
  public ObserverReadProxyProvider(
      Configuration conf, URI uri, Class<T> xface, HAProxyFactory<T> factory) {
    this(conf, uri, xface, factory,
        new ConfiguredFailoverProxyProvider<>(conf, uri, xface,factory));
  }

  @SuppressWarnings("unchecked")
  public ObserverReadProxyProvider(
      Configuration conf, URI uri, Class<T> xface, HAProxyFactory<T> factory,
      AbstractNNFailoverProxyProvider<T> failoverProxy) {
    super(conf, uri, xface, factory);
    this.failoverProxy = failoverProxy;
    this.alignmentContext = new ClientGSIContext();
    ((ClientHAProxyFactory<T>) factory).setAlignmentContext(alignmentContext);

    // Don't bother configuring the number of retries and such on the retry
    // policy since it is mainly only used for determining whether or not an
    // exception is retriable or fatal
    observerRetryPolicy = RetryPolicies.failoverOnNetworkException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, 1);

    // Get all NameNode proxies
    nameNodeProxies = getProxyAddresses(uri,
        HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);

    // Create a wrapped proxy containing all the proxies. Since this combined
    // proxy is just redirecting to other proxies, all invocations can share it.
    StringBuilder combinedInfo = new StringBuilder("[");
    for (int i = 0; i < nameNodeProxies.size(); i++) {
      if (i > 0) {
        combinedInfo.append(",");
      }
      combinedInfo.append(nameNodeProxies.get(i).proxyInfo);
    }
    combinedInfo.append(']');
    T wrappedProxy = (T) Proxy.newProxyInstance(
        ObserverReadInvocationHandler.class.getClassLoader(),
        new Class<?>[] { xface }, new ObserverReadInvocationHandler());
    combinedProxy = new ProxyInfo<>(wrappedProxy, combinedInfo.toString());
    // TODO : make this configurable or remove this variable
    this.observerReadEnabled = true;
  }

  public AlignmentContext getAlignmentContext() {
    return alignmentContext;
  }

  @Override
  public ProxyInfo<T> getProxy() {
    return combinedProxy;
  }

  @Override
  public void performFailover(T currentProxy) {
    failoverProxy.performFailover(currentProxy);
  }

  /**
   * Check if a method is read-only.
   *
   * @return whether the 'method' is a read-only operation.
   */
  private static boolean isRead(Method method) {
    if (!method.isAnnotationPresent(ReadOnly.class)) {
      return false;
    }
    return !method.getAnnotation(ReadOnly.class).activeOnly();
  }

  @VisibleForTesting
  void setObserverReadEnabled(boolean flag) {
    this.observerReadEnabled = flag;
  }

  @VisibleForTesting
  ProxyInfo<T> getLastProxy() {
    return lastProxy;
  }

  /**
   * Return the currently used proxy. If there is none, first calls
   * {@link #changeProxy(NNProxyInfo)} to initialize one.
   */
  private NNProxyInfo<T> getCurrentProxy() {
    if (currentProxy == null) {
      changeProxy(null);
    }
    return currentProxy;
  }

  /**
   * Move to the next proxy in the proxy list. If the NNProxyInfo supplied by
   * the caller does not match the current proxy, the call is ignored; this is
   * to handle concurrent calls (to avoid changing the proxy multiple times).
   * The service state of the newly selected proxy will be updated before
   * returning.
   *
   * @param initial The expected current proxy
   */
  private synchronized void changeProxy(NNProxyInfo<T> initial) {
    if (currentProxy != initial) {
      // Must have been a concurrent modification; ignore the move request
      return;
    }
    // Attempt to force concurrent callers of getCurrentProxy to wait for the
    // new proxy; best-effort by setting currentProxy to null
    currentProxy = null;
    currentIndex = (currentIndex + 1) % nameNodeProxies.size();
    currentProxy = createProxyIfNeeded(nameNodeProxies.get(currentIndex));
    try {
      HAServiceState state = currentProxy.proxy.getHAServiceState();
      currentProxy.setCachedState(state);
    } catch (IOException e) {
      LOG.info("Failed to connect to {}. Setting cached state to Standby",
          currentProxy.getAddress(), e);
      currentProxy.setCachedState(HAServiceState.STANDBY);
    }
    LOG.debug("Changed current proxy from {} to {}",
        initial == null ? "none" : initial.proxyInfo,
        currentProxy.proxyInfo);
  }

  /**
   * An InvocationHandler to handle incoming requests. This class's invoke
   * method contains the primary logic for redirecting to observers.
   * 
   * If observer reads are enabled, attempt to send read operations to the
   * current proxy. If it is not an observer, or the observer fails, adjust
   * the current proxy and retry on the next one. If all proxies are tried
   * without success, the request is forwarded to the active.
   *
   * Write requests are always forwarded to the active.
   */
  private class ObserverReadInvocationHandler implements InvocationHandler {

    @Override
    public Object invoke(Object proxy, final Method method, final Object[] args)
        throws Throwable {
      lastProxy = null;
      Object retVal;

      if (observerReadEnabled && isRead(method)) {
        int failedObserverCount = 0;
        int activeCount = 0;
        int standbyCount = 0;
        for (int i = 0; i < nameNodeProxies.size(); i++) {
          NNProxyInfo<T> current = getCurrentProxy();
          HAServiceState currState = current.getCachedState();
          if (currState != HAServiceState.OBSERVER) {
            if (currState == HAServiceState.ACTIVE) {
              activeCount++;
            } else if (currState == HAServiceState.STANDBY) {
              standbyCount++;
            }
            LOG.debug("Skipping proxy {} for {} because it is in state {}",
                current.proxyInfo, method.getName(), currState);
            changeProxy(current);
            continue;
          }
          LOG.debug("Attempting to service {} using proxy {}",
              method.getName(), current.proxyInfo);
          try {
            retVal = method.invoke(current.proxy, args);
            lastProxy = current;
            LOG.debug("Invocation of {} using {} was successful",
                method.getName(), current.proxyInfo);
            return retVal;
          } catch (InvocationTargetException ite) {
            if (!(ite.getCause() instanceof Exception)) {
              throw ite.getCause();
            }
            Exception e = (Exception) ite.getCause();
            if (e instanceof RemoteException) {
              RemoteException re = (RemoteException) e;
              Exception unwrapped = re.unwrapRemoteException(
                  ObserverRetryOnActiveException.class);
              if (unwrapped instanceof ObserverRetryOnActiveException) {
                LOG.info("Encountered ObserverRetryOnActiveException from {}." +
                    " Retry active namenode directly.", current.proxyInfo);
                break;
              }
            }
            RetryAction retryInfo = observerRetryPolicy.shouldRetry(e, 0, 0,
                method.isAnnotationPresent(Idempotent.class)
                    || method.isAnnotationPresent(AtMostOnce.class));
            if (retryInfo.action == RetryAction.RetryDecision.FAIL) {
              throw e;
            } else {
              failedObserverCount++;
              LOG.warn(
                  "Invocation returned exception on [{}]; {} failure(s) so far",
                  current.proxyInfo, failedObserverCount, e);
              changeProxy(current);
            }
          }
        }

        // If we get here, it means all observers have failed.
        LOG.warn("{} observers have failed for read request {}; also found " +
            "{} standby and {} active. Falling back to active.",
            failedObserverCount, method.getName(), standbyCount, activeCount);
      }

      // Either all observers have failed, or that it is a write request.
      // In either case, we'll forward the request to active NameNode.
      LOG.debug("Using failoverProxy to service {}", method.getName());
      ProxyInfo<T> activeProxy = failoverProxy.getProxy();
      try {
        retVal = method.invoke(activeProxy.proxy, args);
      } catch (InvocationTargetException e) {
        // This exception will be handled by higher layers
        throw e.getCause();
      }
      lastProxy = activeProxy;
      return retVal;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    for (ProxyInfo<T> pi : nameNodeProxies) {
      if (pi.proxy != null) {
        if (pi.proxy instanceof Closeable) {
          ((Closeable)pi.proxy).close();
        } else {
          RPC.stopProxy(pi.proxy);
        }
        // Set to null to avoid the failoverProxy having to re-do the close
        // if it is sharing a proxy instance
        pi.proxy = null;
      }
    }
    failoverProxy.close();
  }

  @Override
  public boolean useLogicalURI() {
    return failoverProxy.useLogicalURI();
  }
}
