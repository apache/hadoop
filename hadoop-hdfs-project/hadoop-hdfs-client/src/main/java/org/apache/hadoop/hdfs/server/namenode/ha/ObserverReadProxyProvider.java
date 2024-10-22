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

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_NAMENODE_MSYNC_RPC_ADDRESS_KEY;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
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
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.ObserverRetryOnActiveException;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RpcInvocationHandler;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

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
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ObserverReadProxyProvider<T>
    extends AbstractNNFailoverProxyProvider<T> {
  @VisibleForTesting
  static final Logger LOG = LoggerFactory.getLogger(
      ObserverReadProxyProvider.class);

  /** Configuration key for {@link #autoMsyncPeriodMs}. */
  static final String AUTO_MSYNC_PERIOD_KEY_PREFIX =
      HdfsClientConfigKeys.Failover.PREFIX + "observer.auto-msync-period";
  /** Auto-msync disabled by default. */
  static final long AUTO_MSYNC_PERIOD_DEFAULT = -1;

  /** Client-side context for syncing with the NameNode server side. */
  private final AlignmentContext alignmentContext;

  /** Configuration key for {@link #observerProbeRetryPeriodMs}. */
  static final String OBSERVER_PROBE_RETRY_PERIOD_KEY =
      HdfsClientConfigKeys.Failover.PREFIX + "observer.probe.retry.period";
  /** Observer probe retry period default to 10 min. */
  static final long OBSERVER_PROBE_RETRY_PERIOD_DEFAULT = 60 * 10 * 1000;

  /**
   * Timeout in ms to cancel the ha-state probe rpc request for an namenode.
   * To disable timeout, set it to 0 or a negative value.
   */
  static final String NAMENODE_HA_STATE_PROBE_TIMEOUT =
      HdfsClientConfigKeys.Failover.PREFIX + "namenode.ha-state.probe.timeout";
  /**
   * Default to disable namenode ha-state probe timeout.
   */
  static final long NAMENODE_HA_STATE_PROBE_TIMEOUT_DEFAULT = 0;

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
   * This adjusts how frequently this proxy provider should auto-msync to the
   * Active NameNode, automatically performing an msync() call to the active
   * to fetch the current transaction ID before submitting read requests to
   * observer nodes. See HDFS-14211 for more description of this feature.
   * If this is below 0, never auto-msync. If this is 0, perform an msync on
   * every read operation. If this is above 0, perform an msync after this many
   * ms have elapsed since the last msync.
   */
  private final long autoMsyncPeriodMs;

  /**
   * The time, in millisecond epoch, that the last msync operation was
   * performed. This includes any implicit msync (any operation which is
   * serviced by the Active NameNode).
   */
  private volatile long lastMsyncTimeMs = -1;

  /**
   * A client using an ObserverReadProxyProvider should first sync with the
   * active NameNode on startup. This ensures that the client reads data which
   * is consistent with the state of the world as of the time of its
   * instantiation. This variable will be true after this initial sync has
   * been performed.
   */
  private volatile boolean msynced = false;

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
   * In case there is no Observer node, for every read call, client will try
   * to loop through all Standby nodes and fail eventually. Since there is no
   * guarantee on when Observer node will be enabled. This can be very
   * inefficient.
   * The following value specify the period on how often to retry all Standby.
   */
  private long observerProbeRetryPeriodMs;

  /**
   * Timeout in ms when we try to get the HA state of a namenode.
   */
  private long namenodeHAStateProbeTimeoutMs;

  /**
   * The previous time where zero observer were found. If there was observer,
   * or it is initialization, this is set to 0.
   */
  private long lastObserverProbeTime;

  /**
   * Threadpool to send the getHAServiceState requests.
   */
  private final BlockingThreadPoolExecutorService nnProbingThreadPool;

  /**
   * By default ObserverReadProxyProvider uses
   * {@link ConfiguredFailoverProxyProvider} for failover.
   */
  public ObserverReadProxyProvider(
      Configuration conf, URI uri, Class<T> xface, HAProxyFactory<T> factory) {
    this(conf, uri, xface, factory,
        new ConfiguredFailoverProxyProvider<>(conf, uri, xface, factory, DFS_NAMENODE_MSYNC_RPC_ADDRESS_KEY));
  }

  @SuppressWarnings("unchecked")
  public ObserverReadProxyProvider(
      Configuration conf, URI uri, Class<T> xface, HAProxyFactory<T> factory,
      AbstractNNFailoverProxyProvider<T> failoverProxy) {
    super(conf, uri, xface, factory);
    this.failoverProxy = failoverProxy;
    this.alignmentContext = new ClientGSIContext();
    factory.setAlignmentContext(alignmentContext);
    this.lastObserverProbeTime = 0;

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
        new Class<?>[] {xface}, new ObserverReadInvocationHandler());
    combinedProxy = new ProxyInfo<>(wrappedProxy, combinedInfo.toString());

    autoMsyncPeriodMs = conf.getTimeDuration(
        // The host of the URI is the nameservice ID
        AUTO_MSYNC_PERIOD_KEY_PREFIX + "." + uri.getHost(),
        AUTO_MSYNC_PERIOD_DEFAULT, TimeUnit.MILLISECONDS);
    observerProbeRetryPeriodMs = conf.getTimeDuration(
        OBSERVER_PROBE_RETRY_PERIOD_KEY,
        OBSERVER_PROBE_RETRY_PERIOD_DEFAULT, TimeUnit.MILLISECONDS);
    namenodeHAStateProbeTimeoutMs = conf.getTimeDuration(NAMENODE_HA_STATE_PROBE_TIMEOUT,
        NAMENODE_HA_STATE_PROBE_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);

    if (wrappedProxy instanceof ClientProtocol) {
      this.observerReadEnabled = true;
    } else {
      LOG.info("Disabling observer reads for {} because the requested proxy "
          + "class does not implement {}", uri, ClientProtocol.class.getName());
      this.observerReadEnabled = false;
    }

    /*
     * At most 4 threads will be running and each thread will die after 10
     * seconds of no use. Up to 132 tasks (4 active + 128 waiting) can be
     * submitted simultaneously.
     */
    nnProbingThreadPool =
        BlockingThreadPoolExecutorService.newInstance(4, 128, 10L, TimeUnit.SECONDS,
            "nn-ha-state-probing");
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
    return !method.getAnnotationsByType(ReadOnly.class)[0].activeOnly();
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
    return changeProxy(null);
  }

  /**
   * Move to the next proxy in the proxy list. If the NNProxyInfo supplied by
   * the caller does not match the current proxy, the call is ignored; this is
   * to handle concurrent calls (to avoid changing the proxy multiple times).
   * The service state of the newly selected proxy will be updated before
   * returning.
   *
   * @param initial The expected current proxy
   * @return The new proxy that should be used.
   */
  private synchronized NNProxyInfo<T> changeProxy(NNProxyInfo<T> initial) {
    if (currentProxy != initial) {
      // Must have been a concurrent modification; ignore the move request
      return currentProxy;
    }
    currentIndex = (currentIndex + 1) % nameNodeProxies.size();
    currentProxy = createProxyIfNeeded(nameNodeProxies.get(currentIndex));
    currentProxy.setCachedState(getHAServiceStateWithTimeout(currentProxy));
    LOG.debug("Changed current proxy from {} to {}",
        initial == null ? "none" : initial.proxyInfo,
        currentProxy.proxyInfo);
    return currentProxy;
  }

  /**
   * Execute getHAServiceState() call with a timeout, to avoid a long wait when
   * an NN becomes irresponsive to rpc requests
   * (when a thread/heap dump is being taken, e.g.).
   *
   * For each getHAServiceState() call, a task is created and submitted to a
   * threadpool for execution. We will wait for a response up to
   * namenodeHAStateProbeTimeoutSec and cancel these requests if they time out.
   *
   * The implementation is split into two functions so that we can unit test
   * the second function.
   */
  HAServiceState getHAServiceStateWithTimeout(final NNProxyInfo<T> proxyInfo) {
    Callable<HAServiceState> getHAServiceStateTask = () -> getHAServiceState(proxyInfo);

    try {
      Future<HAServiceState> task =
          nnProbingThreadPool.submit(getHAServiceStateTask);
      return getHAServiceStateWithTimeout(proxyInfo, task);
    } catch (RejectedExecutionException e) {
      LOG.warn("Run out of threads to submit the request to query HA state. "
          + "Ok to return null and we will fallback to use active NN to serve "
          + "this request.");
      return null;
    }
  }

  HAServiceState getHAServiceStateWithTimeout(final NNProxyInfo<T> proxyInfo,
      Future<HAServiceState> task) {
    HAServiceState state = null;
    try {
      if (namenodeHAStateProbeTimeoutMs > 0) {
        state = task.get(namenodeHAStateProbeTimeoutMs, TimeUnit.MILLISECONDS);
      } else {
        // Disable timeout by waiting indefinitely when namenodeHAStateProbeTimeoutSec is set to 0
        // or a negative value.
        state = task.get();
      }
      LOG.debug("HA State for {} is {}", proxyInfo.proxyInfo, state);
    } catch (TimeoutException e) {
      // Cancel the task on timeout
      String msg = String.format("Cancel NN probe task due to timeout for %s", proxyInfo.proxyInfo);
      LOG.warn(msg, e);
      if (task != null) {
        task.cancel(true);
      }
    } catch (InterruptedException|ExecutionException e) {
      String msg = String.format("Exception in NN probe task for %s", proxyInfo.proxyInfo);
      LOG.warn(msg, e);
    }

    return state;
  }

  /**
   * Fetch the service state from a proxy. If it is unable to be fetched,
   * assume it is in standby state, but log the exception.
   */
  private HAServiceState getHAServiceState(NNProxyInfo<T> proxyInfo) {
    IOException ioe;
    try {
      return getProxyAsClientProtocol(proxyInfo.proxy).getHAServiceState();
    } catch (RemoteException re) {
      // Though a Standby will allow a getHAServiceState call, it won't allow
      // delegation token lookup, so if DT is used it throws StandbyException
      if (re.unwrapRemoteException() instanceof StandbyException) {
        LOG.debug("NameNode {} threw StandbyException when fetching HAState",
            proxyInfo.getAddress());
        return HAServiceState.STANDBY;
      }
      ioe = re;
    } catch (IOException e) {
      ioe = e;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Failed to connect to {} while fetching HAServiceState",
          proxyInfo.getAddress(), ioe);
    }
    return null;
  }

  /**
   * Return the input proxy, cast as a {@link ClientProtocol}. This catches any
   * {@link ClassCastException} and wraps it in a more helpful message. This
   * should ONLY be called if the caller is certain that the proxy is, in fact,
   * a {@link ClientProtocol}.
   */
  private ClientProtocol getProxyAsClientProtocol(T proxy) {
    assert proxy instanceof ClientProtocol : "BUG: Attempted to use proxy "
        + "of class " + proxy.getClass() + " as if it was a ClientProtocol.";
    return (ClientProtocol) proxy;
  }

  /**
   * This will call {@link ClientProtocol#msync()} on the active NameNode
   * (via the {@link #failoverProxy}) to initialize the state of this client.
   * Calling it multiple times is a no-op; only the first will perform an
   * msync.
   *
   * @see #msynced
   */
  private synchronized void initializeMsync() throws IOException {
    if (msynced) {
      return; // No need for an msync
    }
    getProxyAsClientProtocol(failoverProxy.getProxy().proxy).msync();
    msynced = true;
    lastMsyncTimeMs = Time.monotonicNow();
  }

  /**
   * Check if client need to find an Observer proxy.
   * If current proxy is Active then we should stick to it and postpone probing
   * for Observers for a period of time. When this time expires the client will
   * try to find an Observer again.
   * *
   * @return true if we did not reach the threshold
   * to start looking for Observer, or false otherwise.
   */
  private boolean shouldFindObserver() {
    // lastObserverProbeTime > 0 means we tried, but did not find any
    // Observers yet
    // If lastObserverProbeTime <= 0, previous check found observer, so
    // we should not skip observer read.
    if (lastObserverProbeTime > 0) {
      return Time.monotonicNow() - lastObserverProbeTime
          >= observerProbeRetryPeriodMs;
    }
    return true;
  }

  /**
   * This will call {@link ClientProtocol#msync()} on the active NameNode
   * (via the {@link #failoverProxy}) to update the state of this client, only
   * if at least {@link #autoMsyncPeriodMs} ms has elapsed since the last time
   * an msync was performed.
   *
   * @see #autoMsyncPeriodMs
   */
  private void autoMsyncIfNecessary() throws IOException {
    if (autoMsyncPeriodMs == 0) {
      // Always msync
      getProxyAsClientProtocol(failoverProxy.getProxy().proxy).msync();
    } else if (autoMsyncPeriodMs > 0) {
      if (Time.monotonicNow() - lastMsyncTimeMs > autoMsyncPeriodMs) {
        synchronized (this) {
          // Use a synchronized block so that only one thread will msync
          // if many operations are submitted around the same time.
          // Re-check the entry criterion since the status may have changed
          // while waiting for the lock.
          if (Time.monotonicNow() - lastMsyncTimeMs > autoMsyncPeriodMs) {
            getProxyAsClientProtocol(failoverProxy.getProxy().proxy).msync();
            lastMsyncTimeMs = Time.monotonicNow();
          }
        }
      }
    }
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
  private class ObserverReadInvocationHandler implements RpcInvocationHandler {

    @Override
    public Object invoke(Object proxy, final Method method, final Object[] args)
        throws Throwable {
      lastProxy = null;
      Object retVal;

      if (observerReadEnabled && shouldFindObserver() && isRead(method)) {
        if (!msynced) {
          // An msync() must first be performed to ensure that this client is
          // up-to-date with the active's state. This will only be done once.
          initializeMsync();
        } else {
          autoMsyncIfNecessary();
        }

        int failedObserverCount = 0;
        int activeCount = 0;
        int standbyCount = 0;
        int unreachableCount = 0;
        for (int i = 0; i < nameNodeProxies.size(); i++) {
          NNProxyInfo<T> current = getCurrentProxy();
          HAServiceState currState = current.getCachedState();
          if (currState != HAServiceState.OBSERVER) {
            if (currState == HAServiceState.ACTIVE) {
              activeCount++;
            } else if (currState == HAServiceState.STANDBY) {
              standbyCount++;
            } else if (currState == null) {
              unreachableCount++;
            }
            LOG.debug("Skipping proxy {} for {} because it is in state {}",
                current.proxyInfo, method.getName(),
                currState == null ? "unreachable" : currState);
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
            if (e instanceof InterruptedIOException ||
                e instanceof InterruptedException) {
              // If interrupted, do not retry.
              LOG.warn("Invocation returned interrupted exception on [{}];",
                  current.proxyInfo, e);
              throw e;
            }
            if (e instanceof RemoteException) {
              RemoteException re = (RemoteException) e;
              Exception unwrapped = re.unwrapRemoteException(
                  ObserverRetryOnActiveException.class);
              if (unwrapped instanceof ObserverRetryOnActiveException) {
                LOG.debug("Encountered ObserverRetryOnActiveException from {}." +
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

        // Only log message if there are actual observer failures.
        // Getting here with failedObserverCount = 0 could
        // be that there is simply no Observer node running at all.
        if (failedObserverCount > 0) {
          // If we get here, it means all observers have failed.
          LOG.warn("{} observers have failed for read request {}; "
                  + "also found {} standby, {} active, and {} unreachable. "
                  + "Falling back to active.", failedObserverCount,
              method.getName(), standbyCount, activeCount, unreachableCount);
          lastObserverProbeTime = 0;
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Read falling back to active without observer read "
                + "fail, is there no observer node running?");
          }
          lastObserverProbeTime = Time.monotonicNow();
        }
      }

      // Either all observers have failed, observer reads are disabled,
      // or this is a write request. In any case, forward the request to
      // the active NameNode.
      LOG.debug("Using failoverProxy to service {}", method.getName());
      ProxyInfo<T> activeProxy = failoverProxy.getProxy();
      try {
        retVal = method.invoke(activeProxy.proxy, args);
      } catch (InvocationTargetException e) {
        // This exception will be handled by higher layers
        throw e.getCause();
      }
      // If this was reached, the request reached the active, so the
      // state is up-to-date with active and no further msync is needed.
      msynced = true;
      lastMsyncTimeMs = Time.monotonicNow();
      lastProxy = activeProxy;
      return retVal;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public ConnectionId getConnectionId() {
      return RPC.getConnectionIdForProxy(observerReadEnabled
          ? getCurrentProxy().proxy : failoverProxy.getProxy().proxy);
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
    nnProbingThreadPool.shutdown();
  }

  @Override
  public boolean useLogicalURI() {
    return failoverProxy.useLogicalURI();
  }
}
