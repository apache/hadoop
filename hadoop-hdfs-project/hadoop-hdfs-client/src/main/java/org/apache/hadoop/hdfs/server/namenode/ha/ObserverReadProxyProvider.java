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

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.ClientGSIContext;
import org.apache.hadoop.hdfs.NameNodeProxiesClient;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    extends ConfiguredFailoverProxyProvider<T> {
  private static final Logger LOG = LoggerFactory.getLogger(
      ObserverReadProxyProvider.class);

  /** Client-side context for syncing with the NameNode server side */
  private AlignmentContext alignmentContext;

  /** Proxies for the observer namenodes */
  private final List<AddressRpcProxyPair<T>> observerProxies =
      new ArrayList<>();

  /**
   * Whether reading from observer is enabled. If this is false, all read
   * requests will still go to active NN.
   */
  private boolean observerReadEnabled;

  /**
   * Thread-local index to record the current index in the observer list.
   */
  private static final ThreadLocal<Integer> currentIndex =
      ThreadLocal.withInitial(() -> 0);

  /** The last proxy that has been used. Only used for testing */
  private volatile ProxyInfo<T> lastProxy = null;

  @SuppressWarnings("unchecked")
  public ObserverReadProxyProvider(
      Configuration conf, URI uri, Class<T> xface, HAProxyFactory<T> factory)
      throws IOException {
    super(conf, uri, xface, factory);
    alignmentContext = new ClientGSIContext();
    ((ClientHAProxyFactory<T>) factory).setAlignmentContext(alignmentContext);

    // Find out all the observer proxies
    for (AddressRpcProxyPair<T> ap : this.proxies) {
      ap.namenode = (T) NameNodeProxiesClient.createProxyWithAlignmentContext(
          ap.address, conf, ugi, false, getFallbackToSimpleAuth(),
          alignmentContext);
      if (isObserverState(ap)) {
        observerProxies.add(ap);
      }
    }

    if (observerProxies.isEmpty()) {
      throw new RuntimeException("Couldn't find any namenode proxy in " +
          "OBSERVER state");
    }

    // Randomize the list to prevent all clients pointing to the same one
    boolean randomized = conf.getBoolean(
        HdfsClientConfigKeys.Failover.RANDOM_ORDER,
        HdfsClientConfigKeys.Failover.RANDOM_ORDER_DEFAULT);
    if (randomized) {
      Collections.shuffle(observerProxies);
    }
  }

  @Override
  public synchronized AlignmentContext getAlignmentContext() {
    return alignmentContext;
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized ProxyInfo<T> getProxy() {
    // We just create a wrapped proxy containing all the proxies
    List<ProxyInfo<T>> observerProxies = new ArrayList<>();
    StringBuilder combinedInfo = new StringBuilder("[");

    for (int i = 0; i < this.observerProxies.size(); i++) {
      if (i > 0) {
        combinedInfo.append(",");
      }
      AddressRpcProxyPair<T> p = this.observerProxies.get(i);
      ProxyInfo<T> pInfo = getProxy(p);
      observerProxies.add(pInfo);
      combinedInfo.append(pInfo.proxyInfo);
    }

    combinedInfo.append(']');
    T wrappedProxy = (T) Proxy.newProxyInstance(
        ObserverReadInvocationHandler.class.getClassLoader(),
        new Class<?>[]{xface},
        new ObserverReadInvocationHandler(observerProxies));
    return new ProxyInfo<>(wrappedProxy, combinedInfo.toString());
  }

  /**
   * Check if a method is read-only.
   *
   * @return whether the 'method' is a read-only operation.
   */
  private boolean isRead(Method method) {
    return method.isAnnotationPresent(ReadOnly.class);
  }

  @VisibleForTesting
  void setObserverReadEnabled(boolean flag) {
    this.observerReadEnabled = flag;
  }

  /**
   * After getting exception 'ex', whether we should retry the current request
   * on a different observer.
   */
  private boolean shouldRetry(Exception ex) throws Exception {
    // TODO: implement retry policy
    return true;
  }

  @VisibleForTesting
  ProxyInfo<T> getLastProxy() {
    return lastProxy;
  }

  boolean isObserverState(AddressRpcProxyPair<T> ap) {
    // TODO: should introduce new ClientProtocol method to verify the
    // underlying service state, which does not require superuser access
    // The is a workaround
    IOException ioe = null;
    try {
      // Verify write access first
      ap.namenode.reportBadBlocks(new LocatedBlock[0]);
      return false; // Only active NameNode allows write
    } catch (RemoteException re) {
      IOException sbe = re.unwrapRemoteException(StandbyException.class);
      if (!(sbe instanceof StandbyException)) {
        ioe = re;
      }
    } catch (IOException e) {
      ioe = e;
    }
    if (ioe != null) {
      LOG.error("Failed to connect to {}", ap.address, ioe);
      return false;
    }
    // Verify read access
    // For now we assume only Observer nodes allow reads
    // Stale reads on StandbyNode should be turned off
    try {
      ap.namenode.checkAccess("/", FsAction.READ);
      return true;
    } catch (RemoteException re) {
      IOException sbe = re.unwrapRemoteException(StandbyException.class);
      if (!(sbe instanceof StandbyException)) {
        ioe = re;
      }
    } catch (IOException e) {
      ioe = e;
    }
    if (ioe != null) {
      LOG.error("Failed to connect to {}", ap.address, ioe);
    }
    return false;
  }


  class ObserverReadInvocationHandler implements InvocationHandler {
    final List<ProxyInfo<T>> observerProxies;
    final ProxyInfo<T> activeProxy;

    ObserverReadInvocationHandler(List<ProxyInfo<T>> observerProxies) {
      this.observerProxies = observerProxies;
      this.activeProxy = ObserverReadProxyProvider.super.getProxy();
    }

    /**
     * Sends read operations to the observer (if enabled) specified by the
     * current index, and send write operations to the active. If a observer
     * fails, we increment the index and retry the next one. If all observers
     * fail, the request is forwarded to the active.
     *
     * Write requests are always forwarded to the active.
     */
    @Override
    public Object invoke(Object proxy, final Method method, final Object[] args)
        throws Throwable {
      lastProxy = null;
      Object retVal;

      if (observerReadEnabled && isRead(method)) {
        // Loop through all the proxies, starting from the current index.
        for (int i = 0; i < observerProxies.size(); i++) {
          ProxyInfo<T> current = observerProxies.get(currentIndex.get());
          try {
            retVal = method.invoke(current.proxy, args);
            lastProxy = current;
            return retVal;
          } catch (Exception e) {
            if (!shouldRetry(e)) {
              throw e;
            }
            currentIndex.set((currentIndex.get() + 1) % observerProxies.size());
            LOG.warn("Invocation returned exception on [{}]",
                current.proxyInfo, e.getCause());
          }
        }

        // If we get here, it means all observers have failed.
        LOG.warn("All observers have failed for read request {}. " +
            "Fall back on active: {}", method.getName(), activeProxy);
      }

      // Either all observers have failed, or that it is a write request.
      // In either case, we'll forward the request to active NameNode.
      try {
        retVal = method.invoke(activeProxy.proxy, args);
      } catch (Exception e) {
        throw e.getCause();
      }
      lastProxy = activeProxy;
      return retVal;
    }
  }
}
