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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.ClientGSIContext;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcInvocationHandler;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider.AUTO_MSYNC_PERIOD_KEY_PREFIX;
import static org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider.AUTO_MSYNC_PERIOD_DEFAULT;

/**
 * A {@link org.apache.hadoop.io.retry.FailoverProxyProvider} implementation
 * to support automatic msync-ing when using routers.
 *
 * This constructs a wrapper proxy around an internal one, and
 * injects msync calls when necessary via the InvocationHandler.
 */
public class RouterObserverReadProxyProvider<T> extends AbstractNNFailoverProxyProvider<T> {
  @VisibleForTesting
  static final Logger LOG = LoggerFactory.getLogger(ObserverReadProxyProvider.class);

  /** Client-side context for syncing with the NameNode server side. */
  private final AlignmentContext alignmentContext;

  /** The inner proxy provider used for active/standby failover. */
  private final AbstractNNFailoverProxyProvider<T> innerProxy;

  /** The proxy which redirects the internal one. */
  private final ProxyInfo<T> wrapperProxy;

  /**
   * Whether reading from observer is enabled. If this is false, this proxy
   * will not call msync.
   */
  private final boolean observerReadEnabled;

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

  public RouterObserverReadProxyProvider(Configuration conf, URI uri, Class<T> xface,
      HAProxyFactory<T> factory) {
    this(conf, uri, xface, factory, new IPFailoverProxyProvider<>(conf, uri, xface, factory));
  }

  @SuppressWarnings("unchecked")
  public RouterObserverReadProxyProvider(Configuration conf, URI uri, Class<T> xface,
      HAProxyFactory<T> factory, AbstractNNFailoverProxyProvider<T> failoverProxy) {
    super(conf, uri, xface, factory);
    this.alignmentContext = new ClientGSIContext();
    factory.setAlignmentContext(alignmentContext);
    this.innerProxy = failoverProxy;

    String proxyInfoString = "RouterObserverReadProxyProvider for " + innerProxy.getProxy();

    T wrappedProxy = (T) Proxy.newProxyInstance(
        RouterObserverReadInvocationHandler.class.getClassLoader(),
        new Class<?>[]{xface}, new RouterObserverReadInvocationHandler());
    this.wrapperProxy = new ProxyInfo<>(wrappedProxy, proxyInfoString);

    autoMsyncPeriodMs = conf.getTimeDuration(
        // The host of the URI is the name service ID
        AUTO_MSYNC_PERIOD_KEY_PREFIX + "." + uri.getHost(),
        AUTO_MSYNC_PERIOD_DEFAULT, TimeUnit.MILLISECONDS);

    if (wrappedProxy instanceof ClientProtocol) {
      this.observerReadEnabled = true;
    } else {
      LOG.info("Disabling observer reads for {} because the requested proxy "
          + "class does not implement {}", uri, ClientProtocol.class.getName());
      this.observerReadEnabled = false;
    }
  }


  public AlignmentContext getAlignmentContext() {
    return alignmentContext;
  }

  @Override
  public ProxyInfo<T> getProxy() {
    return wrapperProxy;
  }

  @Override
  public void performFailover(T currentProxy) {
    innerProxy.performFailover(currentProxy);
  }

  @Override
  public boolean useLogicalURI() {
    return innerProxy.useLogicalURI();
  }

  @Override
  public void close() throws IOException {
    innerProxy.close();
  }

  /**
   * Return the input proxy, cast as a {@link ClientProtocol}. This catches any
   * {@link ClassCastException} and wraps it in a more helpful message. This
   * should ONLY be called if the caller is certain that the proxy is, in fact,
   * a {@link ClientProtocol}.
   */
  private ClientProtocol getProxyAsClientProtocol(T proxy) {
    assert proxy instanceof ClientProtocol : "BUG: Attempted to use proxy of class "
        + proxy.getClass()
        + " as if it was a ClientProtocol.";
    return (ClientProtocol) proxy;
  }

  /**
   * This will call {@link ClientProtocol#msync()} on the active NameNode
   * (via the {@link #innerProxy}) to update the state of this client, only
   * if at least {@link #autoMsyncPeriodMs} ms has elapsed since the last time
   * an msync was performed.
   *
   * @see #autoMsyncPeriodMs
   */
  private void autoMsyncIfNecessary() throws IOException {
    if (autoMsyncPeriodMs == 0) {
      // Always msync
      getProxyAsClientProtocol(innerProxy.getProxy().proxy).msync();
    } else if (autoMsyncPeriodMs > 0) {
      if (Time.monotonicNow() - lastMsyncTimeMs > autoMsyncPeriodMs) {
        synchronized (this) {
          // Use a synchronized block so that only one thread will msync
          // if many operations are submitted around the same time.
          // Re-check the entry criterion since the status may have changed
          // while waiting for the lock.
          if (Time.monotonicNow() - lastMsyncTimeMs > autoMsyncPeriodMs) {
            getProxyAsClientProtocol(innerProxy.getProxy().proxy).msync();
            lastMsyncTimeMs = Time.monotonicNow();
          }
        }
      }
    }
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

  private class RouterObserverReadInvocationHandler implements RpcInvocationHandler {

    @Override
    public Client.ConnectionId getConnectionId() {
      return RPC.getConnectionIdForProxy(innerProxy.getProxy().proxy);
    }

    @Override
    public void close() throws IOException {
      innerProxy.close();
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (observerReadEnabled && isRead(method)) {
        autoMsyncIfNecessary();
      }

      try {
        return method.invoke(innerProxy.getProxy().proxy, args);
      } catch (InvocationTargetException e) {
        // This exception will be handled by higher layers
        throw e.getCause();
      }
    }
  }
}
