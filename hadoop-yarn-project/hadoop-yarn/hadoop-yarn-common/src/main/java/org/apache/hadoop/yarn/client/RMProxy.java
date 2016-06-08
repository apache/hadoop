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

package org.apache.hadoop.yarn.client;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Public
@InterfaceStability.Evolving
@SuppressWarnings("unchecked")
public class RMProxy<T> {

  private static final Log LOG = LogFactory.getLog(RMProxy.class);

  protected RMProxy() {}

  /**
   * Verify the passed protocol is supported.
   */
  @Private
  protected void checkAllowedProtocols(Class<?> protocol) {}

  /**
   * Get the ResourceManager address from the provided Configuration for the
   * given protocol.
   */
  @Private
  protected InetSocketAddress getRMAddress(
      YarnConfiguration conf, Class<?> protocol) throws IOException {
    throw new UnsupportedOperationException("This method should be invoked " +
        "from an instance of ClientRMProxy or ServerRMProxy");
  }

  /**
   * Create a proxy for the specified protocol. For non-HA,
   * this is a direct connection to the ResourceManager address. When HA is
   * enabled, the proxy handles the failover between the ResourceManagers as
   * well.
   */
  @Private
  protected static <T> T createRMProxy(final Configuration configuration,
      final Class<T> protocol, RMProxy instance) throws IOException {
    YarnConfiguration conf = (configuration instanceof YarnConfiguration)
        ? (YarnConfiguration) configuration
        : new YarnConfiguration(configuration);
    RetryPolicy retryPolicy = createRetryPolicy(conf);
    if (HAUtil.isHAEnabled(conf)) {
      RMFailoverProxyProvider<T> provider =
          instance.createRMFailoverProxyProvider(conf, protocol);
      return (T) RetryProxy.create(protocol, provider, retryPolicy);
    } else {
      InetSocketAddress rmAddress = instance.getRMAddress(conf, protocol);
      LOG.info("Connecting to ResourceManager at " + rmAddress);
      T proxy = RMProxy.<T>getProxy(conf, protocol, rmAddress);
      return (T) RetryProxy.create(protocol, proxy, retryPolicy);
    }
  }

  /**
   * @deprecated
   * This method is deprecated and is not used by YARN internally any more.
   * To create a proxy to the RM, use ClientRMProxy#createRMProxy or
   * ServerRMProxy#createRMProxy.
   *
   * Create a proxy to the ResourceManager at the specified address.
   *
   * @param conf Configuration to generate retry policy
   * @param protocol Protocol for the proxy
   * @param rmAddress Address of the ResourceManager
   * @param <T> Type information of the proxy
   * @return Proxy to the RM
   * @throws IOException
   */
  @Deprecated
  public static <T> T createRMProxy(final Configuration conf,
      final Class<T> protocol, InetSocketAddress rmAddress) throws IOException {
    RetryPolicy retryPolicy = createRetryPolicy(conf);
    T proxy = RMProxy.<T>getProxy(conf, protocol, rmAddress);
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    return (T) RetryProxy.create(protocol, proxy, retryPolicy);
  }

  /**
   * Get a proxy to the RM at the specified address. To be used to create a
   * RetryProxy.
   */
  @Private
  static <T> T getProxy(final Configuration conf,
      final Class<T> protocol, final InetSocketAddress rmAddress)
      throws IOException {
    return UserGroupInformation.getCurrentUser().doAs(
      new PrivilegedAction<T>() {
        @Override
        public T run() {
          return (T) YarnRPC.create(conf).getProxy(protocol, rmAddress, conf);
        }
      });
  }

  /**
   * Helper method to create FailoverProxyProvider.
   */
  private <T> RMFailoverProxyProvider<T> createRMFailoverProxyProvider(
      Configuration conf, Class<T> protocol) {
    Class<? extends RMFailoverProxyProvider<T>> defaultProviderClass;
    try {
      defaultProviderClass = (Class<? extends RMFailoverProxyProvider<T>>)
          Class.forName(
              YarnConfiguration.DEFAULT_CLIENT_FAILOVER_PROXY_PROVIDER);
    } catch (Exception e) {
      throw new YarnRuntimeException("Invalid default failover provider class" +
          YarnConfiguration.DEFAULT_CLIENT_FAILOVER_PROXY_PROVIDER, e);
    }

    RMFailoverProxyProvider<T> provider = ReflectionUtils.newInstance(
        conf.getClass(YarnConfiguration.CLIENT_FAILOVER_PROXY_PROVIDER,
            defaultProviderClass, RMFailoverProxyProvider.class), conf);
    provider.init(conf, (RMProxy<T>) this, protocol);
    return provider;
  }

  /**
   * Fetch retry policy from Configuration
   */
  @Private
  @VisibleForTesting
  public static RetryPolicy createRetryPolicy(Configuration conf) {
    long rmConnectWaitMS =
        conf.getLong(
            YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS,
            YarnConfiguration.DEFAULT_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS);
    long rmConnectionRetryIntervalMS =
        conf.getLong(
            YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
            YarnConfiguration
                .DEFAULT_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS);

    boolean waitForEver = (rmConnectWaitMS == -1);
    if (!waitForEver) {
      if (rmConnectWaitMS < 0) {
        throw new YarnRuntimeException("Invalid Configuration. "
            + YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS
            + " can be -1, but can not be other negative numbers");
      }

      // try connect once
      if (rmConnectWaitMS < rmConnectionRetryIntervalMS) {
        LOG.warn(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS
            + " is smaller than "
            + YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS
            + ". Only try connect once.");
        rmConnectWaitMS = 0;
      }
    }

    // Handle HA case first
    if (HAUtil.isHAEnabled(conf)) {
      final long failoverSleepBaseMs = conf.getLong(
          YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_BASE_MS,
          rmConnectionRetryIntervalMS);

      final long failoverSleepMaxMs = conf.getLong(
          YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_MAX_MS,
          rmConnectionRetryIntervalMS);

      int maxFailoverAttempts = conf.getInt(
          YarnConfiguration.CLIENT_FAILOVER_MAX_ATTEMPTS, -1);

      if (maxFailoverAttempts == -1) {
        if (waitForEver) {
          maxFailoverAttempts = Integer.MAX_VALUE;
        } else {
          maxFailoverAttempts = (int) (rmConnectWaitMS / failoverSleepBaseMs);
        }
      }

      return RetryPolicies.failoverOnNetworkException(
          RetryPolicies.TRY_ONCE_THEN_FAIL, maxFailoverAttempts,
          failoverSleepBaseMs, failoverSleepMaxMs);
    }

    if (rmConnectionRetryIntervalMS < 0) {
      throw new YarnRuntimeException("Invalid Configuration. " +
          YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS +
          " should not be negative.");
    }

    RetryPolicy retryPolicy = null;
    if (waitForEver) {
      retryPolicy = RetryPolicies.RETRY_FOREVER;
    } else {
      retryPolicy =
          RetryPolicies.retryUpToMaximumTimeWithFixedSleep(rmConnectWaitMS,
              rmConnectionRetryIntervalMS, TimeUnit.MILLISECONDS);
    }

    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
        new HashMap<Class<? extends Exception>, RetryPolicy>();

    exceptionToPolicyMap.put(EOFException.class, retryPolicy);
    exceptionToPolicyMap.put(ConnectException.class, retryPolicy);
    exceptionToPolicyMap.put(NoRouteToHostException.class, retryPolicy);
    exceptionToPolicyMap.put(UnknownHostException.class, retryPolicy);
    exceptionToPolicyMap.put(ConnectTimeoutException.class, retryPolicy);
    exceptionToPolicyMap.put(RetriableException.class, retryPolicy);
    exceptionToPolicyMap.put(SocketException.class, retryPolicy);
    // YARN-4288: local IOException is also possible.
    exceptionToPolicyMap.put(IOException.class, retryPolicy);
    // Not retry on remote IO exception.
    return RetryPolicies.retryOtherThanRemoteException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
  }
}
