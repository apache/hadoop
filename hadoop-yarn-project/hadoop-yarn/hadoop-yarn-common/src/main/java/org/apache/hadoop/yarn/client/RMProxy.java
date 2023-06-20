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
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import org.apache.hadoop.classification.VisibleForTesting;

@InterfaceAudience.Public
@InterfaceStability.Evolving
@SuppressWarnings("unchecked")
public class RMProxy<T> {

  private static final Logger LOG = LoggerFactory.getLogger(RMProxy.class);
  private UserGroupInformation user;

  protected RMProxy() {
    try {
      this.user = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      throw new YarnRuntimeException("Unable to determine user", ioe);
    }
  }

  /**
   * Verify the passed protocol is supported.
   *
   * @param protocol protocol.
   */
  @Private
  public void checkAllowedProtocols(Class<?> protocol) {}

  /**
   * Get the ResourceManager address from the provided Configuration for the
   * given protocol.
   *
   * @param conf configuration.
   * @param protocol protocol.
   * @return inet socket address.
   * @throws IOException io error occur.
   */
  @Private
  public InetSocketAddress getRMAddress(
      YarnConfiguration conf, Class<?> protocol) throws IOException {
    throw new UnsupportedOperationException("This method should be invoked " +
        "from an instance of ClientRMProxy or ServerRMProxy");
  }

  /**
   * Currently, used by Client and AM only
   * Create a proxy for the specified protocol. For non-HA,
   * this is a direct connection to the ResourceManager address. When HA is
   * enabled, the proxy handles the failover between the ResourceManagers as
   * well.
   *
   * @param configuration configuration.
   * @param protocol protocol.
   * @param instance RMProxy instance.
   * @param <T> Generic T.
   * @return RMProxy.
   * @throws IOException io error occur.
   */
  @Private
  protected static <T> T createRMProxy(final Configuration configuration,
      final Class<T> protocol, RMProxy<T> instance) throws IOException {
    YarnConfiguration conf = (configuration instanceof YarnConfiguration)
        ? (YarnConfiguration) configuration
        : new YarnConfiguration(configuration);
    RetryPolicy retryPolicy = createRetryPolicy(conf, isFailoverEnabled(conf));
    return newProxyInstance(conf, protocol, instance, retryPolicy);
  }

  /**
   * This functionality is only used for NodeManager and only in non-HA mode.
   * Its purpose is to ensure that when initializes UAM, it can find the correct cluster.
   *
   * @param configuration configuration.
   * @param protocol protocol.
   * @param instance RMProxy instance.
   * @return RMProxy.
   * @param <T> Generic T.
   * @throws IOException io error occur.
   */
  protected static <T> T createRMProxyFederation(final Configuration configuration,
      final Class<T> protocol, RMProxy<T> instance) throws IOException {
    YarnConfiguration yarnConf = new YarnConfiguration(configuration);
    if (isFederationNonHAEnabled(yarnConf)) {
      RetryPolicy retryPolicy = createRetryPolicy(yarnConf, isFailoverEnabled(yarnConf));
      return newProxyInstanceFederation(yarnConf, protocol, instance, retryPolicy);
    }
    return createRMProxy(configuration, protocol, instance);
  }

  protected static <T> T newProxyInstanceFederation(final YarnConfiguration conf,
      final Class<T> protocol, RMProxy<T> instance, RetryPolicy retryPolicy) {
    RMFailoverProxyProvider<T> provider = getRMFailoverProxyProvider(conf, protocol, instance);
    return (T) RetryProxy.create(protocol, provider, retryPolicy);
  }

  protected static <T> RMFailoverProxyProvider<T> getRMFailoverProxyProvider(
      final YarnConfiguration conf, final Class<T> protocol, RMProxy<T> instance) {
    RMFailoverProxyProvider<T> provider;
    if (isFederationNonHAEnabled(conf)) {
      provider = instance.createRMFailoverProxyProvider(conf, protocol);
    } else {
      provider = instance.createNonHaRMFailoverProxyProvider(conf, protocol);
    }
    return provider;
  }

  /**
   * Currently, used by NodeManagers only.
   * Create a proxy for the specified protocol. For non-HA,
   * this is a direct connection to the ResourceManager address. When HA is
   * enabled, the proxy handles the failover between the ResourceManagers as
   * well.
   *
   * @param configuration configuration.
   * @param protocol protocol.
   * @param instance RMProxy instance.
   * @param retryTime retry Time.
   * @param retryInterval retry Interval.
   * @param <T> Generic T.
   * @return RMProxy.
   * @throws IOException io error occur.
   */
  @Private
  protected static <T> T createRMProxy(final Configuration configuration,
      final Class<T> protocol, RMProxy<T> instance, final long retryTime,
      final long retryInterval) throws IOException {
    YarnConfiguration conf = (configuration instanceof YarnConfiguration)
        ? (YarnConfiguration) configuration
        : new YarnConfiguration(configuration);
    RetryPolicy retryPolicy = createRetryPolicy(conf, retryTime, retryInterval,
        HAUtil.isHAEnabled(conf));
    return newProxyInstance(conf, protocol, instance, retryPolicy);
  }

  private static <T> T newProxyInstance(final YarnConfiguration conf,
      final Class<T> protocol, RMProxy<T> instance, RetryPolicy retryPolicy)
          throws IOException{
    RMFailoverProxyProvider<T> provider;
    if (isFailoverEnabled(conf)) {
      provider = instance.createRMFailoverProxyProvider(conf, protocol);
    } else {
      provider = instance.createNonHaRMFailoverProxyProvider(conf, protocol);
    }
    return (T) RetryProxy.create(protocol, provider, retryPolicy);
  }

  /**
   * Get a proxy to the RM at the specified address. To be used to create a
   * RetryProxy.
   *
   * @param conf configuration.
   * @param protocol protocol.
   * @param rmAddress rmAddress.
   * @param <T> Generic T.
   * @return RM proxy.
   * @throws IOException io error occur.
   */
  @Private
  public <T> T getProxy(final Configuration conf,
      final Class<T> protocol, final InetSocketAddress rmAddress)
      throws IOException {
    return user.doAs(
      new PrivilegedAction<T>() {
        @Override
        public T run() {
          return (T) YarnRPC.create(conf).getProxy(protocol, rmAddress, conf);
        }
      });
  }

  /**
   * Helper method to create non-HA RMFailoverProxyProvider.
   */
  private <T> RMFailoverProxyProvider<T> createNonHaRMFailoverProxyProvider(
      Configuration conf, Class<T> protocol) {
    Class<? extends RMFailoverProxyProvider<T>> defaultProviderClass;
    try {
      defaultProviderClass = (Class<? extends RMFailoverProxyProvider<T>>)
          Class.forName(
              YarnConfiguration.DEFAULT_CLIENT_FAILOVER_NO_HA_PROXY_PROVIDER);
    } catch (Exception e) {
      throw new YarnRuntimeException("Invalid default failover provider class" +
          YarnConfiguration.DEFAULT_CLIENT_FAILOVER_NO_HA_PROXY_PROVIDER, e);
    }

    RMFailoverProxyProvider<T> provider = ReflectionUtils.newInstance(
        conf.getClass(YarnConfiguration.CLIENT_FAILOVER_NO_HA_PROXY_PROVIDER,
            defaultProviderClass, RMFailoverProxyProvider.class), conf);
    provider.init(conf, (RMProxy<T>) this, protocol);
    return provider;
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
   * Fetch retry policy from Configuration.
   *
   * @param conf configuration.
   * @param isHAEnabled is HA enabled.
   * @return RetryPolicy.
   */
  @Private
  @VisibleForTesting
  public static RetryPolicy createRetryPolicy(Configuration conf,
      boolean isHAEnabled) {
    long rmConnectWaitMS =
        conf.getLong(
            YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS,
            YarnConfiguration.DEFAULT_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS);
    long rmConnectionRetryIntervalMS =
        conf.getLong(
            YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
            YarnConfiguration
                .DEFAULT_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS);

    return createRetryPolicy(conf, rmConnectWaitMS, rmConnectionRetryIntervalMS,
        isHAEnabled);
  }

  /**
   * Fetch retry policy from Configuration and create the
   * retry policy with specified retryTime and retry interval.
   *
   * @param conf configuration.
   * @param retryTime retry time.
   * @param retryInterval retry interval.
   * @param isHAEnabled is HA enabled.
   * @return RetryPolicy.
   */
  protected static RetryPolicy createRetryPolicy(Configuration conf,
      long retryTime, long retryInterval, boolean isHAEnabled) {
    long rmConnectWaitMS = retryTime;
    long rmConnectionRetryIntervalMS = retryInterval;

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
    if (isHAEnabled) {
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
      retryPolicy = RetryPolicies.retryForeverWithFixedSleep(
          rmConnectionRetryIntervalMS, TimeUnit.MILLISECONDS);
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
    exceptionToPolicyMap.put(SocketTimeoutException.class, retryPolicy);
    exceptionToPolicyMap.put(StandbyException.class, retryPolicy);
    // YARN-4288: local IOException is also possible.
    exceptionToPolicyMap.put(IOException.class, retryPolicy);
    // Not retry on remote IO exception.
    return RetryPolicies.retryOtherThanRemoteAndSaslException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
  }

  private static boolean isFailoverEnabled(YarnConfiguration conf) {
    if (HAUtil.isHAEnabled(conf)) {
      // Considering Resource Manager HA is enabled.
      return true;
    }
    if (HAUtil.isFederationEnabled(conf) && HAUtil.isFederationFailoverEnabled(conf)) {
      // Considering both federation and federation failover are enabled.
      return true;
    }
    return false;
  }

  /**
   * If RM is not configured with HA, NM will not configure yarn.resourcemanager.ha.rmIds locally.
   *
   * If federation mode is enabled and RMProxy#isFailoverEnabled returns true,
   * when NM starts Container, it will try to find the yarn.resourcemanager.ha.rmIds property.
   *
   * However, an error will occur because this property is not configured
   * if the user has not configured HA.
   *
   * To solve this issue, we can configure the yarn.federation.no-ha.enabled property in NM,
   * which tells NM to run in a non-HA environment.
   *
   * @param conf YarnConfiguration
   * @return true, federation support non-HA, false, federation not support non-HA.
   */
  private static boolean isFederationNonHAEnabled(YarnConfiguration conf) {
    boolean isNonHAEnabled = conf.getBoolean(YarnConfiguration.FEDERATION_NON_HA_ENABLED,
        YarnConfiguration.DEFAULT_FEDERATION_NON_HA_ENABLED);
    return isNonHAEnabled;
  }
}
