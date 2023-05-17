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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ConfiguredRMFailoverProxyProvider<T>
    implements RMFailoverProxyProvider<T> {
  private static final Logger LOG =
      LoggerFactory.getLogger(ConfiguredRMFailoverProxyProvider.class);

  private int currentProxyIndex = 0;
  Map<String, T> proxies = new HashMap<String, T>();

  protected RMProxy<T> rmProxy;
  protected Class<T> protocol;
  protected YarnConfiguration conf;
  protected String[] rmServiceIds;

  private long rmIdsLastRefreshTime;
  private long rmIdsRefreshInterval;

  @Override
  public void init(Configuration configuration, RMProxy<T> rmProxy,
                    Class<T> protocol) {
    this.rmProxy = rmProxy;
    this.protocol = protocol;
    this.rmProxy.checkAllowedProtocols(this.protocol);
    this.conf = new YarnConfiguration(configuration);
    this.rmIdsRefreshInterval = conf.getLong(YarnConfiguration.RM_ID_REFRESH_INTERVAL,
        YarnConfiguration.RM_ID_REFRESH_INTERVAL_DEFAULT);
    refreshRMIds(true);

    conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        conf.getInt(YarnConfiguration.CLIENT_FAILOVER_RETRIES,
            YarnConfiguration.DEFAULT_CLIENT_FAILOVER_RETRIES));

    conf.setInt(CommonConfigurationKeysPublic.
        IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        conf.getInt(YarnConfiguration.CLIENT_FAILOVER_RETRIES_ON_SOCKET_TIMEOUTS,
            YarnConfiguration.DEFAULT_CLIENT_FAILOVER_RETRIES_ON_SOCKET_TIMEOUTS));
  }

  protected T getProxyInternal() {
    try {
      final InetSocketAddress rmAddress = rmProxy.getRMAddress(conf, protocol);
      return rmProxy.getProxy(conf, protocol, rmAddress);
    } catch (IOException ioe) {
      LOG.error("Unable to create proxy to the ResourceManager " +
          rmServiceIds[currentProxyIndex], ioe);
      return null;
    }
  }

  @Override
  public synchronized ProxyInfo<T> getProxy() {
    if (shouldRefreshAddress()) {
      refreshRMIds(false);
    }
    String rmId = rmServiceIds[currentProxyIndex];
    T current = proxies.get(rmId);
    if (current == null) {
      current = getProxyInternal();
      proxies.put(rmId, current);
    }
    return new ProxyInfo<T>(current, rmId);
  }

  @Override
  public synchronized void performFailover(T currentProxy) {
    currentProxyIndex = (currentProxyIndex + 1) % rmServiceIds.length;
    conf.set(YarnConfiguration.RM_HA_ID, rmServiceIds[currentProxyIndex]);
    LOG.info("Failing over to " + rmServiceIds[currentProxyIndex]);
  }

  @Override
  public Class<T> getInterface() {
    return protocol;
  }

  /**
   * Close all the proxy objects which have been opened over the lifetime of
   * this proxy provider.
   */
  @Override
  public synchronized void close() throws IOException {
    for (T proxy : proxies.values()) {
      if (proxy instanceof Closeable) {
        ((Closeable)proxy).close();
      } else {
        RPC.stopProxy(proxy);
      }
    }
  }

  private boolean shouldRefreshAddress() {
    long currentTime = System.currentTimeMillis();
    return rmIdsRefreshInterval > 0 &&
        currentTime > rmIdsLastRefreshTime + rmIdsRefreshInterval;
  }

  private synchronized void refreshRMIds(boolean forceRefresh) throws IllegalStateException {
    if (!forceRefresh && !shouldRefreshAddress()) {
      return;
    }
    String currentRMId = HAUtil.getRMHAId(conf);
    InetSocketAddress currentAddress = null;
    if (currentRMId != null) {
      // explicitly refresh it if not done
      currentAddress = HAUtil.getInetSocketAddressFromString(conf.get(HAUtil.addSuffix(
          YarnConfiguration.RM_ADDRESS, currentRMId)));
      HAUtil.getResolvedRMIdPairs(conf);
    }
    rmIdsLastRefreshTime = System.currentTimeMillis();
    Collection<String> rmIds = HAUtil.getRMHAIds(conf);
    if (rmIds == null || rmIds.isEmpty()) {
      String message = "no instances configured.";
      LOG.error(message);
      throw new IllegalStateException(message);
    }
    proxies.clear();
    this.rmServiceIds = rmIds.toArray(new String[rmIds.size()]);
    // After refresh, we should keep current rm pointing to the previous active one
    if (currentRMId != null) {
      for (int i = 0; i < rmServiceIds.length; i++) {
        String confKey = HAUtil.addSuffix(
            YarnConfiguration.RM_ADDRESS, rmServiceIds[i]);
        InetSocketAddress thatAddress =
            HAUtil.getInetSocketAddressFromString(conf.get(confKey));
        if (currentAddress.equals(thatAddress)) {
          currentProxyIndex = i;
          break;
        }
      }
    }
    conf.set(YarnConfiguration.RM_HA_ID, rmServiceIds[currentProxyIndex]);
  }
}
