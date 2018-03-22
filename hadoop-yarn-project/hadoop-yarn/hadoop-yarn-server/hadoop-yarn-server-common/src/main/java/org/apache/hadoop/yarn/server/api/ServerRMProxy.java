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

package org.apache.hadoop.yarn.server.api;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.RMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerRMProxy<T> extends RMProxy<T> {
  private static final Logger LOG =
      LoggerFactory.getLogger(ServerRMProxy.class);

  private ServerRMProxy() {
    super();
  }

  /**
   * Create a proxy to the ResourceManager for the specified protocol.
   * @param configuration Configuration with all the required information.
   * @param protocol Server protocol for which proxy is being requested.
   * @param <T> Type of proxy.
   * @return Proxy to the ResourceManager for the specified server protocol.
   * @throws IOException
   */
  public static <T> T createRMProxy(final Configuration configuration,
      final Class<T> protocol) throws IOException {
    long rmConnectWait =
        configuration.getLong(
            YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS,
            YarnConfiguration.DEFAULT_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS);
    long rmRetryInterval =
        configuration.getLong(
            YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
            YarnConfiguration
                .DEFAULT_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS);
    long nmRmConnectWait =
        configuration.getLong(
            YarnConfiguration.NM_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS,
                rmConnectWait);
    long nmRmRetryInterval =
        configuration.getLong(
            YarnConfiguration.NM_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
                rmRetryInterval);
    ServerRMProxy<T> serverRMProxy = new ServerRMProxy<>();
    return createRMProxy(configuration, protocol, serverRMProxy,
        nmRmConnectWait, nmRmRetryInterval);
  }

  @InterfaceAudience.Private
  @Override
  public InetSocketAddress getRMAddress(YarnConfiguration conf,
                                           Class<?> protocol) {
    if (protocol == ResourceTracker.class) {
      return conf.getSocketAddr(
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT);
    } else if (protocol == DistributedSchedulingAMProtocol.class) {
      return conf.getSocketAddr(YarnConfiguration.RM_SCHEDULER_ADDRESS,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
    } else {
      String message = "Unsupported protocol found when creating the proxy " +
          "connection to ResourceManager: " +
          ((protocol != null) ? protocol.getClass().getName() : "null");
      LOG.error(message);
      throw new IllegalStateException(message);
    }
  }

  @InterfaceAudience.Private
  @Override
  public void checkAllowedProtocols(Class<?> protocol) {
    Preconditions.checkArgument(
        protocol.isAssignableFrom(ResourceTracker.class),
        "ResourceManager does not support this protocol");
  }
}