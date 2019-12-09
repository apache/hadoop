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

package org.apache.hadoop.yarn.server.federation.failover;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.client.RMFailoverProxyProvider;
import org.apache.hadoop.yarn.client.RMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A FailoverProxyProvider implementation that uses the
 * {@code FederationStateStore} to determine the ResourceManager to connect to.
 * This supports both HA and regular mode which is controlled by configuration.
 */
@Private
@Unstable
public class FederationRMFailoverProxyProvider<T>
    implements RMFailoverProxyProvider<T> {
  private static final Logger LOG =
      LoggerFactory.getLogger(FederationRMFailoverProxyProvider.class);

  private RMProxy<T> rmProxy;
  private Class<T> protocol;
  private T current;
  private YarnConfiguration conf;
  private FederationStateStoreFacade facade;
  private SubClusterId subClusterId;
  private UserGroupInformation originalUser;
  private boolean federationFailoverEnabled;
  private boolean flushFacadeCacheForYarnRMAddr;

  @Override
  public void init(Configuration configuration, RMProxy<T> proxy,
      Class<T> proto) {
    this.rmProxy = proxy;
    this.protocol = proto;
    this.rmProxy.checkAllowedProtocols(this.protocol);
    String clusterId = configuration.get(YarnConfiguration.RM_CLUSTER_ID);
    Preconditions.checkNotNull(clusterId, "Missing RM ClusterId");
    this.subClusterId = SubClusterId.newInstance(clusterId);
    this.facade = FederationStateStoreFacade.getInstance();
    if (configuration instanceof YarnConfiguration) {
      this.conf = (YarnConfiguration) configuration;
    }
    federationFailoverEnabled =
        conf.getBoolean(YarnConfiguration.FEDERATION_FAILOVER_ENABLED,
            YarnConfiguration.DEFAULT_FEDERATION_FAILOVER_ENABLED);
    flushFacadeCacheForYarnRMAddr =
        conf.getBoolean(YarnConfiguration.FEDERATION_FLUSH_CACHE_FOR_RM_ADDR,
            YarnConfiguration.DEFAULT_FEDERATION_FLUSH_CACHE_FOR_RM_ADDR);

    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        conf.getInt(YarnConfiguration.CLIENT_FAILOVER_RETRIES,
            YarnConfiguration.DEFAULT_CLIENT_FAILOVER_RETRIES));

    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        conf.getInt(
            YarnConfiguration.CLIENT_FAILOVER_RETRIES_ON_SOCKET_TIMEOUTS,
            YarnConfiguration.DEFAULT_CLIENT_FAILOVER_RETRIES_ON_SOCKET_TIMEOUTS));

    try {
      this.originalUser = UserGroupInformation.getCurrentUser();
      LOG.info("Initialized Federation proxy for user: {}",
          this.originalUser.getUserName());
    } catch (IOException e) {
      LOG.warn("Could not get information of requester, ignoring for now.");
      this.originalUser = null;
    }

  }

  @VisibleForTesting
  protected T createRMProxy(InetSocketAddress rmAddress) throws IOException {
    return rmProxy.getProxy(conf, protocol, rmAddress);
  }

  private T getProxyInternal(boolean isFailover) {
    SubClusterInfo subClusterInfo;
    // Use the existing proxy as a backup in case getting the new proxy fails.
    // Note that if the first time it fails, the backup is also null. In that
    // case we will hit NullPointerException and throw it back to AM.
    T proxy = this.current;
    try {
      LOG.info("Failing over to the ResourceManager for SubClusterId: {}",
          subClusterId);
      subClusterInfo = facade.getSubCluster(subClusterId,
          this.flushFacadeCacheForYarnRMAddr && isFailover);
      // updating the conf with the refreshed RM addresses as proxy
      // creations are based out of conf
      updateRMAddress(subClusterInfo);
      if (this.originalUser == null) {
        InetSocketAddress rmAddress = rmProxy.getRMAddress(conf, protocol);
        LOG.info(
            "Connecting to {} subClusterId {} with protocol {}"
                + " without a proxy user",
            rmAddress, subClusterId, protocol.getSimpleName());
        proxy = createRMProxy(rmAddress);
      } else {
        // If the original ugi exists, always use that to create proxy because
        // it contains up-to-date AMRMToken
        proxy = this.originalUser.doAs(new PrivilegedExceptionAction<T>() {
          @Override
          public T run() throws IOException {
            InetSocketAddress rmAddress = rmProxy.getRMAddress(conf, protocol);
            LOG.info(
                "Connecting to {} subClusterId {} with protocol {} as user {}",
                rmAddress, subClusterId, protocol.getSimpleName(),
                originalUser);
            return createRMProxy(rmAddress);
          }
        });
      }
    } catch (Exception e) {
      LOG.error("Exception while trying to create proxy to the ResourceManager"
          + " for SubClusterId: {}", subClusterId, e);
      if (proxy == null) {
        throw new YarnRuntimeException(
            String.format("Create initial proxy to the ResourceManager for"
                + " SubClusterId %s failed", subClusterId),
            e);
      }
    }
    return proxy;
  }

  private void updateRMAddress(SubClusterInfo subClusterInfo) {
    if (subClusterInfo != null) {
      if (protocol == ApplicationClientProtocol.class) {
        conf.set(YarnConfiguration.RM_ADDRESS,
            subClusterInfo.getClientRMServiceAddress());
      } else if (protocol == ApplicationMasterProtocol.class) {
        conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS,
            subClusterInfo.getAMRMServiceAddress());
      } else if (protocol == ResourceManagerAdministrationProtocol.class) {
        conf.set(YarnConfiguration.RM_ADMIN_ADDRESS,
            subClusterInfo.getRMAdminServiceAddress());
      }
    }
  }

  @Override
  public synchronized ProxyInfo<T> getProxy() {
    if (current == null) {
      current = getProxyInternal(false);
    }
    return new ProxyInfo<T>(current, subClusterId.getId());
  }

  @Override
  public synchronized void performFailover(T currentProxy) {
    // It will not return null proxy here
    current = getProxyInternal(federationFailoverEnabled);
    if (current != currentProxy) {
      closeInternal(currentProxy);
    }
  }

  @Override
  public Class<T> getInterface() {
    return protocol;
  }

  private void closeInternal(T currentProxy) {
    if (currentProxy != null) {
      if (currentProxy instanceof Closeable) {
        try {
          ((Closeable) currentProxy).close();
        } catch (IOException e) {
          LOG.warn("Exception while trying to close proxy", e);
        }
      } else {
        RPC.stopProxy(currentProxy);
      }
    }
  }

  /**
   * Close all the proxy objects which have been opened over the lifetime of
   * this proxy provider.
   */
  @Override
  public synchronized void close() throws IOException {
    closeInternal(current);
  }

}
