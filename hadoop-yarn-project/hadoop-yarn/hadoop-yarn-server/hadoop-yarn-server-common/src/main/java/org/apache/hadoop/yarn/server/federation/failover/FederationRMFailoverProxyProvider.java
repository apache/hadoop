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
import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.client.RMFailoverProxyProvider;
import org.apache.hadoop.yarn.client.RMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private Collection<Token<? extends TokenIdentifier>> originalTokens;
  private boolean federationFailoverEnabled = false;

  @Override
  public void init(Configuration configuration, RMProxy<T> proxy,
      Class<T> proto) {
    this.rmProxy = proxy;
    this.protocol = proto;
    this.rmProxy.checkAllowedProtocols(this.protocol);
    String clusterId =
        configuration.get(YarnConfiguration.FEDERATION_SUBCLUSTER_ID);
    Preconditions.checkNotNull(clusterId, "Missing Federation SubClusterId");
    this.subClusterId = SubClusterId.newInstance(clusterId);
    this.facade = facade.getInstance();
    if (configuration instanceof YarnConfiguration) {
      this.conf = (YarnConfiguration) configuration;
    }
    federationFailoverEnabled =
        conf.getBoolean(YarnConfiguration.FEDERATION_FAILOVER_ENABLED,
            YarnConfiguration.DEFAULT_FEDERATION_FAILOVER_ENABLED);

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
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      originalTokens = currentUser.getTokens();
      LOG.info("Initialized Federation proxy for user: {}",
          currentUser.getUserName());
    } catch (IOException e) {
      LOG.warn("Could not get information of requester, ignoring for now.");
    }

  }

  private void addOriginalTokens(UserGroupInformation currentUser) {
    if (originalTokens == null || originalTokens.isEmpty()) {
      return;
    }
    for (Token<? extends TokenIdentifier> token : originalTokens) {
      currentUser.addToken(token);
    }
  }

  private T getProxyInternal(boolean isFailover) {
    SubClusterInfo subClusterInfo;
    UserGroupInformation currentUser = null;
    try {
      LOG.info("Failing over to the ResourceManager for SubClusterId: {}",
          subClusterId);
      subClusterInfo = facade.getSubCluster(subClusterId, isFailover);
      // updating the conf with the refreshed RM addresses as proxy
      // creations
      // are based out of conf
      updateRMAddress(subClusterInfo);
      currentUser = UserGroupInformation.getCurrentUser();
      addOriginalTokens(currentUser);
    } catch (YarnException e) {
      LOG.error("Exception while trying to create proxy to the ResourceManager"
          + " for SubClusterId: {}", subClusterId, e);
      return null;
    } catch (IOException e) {
      LOG.warn("Could not get information of requester, ignoring for now.");
    }
    try {
      final InetSocketAddress rmAddress = rmProxy.getRMAddress(conf, protocol);
      LOG.info("Connecting to {} with protocol {} as user: {}", rmAddress,
          protocol.getSimpleName(), currentUser);
      LOG.info("Failed over to the RM at {} for SubClusterId: {}", rmAddress,
          subClusterId);
      return rmProxy.getProxy(conf, protocol, rmAddress);
    } catch (IOException ioe) {
      LOG.error(
          "IOException while trying to create proxy to the ResourceManager"
              + " for SubClusterId: {}",
          subClusterId, ioe);
      return null;
    }
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
    closeInternal(currentProxy);
    current = getProxyInternal(federationFailoverEnabled);
  }

  @Override
  public Class<T> getInterface() {
    return protocol;
  }

  private void closeInternal(T currentProxy) {
    if ((currentProxy != null) && (currentProxy instanceof Closeable)) {
      try {
        ((Closeable) currentProxy).close();
      } catch (IOException e) {
        LOG.warn("Exception while trying to close proxy", e);
      }
    } else {
      RPC.stopProxy(currentProxy);
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
