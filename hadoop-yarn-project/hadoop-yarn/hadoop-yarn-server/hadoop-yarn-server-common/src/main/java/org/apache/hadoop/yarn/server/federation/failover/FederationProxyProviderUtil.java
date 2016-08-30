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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.RMFailoverProxyProvider;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class that creates proxy for specified protocols when federation is
 * enabled. The class creates a federation aware failover provider, i.e. the
 * failover provider uses the {@code FederationStateStore} to determine the
 * current active ResourceManager
 */
@Private
@Unstable
public final class FederationProxyProviderUtil {

  public static final Logger LOG =
      LoggerFactory.getLogger(FederationProxyProviderUtil.class);

  /**
   * Create a proxy for the specified protocol. For non-HA, this is a direct
   * connection to the ResourceManager address. When HA is enabled, the proxy
   * handles the failover between the ResourceManagers as well.
   *
   * @param configuration Configuration to generate {@link ClientRMProxy}
   * @param protocol Protocol for the proxy
   * @param subClusterId the unique identifier or the sub-cluster
   * @param user the user on whose behalf the proxy is being created
   * @param <T> Type information of the proxy
   * @return Proxy to the RM
   * @throws IOException on failure
   */
  @Public
  @Unstable
  public static <T> T createRMProxy(Configuration configuration,
      final Class<T> protocol, SubClusterId subClusterId,
      UserGroupInformation user) throws IOException {
    return createRMProxy(configuration, protocol, subClusterId, user, null);
  }

  /**
   * Create a proxy for the specified protocol. For non-HA, this is a direct
   * connection to the ResourceManager address. When HA is enabled, the proxy
   * handles the failover between the ResourceManagers as well.
   *
   * @param configuration Configuration to generate {@link ClientRMProxy}
   * @param protocol Protocol for the proxy
   * @param subClusterId the unique identifier or the sub-cluster
   * @param user the user on whose behalf the proxy is being created
   * @param token the auth token to use for connection
   * @param <T> Type information of the proxy
   * @return Proxy to the RM
   * @throws IOException on failure
   */
  @Public
  @Unstable
  @SuppressWarnings("unchecked")
  public static <T> T createRMProxy(final Configuration configuration,
      final Class<T> protocol, SubClusterId subClusterId,
      UserGroupInformation user, final Token token) throws IOException {
    try {
      final YarnConfiguration conf = new YarnConfiguration(configuration);
      updateConf(conf, subClusterId);
      if (token != null) {
        LOG.info(
            "Creating RMProxy with a token: {} to subcluster: {}"
                + " for protocol: {}",
            token, subClusterId, protocol.getSimpleName());
        user.addToken(token);
        setAuthModeInConf(conf);
      } else {
        LOG.info("Creating RMProxy without a token to subcluster: {}"
            + " for protocol: {}", subClusterId, protocol.getSimpleName());
      }
      final T proxyConnection = user.doAs(new PrivilegedExceptionAction<T>() {
        @Override
        public T run() throws Exception {
          return ClientRMProxy.createRMProxy(conf, protocol);
        }
      });

      return proxyConnection;
    } catch (IOException e) {
      String message =
          "Error while creating of RM application master service proxy for"
              + " appAttemptId: " + user;
      LOG.info(message);
      throw new YarnRuntimeException(message, e);
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
  }

  private static void setAuthModeInConf(Configuration conf) {
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        SaslRpcServer.AuthMethod.TOKEN.toString());
  }

  // updating the conf with the refreshed RM addresses as proxy creations
  // are based out of conf
  private static void updateConf(Configuration conf,
      SubClusterId subClusterId) {
    conf.set(YarnConfiguration.RM_CLUSTER_ID, subClusterId.getId());
    // In a Federation setting, we will connect to not just the local cluster RM
    // but also multiple external RMs. The membership information of all the RMs
    // that are currently
    // participating in Federation is available in the central
    // FederationStateStore.
    // So we will:
    // 1. obtain the RM service addresses from FederationStateStore using the
    // FederationRMFailoverProxyProvider.
    // 2. disable traditional HA as that depends on local configuration lookup
    // for RMs using indexes.
    // 3. we will enable federation failover IF traditional HA is enabled so
    // that the appropriate failover RetryPolicy is initialized.
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    conf.setClass(YarnConfiguration.CLIENT_FAILOVER_PROXY_PROVIDER,
        FederationRMFailoverProxyProvider.class, RMFailoverProxyProvider.class);
    if (HAUtil.isHAEnabled(conf)) {
      conf.setBoolean(YarnConfiguration.FEDERATION_FAILOVER_ENABLED, true);
      conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, false);
    }
  }

  // disable instantiation
  private FederationProxyProviderUtil() {
  }

}
