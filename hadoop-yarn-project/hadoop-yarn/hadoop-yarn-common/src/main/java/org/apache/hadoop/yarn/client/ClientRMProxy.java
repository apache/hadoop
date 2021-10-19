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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class ClientRMProxy<T> extends RMProxy<T>  {
  private static final Logger LOG =
      LoggerFactory.getLogger(ClientRMProxy.class);

  private interface ClientRMProtocols extends ApplicationClientProtocol,
      ApplicationMasterProtocol, ResourceManagerAdministrationProtocol {
    // Add nothing
  }

  private ClientRMProxy(){
    super();
  }

  /**
   * Create a proxy to the ResourceManager for the specified protocol.
   * @param configuration Configuration with all the required information.
   * @param protocol Client protocol for which proxy is being requested.
   * @param <T> Type of proxy.
   * @return Proxy to the ResourceManager for the specified client protocol.
   * @throws IOException
   */
  public static <T> T createRMProxy(final Configuration configuration,
      final Class<T> protocol) throws IOException {
    ClientRMProxy<T> clientRMProxy = new ClientRMProxy<>();
    return createRMProxy(configuration, protocol, clientRMProxy);
  }

  private static void setAMRMTokenService(final Configuration conf)
      throws IOException {
    for (Token<? extends TokenIdentifier> token : UserGroupInformation
      .getCurrentUser().getTokens()) {
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        token.setService(getAMRMTokenService(conf));
      }
    }
  }

  @Private
  @Override
  public InetSocketAddress getRMAddress(YarnConfiguration conf,
      Class<?> protocol) throws IOException {
    if (protocol == ApplicationClientProtocol.class) {
      return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
          YarnConfiguration.DEFAULT_RM_ADDRESS,
          YarnConfiguration.DEFAULT_RM_PORT);
    } else if (protocol == ResourceManagerAdministrationProtocol.class) {
      return conf.getSocketAddr(
          YarnConfiguration.RM_ADMIN_ADDRESS,
          YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS,
          YarnConfiguration.DEFAULT_RM_ADMIN_PORT);
    } else if (protocol == ApplicationMasterProtocol.class) {
      setAMRMTokenService(conf);
      return conf.getSocketAddr(YarnConfiguration.RM_SCHEDULER_ADDRESS,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
    } else {
      String message = "Unsupported protocol found when creating the proxy " +
          "connection to ResourceManager: " +
          ((protocol != null) ? protocol.getName() : "null");
      LOG.error(message);
      throw new IllegalStateException(message);
    }
  }

  @Private
  @Override
  public void checkAllowedProtocols(Class<?> protocol) {
    Preconditions.checkArgument(
        protocol.isAssignableFrom(ClientRMProtocols.class),
        "RM does not support this client protocol");
  }

  /**
   * Get the token service name to be used for RMDelegationToken. Depending
   * on whether HA is enabled or not, this method generates the appropriate
   * service name as a comma-separated list of service addresses.
   *
   * @param conf Configuration corresponding to the cluster we need the
   *             RMDelegationToken for
   * @return - Service name for RMDelegationToken
   */
  @Unstable
  public static Text getRMDelegationTokenService(Configuration conf) {
    return getTokenService(conf, YarnConfiguration.RM_ADDRESS,
            YarnConfiguration.DEFAULT_RM_ADDRESS,
            YarnConfiguration.DEFAULT_RM_PORT);
  }

  @Unstable
  public static Text getAMRMTokenService(Configuration conf) {
    return getTokenService(conf, YarnConfiguration.RM_SCHEDULER_ADDRESS,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
  }

  @Unstable
  public static Text getTokenService(Configuration conf, String address,
      String defaultAddr, int defaultPort) {
    if (HAUtil.isHAEnabled(conf)) {
      // Build a list of service addresses to form the service name
      ArrayList<String> services = new ArrayList<String>();
      YarnConfiguration yarnConf = new YarnConfiguration(conf);
      for (String rmId : HAUtil.getRMHAIds(conf)) {
        // Set RM_ID to get the corresponding RM_ADDRESS
        yarnConf.set(YarnConfiguration.RM_HA_ID, rmId);
        services.add(SecurityUtil.buildTokenService(
            yarnConf.getSocketAddr(address, defaultAddr, defaultPort))
            .toString());
      }
      return new Text(Joiner.on(',').join(services));
    }

    // Non-HA case - no need to set RM_ID
    return SecurityUtil.buildTokenService(conf.getSocketAddr(address,
      defaultAddr, defaultPort));
  }
}
