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
package org.apache.hadoop.yarn.server.resourcemanager.security.authorize;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocolPB;
import org.apache.hadoop.yarn.api.ApplicationClientProtocolPB;
import org.apache.hadoop.yarn.api.ContainerManagementProtocolPB;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocolPB;
import org.apache.hadoop.yarn.server.api.ResourceTrackerPB;

/**
 * {@link PolicyProvider} for YARN ResourceManager protocols.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RMPolicyProvider extends PolicyProvider {

  private static RMPolicyProvider rmPolicyProvider = null;

  private RMPolicyProvider() {}

  @Private
  @Unstable
  public static RMPolicyProvider getInstance() {
    if (rmPolicyProvider == null) {
      synchronized(RMPolicyProvider.class) {
        if (rmPolicyProvider == null) {
          rmPolicyProvider = new RMPolicyProvider();
        }
      }
    }
    return rmPolicyProvider;
  }

  private static final Service[] resourceManagerServices = 
      new Service[] {
    new Service(
        YarnConfiguration.YARN_SECURITY_SERVICE_AUTHORIZATION_RESOURCETRACKER_PROTOCOL, 
        ResourceTrackerPB.class),
    new Service(
        YarnConfiguration.YARN_SECURITY_SERVICE_AUTHORIZATION_APPLICATIONCLIENT_PROTOCOL, 
        ApplicationClientProtocolPB.class),
    new Service(
        YarnConfiguration.YARN_SECURITY_SERVICE_AUTHORIZATION_APPLICATIONMASTER_PROTOCOL, 
        ApplicationMasterProtocolPB.class),
    new Service(
        YarnConfiguration.YARN_SECURITY_SERVICE_AUTHORIZATION_RESOURCEMANAGER_ADMINISTRATION_PROTOCOL, 
        ResourceManagerAdministrationProtocolPB.class),
    new Service(
        YarnConfiguration.YARN_SECURITY_SERVICE_AUTHORIZATION_CONTAINER_MANAGEMENT_PROTOCOL, 
        ContainerManagementProtocolPB.class),
    new Service(
        CommonConfigurationKeys.SECURITY_HA_SERVICE_PROTOCOL_ACL,
        HAServiceProtocol.class),
  };

  @Override
  public Service[] getServices() {
    return resourceManagerServices;
  }

}
