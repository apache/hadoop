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
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.ContainerManager;
import org.apache.hadoop.yarn.proto.ResourceTracker;
import org.apache.hadoop.yarn.proto.RMAdminProtocol;
import org.apache.hadoop.yarn.proto.ClientRMProtocol;
import org.apache.hadoop.yarn.proto.AMRMProtocol;

/**
 * {@link PolicyProvider} for YARN ResourceManager protocols.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RMPolicyProvider extends PolicyProvider {
  
  private static final Service[] resourceManagerServices = 
      new Service[] {
    new Service(
        YarnConfiguration.YARN_SECURITY_SERVICE_AUTHORIZATION_RESOURCETRACKER, 
        ResourceTracker.ResourceTrackerService.BlockingInterface.class),
    new Service(
        YarnConfiguration.YARN_SECURITY_SERVICE_AUTHORIZATION_CLIENT_RESOURCEMANAGER, 
        ClientRMProtocol.ClientRMProtocolService.BlockingInterface.class),
    new Service(
        YarnConfiguration.YARN_SECURITY_SERVICE_AUTHORIZATION_APPLICATIONMASTER_RESOURCEMANAGER, 
        AMRMProtocol.AMRMProtocolService.BlockingInterface.class),
    new Service(
        YarnConfiguration.YARN_SECURITY_SERVICE_AUTHORIZATION_ADMIN, 
        RMAdminProtocol.RMAdminProtocolService.BlockingInterface.class),
    new Service(
        YarnConfiguration.YARN_SECURITY_SERVICE_AUTHORIZATION_CONTAINER_MANAGER, 
        ContainerManager.ContainerManagerService.BlockingInterface.class),
  };

  @Override
  public Service[] getServices() {
    return resourceManagerServices;
  }

}
