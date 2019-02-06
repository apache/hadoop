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
package org.apache.hadoop.yarn.server.nodemanager.security.authorize;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocolPB;
import org.apache.hadoop.yarn.api.ContainerManagementProtocolPB;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocolPB;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocolPB;

/**
 * {@link PolicyProvider} for YARN NodeManager protocols.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NMPolicyProvider extends PolicyProvider {

  private static NMPolicyProvider nmPolicyProvider = null;

  private NMPolicyProvider() {}

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static NMPolicyProvider getInstance() {
    if (nmPolicyProvider == null) {
      synchronized(NMPolicyProvider.class) {
        if (nmPolicyProvider == null) {
          nmPolicyProvider = new NMPolicyProvider();
        }
      }
    }
    return nmPolicyProvider;
  }

  private static final Service[] NODE_MANAGER_SERVICES =
      new Service[] {
          new Service(YarnConfiguration.
            YARN_SECURITY_SERVICE_AUTHORIZATION_CONTAINER_MANAGEMENT_PROTOCOL,
            ContainerManagementProtocolPB.class),
          new Service(YarnConfiguration.
            YARN_SECURITY_SERVICE_AUTHORIZATION_RESOURCE_LOCALIZER,
            LocalizationProtocolPB.class),
          new Service(YarnConfiguration.
            YARN_SECURITY_SERVICE_AUTHORIZATION_COLLECTOR_NODEMANAGER_PROTOCOL,
            CollectorNodemanagerProtocolPB.class),
          new Service(YarnConfiguration.
              YARN_SECURITY_SERVICE_AUTHORIZATION_APPLICATIONMASTER_NODEMANAGER_PROTOCOL,
              ApplicationMasterProtocolPB.class),
      };

  @Override
  public Service[] getServices() {
    return NODE_MANAGER_SERVICES;
  }
}
