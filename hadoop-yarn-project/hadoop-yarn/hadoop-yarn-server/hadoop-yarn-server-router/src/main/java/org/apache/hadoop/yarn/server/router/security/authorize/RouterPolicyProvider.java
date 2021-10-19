/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.hadoop.yarn.server.router.security.authorize;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.yarn.api.ApplicationClientProtocolPB;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocolPB;

/**
 * {@link PolicyProvider} for YARN Router server protocols.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RouterPolicyProvider extends PolicyProvider {

  private static volatile RouterPolicyProvider routerPolicyProvider = null;

  private RouterPolicyProvider() {
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static RouterPolicyProvider getInstance() {
    if (routerPolicyProvider == null) {
      synchronized (RouterPolicyProvider.class) {
        if (routerPolicyProvider == null) {
          routerPolicyProvider = new RouterPolicyProvider();
        }
      }
    }
    return routerPolicyProvider;
  }

  private static final Service[] ROUTER_SERVICES = new Service[] {
      new Service(
          YarnConfiguration.YARN_SECURITY_SERVICE_AUTHORIZATION_APPLICATIONCLIENT_PROTOCOL,
          ApplicationClientProtocolPB.class),
      new Service(
          YarnConfiguration.YARN_SECURITY_SERVICE_AUTHORIZATION_RESOURCEMANAGER_ADMINISTRATION_PROTOCOL,
          ResourceManagerAdministrationProtocolPB.class), };

  @Override
  public Service[] getServices() {
    return ROUTER_SERVICES;
  }

}
