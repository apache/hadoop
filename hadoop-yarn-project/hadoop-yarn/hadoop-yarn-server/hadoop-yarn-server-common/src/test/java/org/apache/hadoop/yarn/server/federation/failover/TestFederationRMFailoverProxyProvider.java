/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.federation.failover;

import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.DefaultNoHARMFailoverProxyProvider;
import org.apache.hadoop.yarn.client.RMFailoverProxyProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * We will test the failover of Federation.
 */
public class TestFederationRMFailoverProxyProvider {

  @Test
  public void testRMFailoverProxyProvider() throws YarnException {
    YarnConfiguration configuration = new YarnConfiguration();

    RMFailoverProxyProvider<ApplicationClientProtocol> clientRMFailoverProxyProvider =
        ClientRMProxy.getClientRMFailoverProxyProvider(configuration, ApplicationClientProtocol.class);
    assertTrue(clientRMFailoverProxyProvider instanceof DefaultNoHARMFailoverProxyProvider);

    FederationProxyProviderUtil.updateConfForFederation(configuration, "SC-1");
    configuration.setBoolean(YarnConfiguration.FEDERATION_NON_HA_ENABLED,true);
    RMFailoverProxyProvider<ApplicationClientProtocol> clientRMFailoverProxyProvider2 =
        ClientRMProxy.getClientRMFailoverProxyProvider(configuration, ApplicationClientProtocol.class);
    assertTrue(clientRMFailoverProxyProvider2 instanceof FederationRMFailoverProxyProvider);
  }
}
