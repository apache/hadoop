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

package org.apache.hadoop.yarn.server.federation.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.impl.FSRegistryOperationsService;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for FederationRegistryClient.
 */
public class TestFederationRegistryClient {
  private Configuration conf;
  private UserGroupInformation user;
  private RegistryOperations registry;
  private FederationRegistryClient registryClient;

  @Before
  public void setup() throws Exception {
    this.conf = new YarnConfiguration();

    this.registry = new FSRegistryOperationsService();
    this.registry.init(this.conf);
    this.registry.start();

    this.user = UserGroupInformation.getCurrentUser();
    this.registryClient =
        new FederationRegistryClient(this.conf, this.registry, this.user);
    this.registryClient.cleanAllApplications();
    Assert.assertEquals(0, this.registryClient.getAllApplications().size());
  }

  @After
  public void breakDown() {
    registryClient.cleanAllApplications();
    Assert.assertEquals(0, registryClient.getAllApplications().size());
    registry.stop();
  }

  @Test
  public void testBasicCase() {
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    String scId1 = "subcluster1";
    String scId2 = "subcluster2";

    this.registryClient.writeAMRMTokenForUAM(appId, scId1,
        new Token<AMRMTokenIdentifier>());
    this.registryClient.writeAMRMTokenForUAM(appId, scId2,
        new Token<AMRMTokenIdentifier>());
    // Duplicate entry, should overwrite
    this.registryClient.writeAMRMTokenForUAM(appId, scId1,
        new Token<AMRMTokenIdentifier>());

    Assert.assertEquals(1, this.registryClient.getAllApplications().size());
    Assert.assertEquals(2,
        this.registryClient.loadStateFromRegistry(appId).size());

    this.registryClient.removeAppFromRegistry(appId);

    Assert.assertEquals(0, this.registryClient.getAllApplications().size());
    Assert.assertEquals(0,
        this.registryClient.loadStateFromRegistry(appId).size());
  }

}
