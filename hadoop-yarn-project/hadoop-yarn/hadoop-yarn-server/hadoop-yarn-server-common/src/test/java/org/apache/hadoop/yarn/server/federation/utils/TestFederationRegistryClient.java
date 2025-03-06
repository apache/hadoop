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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for FederationRegistryClient.
 */
public class TestFederationRegistryClient {
  private Configuration conf;
  private UserGroupInformation user;
  private RegistryOperations registry;
  private FederationRegistryClient registryClient;

  @BeforeEach
  public void setup() throws Exception {
    this.conf = new YarnConfiguration();

    this.registry = new FSRegistryOperationsService();
    this.registry.init(this.conf);
    this.registry.start();

    this.user = UserGroupInformation.getCurrentUser();
    this.registryClient =
        new FederationRegistryClient(this.conf, this.registry, this.user);
    this.registryClient.cleanAllApplications();
    assertEquals(0, this.registryClient.getAllApplications().size());
  }

  @AfterEach
  public void breakDown() {
    registryClient.cleanAllApplications();
    assertEquals(0, registryClient.getAllApplications().size());
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

    assertEquals(1, this.registryClient.getAllApplications().size());
    assertEquals(2,
        this.registryClient.loadStateFromRegistry(appId).size());

    this.registryClient.removeAppFromRegistry(appId);

    assertEquals(0, this.registryClient.getAllApplications().size());
    assertEquals(0,
        this.registryClient.loadStateFromRegistry(appId).size());
  }

  @Test
  public void testRemoveWithMemoryState() {
    ApplicationId appId1 = ApplicationId.newInstance(0, 0);
    ApplicationId appId2 = ApplicationId.newInstance(0, 1);
    String scId0 = "subcluster0";

    this.registryClient.writeAMRMTokenForUAM(appId1, scId0, new Token<>());
    this.registryClient.writeAMRMTokenForUAM(appId2, scId0, new Token<>());
    assertEquals(2, this.registryClient.getAllApplications().size());

    // Create a new client instance
    this.registryClient =
        new FederationRegistryClient(this.conf, this.registry, this.user);

    this.registryClient.loadStateFromRegistry(appId2);
    // Should remove app2
    this.registryClient.removeAppFromRegistry(appId2, false);
    assertEquals(1, this.registryClient.getAllApplications().size());

    // Should not remove app1 since memory state don't have it
    this.registryClient.removeAppFromRegistry(appId1, false);
    assertEquals(1, this.registryClient.getAllApplications().size());

    // Should remove app1
    this.registryClient.removeAppFromRegistry(appId1, true);
    assertEquals(0, this.registryClient.getAllApplications().size());
  }
}
