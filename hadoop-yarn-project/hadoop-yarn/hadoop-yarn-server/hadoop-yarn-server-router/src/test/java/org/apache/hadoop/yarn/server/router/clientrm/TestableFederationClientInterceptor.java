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

package org.apache.hadoop.yarn.server.router.clientrm;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.junit.Assert;

/**
 * Extends the FederationClientInterceptor and overrides methods to provide a
 * testable implementation of FederationClientInterceptor.
 */
public class TestableFederationClientInterceptor
    extends FederationClientInterceptor {

  private ConcurrentHashMap<SubClusterId, MockRM> mockRMs =
      new ConcurrentHashMap<>();

  private List<SubClusterId> badSubCluster = new ArrayList<SubClusterId>();

  @Override
  protected ApplicationClientProtocol getClientRMProxyForSubCluster(
      SubClusterId subClusterId) throws YarnException {

    MockRM mockRM = null;
    synchronized (this) {
      if (mockRMs.containsKey(subClusterId)) {
        mockRM = mockRMs.get(subClusterId);
      } else {
        mockRM = new MockRM();
        if (badSubCluster.contains(subClusterId)) {
          RMContext rmContext = mock(RMContext.class);
          return new MockClientRMService(rmContext, null, null, null, null,
              null);
        }
        mockRM.init(super.getConf());
        mockRM.start();
        try {
          mockRM.registerNode("h1:1234", 1024);
        } catch (Exception e) {
          Assert.fail(e.getMessage());
        }
        mockRMs.put(subClusterId, mockRM);
      }
      return mockRM.getClientRMService();
    }
  }

  private static class MockClientRMService extends ClientRMService {

    MockClientRMService(RMContext rmContext, YarnScheduler scheduler,
        RMAppManager rmAppManager,
        ApplicationACLsManager applicationACLsManager,
        QueueACLsManager queueACLsManager,
        RMDelegationTokenSecretManager rmDTSecretManager) {
      super(rmContext, scheduler, rmAppManager, applicationACLsManager,
          queueACLsManager, rmDTSecretManager);
    }

    @Override
    public SubmitApplicationResponse submitApplication(
        SubmitApplicationRequest request) throws YarnException, IOException {
      throw new ConnectException("RM is stopped");
    }

  }

  /**
   * For testing purpose, some subclusters has to be down to simulate particular
   * scenarios as RM Failover, network issues. For this reason we keep track of
   * these bad subclusters. This method make the subcluster unusable.
   *
   * @param badSC the subcluster to make unusable
   * @throws IOException
   */
  protected void registerBadSubCluster(SubClusterId badSC) throws IOException {
    badSubCluster.add(badSC);
    if (mockRMs.contains(badSC)) {
      mockRMs.get(badSC).close();
    }
  }

}
