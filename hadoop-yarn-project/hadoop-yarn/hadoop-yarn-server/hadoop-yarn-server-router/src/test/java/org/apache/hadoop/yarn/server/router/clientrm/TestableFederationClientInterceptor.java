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
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.router.security.RouterDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends the FederationClientInterceptor and overrides methods to provide a
 * testable implementation of FederationClientInterceptor.
 */
public class TestableFederationClientInterceptor
    extends FederationClientInterceptor {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestableFederationClientInterceptor.class);

  private ConcurrentHashMap<SubClusterId, MockRM> mockRMs =
      new ConcurrentHashMap<>();

  private ConcurrentHashMap<SubClusterId, MockNM> mockNMs =
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
          MockNM nm = mockRM.registerNode("127.0.0.1:1234", 8*1024, 4);
          mockNMs.put(subClusterId, nm);
        } catch (Exception e) {
          Assert.fail(e.getMessage());
        }
        mockRMs.put(subClusterId, mockRM);
      }
      initNodeAttributes(subClusterId, mockRM);
      initReservationSystem(mockRM);
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

    @Override
    public GetClusterMetricsResponse getClusterMetrics(GetClusterMetricsRequest request)
        throws YarnException {
      throw new YarnException("RM is stopped");
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

  public ConcurrentHashMap<SubClusterId, MockRM> getMockRMs() {
    return mockRMs;
  }

  public ConcurrentHashMap<SubClusterId, MockNM> getMockNMs() {
    return mockNMs;
  }

  private void initNodeAttributes(SubClusterId subClusterId, MockRM mockRM)  {
    String node1 = subClusterId.getId() +"-host1";
    String node2 = subClusterId.getId() +"-host2";
    NodeAttributesManager mgr = mockRM.getRMContext().getNodeAttributesManager();
    NodeAttribute gpu =
        NodeAttribute.newInstance(NodeAttribute.PREFIX_CENTRALIZED, "GPU",
        NodeAttributeType.STRING, "nvidia");
    NodeAttribute os =
        NodeAttribute.newInstance(NodeAttribute.PREFIX_CENTRALIZED, "OS",
        NodeAttributeType.STRING, "windows64");
    NodeAttribute docker =
        NodeAttribute.newInstance(NodeAttribute.PREFIX_DISTRIBUTED, "DOCKER",
        NodeAttributeType.STRING, "docker0");
    NodeAttribute dist =
        NodeAttribute.newInstance(NodeAttribute.PREFIX_DISTRIBUTED, "VERSION",
        NodeAttributeType.STRING, "3_0_2");
    Map<String, Set<NodeAttribute>> nodes = new HashMap<>();
    nodes.put(node1, ImmutableSet.of(gpu, os, dist));
    nodes.put(node2, ImmutableSet.of(docker, dist));
    try {
      mgr.addNodeAttributes(nodes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void initReservationSystem(MockRM mockRM) throws YarnException {
    try {
      // Ensure that the reserved resources of the RM#Reservation System are allocated
      String planName = "root.decided";
      ReservationSystem reservationSystem = mockRM.getReservationSystem();
      reservationSystem.synchronizePlan(planName, true);

      GenericTestUtils.waitFor(() -> {
        Plan plan = reservationSystem.getPlan(planName);
        Resource resource = plan.getTotalCapacity();
        return (resource.getMemorySize() > 0 && resource.getVirtualCores() > 0);
      }, 100, 2000);
    } catch (TimeoutException | InterruptedException e) {
      throw new YarnException(e);
    }
  }

  @Override
  public void shutdown() {
    if (mockRMs != null && !mockRMs.isEmpty()) {
      for (Map.Entry<SubClusterId, MockRM> item : mockRMs.entrySet()) {
        SubClusterId subClusterId = item.getKey();

        // close mockNM
        MockNM mockNM = mockNMs.getOrDefault(subClusterId, null);
        try {
          mockNM.unRegisterNode();
          mockNM = null;
        } catch (Exception e) {
          LOG.error("mockNM unRegisterNode error.", e);
        }

        // close mockRM
        MockRM mockRM = item.getValue();
        if (mockRM != null) {
          mockRM.stop();
        }
      }
    }
    mockNMs.clear();
    mockRMs.clear();
    super.shutdown();
  }

  public RouterDelegationTokenSecretManager createRouterRMDelegationTokenSecretManager(
      Configuration conf) {

    long secretKeyInterval = conf.getTimeDuration(
        YarnConfiguration.RM_DELEGATION_KEY_UPDATE_INTERVAL_KEY,
        YarnConfiguration.RM_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);

    long tokenMaxLifetime = conf.getTimeDuration(
        YarnConfiguration.RM_DELEGATION_TOKEN_MAX_LIFETIME_KEY,
        YarnConfiguration.RM_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT,
        TimeUnit.MILLISECONDS);

    long tokenRenewInterval = conf.getTimeDuration(
        YarnConfiguration.RM_DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
        YarnConfiguration.RM_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);

    long removeScanInterval = conf.getTimeDuration(
        YarnConfiguration.RM_DELEGATION_TOKEN_REMOVE_SCAN_INTERVAL_KEY,
        YarnConfiguration.RM_DELEGATION_TOKEN_REMOVE_SCAN_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);

    return new RouterDelegationTokenSecretManager(secretKeyInterval,
        tokenMaxLifetime, tokenRenewInterval, removeScanInterval, conf);
  }
}