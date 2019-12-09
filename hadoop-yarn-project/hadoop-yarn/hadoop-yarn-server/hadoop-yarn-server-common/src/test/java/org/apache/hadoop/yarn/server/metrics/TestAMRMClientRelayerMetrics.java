/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.AMRMClientRelayer;
import org.apache.hadoop.yarn.server.metrics.AMRMClientRelayerMetrics.RequestType;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for AMRMClientRelayer.
 */
public class TestAMRMClientRelayerMetrics {

  /**
   * Mock AMS for easier testing and mocking of request/responses.
   */
  public static class MockApplicationMasterService
      implements ApplicationMasterProtocol {

    private boolean failover = false;
    private boolean exception = false;
    private List<ResourceRequest> lastAsk;
    private List<ContainerId> lastRelease;
    private List<UpdateContainerRequest> lastUpdates;
    private List<String> lastBlacklistAdditions;
    private List<String> lastBlacklistRemovals;
    private AllocateResponse response = AllocateResponse
        .newInstance(0, null, null, new ArrayList<NodeReport>(),
            Resource.newInstance(0, 0), null, 0, null, null);

    @Override
    public RegisterApplicationMasterResponse registerApplicationMaster(
        RegisterApplicationMasterRequest request)
        throws YarnException, IOException {
      return null;
    }

    @Override
    public FinishApplicationMasterResponse finishApplicationMaster(
        FinishApplicationMasterRequest request)
        throws YarnException, IOException {
      if (this.failover) {
        this.failover = false;
        throw new ApplicationMasterNotRegisteredException("Mock RM restarted");
      }
      return null;
    }

    @Override
    public AllocateResponse allocate(AllocateRequest request)
        throws YarnException, IOException {
      if (this.failover) {
        this.failover = false;
        throw new ApplicationMasterNotRegisteredException("Mock RM restarted");
      }
      if(this.exception){
        this.exception = false;
        throw new YarnException("Mock RM encountered exception");
      }
      this.lastAsk = request.getAskList();
      this.lastRelease = request.getReleaseList();
      this.lastUpdates = request.getUpdateRequests();
      this.lastBlacklistAdditions =
          request.getResourceBlacklistRequest().getBlacklistAdditions();
      this.lastBlacklistRemovals =
          request.getResourceBlacklistRequest().getBlacklistRemovals();
      return response;
    }

    public void setFailoverFlag() {
      this.failover = true;
    }
  }

  private Configuration conf;
  private MockApplicationMasterService mockAMS;
  private String homeID = "home";
  private AMRMClientRelayer homeRelayer;
  private String uamID = "uam";
  private AMRMClientRelayer uamRelayer;

  private List<ResourceRequest> asks = new ArrayList<>();
  private List<ContainerId> releases = new ArrayList<>();
  private List<UpdateContainerRequest> updates = new ArrayList<>();
  private List<String> blacklistAdditions = new ArrayList<>();
  private List<String> blacklistRemoval = new ArrayList<>();

  @Before
  public void setup() throws YarnException, IOException {
    this.conf = new Configuration();

    this.mockAMS = new MockApplicationMasterService();

    this.homeRelayer = new AMRMClientRelayer(this.mockAMS,
        ApplicationId.newInstance(0, 0), this.homeID);
    this.homeRelayer.init(conf);
    this.homeRelayer.start();

    this.homeRelayer.registerApplicationMaster(
        RegisterApplicationMasterRequest.newInstance("", 0, ""));

    this.uamRelayer = new AMRMClientRelayer(this.mockAMS,
        ApplicationId.newInstance(0, 0), this.uamID);
    this.uamRelayer.init(conf);
    this.uamRelayer.start();

    this.uamRelayer.registerApplicationMaster(
        RegisterApplicationMasterRequest.newInstance("", 0, ""));

    clearAllocateRequestLists();

    AMRMClientRelayerMetrics.getInstance()
        .setClientPending(homeID, RequestType.Guaranteed, 0);
    AMRMClientRelayerMetrics.getInstance()
        .setClientPending(homeID, RequestType.Opportunistic, 0);
    AMRMClientRelayerMetrics.getInstance()
        .setClientPending(homeID, RequestType.Promote, 0);
    AMRMClientRelayerMetrics.getInstance()
        .setClientPending(homeID, RequestType.Demote, 0);

    AMRMClientRelayerMetrics.getInstance()
        .setClientPending(uamID, RequestType.Guaranteed, 0);
    AMRMClientRelayerMetrics.getInstance()
        .setClientPending(uamID, RequestType.Opportunistic, 0);
    AMRMClientRelayerMetrics.getInstance()
        .setClientPending(uamID, RequestType.Promote, 0);
    AMRMClientRelayerMetrics.getInstance()
        .setClientPending(uamID, RequestType.Demote, 0);
  }

  private AllocateRequest getAllocateRequest() {
    // Need to create a new one every time because rather than directly
    // referring the lists, the protobuf impl makes a copy of the lists
    return AllocateRequest.newBuilder()
        .responseId(0)
        .progress(0).askList(asks)
        .releaseList(new ArrayList<>(this.releases))
        .resourceBlacklistRequest(ResourceBlacklistRequest.newInstance(
            new ArrayList<>(this.blacklistAdditions),
            new ArrayList<>(this.blacklistRemoval)))
        .updateRequests(new ArrayList<>(this.updates))
        .build();
  }

  private void clearAllocateRequestLists() {
    this.asks.clear();
    this.releases.clear();
    this.updates.clear();
    this.blacklistAdditions.clear();
    this.blacklistRemoval.clear();
  }

  private static UpdateContainerRequest createPromote(int id){
    return UpdateContainerRequest.newInstance(0, createContainerId(id),
        ContainerUpdateType.PROMOTE_EXECUTION_TYPE, Resource.newInstance(0, 0),
        ExecutionType.GUARANTEED);
  }

  private static UpdateContainerRequest createDemote(int id){
    return UpdateContainerRequest.newInstance(0, createContainerId(id),
        ContainerUpdateType.DEMOTE_EXECUTION_TYPE, Resource.newInstance(0, 0),
        ExecutionType.OPPORTUNISTIC);
  }

  private static ContainerId createContainerId(int id) {
    return ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1),
        id);
  }

  public ResourceRequest createResourceRequest(long id, String resource,
      int memory, int vCores, int priority, ExecutionType execType,
      int containers) {
    ResourceRequest req = Records.newRecord(ResourceRequest.class);
    req.setAllocationRequestId(id);
    req.setResourceName(resource);
    req.setCapability(Resource.newInstance(memory, vCores));
    req.setPriority(Priority.newInstance(priority));
    req.setExecutionTypeRequest(ExecutionTypeRequest.newInstance(execType));
    req.setNumContainers(containers);
    return req;
  }

  @Test
  public void testGPending() throws YarnException, IOException {
    // Ask for two containers, one with location preference
    this.asks.add(
        createResourceRequest(0, "node", 2048, 1, 1, ExecutionType.GUARANTEED,
            1));
    this.asks.add(
        createResourceRequest(0, "rack", 2048, 1, 1, ExecutionType.GUARANTEED,
            1));
    this.asks.add(createResourceRequest(0, ResourceRequest.ANY, 2048, 1, 1,
        ExecutionType.GUARANTEED, 2));
    this.homeRelayer.allocate(getAllocateRequest());

    Assert.assertEquals(2, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(homeID, RequestType.Guaranteed).value());

    Assert.assertEquals(0, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(uamID, RequestType.Guaranteed).value());

    // Ask from the uam
    this.uamRelayer.allocate(getAllocateRequest());

    Assert.assertEquals(2, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(homeID, RequestType.Guaranteed).value());

    Assert.assertEquals(2, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(uamID, RequestType.Guaranteed).value());

    // Update the any to ask for an extra container
    this.asks.get(2).setNumContainers(3);
    this.homeRelayer.allocate(getAllocateRequest());

    Assert.assertEquals(3, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(homeID, RequestType.Guaranteed).value());

    Assert.assertEquals(2, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(uamID, RequestType.Guaranteed).value());

    // Update the any to ask to pretend a container was allocated
    this.asks.get(2).setNumContainers(2);
    this.homeRelayer.allocate(getAllocateRequest());

    Assert.assertEquals(2, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(homeID, RequestType.Guaranteed).value());

    Assert.assertEquals(2, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(uamID, RequestType.Guaranteed).value());
  }

  @Test
  public void testPromotePending() throws YarnException, IOException {
    // Ask to promote 3 containers
    this.updates.add(createPromote(1));
    this.updates.add(createPromote(2));
    this.updates.add(createPromote(3));

    this.homeRelayer.allocate(getAllocateRequest());

    Assert.assertEquals(3, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(homeID, RequestType.Promote).value());

    // Demote 2 containers, one of which is pending promote
    this.updates.remove(createPromote(3));
    this.updates.add(createDemote(3));
    this.updates.add(createDemote(4));

    this.homeRelayer.allocate(getAllocateRequest());

    Assert.assertEquals(2, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(homeID, RequestType.Promote).value());

    // Let the RM respond with two successful promotions, one of which
    // was pending promote
    List<UpdatedContainer> updated = new ArrayList<>();
    updated.add(UpdatedContainer
        .newInstance(ContainerUpdateType.PROMOTE_EXECUTION_TYPE, Container
            .newInstance(createContainerId(2), null, null, null, null, null)));
    updated.add(UpdatedContainer
        .newInstance(ContainerUpdateType.PROMOTE_EXECUTION_TYPE, Container
            .newInstance(createContainerId(5), null, null, null, null, null)));
    this.mockAMS.response.setUpdatedContainers(updated);

    this.homeRelayer.allocate(getAllocateRequest());

    Assert.assertEquals(1, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(homeID, RequestType.Promote).value());

    // Remove the promoted container and clean up response
    this.mockAMS.response.getUpdatedContainers().clear();
    this.updates.remove(createPromote(2));

    // Let the RM respond with two completed containers, one of which was
    // pending promote
    List<ContainerStatus> completed = new ArrayList<>();
    completed
        .add(ContainerStatus.newInstance(createContainerId(1), null, "", 0));
    completed
        .add(ContainerStatus.newInstance(createContainerId(6), null, "", 0));
    this.mockAMS.response.setCompletedContainersStatuses(completed);

    this.homeRelayer.allocate(getAllocateRequest());

    Assert.assertEquals(0, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(homeID, RequestType.Promote).value());
  }

  @Test
  public void testCleanUpOnFinish() throws YarnException, IOException {
    // Ask for two containers, one with location preference
    this.asks.add(
        createResourceRequest(0, "node", 2048, 1, 1, ExecutionType.GUARANTEED,
            1));
    this.asks.add(
        createResourceRequest(0, "rack", 2048, 1, 1, ExecutionType.GUARANTEED,
            1));
    this.asks.add(createResourceRequest(0, ResourceRequest.ANY, 2048, 1, 1,
        ExecutionType.GUARANTEED, 2));

    // Ask to promote 3 containers
    this.updates.add(createPromote(1));
    this.updates.add(createPromote(2));
    this.updates.add(createPromote(3));

    // Run the allocate call to start tracking pending
    this.homeRelayer.allocate(getAllocateRequest());

    // After finish, the metrics should reset to zero
    this.homeRelayer.shutdown();

    Assert.assertEquals(0, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(homeID, RequestType.Guaranteed).value());

    Assert.assertEquals(0, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(homeID, RequestType.Promote).value());
  }

  @Test
  public void testFailover() throws YarnException, IOException {
    // Ask for two containers, one with location preference
    this.asks.add(
        createResourceRequest(0, "node", 2048, 1, 1, ExecutionType.GUARANTEED,
            1));
    this.asks.add(
        createResourceRequest(0, "rack", 2048, 1, 1, ExecutionType.GUARANTEED,
            1));
    this.asks.add(createResourceRequest(0, ResourceRequest.ANY, 2048, 1, 1,
        ExecutionType.GUARANTEED, 2));

    long previousSuccess = AMRMClientRelayerMetrics.getInstance()
        .getHeartbeatSuccessMetric(homeID).value();
    long previousFailover = AMRMClientRelayerMetrics.getInstance()
        .getRMMasterSlaveSwitchMetric(homeID).value();
    // Set failover to trigger
    mockAMS.failover = true;
    this.homeRelayer.allocate(getAllocateRequest());
    // The failover metric should be incremented
    Assert.assertEquals(++previousFailover,
        AMRMClientRelayerMetrics.getInstance()
        .getRMMasterSlaveSwitchMetric(homeID).value());

    // The success metric should be incremented once
    Assert.assertEquals(++previousSuccess,
        AMRMClientRelayerMetrics.getInstance()
            .getHeartbeatSuccessMetric(homeID).value());

    Assert.assertEquals(2, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(homeID, RequestType.Guaranteed).value());

    Assert.assertEquals(0, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(uamID, RequestType.Guaranteed).value());

    // Ask from the uam
    this.uamRelayer.allocate(getAllocateRequest());

    Assert.assertEquals(2, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(homeID, RequestType.Guaranteed).value());

    Assert.assertEquals(2, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(uamID, RequestType.Guaranteed).value());

    // Update the any to ask for an extra container
    this.asks.get(2).setNumContainers(3);
    mockAMS.failover = true;
    this.homeRelayer.allocate(getAllocateRequest());
    // The failover metric should be incremented
    Assert.assertEquals(++previousFailover,
        AMRMClientRelayerMetrics.getInstance()
            .getRMMasterSlaveSwitchMetric(homeID).value());

    // The success metric should be incremented once
    Assert.assertEquals(++previousSuccess,
        AMRMClientRelayerMetrics.getInstance()
            .getHeartbeatSuccessMetric(homeID).value());

    Assert.assertEquals(3, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(homeID, RequestType.Guaranteed).value());

    Assert.assertEquals(2, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(uamID, RequestType.Guaranteed).value());

    // Update the any to ask to pretend a container was allocated
    this.asks.get(2).setNumContainers(2);
    mockAMS.failover = true;
    this.homeRelayer.allocate(getAllocateRequest());
    // The failover metric should be incremented
    Assert.assertEquals(++previousFailover,
        AMRMClientRelayerMetrics.getInstance()
            .getRMMasterSlaveSwitchMetric(homeID).value());

    // The success metric should be incremented once
    Assert.assertEquals(++previousSuccess,
        AMRMClientRelayerMetrics.getInstance()
            .getHeartbeatSuccessMetric(homeID).value());

    Assert.assertEquals(2, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(homeID, RequestType.Guaranteed).value());

    Assert.assertEquals(2, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(uamID, RequestType.Guaranteed).value());

    long previousFailure = AMRMClientRelayerMetrics.getInstance()
        .getHeartbeatFailureMetric(homeID).value();

    mockAMS.exception = true;
    try{
      this.homeRelayer.allocate(getAllocateRequest());
      Assert.fail();
    } catch (YarnException e){
    }
    // The failover metric should not be incremented
    Assert.assertEquals(previousFailover,
        AMRMClientRelayerMetrics.getInstance()
            .getRMMasterSlaveSwitchMetric(homeID).value());

    // The success metric should not be incremented
    Assert.assertEquals(previousSuccess,
        AMRMClientRelayerMetrics.getInstance()
            .getHeartbeatSuccessMetric(homeID).value());

    // The failure metric should be incremented
    Assert.assertEquals(++previousFailure,
        AMRMClientRelayerMetrics.getInstance()
            .getHeartbeatFailureMetric(homeID).value());

    mockAMS.failover = true;
    mockAMS.exception = true;
    try{
      this.homeRelayer.allocate(getAllocateRequest());
      Assert.fail();
    } catch (YarnException e){
    }
    // The failover metric should be incremented
    Assert.assertEquals(++previousFailover,
        AMRMClientRelayerMetrics.getInstance()
            .getRMMasterSlaveSwitchMetric(homeID).value());

    // The success metric should not be incremented
    Assert.assertEquals(previousSuccess,
        AMRMClientRelayerMetrics.getInstance()
            .getHeartbeatSuccessMetric(homeID).value());

    // The failure metric should be incremented
    Assert.assertEquals(++previousFailure,
        AMRMClientRelayerMetrics.getInstance()
            .getHeartbeatFailureMetric(homeID).value());
  }

  @Test
  public void testNewEmptyRequest()
      throws YarnException, IOException {
    // Ask for zero containers
    this.asks.add(createResourceRequest(1, ResourceRequest.ANY, 2048, 1, 1,
        ExecutionType.GUARANTEED, 0));
    this.homeRelayer.allocate(getAllocateRequest());

    Assert.assertEquals(0, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(homeID, RequestType.Guaranteed).value());

    Assert.assertEquals(0, AMRMClientRelayerMetrics.getInstance()
        .getPendingMetric(uamID, RequestType.Guaranteed).value());
  }
}
