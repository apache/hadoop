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

package org.apache.hadoop.yarn.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.scheduler.ResourceRequestSet;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for AMRMClientRelayer.
 */
public class TestAMRMClientRelayer {

  /**
   * Mocked ApplicationMasterService in RM.
   */
  public static class MockApplicationMasterService
      implements ApplicationMasterProtocol {

    // Whether this mockRM will throw failover exception upon next heartbeat
    // from AM
    private boolean failover = false;

    // Whether this mockRM will throw application already registered exception
    // upon next registerApplicationMaster call
    private boolean throwAlreadyRegister = false;

    private int responseIdReset = -1;
    private List<ResourceRequest> lastAsk;
    private List<ContainerId> lastRelease;
    private List<String> lastBlacklistAdditions;
    private List<String> lastBlacklistRemovals;

    @Override
    public RegisterApplicationMasterResponse registerApplicationMaster(
        RegisterApplicationMasterRequest request)
        throws YarnException, IOException {
      if (this.throwAlreadyRegister) {
        this.throwAlreadyRegister = false;
        throw new InvalidApplicationMasterRequestException(
            AMRMClientUtils.APP_ALREADY_REGISTERED_MESSAGE + "appId");
      }
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
      if (this.responseIdReset != -1) {
        String errorMessage =
            AMRMClientUtils.assembleInvalidResponseIdExceptionMessage(null,
                this.responseIdReset, request.getResponseId());
        this.responseIdReset = -1;
        throw new InvalidApplicationMasterRequestException(errorMessage);
      }

      this.lastAsk = request.getAskList();
      this.lastRelease = request.getReleaseList();
      this.lastBlacklistAdditions =
          request.getResourceBlacklistRequest().getBlacklistAdditions();
      this.lastBlacklistRemovals =
          request.getResourceBlacklistRequest().getBlacklistRemovals();
      return AllocateResponse.newInstance(request.getResponseId() + 1, null,
          null, new ArrayList<NodeReport>(), Resource.newInstance(0, 0), null,
          0, null, null);
    }

    public void setFailoverFlag() {
      this.failover = true;
    }

    public void setThrowAlreadyRegister() {
      this.throwAlreadyRegister = true;
    }

    public void setResponseIdReset(int expectedResponseId) {
      this.responseIdReset = expectedResponseId;
    }
  }

  private Configuration conf;
  private MockApplicationMasterService mockAMS;
  private AMRMClientRelayer relayer;

  private int responseId = 0;

  // Buffer of asks that will be sent to RM in the next AM heartbeat
  private List<ResourceRequest> asks = new ArrayList<>();
  private List<ContainerId> releases = new ArrayList<>();
  private List<String> blacklistAdditions = new ArrayList<>();
  private List<String> blacklistRemoval = new ArrayList<>();

  @Before
  public void setup() throws YarnException, IOException {
    this.conf = new Configuration();

    this.mockAMS = new MockApplicationMasterService();
    this.relayer = new AMRMClientRelayer(this.mockAMS, null, "TEST", conf);
    this.relayer.registerApplicationMaster(
        RegisterApplicationMasterRequest.newInstance("", 0, ""));

    clearAllocateRequestLists();
  }

  @After
  public void cleanup() {
    this.relayer.shutdown();
  }

  private void assertAsksAndReleases(int expectedAsk, int expectedRelease) {
    Assert.assertEquals(expectedAsk, this.mockAMS.lastAsk.size());
    Assert.assertEquals(expectedRelease, this.mockAMS.lastRelease.size());
  }

  private void assertBlacklistAdditionsAndRemovals(int expectedAdditions,
      int expectedRemovals) {
    Assert.assertEquals(expectedAdditions,
        this.mockAMS.lastBlacklistAdditions.size());
    Assert.assertEquals(expectedRemovals,
        this.mockAMS.lastBlacklistRemovals.size());
  }

  private AllocateRequest getAllocateRequest() {
    // Need to create a new one every time because rather than directly
    // referring the lists, the protobuf impl makes a copy of the lists
    return AllocateRequest.newInstance(responseId, 0, asks, releases,
        ResourceBlacklistRequest.newInstance(blacklistAdditions,
            blacklistRemoval));
  }

  private void clearAllocateRequestLists() {
    this.asks.clear();
    this.releases.clear();
    this.blacklistAdditions.clear();
    this.blacklistRemoval.clear();
  }

  private static ContainerId createContainerId(int id) {
    return ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1),
        id);
  }

  protected ResourceRequest createResourceRequest(long id, String resource,
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

  /**
   * Test the proper handling of removal/cancel of resource requests.
   */
  @Test
  public void testResourceRequestCleanup() throws YarnException, IOException {
    // Ask for two containers, one with location preference
    this.asks.add(createResourceRequest(0, "node", 2048, 1, 1,
        ExecutionType.GUARANTEED, 1));
    this.asks.add(createResourceRequest(0, "rack", 2048, 1, 1,
        ExecutionType.GUARANTEED, 1));
    this.asks.add(createResourceRequest(0, ResourceRequest.ANY, 2048, 1, 1,
        ExecutionType.GUARANTEED, 2));
    this.relayer.allocate(getAllocateRequest());

    assertAsksAndReleases(3, 0);
    Assert.assertEquals(1, this.relayer.getRemotePendingAsks().size());
    ResourceRequestSet set =
        this.relayer.getRemotePendingAsks().values().iterator().next();
    Assert.assertEquals(3, set.getAsks().size());
    clearAllocateRequestLists();

    // Cancel one ask
    this.asks.add(createResourceRequest(0, "node", 2048, 1, 1,
        ExecutionType.GUARANTEED, 0));
    this.asks.add(createResourceRequest(0, ResourceRequest.ANY, 2048, 1, 1,
        ExecutionType.GUARANTEED, 1));
    this.relayer.allocate(getAllocateRequest());

    assertAsksAndReleases(2, 0);
    Assert.assertEquals(1, relayer.getRemotePendingAsks().size());
    set = this.relayer.getRemotePendingAsks().values().iterator().next();
    Assert.assertEquals(2, set.getAsks().size());
    clearAllocateRequestLists();

    // Cancel the other ask, the pending askSet should be removed
    this.asks.add(createResourceRequest(0, ResourceRequest.ANY, 2048, 1, 1,
        ExecutionType.GUARANTEED, 0));
    this.relayer.allocate(AllocateRequest.newInstance(0, 0, asks, null, null));

    assertAsksAndReleases(1, 0);
    Assert.assertEquals(0, this.relayer.getRemotePendingAsks().size());
  }

  /**
   * Test the full pending resend after RM fails over.
   */
  @Test
  public void testResendRequestsOnRMRestart()
      throws YarnException, IOException {
    ContainerId c1 = createContainerId(1);
    ContainerId c2 = createContainerId(2);
    ContainerId c3 = createContainerId(3);

    // Ask for two containers, one with location preference
    this.asks.add(createResourceRequest(0, "node1", 2048, 1, 1,
        ExecutionType.GUARANTEED, 1));
    this.asks.add(createResourceRequest(0, "rack", 2048, 1, 1,
        ExecutionType.GUARANTEED, 1));
    this.asks.add(createResourceRequest(0, ResourceRequest.ANY, 2048, 1, 1,
        ExecutionType.GUARANTEED, 2));

    this.releases.add(c1);
    this.blacklistAdditions.add("node1");
    this.blacklistRemoval.add("node0");

    // 1. a fully loaded request
    this.relayer.allocate(getAllocateRequest());
    assertAsksAndReleases(3, 1);
    assertBlacklistAdditionsAndRemovals(1, 1);
    clearAllocateRequestLists();

    // 2. empty request
    this.relayer.allocate(getAllocateRequest());
    assertAsksAndReleases(0, 0);
    assertBlacklistAdditionsAndRemovals(0, 0);
    clearAllocateRequestLists();

    // Set RM restart and failover flag
    this.mockAMS.setFailoverFlag();

    // More requests
    this.blacklistAdditions.add("node2");
    this.releases.add(c2);
    this.relayer.allocate(getAllocateRequest());

    // verify pending requests are fully re-sent
    assertAsksAndReleases(3, 2);
    assertBlacklistAdditionsAndRemovals(2, 0);
    clearAllocateRequestLists();
  }

  @Test
  public void testResponseIdResync() throws YarnException, IOException {
    this.responseId = 10;

    AllocateResponse response = this.relayer.allocate(getAllocateRequest());
    Assert.assertEquals(this.responseId + 1, response.getResponseId());

    int expected = 5;
    this.mockAMS.setResponseIdReset(expected);

    try {
      this.relayer.allocate(getAllocateRequest());
      Assert.fail("Expecting exception from RM");
    } catch (InvalidApplicationMasterRequestException e) {
      // Expected exception
    }

    // Verify that the responseId is overridden
    response = this.relayer.allocate(getAllocateRequest());
    Assert.assertEquals(expected + 1, response.getResponseId());

    // Verify it is no longer overriden
    this.responseId = response.getResponseId();
    response = this.relayer.allocate(getAllocateRequest());
    Assert.assertEquals(this.responseId + 1, response.getResponseId());
  }

  @Test
  public void testConcurrentReregister() throws YarnException, IOException {

    // Set RM restart and failover flag
    this.mockAMS.setFailoverFlag();

    this.mockAMS.setThrowAlreadyRegister();

    relayer.finishApplicationMaster(null);
  }
}
