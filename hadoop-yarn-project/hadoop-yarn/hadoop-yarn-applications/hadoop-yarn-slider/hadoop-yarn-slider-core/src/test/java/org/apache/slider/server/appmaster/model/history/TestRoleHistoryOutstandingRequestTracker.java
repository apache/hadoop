/*
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

package org.apache.slider.server.appmaster.model.history;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Component;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.providers.PlacementPolicy;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockAppState;
import org.apache.slider.server.appmaster.model.mock.MockContainer;
import org.apache.slider.server.appmaster.model.mock.MockNodeId;
import org.apache.slider.server.appmaster.model.mock.MockPriority;
import org.apache.slider.server.appmaster.model.mock.MockResource;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.operations.CancelSingleRequest;
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation;
import org.apache.slider.server.appmaster.state.ContainerAllocationOutcome;
import org.apache.slider.server.appmaster.state.ContainerAllocationResults;
import org.apache.slider.server.appmaster.state.ContainerPriority;
import org.apache.slider.server.appmaster.state.NodeInstance;
import org.apache.slider.server.appmaster.state.OutstandingRequest;
import org.apache.slider.server.appmaster.state.OutstandingRequestTracker;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test outstanding request tracker.
 */
public class TestRoleHistoryOutstandingRequestTracker extends
    BaseMockAppStateTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRoleHistoryOutstandingRequestTracker.class);

  public static final String WORKERS_LABEL = "workers";
  private NodeInstance host1 = new NodeInstance("host1", 3);
  private NodeInstance host2 = new NodeInstance("host2", 3);
  private MockResource resource = factory.newResource(48, 1);

  private OutstandingRequestTracker tracker = new OutstandingRequestTracker();

  public static final String WORKER = "worker";

  @Override
  public Application buildApplication() {
    Application application = super.buildApplication();
    Component component = new Component().name("worker").numberOfContainers(0L);
    component.getConfiguration().setProperty(ResourceKeys.YARN_LABEL_EXPRESSION,
        WORKERS_LABEL);
    application.getComponents().add(component);
    return application;
  }

  //@Test
  public void testAddRetrieveEntry() throws Throwable {
    OutstandingRequest request = tracker.newRequest(host1, 0);
    assertEquals(tracker.lookupPlacedRequest(0, "host1"), request);
    assertEquals(tracker.removePlacedRequest(request), request);
    assertNull(tracker.lookupPlacedRequest(0, "host1"));
  }

  //@Test
  public void testAddCompleteEntry() throws Throwable {
    OutstandingRequest req1 = tracker.newRequest(host1, 0);
    req1.buildContainerRequest(resource, getRole0Status(), 0);

    tracker.newRequest(host2, 0).buildContainerRequest(resource,
        getRole0Status(), 0);
    tracker.newRequest(host1, 1).buildContainerRequest(resource,
        getRole0Status(), 0);

    ContainerAllocationResults allocation = tracker.onContainerAllocated(1,
        "host1", null);
    assertEquals(allocation.outcome, ContainerAllocationOutcome.Placed);
    assertTrue(allocation.operations.get(0) instanceof CancelSingleRequest);

    assertNull(tracker.lookupPlacedRequest(1, "host1"));
    assertNotNull(tracker.lookupPlacedRequest(0, "host1"));
  }

  //@Test
  public void testResetOpenRequests() throws Throwable {
    OutstandingRequest req1 = tracker.newRequest(null, 0);
    assertFalse(req1.isLocated());
    tracker.newRequest(host1, 0);
    List<OutstandingRequest> openRequests = tracker.listOpenRequests();
    assertEquals(1, openRequests.size());
    tracker.resetOutstandingRequests(0);
    assertTrue(tracker.listOpenRequests().isEmpty());
    assertTrue(tracker.listPlacedRequests().isEmpty());
  }

  //@Test
  public void testRemoveOpenRequestUnissued() throws Throwable {
    OutstandingRequest req1 = tracker.newRequest(null, 0);
    req1.buildContainerRequest(resource, getRole0Status(), 0);
    assertEquals(1, tracker.listOpenRequests().size());
    MockContainer c1 = factory.newContainer(null, new MockPriority(0));
    c1.setResource(resource);

    ContainerAllocationResults allocation =
        tracker.onContainerAllocated(0, "host1", c1);
    ContainerAllocationOutcome outcome = allocation.outcome;
    assertEquals(outcome, ContainerAllocationOutcome.Unallocated);
    assertTrue(allocation.operations.isEmpty());
    assertEquals(1, tracker.listOpenRequests().size());
  }

  //@Test
  public void testIssuedOpenRequest() throws Throwable {
    OutstandingRequest req1 = tracker.newRequest(null, 0);
    req1.buildContainerRequest(resource, getRole0Status(), 0);
    assertEquals(1, tracker.listOpenRequests().size());

    int pri = ContainerPriority.buildPriority(0, false);
    assertTrue(pri > 0);
    MockNodeId nodeId = factory.newNodeId("hostname-1");
    MockContainer c1 = factory.newContainer(nodeId, new MockPriority(pri));

    c1.setResource(resource);

    ContainerRequest issued = req1.getIssuedRequest();
    assertEquals(issued.getCapability(), resource);
    assertEquals(issued.getPriority().getPriority(), c1.getPriority()
        .getPriority());
    assertTrue(req1.resourceRequirementsMatch(resource));

    ContainerAllocationResults allocation =
        tracker.onContainerAllocated(0, nodeId.getHost(), c1);
    assertEquals(0, tracker.listOpenRequests().size());
    assertTrue(allocation.operations.get(0) instanceof CancelSingleRequest);

    assertEquals(allocation.outcome, ContainerAllocationOutcome.Open);
    assertEquals(allocation.origin, req1);
  }

  //@Test
  public void testResetEntries() throws Throwable {
    tracker.newRequest(host1, 0);
    tracker.newRequest(host2, 0);
    tracker.newRequest(host1, 1);
    List<NodeInstance> canceled = tracker.resetOutstandingRequests(0);
    assertEquals(2, canceled.size());
    assertTrue(canceled.contains(host1));
    assertTrue(canceled.contains(host2));
    assertNotNull(tracker.lookupPlacedRequest(1, "host1"));
    assertNull(tracker.lookupPlacedRequest(0, "host1"));
    canceled = tracker.resetOutstandingRequests(0);
    assertEquals(0, canceled.size());
    assertEquals(1, tracker.resetOutstandingRequests(1).size());
  }

  //@Test
  public void testEscalation() throws Throwable {
    // first request: default placement
    assertEquals(getRole0Status().getPlacementPolicy(), PlacementPolicy
        .DEFAULT);
    Resource res0 = newResource(getRole0Status());
    OutstandingRequest outstanding0 = tracker.newRequest(host1,
        getRole0Status().getKey());
    ContainerRequest initialRequest =
        outstanding0.buildContainerRequest(res0, getRole0Status(), 0);
    assertNotNull(outstanding0.getIssuedRequest());
    assertTrue(outstanding0.isLocated());
    assertFalse(outstanding0.isEscalated());
    assertFalse(initialRequest.getRelaxLocality());
    assertEquals(1, tracker.listPlacedRequests().size());

    // second. This one doesn't get launched. This is to verify that the
    // escalation process skips entries which are in the list but have not
    // been issued, which can be a race condition between request issuance &
    // escalation.
    // (not one observed outside test authoring, but retained for completeness)
    Resource res2 = newResource(getRole2Status());
    OutstandingRequest outstanding2 = tracker.newRequest(host1,
        getRole2Status().getKey());

    // simulate some time escalation of role 1 MUST now be triggered
    long interval = getRole0Status().getPlacementTimeoutSeconds() * 1000 + 500;
    long now = interval;
    final List<AbstractRMOperation> escalations = tracker
        .escalateOutstandingRequests(now);

    assertTrue(outstanding0.isEscalated());
    assertFalse(outstanding2.isEscalated());

    // two entries
    assertEquals(2, escalations.size());
    AbstractRMOperation e1 = escalations.get(0);
    assertTrue(e1 instanceof CancelSingleRequest);
    final CancelSingleRequest cancel = (CancelSingleRequest) e1;
    assertEquals(initialRequest, cancel.getRequest());
    AbstractRMOperation e2 = escalations.get(1);
    assertTrue(e2 instanceof ContainerRequestOperation);
    ContainerRequestOperation escRequest = (ContainerRequestOperation) e2;
    assertTrue(escRequest.getRequest().getRelaxLocality());

    // build that second request from an anti-affine entry
    // these get placed as well
    now += interval;
    ContainerRequest containerReq2 =
        outstanding2.buildContainerRequest(res2, getRole2Status(), now);
    // escalate a little bit more
    final List<AbstractRMOperation> escalations2 = tracker
        .escalateOutstandingRequests(now);
    // and expect no new entries
    assertEquals(0, escalations2.size());

    // go past the role2 timeout
    now += getRole2Status().getPlacementTimeoutSeconds() * 1000 + 500;
    // escalate a little bit more
    final List<AbstractRMOperation> escalations3 = tracker
        .escalateOutstandingRequests(now);
    // and expect another escalation
    assertEquals(2, escalations3.size());
    assertTrue(outstanding2.isEscalated());

    // finally add a strict entry to the mix
    Resource res3 = newResource(getRole1Status());
    OutstandingRequest outstanding3 = tracker.newRequest(host1,
        getRole1Status().getKey());

    final ProviderRole providerRole1 = getRole1Status().getProviderRole();
    assertEquals(providerRole1.placementPolicy, PlacementPolicy.STRICT);
    now += interval;
    assertFalse(outstanding3.mayEscalate());
    final List<AbstractRMOperation> escalations4 = tracker
        .escalateOutstandingRequests(now);
    assertTrue(escalations4.isEmpty());

  }

  /**
   * If the placement does include a label, the initial request must
   * <i>not</i> include it.
   * The escalation request will contain the label, while
   * leaving out the node list.
   * retains the node list, but sets relaxLocality==true
   * @throws Throwable
   */
  //@Test
  public void testRequestLabelledPlacement() throws Throwable {
    NodeInstance ni = new NodeInstance("host1", 0);
    OutstandingRequest req1 = tracker.newRequest(ni, 0);
    Resource res0 = factory.newResource(48, 1);

    RoleStatus workerRole = lookupRole(WORKER);
    // initial request
    ContainerRequest yarnRequest =
        req1.buildContainerRequest(res0, workerRole, 0);
    assertEquals(req1.label, WORKERS_LABEL);

    assertNull(yarnRequest.getNodeLabelExpression());
    assertFalse(yarnRequest.getRelaxLocality());
    // escalation
    ContainerRequest yarnRequest2 = req1.escalate();
    assertNull(yarnRequest2.getNodes());
    assertTrue(yarnRequest2.getRelaxLocality());
    assertEquals(yarnRequest2.getNodeLabelExpression(), WORKERS_LABEL);
  }

  /**
   * If the placement doesnt include a label, then the escalation request
   * retains the node list, but sets relaxLocality==true.
   * @throws Throwable
   */
  //@Test
  public void testRequestUnlabelledPlacement() throws Throwable {
    NodeInstance ni = new NodeInstance("host1", 0);
    OutstandingRequest req1 = tracker.newRequest(ni, 0);
    Resource res0 = factory.newResource(48, 1);

    // initial request
    ContainerRequest yarnRequest = req1.buildContainerRequest(res0,
        getRole0Status(), 0);
    assertNotNull(yarnRequest.getNodes());
    assertTrue(SliderUtils.isUnset(yarnRequest.getNodeLabelExpression()));
    assertFalse(yarnRequest.getRelaxLocality());
    ContainerRequest yarnRequest2 = req1.escalate();
    assertNotNull(yarnRequest2.getNodes());
    assertTrue(yarnRequest2.getRelaxLocality());
  }

  //@Test(expected = IllegalArgumentException.class)
  public void testAARequestNoNodes() throws Throwable {
    tracker.newAARequest(getRole0Status().getKey(), new ArrayList<>(), "");
  }

  //@Test
  public void testAARequest() throws Throwable {
    int role0 = getRole0Status().getKey();
    OutstandingRequest request = tracker.newAARequest(role0, Arrays
        .asList(host1), "");
    assertEquals(host1.hostname, request.hostname);
    assertFalse(request.isLocated());
  }

  //@Test
  public void testAARequestPair() throws Throwable {
    int role0 = getRole0Status().getKey();
    OutstandingRequest request = tracker.newAARequest(role0, Arrays.asList(
        host1, host2), "");
    assertEquals(host1.hostname, request.hostname);
    assertFalse(request.isLocated());
    ContainerRequest yarnRequest = request.buildContainerRequest(
        getRole0Status().copyResourceRequirements(new MockResource(0, 0)),
        getRole0Status(),
        0);
    assertFalse(yarnRequest.getRelaxLocality());
    assertFalse(request.mayEscalate());

    assertEquals(2, yarnRequest.getNodes().size());
  }

  //@Test
  public void testBuildResourceRequirements() throws Throwable {
    // Store original values
    Application application = appState.getClusterStatus();
    Component role0 = application.getComponent(getRole0Status().getName());
    String origMem = role0.getResource().getMemory();
    Integer origVcores = role0.getResource().getCpus();

    // Resource values to be used for this test
    int testMem = 32768;
    int testVcores = 2;
    role0.resource(new org.apache.slider.api.resource.Resource().memory(Integer
        .toString(testMem)).cpus(testVcores));

    // Test normalization disabled
    LOG.info("Test normalization: disabled");
    role0.getConfiguration().setProperty(
        ResourceKeys.YARN_RESOURCE_NORMALIZATION_ENABLED, "false");
    MockResource requestedRes = new MockResource(testMem, testVcores);
    MockResource expectedRes = new MockResource(testMem, testVcores);
    LOG.info("Resource requested: {}", requestedRes);
    Resource resFinal = appState.buildResourceRequirements(getRole0Status());
    LOG.info("Resource actual: {}", resFinal);
    assertTrue(Resources.equals(expectedRes, resFinal));

    // Test normalization enabled
    LOG.info("Test normalization: enabled");
    role0.getConfiguration().setProperty(
        ResourceKeys.YARN_RESOURCE_NORMALIZATION_ENABLED, "true");
    expectedRes = new MockResource(MockAppState.RM_MAX_RAM, testVcores);
    LOG.info("Resource requested: {}", requestedRes);
    resFinal = appState.buildResourceRequirements(getRole0Status());
    LOG.info("Resource actual: {}", resFinal);
    assertTrue(Resources.equals(expectedRes, resFinal));

    // revert resource configuration to original value
    role0.resource(new org.apache.slider.api.resource.Resource().memory(origMem)
        .cpus(origVcores));
  }

  public Resource newResource(RoleStatus r) {
    return appState.buildResourceRequirements(r);
  }
}
