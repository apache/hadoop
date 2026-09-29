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
package org.apache.hadoop.yarn.server.scheduler;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Spy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * Test cases for OpportunisticContainerContext.
 */
public class TestOpportunisticContainerContext {

  @Spy
  private OpportunisticContainerContext opportunisticContainerContext;
  private Map<Resource, OpportunisticContainerAllocator.EnrichedResourceRequest> reqMap =
          new HashMap<>();
  private TreeMap<SchedulerRequestKey,
          Map<Resource, OpportunisticContainerAllocator.EnrichedResourceRequest>> outstandingOpReqs;

  @BeforeEach
  public void setUp() {
    opportunisticContainerContext = spy(new OpportunisticContainerContext());
    outstandingOpReqs = new TreeMap<>();
  }

  /**
   * Resource Request - {
   * Location = ANY
   * No of container != 0
   * }.
   */
  @Test
  public void testAddToOutstandingReqsWithANYRequest() {
    ResourceRequest request = getResourceRequest(ResourceRequest.ANY, 1);
    List<ResourceRequest> resourceRequestList = new ArrayList<>();
    resourceRequestList.add(request);
    opportunisticContainerContext.addToOutstandingReqs(resourceRequestList);
    assertEquals(opportunisticContainerContext.getOutstandingOpReqs().size(), 1);
  }

  /**
   * Resource Request - {
   * Location != ANY
   * No of container = 0
   * }.
   */
  @Test
  public void testAddToOutstandingReqsWithZeroContainer() {
    ResourceRequest request = getResourceRequest("resource", 0);
    createOutstandingOpReqs(request, getResource());
    doReturn(outstandingOpReqs).when(opportunisticContainerContext)
            .getOutstandingOpReqs();
    List<ResourceRequest> resourceRequestList = new ArrayList<>();
    resourceRequestList.add(request);
    opportunisticContainerContext.addToOutstandingReqs(resourceRequestList);
    assertEquals(opportunisticContainerContext.getOutstandingOpReqs().size(), 1);
  }

  /**
   * Resource Request - [
   * {Location != ANY, No of Container = 0}
   * {Location = ANY, No of Container = 0}
   * ].
   */
  @Test
  public void testAddToOutstandingReqsWithZeroContainerAndMultipleSchedulerKey() {
    ResourceRequest req1 = getResourceRequest("resource", 0);
    ResourceRequest req2 = getResourceRequest(ResourceRequest.ANY, 0);
    createOutstandingOpReqs(req1, getResource());
    createOutstandingOpReqs(req2, getResource());
    doReturn(outstandingOpReqs).when(opportunisticContainerContext)
            .getOutstandingOpReqs();
    List<ResourceRequest> resourceRequestList = new ArrayList<>();
    resourceRequestList.add(req1);
    resourceRequestList.add(req2);
    opportunisticContainerContext.addToOutstandingReqs(resourceRequestList);
    assertEquals(opportunisticContainerContext.getOutstandingOpReqs().size(), 1);
  }

  /**
   * Resource Request - [
   * {Location != ANY, No of Container = 0}
   * {Location = ANY, No of Container != 0}
   * ].
   */
  @Test
  public void testAddToOutstandingReqsWithMultipleSchedulerKey() {
    ResourceRequest req1 = getResourceRequest("resource", 0);
    ResourceRequest req2 = getResourceRequest(ResourceRequest.ANY, 1);
    createOutstandingOpReqs(req1, getResource());
    createOutstandingOpReqs(req2, getResource());
    doReturn(outstandingOpReqs).when(opportunisticContainerContext)
            .getOutstandingOpReqs();
    List<ResourceRequest> resourceRequestList = new ArrayList<>();
    resourceRequestList.add(req1);
    resourceRequestList.add(req2);
    opportunisticContainerContext.addToOutstandingReqs(resourceRequestList);
    assertEquals(opportunisticContainerContext.getOutstandingOpReqs().size(), 1);
  }

  /**
   * Resource Request - {
   * Location != ANY
   * No of container = 0
   * Capability = NULL
   * }.
   */
  @Test
  public void testAddToOutstandingReqsWithZeroContainerAndNullCapability() {
    ResourceRequest request = getResourceRequestWithoutCapability();
    createOutstandingOpReqs(request, getResource());
    doReturn(outstandingOpReqs).when(opportunisticContainerContext)
            .getOutstandingOpReqs();
    List<ResourceRequest> resourceRequestList = new ArrayList<>();
    resourceRequestList.add(request);
    opportunisticContainerContext.addToOutstandingReqs(resourceRequestList);
    assertEquals(opportunisticContainerContext.getOutstandingOpReqs().size(), 1);
  }

  /**
   * Resource Request - {
   * Location != ANY
   * No of container = 0
   * Req map is NULL
   * }.
   */
  @Test
  public void testAddToOutstandingReqsWithEmptyReqMap() {
    ResourceRequest request = getResourceRequest("resource", 0);
    doReturn(new TreeMap<>()).when(opportunisticContainerContext)
            .getOutstandingOpReqs();
    List<ResourceRequest> resourceRequestList = new ArrayList<>();
    resourceRequestList.add(request);
    opportunisticContainerContext.addToOutstandingReqs(resourceRequestList);
    assertEquals(opportunisticContainerContext.getOutstandingOpReqs().size(), 0);
  }

  private void createOutstandingOpReqs(ResourceRequest req, Resource resource) {
    SchedulerRequestKey schedulerRequestKey = SchedulerRequestKey.create(req);
    reqMap.put(resource, new OpportunisticContainerAllocator.EnrichedResourceRequest(req));
    outstandingOpReqs.put(schedulerRequestKey, reqMap);
  }

  private ResourceRequest getResourceRequest(String resourceName, int numContainer) {
    return ResourceRequest.newBuilder().resourceName(resourceName).numContainers(numContainer)
            .allocationRequestId(1).priority(Priority.newInstance(1)).capability(getResource())
            .build();
  }

  private ResourceRequest getResourceRequestWithoutCapability() {
    return ResourceRequest.newBuilder().resourceName("resource").numContainers(0)
            .allocationRequestId(1).priority(Priority.newInstance(1)).build();
  }

  private Resource getResource() {
    return Resource.newInstance(1024, 2);
  }
}
