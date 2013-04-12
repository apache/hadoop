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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceRequestPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;

public class TestSchedulerUtils {

  @Test (timeout = 30000)
  public void testNormalizeRequest() {
    ResourceCalculator resourceCalculator = new DefaultResourceCalculator();
    
    final int minMemory = 1024;
    final int maxMemory = 8192;
    Resource minResource = Resources.createResource(minMemory, 0);
    Resource maxResource = Resources.createResource(maxMemory, 0);
    
    ResourceRequest ask = new ResourceRequestPBImpl();

    // case negative memory
    ask.setCapability(Resources.createResource(-1024));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource,
        maxResource);
    assertEquals(minMemory, ask.getCapability().getMemory());

    // case zero memory
    ask.setCapability(Resources.createResource(0));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource,
        maxResource);
    assertEquals(minMemory, ask.getCapability().getMemory());

    // case memory is a multiple of minMemory
    ask.setCapability(Resources.createResource(2 * minMemory));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource,
        maxResource);
    assertEquals(2 * minMemory, ask.getCapability().getMemory());

    // case memory is not a multiple of minMemory
    ask.setCapability(Resources.createResource(minMemory + 10));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource,
        maxResource);
    assertEquals(2 * minMemory, ask.getCapability().getMemory());

    // case memory is equal to max allowed
    ask.setCapability(Resources.createResource(maxMemory));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource,
        maxResource);
    assertEquals(maxMemory, ask.getCapability().getMemory());

    // case memory is just less than max
    ask.setCapability(Resources.createResource(maxMemory - 10));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource,
        maxResource);
    assertEquals(maxMemory, ask.getCapability().getMemory());

    // max is not a multiple of min
    maxResource = Resources.createResource(maxMemory - 10, 0);
    ask.setCapability(Resources.createResource(maxMemory - 100));
    // multiple of minMemory > maxMemory, then reduce to maxMemory
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource,
        maxResource);
    assertEquals(maxResource.getMemory(), ask.getCapability().getMemory());

    // ask is more than max
    maxResource = Resources.createResource(maxMemory, 0);
    ask.setCapability(Resources.createResource(maxMemory + 100));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource,
        maxResource);
    assertEquals(maxResource.getMemory(), ask.getCapability().getMemory());
  }
  
  @Test (timeout = 30000)
  public void testNormalizeRequestWithDominantResourceCalculator() {
    ResourceCalculator resourceCalculator = new DominantResourceCalculator();
    
    Resource minResource = Resources.createResource(1024, 1);
    Resource maxResource = Resources.createResource(10240, 10);
    Resource clusterResource = Resources.createResource(10 * 1024, 10);
    
    ResourceRequest ask = new ResourceRequestPBImpl();

    // case negative memory/vcores
    ask.setCapability(Resources.createResource(-1024, -1));
    SchedulerUtils.normalizeRequest(
        ask, resourceCalculator, clusterResource, minResource, maxResource);
    assertEquals(minResource, ask.getCapability());

    // case zero memory/vcores
    ask.setCapability(Resources.createResource(0, 0));
    SchedulerUtils.normalizeRequest(
        ask, resourceCalculator, clusterResource, minResource, maxResource);
    assertEquals(minResource, ask.getCapability());
    assertEquals(1, ask.getCapability().getVirtualCores());
    assertEquals(1024, ask.getCapability().getMemory());

    // case non-zero memory & zero cores
    ask.setCapability(Resources.createResource(1536, 0));
    SchedulerUtils.normalizeRequest(
        ask, resourceCalculator, clusterResource, minResource, maxResource);
    assertEquals(Resources.createResource(2048, 1), ask.getCapability());
    assertEquals(1, ask.getCapability().getVirtualCores());
    assertEquals(2048, ask.getCapability().getMemory());
  }

  @Test (timeout = 30000)
  public void testValidateResourceRequest() {
    Resource maxResource = Resources.createResource(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    // zero memory
    try {
      Resource resource = Resources.createResource(
          0,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      SchedulerUtils.validateResourceRequest(resReq, maxResource);
    } catch (InvalidResourceRequestException e) {
      fail("Zero memory should be accepted");
    }

    // zero vcores
    try {
      Resource resource = Resources.createResource(
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
          0);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      SchedulerUtils.validateResourceRequest(resReq, maxResource);
    } catch (InvalidResourceRequestException e) {
      fail("Zero vcores should be accepted");
    }

    // max memory
    try {
      Resource resource = Resources.createResource(
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      SchedulerUtils.validateResourceRequest(resReq, maxResource);
    } catch (InvalidResourceRequestException e) {
      fail("Max memory should be accepted");
    }

    // max vcores
    try {
      Resource resource = Resources.createResource(
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      SchedulerUtils.validateResourceRequest(resReq, maxResource);
    } catch (InvalidResourceRequestException e) {
      fail("Max vcores should not be accepted");
    }

    // negative memory
    try {
      Resource resource = Resources.createResource(
          -1,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      SchedulerUtils.validateResourceRequest(resReq, maxResource);
      fail("Negative memory should not be accepted");
    } catch (InvalidResourceRequestException e) {
      // expected
    }

    // negative vcores
    try {
      Resource resource = Resources.createResource(
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
          -1);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      SchedulerUtils.validateResourceRequest(resReq, maxResource);
      fail("Negative vcores should not be accepted");
    } catch (InvalidResourceRequestException e) {
      // expected
    }

    // more than max memory
    try {
      Resource resource = Resources.createResource(
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB + 1,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      SchedulerUtils.validateResourceRequest(resReq, maxResource);
      fail("More than max memory should not be accepted");
    } catch (InvalidResourceRequestException e) {
      // expected
    }

    // more than max vcores
    try {
      Resource resource = Resources.createResource(
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES
          + 1);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      SchedulerUtils.validateResourceRequest(resReq, maxResource);
      fail("More than max vcores should not be accepted");
    } catch (InvalidResourceRequestException e) {
      // expected
    }
  }

}
