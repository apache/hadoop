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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link DefaultAMSProcessor}.
 */
public class TestDefaultAMSProcessor {

  /**
   * Simulates transient YARN RM over-allocation by returning a negative
   * resource limit from the scheduler.
   */
  public static class NegativeHeadroomScheduler extends FifoScheduler {
    @Override
    public Allocation allocate(ApplicationAttemptId applicationAttemptId,
        List<ResourceRequest> ask,
        List<SchedulingRequest> schedulingRequests,
        List<ContainerId> release,
        List<String> blacklistAdditions,
        List<String> blacklistRemovals,
        ContainerUpdates updateRequests) {
      Allocation allocation = super.allocate(applicationAttemptId, ask,
          schedulingRequests, release, blacklistAdditions,
          blacklistRemovals, updateRequests);
      allocation.setResourceLimit(Resource.newInstance(-1024, -4));
      return allocation;
    }
  }

  /**
   * Simulates a scheduler that does not compute a resource limit,
   * returning null from {@link Allocation#getResourceLimit()}.
   */
  public static class NullHeadroomScheduler extends FifoScheduler {
    @Override
    public Allocation allocate(ApplicationAttemptId applicationAttemptId,
        List<ResourceRequest> ask,
        List<SchedulingRequest> schedulingRequests,
        List<ContainerId> release,
        List<String> blacklistAdditions,
        List<String> blacklistRemovals,
        ContainerUpdates updateRequests) {
      Allocation allocation = super.allocate(applicationAttemptId, ask,
          schedulingRequests, release, blacklistAdditions,
          blacklistRemovals, updateRequests);
      allocation.setResourceLimit(null);
      return allocation;
    }
  }

  @Test
  @Timeout(60)
  public void testAvailableResourcesClampedToNonNegative() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER,
        NegativeHeadroomScheduler.class, ResourceScheduler.class);

    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM nm = rm.registerNode("127.0.0.1:1234", 8 * 1024, 8);

    RMApp app = MockRMAppSubmitter.submitWithMemory(1024, rm);
    MockAM am = MockRM.launchAndRegisterAM(app, rm, nm);

    AllocateResponse response = am.doHeartbeat();
    Resource available = response.getAvailableResources();

    assertTrue(available.getMemorySize() >= 0,
        "Available memory must be non-negative, but was: "
            + available.getMemorySize());
    assertTrue(available.getVirtualCores() >= 0,
        "Available vCores must be non-negative, but was: "
            + available.getVirtualCores());

    rm.stop();
  }

  @Test
  @Timeout(60)
  public void testNullResourceLimitDoesNotThrow() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER,
        NullHeadroomScheduler.class, ResourceScheduler.class);

    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM nm = rm.registerNode("127.0.0.1:1234", 8 * 1024, 8);

    RMApp app = MockRMAppSubmitter.submitWithMemory(1024, rm);
    MockAM am = MockRM.launchAndRegisterAM(app, rm, nm);

    AllocateResponse response = am.doHeartbeat();

    assertNull(response.getAvailableResources(),
        "Available resources must remain null when the scheduler "
            + "returns a null resource limit");

    rm.stop();
  }
}
