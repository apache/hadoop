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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link AllocationBasedResourceUtilizationTracker} class.
 */
public class TestAllocationBasedResourceUtilizationTracker {

  private ContainerScheduler mockContainerScheduler;

  @Before
  public void setup() {
    mockContainerScheduler = mock(ContainerScheduler.class);
    ContainersMonitor containersMonitor =
        new ContainersMonitorImpl(mock(ContainerExecutor.class),
            mock(AsyncDispatcher.class), mock(Context.class));
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.NM_PMEM_MB, 1024);
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, true);
    conf.setFloat(YarnConfiguration.NM_VMEM_PMEM_RATIO, 2.0f);
    conf.setInt(YarnConfiguration.NM_VCORES, 8);
    containersMonitor.init(conf);
    when(mockContainerScheduler.getContainersMonitor())
        .thenReturn(containersMonitor);
  }

  /**
   * Node has capacity for 1024 MB and 8 cores. Saturate the node. When full the
   * hasResourceAvailable should return false.
   */
  @Test
  public void testHasResourcesAvailable() {
    AllocationBasedResourceUtilizationTracker tracker =
        new AllocationBasedResourceUtilizationTracker(mockContainerScheduler);
    Container testContainer = mock(Container.class);
    when(testContainer.getResource()).thenReturn(Resource.newInstance(512, 4));
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(tracker.hasResourcesAvailable(testContainer));
      tracker.addContainerResources(testContainer);
    }
    Assert.assertFalse(tracker.hasResourcesAvailable(testContainer));
  }
}
