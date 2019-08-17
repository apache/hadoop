/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.util.resource.ResourceUtils.MAXIMUM_ALLOCATION;
import static org.apache.hadoop.yarn.util.resource.ResourceUtils.UNITS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFairSchedulerWithMultiResourceTypes
    extends FairSchedulerTestBase {

  private static final String CUSTOM_RESOURCE = "custom-resource";

  @Before
  public void setUp() throws IOException {
    scheduler = new FairScheduler();
    conf = createConfiguration();
    initResourceTypes(conf);
    // since this runs outside of the normal context we need to set one
    RMContext rmContext = mock(RMContext.class);
    PlacementManager placementManager = new PlacementManager();
    when(rmContext.getQueuePlacementManager()).thenReturn(placementManager);
    scheduler.setRMContext(rmContext);
  }

  @After
  public void tearDown() {
    if (scheduler != null) {
      scheduler.stop();
      scheduler = null;
    }
  }

  private Configuration initResourceTypes(Configuration conf) {
    Map<String, ResourceInformation> riMap = new HashMap<>();

    // Initialize mandatory resources
    ResourceInformation memory =
        ResourceInformation.newInstance(ResourceInformation.MEMORY_MB.getName(),
            ResourceInformation.MEMORY_MB.getUnits(),
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    ResourceInformation vcores =
        ResourceInformation.newInstance(ResourceInformation.VCORES.getName(),
            ResourceInformation.VCORES.getUnits(),
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
    riMap.put(ResourceInformation.MEMORY_URI, memory);
    riMap.put(ResourceInformation.VCORES_URI, vcores);
    riMap.put(CUSTOM_RESOURCE, ResourceInformation.newInstance(CUSTOM_RESOURCE,
        "", 0, ResourceTypes.COUNTABLE, 0, 3333L));

    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);

    return conf;
  }

  @Test
  public void testMaximumAllocationRefresh() throws IOException {
    conf.set(YarnConfiguration.RESOURCE_TYPES, CUSTOM_RESOURCE);
    conf.set(YarnConfiguration.RESOURCE_TYPES + "." + CUSTOM_RESOURCE + UNITS,
        "k");
    conf.setInt(YarnConfiguration.RESOURCE_TYPES + "." + CUSTOM_RESOURCE
        + MAXIMUM_ALLOCATION, 10000);
    conf.setInt(YarnConfiguration.RESOURCE_TYPES + "."
        + ResourceInformation.VCORES.getName() + MAXIMUM_ALLOCATION, 4);
    conf.setInt(
        YarnConfiguration.RESOURCE_TYPES + "."
            + ResourceInformation.MEMORY_MB.getName() + MAXIMUM_ALLOCATION,
        512);
    scheduler.init(conf);
    scheduler.reinitialize(conf, null);

    Resource maxAllowedAllocation =
        scheduler.getNodeTracker().getMaxAllowedAllocation();
    ResourceInformation customResource =
        maxAllowedAllocation.getResourceInformation(CUSTOM_RESOURCE);
    assertEquals(512, maxAllowedAllocation.getMemorySize());
    assertEquals(4, maxAllowedAllocation.getVirtualCores());
    assertEquals(10000, customResource.getValue());

    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RESOURCE_TYPES, CUSTOM_RESOURCE);
    conf.set(YarnConfiguration.RESOURCE_TYPES + "." + CUSTOM_RESOURCE + UNITS,
        "k");
    conf.setInt(YarnConfiguration.RESOURCE_TYPES + "." + CUSTOM_RESOURCE
        + MAXIMUM_ALLOCATION, 20000);
    conf.setInt(YarnConfiguration.RESOURCE_TYPES + "."
        + ResourceInformation.VCORES.getName() + MAXIMUM_ALLOCATION, 8);
    conf.setInt(
        YarnConfiguration.RESOURCE_TYPES + "."
            + ResourceInformation.MEMORY_MB.getName() + MAXIMUM_ALLOCATION,
        2048);
    scheduler.reinitialize(conf, null);

    maxAllowedAllocation = scheduler.getNodeTracker().getMaxAllowedAllocation();
    customResource =
        maxAllowedAllocation.getResourceInformation(CUSTOM_RESOURCE);
    assertEquals(2048, maxAllowedAllocation.getMemorySize());
    assertEquals(8, maxAllowedAllocation.getVirtualCores());
    assertEquals(20000, customResource.getValue());
  }

}
