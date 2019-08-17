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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.resource.TestResourceProfiles;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Test Capacity Scheduler with multiple resource types.
 */
public class TestCapacitySchedulerWithMultiResourceTypes {
  private static String RESOURCE_1 = "res1";

  @Test
  public void testMaximumAllocationRefreshWithMultipleResourceTypes() throws Exception {

    // Initialize resource map
    Map<String, ResourceInformation> riMap = new HashMap<>();

    // Initialize mandatory resources
    ResourceInformation memory = ResourceInformation.newInstance(
        ResourceInformation.MEMORY_MB.getName(),
        ResourceInformation.MEMORY_MB.getUnits(),
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    ResourceInformation vcores = ResourceInformation.newInstance(
        ResourceInformation.VCORES.getName(),
        ResourceInformation.VCORES.getUnits(),
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
    riMap.put(ResourceInformation.MEMORY_URI, memory);
    riMap.put(ResourceInformation.VCORES_URI, vcores);
    riMap.put(RESOURCE_1, ResourceInformation.newInstance(RESOURCE_1, "", 0,
        ResourceTypes.COUNTABLE, 0, 3333L));

    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);

    CapacitySchedulerConfiguration csconf =
        new CapacitySchedulerConfiguration();
    csconf.setMaximumApplicationMasterResourcePerQueuePercent("root", 100.0f);
    csconf.setMaximumAMResourcePercentPerPartition("root", "", 100.0f);
    csconf.setMaximumApplicationMasterResourcePerQueuePercent("root.default",
        100.0f);
    csconf.setMaximumAMResourcePercentPerPartition("root.default", "", 100.0f);
    csconf.setResourceComparator(DominantResourceCalculator.class);
    csconf.set(YarnConfiguration.RESOURCE_TYPES, RESOURCE_1);
    csconf.setInt(YarnConfiguration.RESOURCE_TYPES + "." + RESOURCE_1
        + ".maximum-allocation", 3333);

    YarnConfiguration conf = new YarnConfiguration(csconf);
    // Don't reset resource types since we have already configured resource
    // types
    conf.setBoolean(TestResourceProfiles.TEST_CONF_RESET_RESOURCE_TYPES, false);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    MockRM rm = new MockRM(conf);
    rm.start();

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    Assert.assertEquals(3333L,
        cs.getMaximumResourceCapability().getResourceValue(RESOURCE_1));
    Assert.assertEquals(3333L,
        cs.getMaximumAllocation().getResourceValue(RESOURCE_1));
    Assert.assertEquals(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumResourceCapability()
            .getResourceValue(ResourceInformation.MEMORY_URI));
    Assert.assertEquals(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumAllocation()
            .getResourceValue(ResourceInformation.MEMORY_URI));
    Assert.assertEquals(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        cs.getMaximumResourceCapability()
            .getResourceValue(ResourceInformation.VCORES_URI));
    Assert.assertEquals(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        cs.getMaximumAllocation()
            .getResourceValue(ResourceInformation.VCORES_URI));

    // Set RES_1 to 3332 (less than 3333) and refresh CS, failures expected.
    csconf.set(YarnConfiguration.RESOURCE_TYPES, RESOURCE_1);
    csconf.setInt(YarnConfiguration.RESOURCE_TYPES + "." + RESOURCE_1
        + ".maximum-allocation", 3332);

    boolean exception = false;
    try {
      cs.reinitialize(csconf, rm.getRMContext());
    } catch (IOException e) {
      exception = true;
    }

    Assert.assertTrue("Should have exception in CS", exception);

    // Maximum allocation won't be updated
    Assert.assertEquals(3333L,
        cs.getMaximumResourceCapability().getResourceValue(RESOURCE_1));
    Assert.assertEquals(3333L,
        cs.getMaximumAllocation().getResourceValue(RESOURCE_1));
    Assert.assertEquals(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumResourceCapability()
            .getResourceValue(ResourceInformation.MEMORY_URI));
    Assert.assertEquals(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumAllocation()
            .getResourceValue(ResourceInformation.MEMORY_URI));
    Assert.assertEquals(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        cs.getMaximumResourceCapability()
            .getResourceValue(ResourceInformation.VCORES_URI));
    Assert.assertEquals(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        cs.getMaximumAllocation()
            .getResourceValue(ResourceInformation.VCORES_URI));

    // Set RES_1 to 3334 and refresh CS, should success
    csconf.set(YarnConfiguration.RESOURCE_TYPES, RESOURCE_1);
    csconf.setInt(YarnConfiguration.RESOURCE_TYPES + "." + RESOURCE_1
        + ".maximum-allocation", 3334);
    cs.reinitialize(csconf, rm.getRMContext());

    // Maximum allocation will be updated
    Assert.assertEquals(3334,
        cs.getMaximumResourceCapability().getResourceValue(RESOURCE_1));

    // Since we haven't updated the real configuration of ResourceUtils,
    // cs.getMaximumAllocation won't be updated.
    Assert.assertEquals(3333,
        cs.getMaximumAllocation().getResourceValue(RESOURCE_1));
    Assert.assertEquals(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumResourceCapability()
            .getResourceValue(ResourceInformation.MEMORY_URI));
    Assert.assertEquals(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        cs.getMaximumAllocation()
            .getResourceValue(ResourceInformation.MEMORY_URI));
    Assert.assertEquals(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        cs.getMaximumResourceCapability()
            .getResourceValue(ResourceInformation.VCORES_URI));
    Assert.assertEquals(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        cs.getMaximumAllocation()
            .getResourceValue(ResourceInformation.VCORES_URI));

    rm.close();
  }

  @Test
  public void testDefaultResourceCalculatorWithThirdResourceTypes() throws Exception {

    CapacitySchedulerConfiguration csconf =
        new CapacitySchedulerConfiguration();
    csconf.setResourceComparator(DefaultResourceCalculator.class);

    YarnConfiguration conf = new YarnConfiguration(csconf);

    String[] res1 = {"resource1", "M"};
    String[] res2 = {"resource2", "G"};
    String[] res3 = {"resource3", "H"};

    String[][] test = {res1, res2, res3};

    String resSt = "";
    for (String[] resources : test) {
      resSt += (resources[0] + ",");
    }
    resSt = resSt.substring(0, resSt.length() - 1);
    conf.set(YarnConfiguration.RESOURCE_TYPES, resSt);

    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    boolean exception = false;
    try {
      MockRM rm = new MockRM(conf);
    } catch (YarnRuntimeException e) {
      exception = true;
    }

    Assert.assertTrue("Should have exception in CS", exception);
  }
}
