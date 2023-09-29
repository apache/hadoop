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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.resourcetypes.ResourceTypesTestHelper;
import org.apache.hadoop.yarn.server.resourcemanager.resource.TestResourceProfiles;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link ApplicationMasterService} with {@link FairScheduler}.
 */

public class TestApplicationMasterServiceFair extends
    ApplicationMasterServiceTestBase {
  private static final String DEFAULT_QUEUE = "root.default";

  @Override
  protected YarnConfiguration createYarnConfig() {
    FairSchedulerConfiguration fsConf = new FairSchedulerConfiguration();
    YarnConfiguration yarnConf = new YarnConfiguration(fsConf);
    yarnConf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
        ResourceScheduler.class);
    return yarnConf;
  }

  @Override
  protected Resource getResourceUsageForQueue(ResourceManager rm,
      String queue) {
    FairScheduler fs = (FairScheduler) rm.getResourceScheduler();
    FSLeafQueue leafQueue =
        fs.getQueueManager().getLeafQueue(DEFAULT_QUEUE, false);
    return leafQueue.getResourceUsage();
  }

  @Override
  protected String getDefaultQueueName() {
    return DEFAULT_QUEUE;
  }

  @Test
  public void testRequestCapacityMinMaxAllocationWithDifferentUnits()
      throws Exception {
    Map<String, ResourceInformation> riMap = initializeMandatoryResources();
    ResourceInformation res1 =
        ResourceInformation.newInstance(CUSTOM_RES, "G", 0, 4);
    riMap.put(CUSTOM_RES, res1);

    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);

    final YarnConfiguration yarnConf = createYarnConfig();
    // Don't reset resource types since we have already configured resource
    // types
    yarnConf.setBoolean(TestResourceProfiles.TEST_CONF_RESET_RESOURCE_TYPES,
        false);
    yarnConf.setBoolean(YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED, false);

    MockRM rm = new MockRM(yarnConf);
    rm.start();

    MockNM nm1 = rm.registerNode("199.99.99.1:" + DEFAULT_PORT,
        ResourceTypesTestHelper.newResource(
            DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
            DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
            ImmutableMap.<String, String> builder()
                .put(CUSTOM_RES, "5G").build()));

    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue(DEFAULT_QUEUE)
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

    // Now request res_1, 500M < 5G so it should be allowed
    try {
      am1.allocate(Collections.singletonList(ResourceRequest.newBuilder()
          .capability(ResourceTypesTestHelper.newResource(4 * GB, 1,
              ImmutableMap.<String, String> builder()
                  .put(CUSTOM_RES, "500M")
                  .build()))
          .numContainers(1).resourceName("*").build()), null);
    } catch (InvalidResourceRequestException e) {
      fail(
          "Allocate request should be accepted but exception was thrown: " + e);
    }

    rm.close();
  }
}
