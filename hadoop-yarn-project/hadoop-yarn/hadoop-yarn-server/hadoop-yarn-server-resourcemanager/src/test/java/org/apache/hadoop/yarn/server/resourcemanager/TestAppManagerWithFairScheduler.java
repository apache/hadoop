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

import static org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException.InvalidResourceType.GREATER_THEN_MAX_ALLOCATION;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Testing applications being retired from RM with fair scheduler.
 *
 */
public class TestAppManagerWithFairScheduler extends AppManagerTestBase{

  private static final String TEST_FOLDER = "test-queues";

  private static YarnConfiguration conf = new YarnConfiguration();

  @BeforeClass
  public static void setup() throws IOException {
    String allocFile =
        GenericTestUtils.getTestDir(TEST_FOLDER).getAbsolutePath();

    int queueMaxAllocation = 512;

    PrintWriter out = new PrintWriter(new FileWriter(allocFile));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println(" <queue name=\"queueA\">");
    out.println("  <maxContainerAllocation>" + queueMaxAllocation
        + " mb 1 vcores" + "</maxContainerAllocation>");
    out.println(" </queue>");
    out.println(" <queue name=\"queueB\">");
    out.println(" </queue>");
    out.println("</allocations>");
    out.close();

    conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
        ResourceScheduler.class);

    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, allocFile);
  }

  @AfterClass
  public static void teardown(){
    File allocFile = GenericTestUtils.getTestDir(TEST_FOLDER);
    allocFile.delete();
  }

  @Test
  public void testQueueSubmitWithHighQueueContainerSize()
      throws YarnException {

    ApplicationId appId = MockApps.newAppID(1);
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

    Resource resource = Resources.createResource(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);

    ApplicationSubmissionContext asContext =
        recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    asContext.setApplicationId(appId);
    asContext.setResource(resource);
    asContext.setPriority(Priority.newInstance(0));
    asContext.setAMContainerSpec(mockContainerLaunchContext(recordFactory));
    asContext.setQueue("queueA");
    QueueInfo mockDefaultQueueInfo = mock(QueueInfo.class);

    // Setup a PlacementManager returns a new queue
    PlacementManager placementMgr = mock(PlacementManager.class);
    doAnswer(new Answer<ApplicationPlacementContext>() {

      @Override
      public ApplicationPlacementContext answer(InvocationOnMock invocation)
          throws Throwable {
        return new ApplicationPlacementContext("queueA");
      }

    }).when(placementMgr).placeApplication(
        any(ApplicationSubmissionContext.class), matches("test1"));
    doAnswer(new Answer<ApplicationPlacementContext>() {

      @Override
      public ApplicationPlacementContext answer(InvocationOnMock invocation)
          throws Throwable {
        return new ApplicationPlacementContext("queueB");
      }

    }).when(placementMgr).placeApplication(
        any(ApplicationSubmissionContext.class), matches("test2"));

    MockRM newMockRM = new MockRM(conf);
    RMContext newMockRMContext = newMockRM.getRMContext();
    newMockRMContext.setQueuePlacementManager(placementMgr);
    ApplicationMasterService masterService = new ApplicationMasterService(
        newMockRMContext, newMockRMContext.getScheduler());

    TestRMAppManager newAppMonitor = new TestRMAppManager(newMockRMContext,
        new ClientToAMTokenSecretManagerInRM(), newMockRMContext.getScheduler(),
        masterService, new ApplicationACLsManager(conf), conf);

    // only user test has permission to submit to 'test' queue

    try {
      newAppMonitor.submitApplication(asContext, "test1");
      Assert.fail("Test should fail on too high allocation!");
    } catch (InvalidResourceRequestException e) {
      Assert.assertEquals(GREATER_THEN_MAX_ALLOCATION,
          e.getInvalidResourceType());
    }

    // Should not throw exception
    newAppMonitor.submitApplication(asContext, "test2");
  }

  private static ContainerLaunchContext mockContainerLaunchContext(
      RecordFactory recordFactory) {
    ContainerLaunchContext amContainer = recordFactory.newRecordInstance(
        ContainerLaunchContext.class);
    amContainer
        .setApplicationACLs(new HashMap<ApplicationAccessType, String>());
    return amContainer;
  }
}
