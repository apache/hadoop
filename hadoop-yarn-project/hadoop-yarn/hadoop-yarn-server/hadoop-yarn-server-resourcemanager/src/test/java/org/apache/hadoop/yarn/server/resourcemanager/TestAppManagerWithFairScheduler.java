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

import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException.InvalidResourceType.GREATER_THEN_MAX_ALLOCATION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;

import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Testing RMAppManager application submission with fair scheduler.
 */
public class TestAppManagerWithFairScheduler extends AppManagerTestBase {

  private static final String TEST_FOLDER = "test-queues";

  private static YarnConfiguration conf = new YarnConfiguration();
  private PlacementManager placementMgr;
  private TestRMAppManager rmAppManager;
  private RMContext rmContext;
  private static String allocFileName =
      GenericTestUtils.getTestDir(TEST_FOLDER).getAbsolutePath();

  @Before
  public void setup() throws IOException {
    // Basic config with one queue (override in test if needed)
    PrintWriter out = new PrintWriter(new FileWriter(allocFileName));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println(" <queue name=\"test\">");
    out.println(" </queue>");
    out.println("</allocations>");
    out.close();

    conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
        ResourceScheduler.class);

    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, allocFileName);
    placementMgr = mock(PlacementManager.class);

    MockRM mockRM = new MockRM(conf);
    rmContext = mockRM.getRMContext();
    rmContext.setQueuePlacementManager(placementMgr);
    ApplicationMasterService masterService = new ApplicationMasterService(
        rmContext, rmContext.getScheduler());

    rmAppManager = new TestRMAppManager(rmContext,
        new ClientToAMTokenSecretManagerInRM(), rmContext.getScheduler(),
        masterService, new ApplicationACLsManager(conf), conf);
  }

  @After
  public void teardown(){
    File allocFile = GenericTestUtils.getTestDir(TEST_FOLDER);
    allocFile.delete();
  }

  @Test
  public void testQueueSubmitWithHighQueueContainerSize()
      throws YarnException, IOException {
    int maxAlloc =
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB;

    // scheduler config with a limited queue
    PrintWriter out = new PrintWriter(new FileWriter(allocFileName));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("  <queue name=\"root\">");
    out.println("    <queue name=\"limited\">");
    out.println("      <maxContainerAllocation>" + maxAlloc + " mb 1 vcores");
    out.println("      </maxContainerAllocation>");
    out.println("    </queue>");
    out.println("    <queue name=\"unlimited\">");
    out.println("    </queue>");
    out.println("  </queue>");
    out.println("</allocations>");
    out.close();
    rmContext.getScheduler().reinitialize(conf, rmContext);

    ApplicationId appId = MockApps.newAppID(1);
    Resource res = Resources.createResource(maxAlloc + 1);
    ApplicationSubmissionContext asContext = createAppSubmitCtx(appId, res);

    // Submit to limited queue
    when(placementMgr.placeApplication(any(), any()))
        .thenReturn(new ApplicationPlacementContext("limited"));
    try {
      rmAppManager.submitApplication(asContext, "test");
      Assert.fail("Test should fail on too high allocation!");
    } catch (InvalidResourceRequestException e) {
      Assert.assertEquals(GREATER_THEN_MAX_ALLOCATION,
          e.getInvalidResourceType());
    }

    // submit same app but now place it in the unlimited queue
    when(placementMgr.placeApplication(any(), any()))
        .thenReturn(new ApplicationPlacementContext("root.unlimited"));
    rmAppManager.submitApplication(asContext, "test");
  }

  @Test
  public void testQueueSubmitWithPermissionLimits()
      throws YarnException, IOException {

    conf.set(YarnConfiguration.YARN_ACL_ENABLE, "true");

    PrintWriter out = new PrintWriter(new FileWriter(allocFileName));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("  <queue name=\"root\">");
    out.println("    <aclSubmitApps> </aclSubmitApps>");
    out.println("    <aclAdministerApps> </aclAdministerApps>");
    out.println("    <queue name=\"noaccess\">");
    out.println("    </queue>");
    out.println("    <queue name=\"submitonly\">");
    out.println("      <aclSubmitApps>test </aclSubmitApps>");
    out.println("      <aclAdministerApps> </aclAdministerApps>");
    out.println("    </queue>");
    out.println("    <queue name=\"adminonly\">");
    out.println("      <aclSubmitApps> </aclSubmitApps>");
    out.println("      <aclAdministerApps>test </aclAdministerApps>");
    out.println("    </queue>");
    out.println("  </queue>");
    out.println("</allocations>");
    out.close();
    rmContext.getScheduler().reinitialize(conf, rmContext);

    ApplicationId appId = MockApps.newAppID(1);
    Resource res = Resources.createResource(1024, 1);
    ApplicationSubmissionContext asContext = createAppSubmitCtx(appId, res);

    // Submit to no access queue
    when(placementMgr.placeApplication(any(), any()))
        .thenReturn(new ApplicationPlacementContext("noaccess"));
    try {
      rmAppManager.submitApplication(asContext, "test");
      Assert.fail("Test should have failed with access denied");
    } catch (YarnException e) {
      assertTrue("Access exception not found",
          e.getCause() instanceof AccessControlException);
    }
    // Submit to submit access queue
    when(placementMgr.placeApplication(any(), any()))
        .thenReturn(new ApplicationPlacementContext("submitonly"));
    rmAppManager.submitApplication(asContext, "test");
    // Submit second app to admin access queue
    appId = MockApps.newAppID(2);
    asContext = createAppSubmitCtx(appId, res);
    when(placementMgr.placeApplication(any(), any()))
        .thenReturn(new ApplicationPlacementContext("adminonly"));
    rmAppManager.submitApplication(asContext, "test");
  }

  @Test
  public void testQueueSubmitWithRootPermission()
      throws YarnException, IOException {

    conf.set(YarnConfiguration.YARN_ACL_ENABLE, "true");

    PrintWriter out = new PrintWriter(new FileWriter(allocFileName));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("  <queue name=\"root\">");
    out.println("    <queue name=\"noaccess\">");
    out.println("      <aclSubmitApps> </aclSubmitApps>");
    out.println("      <aclAdministerApps> </aclAdministerApps>");
    out.println("    </queue>");
    out.println("  </queue>");
    out.println("</allocations>");
    out.close();
    rmContext.getScheduler().reinitialize(conf, rmContext);

    ApplicationId appId = MockApps.newAppID(1);
    Resource res = Resources.createResource(1024, 1);
    ApplicationSubmissionContext asContext = createAppSubmitCtx(appId, res);

    // Submit to noaccess queue should be allowed by root ACL
    when(placementMgr.placeApplication(any(), any()))
        .thenReturn(new ApplicationPlacementContext("noaccess"));
    rmAppManager.submitApplication(asContext, "test");
  }

  @Test
  public void testQueueSubmitWithAutoCreateQueue()
      throws YarnException, IOException {

    conf.set(YarnConfiguration.YARN_ACL_ENABLE, "true");

    PrintWriter out = new PrintWriter(new FileWriter(allocFileName));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("  <queue name=\"root\">");
    out.println("    <aclSubmitApps> </aclSubmitApps>");
    out.println("    <aclAdministerApps> </aclAdministerApps>");
    out.println("    <queue name=\"noaccess\" type=\"parent\">");
    out.println("    </queue>");
    out.println("    <queue name=\"submitonly\" type=\"parent\">");
    out.println("      <aclSubmitApps>test </aclSubmitApps>");
    out.println("    </queue>");
    out.println("  </queue>");
    out.println("</allocations>");
    out.close();
    rmContext.getScheduler().reinitialize(conf, rmContext);

    ApplicationId appId = MockApps.newAppID(1);
    Resource res = Resources.createResource(1024, 1);
    ApplicationSubmissionContext asContext = createAppSubmitCtx(appId, res);

    // Submit to noaccess parent with non existent child queue
    when(placementMgr.placeApplication(any(), any()))
        .thenReturn(new ApplicationPlacementContext("root.noaccess.child"));
    try {
      rmAppManager.submitApplication(asContext, "test");
      Assert.fail("Test should have failed with access denied");
    } catch (YarnException e) {
      assertTrue("Access exception not found",
          e.getCause() instanceof AccessControlException);
    }
    // Submit to submitonly parent with non existent child queue
    when(placementMgr.placeApplication(any(), any()))
        .thenReturn(new ApplicationPlacementContext("root.submitonly.child"));
    rmAppManager.submitApplication(asContext, "test");
  }

  private ApplicationSubmissionContext createAppSubmitCtx(ApplicationId appId,
                                                          Resource res) {
    ApplicationSubmissionContext asContext =
        Records.newRecord(ApplicationSubmissionContext.class);
    asContext.setApplicationId(appId);
    ResourceRequest resReg =
        ResourceRequest.newInstance(Priority.newInstance(0),
            ResourceRequest.ANY, res, 1);
    asContext.setAMContainerResourceRequests(
        Collections.singletonList(resReg));
    asContext.setAMContainerSpec(mock(ContainerLaunchContext.class));
    asContext.setQueue("default");
    return asContext;
  }
}
