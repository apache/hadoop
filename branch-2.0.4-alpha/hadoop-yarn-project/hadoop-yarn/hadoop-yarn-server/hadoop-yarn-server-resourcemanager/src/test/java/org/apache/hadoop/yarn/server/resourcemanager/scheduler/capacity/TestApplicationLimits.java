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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestApplicationLimits {
  
  private static final Log LOG = LogFactory.getLog(TestApplicationLimits.class);
  final static int GB = 1024;

  LeafQueue queue;
  
  private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();
  
  @Before
  public void setUp() throws IOException {
    CapacitySchedulerConfiguration csConf = 
        new CapacitySchedulerConfiguration();
    YarnConfiguration conf = new YarnConfiguration();
    setupQueueConfiguration(csConf);
    
    
    CapacitySchedulerContext csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getConf()).thenReturn(conf);
    when(csContext.getMinimumResourceCapability()).
        thenReturn(Resources.createResource(GB, 1));
    when(csContext.getMaximumResourceCapability()).
        thenReturn(Resources.createResource(16*GB, 32));
    when(csContext.getClusterResources()).
        thenReturn(Resources.createResource(10 * 16 * GB, 10 * 32));
    when(csContext.getApplicationComparator()).
        thenReturn(CapacityScheduler.applicationComparator);
    when(csContext.getQueueComparator()).
        thenReturn(CapacityScheduler.queueComparator);
    when(csContext.getResourceCalculator()).
        thenReturn(resourceCalculator);

    Map<String, CSQueue> queues = new HashMap<String, CSQueue>();
    CSQueue root = 
        CapacityScheduler.parseQueue(csContext, csConf, null, "root", 
            queues, queues, 
            TestUtils.spyHook);

    
    queue = spy(new LeafQueue(csContext, A, root, null));

    // Stub out ACL checks
    doReturn(true).
        when(queue).hasAccess(any(QueueACL.class), 
                              any(UserGroupInformation.class));
    
    // Some default values
    doReturn(100).when(queue).getMaxApplications();
    doReturn(25).when(queue).getMaxApplicationsPerUser();
    doReturn(10).when(queue).getMaximumActiveApplications();
    doReturn(2).when(queue).getMaximumActiveApplicationsPerUser();
  }
  
  private static final String A = "a";
  private static final String B = "b";
  private void setupQueueConfiguration(CapacitySchedulerConfiguration conf) {
    
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {A, B});

    final String Q_A = CapacitySchedulerConfiguration.ROOT + "." + A;
    conf.setCapacity(Q_A, 10);
    
    final String Q_B = CapacitySchedulerConfiguration.ROOT + "." + B;
    conf.setCapacity(Q_B, 90);
    
    LOG.info("Setup top-level queues a and b");
  }

  private FiCaSchedulerApp getMockApplication(int appId, String user) {
    FiCaSchedulerApp application = mock(FiCaSchedulerApp.class);
    ApplicationAttemptId applicationAttemptId =
        TestUtils.getMockApplicationAttemptId(appId, 0);
    doReturn(applicationAttemptId.getApplicationId()).
        when(application).getApplicationId();
    doReturn(applicationAttemptId). when(application).getApplicationAttemptId();
    doReturn(user).when(application).getUser();
    return application;
  }
  
  @Test
  public void testLimitsComputation() throws Exception {
    CapacitySchedulerConfiguration csConf = 
        new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    YarnConfiguration conf = new YarnConfiguration();
    
    CapacitySchedulerContext csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getConf()).thenReturn(conf);
    when(csContext.getMinimumResourceCapability()).
        thenReturn(Resources.createResource(GB, 1));
    when(csContext.getMaximumResourceCapability()).
        thenReturn(Resources.createResource(16*GB, 16));
    when(csContext.getApplicationComparator()).
        thenReturn(CapacityScheduler.applicationComparator);
    when(csContext.getQueueComparator()).
        thenReturn(CapacityScheduler.queueComparator);
    when(csContext.getResourceCalculator()).thenReturn(resourceCalculator);
    
    // Say cluster has 100 nodes of 16G each
    Resource clusterResource = Resources.createResource(100 * 16 * GB, 100 * 16);
    when(csContext.getClusterResources()).thenReturn(clusterResource);
    
    Map<String, CSQueue> queues = new HashMap<String, CSQueue>();
    CSQueue root = 
        CapacityScheduler.parseQueue(csContext, csConf, null, "root", 
            queues, queues, TestUtils.spyHook);

    LeafQueue queue = (LeafQueue)queues.get(A);
    
    LOG.info("Queue 'A' -" +
    		" maxActiveApplications=" + queue.getMaximumActiveApplications() + 
    		" maxActiveApplicationsPerUser=" + 
    		queue.getMaximumActiveApplicationsPerUser());
    int expectedMaxActiveApps = 
        Math.max(1, 
            (int)Math.ceil(((float)clusterResource.getMemory() / (1*GB)) * 
                   csConf.
                     getMaximumApplicationMasterResourcePerQueuePercent(
                                                        queue.getQueuePath()) *
                   queue.getAbsoluteMaximumCapacity()));
    assertEquals(expectedMaxActiveApps, 
                 queue.getMaximumActiveApplications());
    int expectedMaxActiveAppsUsingAbsCap = 
            Math.max(1, 
                (int)Math.ceil(((float)clusterResource.getMemory() / (1*GB)) * 
                       csConf.getMaximumApplicationMasterResourcePercent() *
                       queue.getAbsoluteCapacity()));
    assertEquals(
        (int)Math.ceil(
        		expectedMaxActiveAppsUsingAbsCap * (queue.getUserLimit() / 100.0f) * 
            queue.getUserLimitFactor()), 
        queue.getMaximumActiveApplicationsPerUser());
    assertEquals(
        (int)(clusterResource.getMemory() * queue.getAbsoluteCapacity()),
        queue.getMetrics().getAvailableMB()
        );
    
    // Add some nodes to the cluster & test new limits
    clusterResource = Resources.createResource(120 * 16 * GB);
    root.updateClusterResource(clusterResource);
    expectedMaxActiveApps = 
        Math.max(1, 
            (int)Math.ceil(((float)clusterResource.getMemory() / (1*GB)) * 
                   csConf.
                     getMaximumApplicationMasterResourcePerQueuePercent(
                                                        queue.getQueuePath()) *
                   queue.getAbsoluteMaximumCapacity()));
    assertEquals(expectedMaxActiveApps, 
                 queue.getMaximumActiveApplications());
    expectedMaxActiveAppsUsingAbsCap = 
            Math.max(1, 
                (int)Math.ceil(((float)clusterResource.getMemory() / (1*GB)) * 
                       csConf.getMaximumApplicationMasterResourcePercent() *
                       queue.getAbsoluteCapacity()));
    assertEquals(
        (int)Math.ceil(expectedMaxActiveAppsUsingAbsCap * 
            (queue.getUserLimit() / 100.0f) * queue.getUserLimitFactor()), 
        queue.getMaximumActiveApplicationsPerUser());
    assertEquals(
        (int)(clusterResource.getMemory() * queue.getAbsoluteCapacity()),
        queue.getMetrics().getAvailableMB()
        );

    // should return -1 if per queue setting not set
    assertEquals(
        (int)CapacitySchedulerConfiguration.UNDEFINED, 
        csConf.getMaximumApplicationsPerQueue(queue.getQueuePath()));
    int expectedMaxApps =  
        (int)
        (CapacitySchedulerConfiguration.DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS * 
        queue.getAbsoluteCapacity());
    assertEquals(expectedMaxApps, queue.getMaxApplications());

    int expectedMaxAppsPerUser = (int)(expectedMaxApps *
        (queue.getUserLimit()/100.0f) * queue.getUserLimitFactor());
    assertEquals(expectedMaxAppsPerUser, queue.getMaxApplicationsPerUser());

    // should default to global setting if per queue setting not set
    assertEquals(
        (long)CapacitySchedulerConfiguration.DEFAULT_MAXIMUM_APPLICATIONMASTERS_RESOURCE_PERCENT, 
        (long)csConf.getMaximumApplicationMasterResourcePerQueuePercent(
            queue.getQueuePath())
            );

    // Change the per-queue max AM resources percentage.
    csConf.setFloat(
      "yarn.scheduler.capacity." + 
          queue.getQueuePath() + 
          ".maximum-am-resource-percent",
      0.5f);
    // Re-create queues to get new configs.
    queues = new HashMap<String, CSQueue>();
    root = 
        CapacityScheduler.parseQueue(csContext, csConf, null, "root", 
            queues, queues, TestUtils.spyHook);
    clusterResource = Resources.createResource(100 * 16 * GB);

    queue = (LeafQueue)queues.get(A);
    expectedMaxActiveApps = 
        Math.max(1, 
            (int)Math.ceil(((float)clusterResource.getMemory() / (1*GB)) * 
                   csConf.
                     getMaximumApplicationMasterResourcePerQueuePercent(
                                                        queue.getQueuePath()) *
                   queue.getAbsoluteMaximumCapacity()));

    assertEquals((long) 0.5, 
        (long) csConf.getMaximumApplicationMasterResourcePerQueuePercent(queue.getQueuePath()));
    assertEquals(expectedMaxActiveApps, 
        queue.getMaximumActiveApplications());

    // Change the per-queue max applications.
    csConf.setInt(
      "yarn.scheduler.capacity." + 
          queue.getQueuePath() + 
          ".maximum-applications", 9999);
    // Re-create queues to get new configs.
    queues = new HashMap<String, CSQueue>();
    root = 
        CapacityScheduler.parseQueue(csContext, csConf, null, "root", 
            queues, queues, TestUtils.spyHook);

    queue = (LeafQueue)queues.get(A);
    assertEquals(9999, (int)csConf.getMaximumApplicationsPerQueue(queue.getQueuePath()));
    assertEquals(9999, queue.getMaxApplications());

    expectedMaxAppsPerUser = (int)(9999 *
        (queue.getUserLimit()/100.0f) * queue.getUserLimitFactor());
    assertEquals(expectedMaxAppsPerUser, queue.getMaxApplicationsPerUser());
  }
  
  @Test
  public void testActiveApplicationLimits() throws Exception {
    final String user_0 = "user_0";
    final String user_1 = "user_1";
    
    int APPLICATION_ID = 0;
    // Submit first application
    FiCaSchedulerApp app_0 = getMockApplication(APPLICATION_ID++, user_0);
    queue.submitApplication(app_0, user_0, A);
    assertEquals(1, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));

    // Submit second application
    FiCaSchedulerApp app_1 = getMockApplication(APPLICATION_ID++, user_0);
    queue.submitApplication(app_1, user_0, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    
    // Submit third application, should remain pending
    FiCaSchedulerApp app_2 = getMockApplication(APPLICATION_ID++, user_0);
    queue.submitApplication(app_2, user_0, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    
    // Finish one application, app_2 should be activated
    queue.finishApplication(app_0, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    
    // Submit another one for user_0
    FiCaSchedulerApp app_3 = getMockApplication(APPLICATION_ID++, user_0);
    queue.submitApplication(app_3, user_0, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    
    // Change queue limit to be smaller so 2 users can fill it up
    doReturn(3).when(queue).getMaximumActiveApplications();
    
    // Submit first app for user_1
    FiCaSchedulerApp app_4 = getMockApplication(APPLICATION_ID++, user_1);
    queue.submitApplication(app_4, user_1, A);
    assertEquals(3, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertEquals(1, queue.getNumActiveApplications(user_1));
    assertEquals(0, queue.getNumPendingApplications(user_1));

    // Submit second app for user_1, should block due to queue-limit
    FiCaSchedulerApp app_5 = getMockApplication(APPLICATION_ID++, user_1);
    queue.submitApplication(app_5, user_1, A);
    assertEquals(3, queue.getNumActiveApplications());
    assertEquals(2, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertEquals(1, queue.getNumActiveApplications(user_1));
    assertEquals(1, queue.getNumPendingApplications(user_1));

    // Now finish one app of user_1 so app_5 should be activated
    queue.finishApplication(app_4, A);
    assertEquals(3, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertEquals(1, queue.getNumActiveApplications(user_1));
    assertEquals(0, queue.getNumPendingApplications(user_1));
  }

  @Test
  public void testActiveLimitsWithKilledApps() throws Exception {
    final String user_0 = "user_0";

    int APPLICATION_ID = 0;

    // set max active to 2
    doReturn(2).when(queue).getMaximumActiveApplications();

    // Submit first application
    FiCaSchedulerApp app_0 = getMockApplication(APPLICATION_ID++, user_0);
    queue.submitApplication(app_0, user_0, A);
    assertEquals(1, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertTrue(queue.activeApplications.contains(app_0));

    // Submit second application
    FiCaSchedulerApp app_1 = getMockApplication(APPLICATION_ID++, user_0);
    queue.submitApplication(app_1, user_0, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertTrue(queue.activeApplications.contains(app_1));

    // Submit third application, should remain pending
    FiCaSchedulerApp app_2 = getMockApplication(APPLICATION_ID++, user_0);
    queue.submitApplication(app_2, user_0, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertTrue(queue.pendingApplications.contains(app_2));

    // Submit fourth application, should remain pending
    FiCaSchedulerApp app_3 = getMockApplication(APPLICATION_ID++, user_0);
    queue.submitApplication(app_3, user_0, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(2, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(2, queue.getNumPendingApplications(user_0));
    assertTrue(queue.pendingApplications.contains(app_3));

    // Kill 3rd pending application
    queue.finishApplication(app_2, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertFalse(queue.pendingApplications.contains(app_2));
    assertFalse(queue.activeApplications.contains(app_2));

    // Finish 1st application, app_3 should become active
    queue.finishApplication(app_0, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertTrue(queue.activeApplications.contains(app_3));
    assertFalse(queue.pendingApplications.contains(app_3));
    assertFalse(queue.activeApplications.contains(app_0));

    // Finish 2nd application
    queue.finishApplication(app_1, A);
    assertEquals(1, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertFalse(queue.activeApplications.contains(app_1));

    // Finish 4th application
    queue.finishApplication(app_3, A);
    assertEquals(0, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(0, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertFalse(queue.activeApplications.contains(app_3));
  }

  @Test
  public void testHeadroom() throws Exception {
    CapacitySchedulerConfiguration csConf = 
        new CapacitySchedulerConfiguration();
    csConf.setUserLimit(CapacitySchedulerConfiguration.ROOT + "." + A, 25);
    setupQueueConfiguration(csConf);
    YarnConfiguration conf = new YarnConfiguration();
    
    CapacitySchedulerContext csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getConf()).thenReturn(conf);
    when(csContext.getMinimumResourceCapability()).
        thenReturn(Resources.createResource(GB));
    when(csContext.getMaximumResourceCapability()).
        thenReturn(Resources.createResource(16*GB));
    when(csContext.getApplicationComparator()).
        thenReturn(CapacityScheduler.applicationComparator);
    when(csContext.getQueueComparator()).
        thenReturn(CapacityScheduler.queueComparator);
    when(csContext.getResourceCalculator()).thenReturn(resourceCalculator);
    
    // Say cluster has 100 nodes of 16G each
    Resource clusterResource = Resources.createResource(100 * 16 * GB);
    when(csContext.getClusterResources()).thenReturn(clusterResource);
    
    Map<String, CSQueue> queues = new HashMap<String, CSQueue>();
    CapacityScheduler.parseQueue(csContext, csConf, null, "root", 
        queues, queues, TestUtils.spyHook);

    // Manipulate queue 'a'
    LeafQueue queue = TestLeafQueue.stubLeafQueue((LeafQueue)queues.get(A));
    
    String host_0 = "host_0";
    String rack_0 = "rack_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, rack_0, 0, 16*GB);

    final String user_0 = "user_0";
    final String user_1 = "user_1";
    
    RecordFactory recordFactory = 
        RecordFactoryProvider.getRecordFactory(null);
    RMContext rmContext = TestUtils.getMockRMContext();

    Priority priority_1 = TestUtils.createMockPriority(1);

    // Submit first application with some resource-requests from user_0, 
    // and check headroom
    final ApplicationAttemptId appAttemptId_0_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0_0 = 
        spy(new FiCaSchedulerApp(appAttemptId_0_0, user_0, queue, 
            queue.getActiveUsersManager(), rmContext));
    queue.submitApplication(app_0_0, user_0, A);

    List<ResourceRequest> app_0_0_requests = new ArrayList<ResourceRequest>();
    app_0_0_requests.add(
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 2, 
            priority_1, recordFactory));
    app_0_0.updateResourceRequests(app_0_0_requests);

    // Schedule to compute 
    queue.assignContainers(clusterResource, node_0);
    Resource expectedHeadroom = Resources.createResource(10*16*GB, 1);
    verify(app_0_0).setHeadroom(eq(expectedHeadroom));

    // Submit second application from user_0, check headroom
    final ApplicationAttemptId appAttemptId_0_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_0_1 = 
        spy(new FiCaSchedulerApp(appAttemptId_0_1, user_0, queue, 
            queue.getActiveUsersManager(), rmContext));
    queue.submitApplication(app_0_1, user_0, A);
    
    List<ResourceRequest> app_0_1_requests = new ArrayList<ResourceRequest>();
    app_0_1_requests.add(
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 2, 
            priority_1, recordFactory));
    app_0_1.updateResourceRequests(app_0_1_requests);

    // Schedule to compute 
    queue.assignContainers(clusterResource, node_0); // Schedule to compute
    verify(app_0_0, times(2)).setHeadroom(eq(expectedHeadroom));
    verify(app_0_1).setHeadroom(eq(expectedHeadroom));// no change
    
    // Submit first application from user_1, check  for new headroom
    final ApplicationAttemptId appAttemptId_1_0 = 
        TestUtils.getMockApplicationAttemptId(2, 0); 
    FiCaSchedulerApp app_1_0 = 
        spy(new FiCaSchedulerApp(appAttemptId_1_0, user_1, queue, 
            queue.getActiveUsersManager(), rmContext));
    queue.submitApplication(app_1_0, user_1, A);

    List<ResourceRequest> app_1_0_requests = new ArrayList<ResourceRequest>();
    app_1_0_requests.add(
        TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 2, 
            priority_1, recordFactory));
    app_1_0.updateResourceRequests(app_1_0_requests);
    
    // Schedule to compute 
    queue.assignContainers(clusterResource, node_0); // Schedule to compute
    expectedHeadroom = Resources.createResource(10*16*GB / 2, 1); // changes
    verify(app_0_0).setHeadroom(eq(expectedHeadroom));
    verify(app_0_1).setHeadroom(eq(expectedHeadroom));
    verify(app_1_0).setHeadroom(eq(expectedHeadroom));

    // Now reduce cluster size and check for the smaller headroom
    clusterResource = Resources.createResource(90*16*GB);
    queue.assignContainers(clusterResource, node_0); // Schedule to compute
    expectedHeadroom = Resources.createResource(9*16*GB / 2, 1); // changes
    verify(app_0_0).setHeadroom(eq(expectedHeadroom));
    verify(app_0_1).setHeadroom(eq(expectedHeadroom));
    verify(app_1_0).setHeadroom(eq(expectedHeadroom));
  }
  

  @After
  public void tearDown() {
  
  }
}
