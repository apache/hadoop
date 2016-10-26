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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

public class TestApplicationLimits {
  
  private static final Log LOG = LogFactory.getLog(TestApplicationLimits.class);
  final static int GB = 1024;

  LeafQueue queue;
  
  private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

  RMContext rmContext = null;

  
  @Before
  public void setUp() throws IOException {
    CapacitySchedulerConfiguration csConf = 
        new CapacitySchedulerConfiguration();
    YarnConfiguration conf = new YarnConfiguration();
    setupQueueConfiguration(csConf);
    
    rmContext = TestUtils.getMockRMContext();


    CapacitySchedulerContext csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getConf()).thenReturn(conf);
    when(csContext.getMinimumResourceCapability()).
        thenReturn(Resources.createResource(GB, 1));
    when(csContext.getMaximumResourceCapability()).
        thenReturn(Resources.createResource(16*GB, 32));
    when(csContext.getClusterResource()).
        thenReturn(Resources.createResource(10 * 16 * GB, 10 * 32));
    when(csContext.getNonPartitionedQueueComparator()).
        thenReturn(CapacityScheduler.nonPartitionedQueueComparator);
    when(csContext.getResourceCalculator()).
        thenReturn(resourceCalculator);
    when(csContext.getRMContext()).thenReturn(rmContext);
    
    RMContainerTokenSecretManager containerTokenSecretManager =
        new RMContainerTokenSecretManager(conf);
    containerTokenSecretManager.rollMasterKey();
    when(csContext.getContainerTokenSecretManager()).thenReturn(
        containerTokenSecretManager);

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
    
    conf.setUserLimit(CapacitySchedulerConfiguration.ROOT + "." + A, 50);
    conf.setUserLimitFactor(CapacitySchedulerConfiguration.ROOT + "." + A, 5.0f);
    
    LOG.info("Setup top-level queues a and b");
  }

  private FiCaSchedulerApp getMockApplication(int appId, String user,
    Resource amResource) {
    FiCaSchedulerApp application = mock(FiCaSchedulerApp.class);
    ApplicationAttemptId applicationAttemptId =
        TestUtils.getMockApplicationAttemptId(appId, 0);
    doReturn(applicationAttemptId.getApplicationId()).
        when(application).getApplicationId();
    doReturn(applicationAttemptId). when(application).getApplicationAttemptId();
    doReturn(user).when(application).getUser();
    doReturn(amResource).when(application).getAMResource();
    doReturn(Priority.newInstance(0)).when(application).getPriority();
    doReturn(CommonNodeLabelsManager.NO_LABEL).when(application)
        .getAppAMNodePartitionName();
    doReturn(amResource).when(application).getAMResource(
        CommonNodeLabelsManager.NO_LABEL);
    when(application.compareInputOrderTo(any(FiCaSchedulerApp.class))).thenCallRealMethod();
    return application;
  }
  
  @Test
  public void testAMResourceLimit() throws Exception {
    final String user_0 = "user_0";
    final String user_1 = "user_1";
    
    // This uses the default 10% of cluster value for the max am resources
    // which are allowed, at 80GB = 8GB for AM's at the queue level.  The user
    // am limit is 4G initially (based on the queue absolute capacity)
    // when there is only 1 user, and drops to 2G (the userlimit) when there
    // is a second user
    Resource clusterResource = Resource.newInstance(80 * GB, 40);
    queue.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));
    
    ActiveUsersManager activeUsersManager = mock(ActiveUsersManager.class);
    when(queue.getActiveUsersManager()).thenReturn(activeUsersManager);

    assertEquals(Resource.newInstance(8 * GB, 1),
        queue.calculateAndGetAMResourceLimit());
    assertEquals(Resource.newInstance(4 * GB, 1),
      queue.getUserAMResourceLimit());
    
    // Two apps for user_0, both start
    int APPLICATION_ID = 0;
    FiCaSchedulerApp app_0 = getMockApplication(APPLICATION_ID++, user_0, 
      Resource.newInstance(2 * GB, 1));
    queue.submitApplicationAttempt(app_0, user_0);
    assertEquals(1, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    
    when(activeUsersManager.getNumActiveUsers()).thenReturn(1);

    FiCaSchedulerApp app_1 = getMockApplication(APPLICATION_ID++, user_0, 
      Resource.newInstance(2 * GB, 1));
    queue.submitApplicationAttempt(app_1, user_0);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    
    // AMLimits unchanged
    assertEquals(Resource.newInstance(8 * GB, 1), queue.getAMResourceLimit());
    assertEquals(Resource.newInstance(4 * GB, 1),
      queue.getUserAMResourceLimit());
    
    // One app for user_1, starts
    FiCaSchedulerApp app_2 = getMockApplication(APPLICATION_ID++, user_1, 
      Resource.newInstance(2 * GB, 1));
    queue.submitApplicationAttempt(app_2, user_1);
    assertEquals(3, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_1));
    assertEquals(0, queue.getNumPendingApplications(user_1));
    
    when(activeUsersManager.getNumActiveUsers()).thenReturn(2);
    
    // Now userAMResourceLimit drops to the queue configured 50% as there is
    // another user active
    assertEquals(Resource.newInstance(8 * GB, 1), queue.getAMResourceLimit());
    assertEquals(Resource.newInstance(2 * GB, 1),
      queue.getUserAMResourceLimit());
    
    // Second user_1 app cannot start
    FiCaSchedulerApp app_3 = getMockApplication(APPLICATION_ID++, user_1, 
      Resource.newInstance(2 * GB, 1));
    queue.submitApplicationAttempt(app_3, user_1);
    assertEquals(3, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_1));
    assertEquals(1, queue.getNumPendingApplications(user_1));

    // Now finish app so another should be activated
    queue.finishApplicationAttempt(app_2, A);
    assertEquals(3, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_1));
    assertEquals(0, queue.getNumPendingApplications(user_1));
    
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
    when(csContext.getNonPartitionedQueueComparator()).
        thenReturn(CapacityScheduler.nonPartitionedQueueComparator);
    when(csContext.getResourceCalculator()).thenReturn(resourceCalculator);
    when(csContext.getRMContext()).thenReturn(rmContext);
    when(csContext.getPreemptionManager()).thenReturn(new PreemptionManager());
    
    // Say cluster has 100 nodes of 16G each
    Resource clusterResource = 
      Resources.createResource(100 * 16 * GB, 100 * 16);
    when(csContext.getClusterResource()).thenReturn(clusterResource);
    
    Map<String, CSQueue> queues = new HashMap<String, CSQueue>();
    CSQueue root = 
        CapacityScheduler.parseQueue(csContext, csConf, null, "root", 
            queues, queues, TestUtils.spyHook);

    LeafQueue queue = (LeafQueue)queues.get(A);
    
    LOG.info("Queue 'A' -" +
    		" aMResourceLimit=" + queue.getAMResourceLimit() + 
    		" UserAMResourceLimit=" + 
    		queue.getUserAMResourceLimit());
    
    Resource amResourceLimit = Resource.newInstance(160 * GB, 1);
    assertEquals(queue.calculateAndGetAMResourceLimit(), amResourceLimit);
    assertEquals(queue.getUserAMResourceLimit(), 
      Resource.newInstance(80*GB, 1));
    
    // Assert in metrics
    assertEquals(queue.getMetrics().getAMResourceLimitMB(),
        amResourceLimit.getMemorySize());
    assertEquals(queue.getMetrics().getAMResourceLimitVCores(),
        amResourceLimit.getVirtualCores());

    assertEquals(
        (int)(clusterResource.getMemorySize() * queue.getAbsoluteCapacity()),
        queue.getMetrics().getAvailableMB()
        );
    
    // Add some nodes to the cluster & test new limits
    clusterResource = Resources.createResource(120 * 16 * GB);
    root.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));
    
    assertEquals(queue.calculateAndGetAMResourceLimit(),
        Resource.newInstance(192 * GB, 1));
    assertEquals(queue.getUserAMResourceLimit(), 
      Resource.newInstance(96*GB, 1));
    
    assertEquals(
        (int)(clusterResource.getMemorySize() * queue.getAbsoluteCapacity()),
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

    int expectedMaxAppsPerUser = Math.min(expectedMaxApps,
        (int)(expectedMaxApps * (queue.getUserLimit()/100.0f) *
        queue.getUserLimitFactor()));
    assertEquals(expectedMaxAppsPerUser, queue.getMaxApplicationsPerUser());

    // should default to global setting if per queue setting not set
    assertEquals(
        (long)CapacitySchedulerConfiguration.DEFAULT_MAXIMUM_APPLICATIONMASTERS_RESOURCE_PERCENT, 
        (long)csConf.getMaximumApplicationMasterResourcePerQueuePercent(
            queue.getQueuePath())
            );

    // Change the per-queue max AM resources percentage.
    csConf.setFloat(PREFIX + queue.getQueuePath()
        + ".maximum-am-resource-percent", 0.5f);
    // Re-create queues to get new configs.
    queues = new HashMap<String, CSQueue>();
    root = 
        CapacityScheduler.parseQueue(csContext, csConf, null, "root", 
            queues, queues, TestUtils.spyHook);
    clusterResource = Resources.createResource(100 * 16 * GB);

    queue = (LeafQueue)queues.get(A);

    assertEquals((long) 0.5, 
        (long) csConf.getMaximumApplicationMasterResourcePerQueuePercent(
          queue.getQueuePath())
        );
    
    assertEquals(queue.calculateAndGetAMResourceLimit(),
        Resource.newInstance(800 * GB, 1));
    assertEquals(queue.getUserAMResourceLimit(), 
      Resource.newInstance(400*GB, 1));

    // Change the per-queue max applications.
    csConf.setInt(PREFIX + queue.getQueuePath() + ".maximum-applications",
        9999);
    // Re-create queues to get new configs.
    queues = new HashMap<String, CSQueue>();
    root = 
        CapacityScheduler.parseQueue(csContext, csConf, null, "root", 
            queues, queues, TestUtils.spyHook);

    queue = (LeafQueue)queues.get(A);
    assertEquals(9999, (int)csConf.getMaximumApplicationsPerQueue(queue.getQueuePath()));
    assertEquals(9999, queue.getMaxApplications());

    expectedMaxAppsPerUser = Math.min(9999, (int)(9999 *
        (queue.getUserLimit()/100.0f) * queue.getUserLimitFactor()));
    assertEquals(expectedMaxAppsPerUser, queue.getMaxApplicationsPerUser());
  }
  
  @Test
  public void testActiveApplicationLimits() throws Exception {
    final String user_0 = "user_0";
    final String user_1 = "user_1";
    final String user_2 = "user_2";
    
    assertEquals(Resource.newInstance(16 * GB, 1),
        queue.calculateAndGetAMResourceLimit());
    assertEquals(Resource.newInstance(8 * GB, 1),
      queue.getUserAMResourceLimit());
    
    int APPLICATION_ID = 0;
    // Submit first application
    FiCaSchedulerApp app_0 = getMockApplication(APPLICATION_ID++, user_0,
      Resources.createResource(4 * GB, 0));
    queue.submitApplicationAttempt(app_0, user_0);
    assertEquals(1, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));

    // Submit second application
    FiCaSchedulerApp app_1 = getMockApplication(APPLICATION_ID++, user_0,
      Resources.createResource(4 * GB, 0));
    queue.submitApplicationAttempt(app_1, user_0);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    
    // Submit third application, should remain pending due to user amlimit
    FiCaSchedulerApp app_2 = getMockApplication(APPLICATION_ID++, user_0,
      Resources.createResource(4 * GB, 0));
    queue.submitApplicationAttempt(app_2, user_0);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    
    // Finish one application, app_2 should be activated
    queue.finishApplicationAttempt(app_0, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    
    // Submit another one for user_0
    FiCaSchedulerApp app_3 = getMockApplication(APPLICATION_ID++, user_0,
      Resources.createResource(4 * GB, 0));
    queue.submitApplicationAttempt(app_3, user_0);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    
    // Submit first app for user_1
    FiCaSchedulerApp app_4 = getMockApplication(APPLICATION_ID++, user_1,
      Resources.createResource(8 * GB, 0));
    queue.submitApplicationAttempt(app_4, user_1);
    assertEquals(3, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertEquals(1, queue.getNumActiveApplications(user_1));
    assertEquals(0, queue.getNumPendingApplications(user_1));

    // Submit first app for user_2, should block due to queue amlimit
    FiCaSchedulerApp app_5 = getMockApplication(APPLICATION_ID++, user_2,
      Resources.createResource(8 * GB, 0));
    queue.submitApplicationAttempt(app_5, user_2);
    assertEquals(3, queue.getNumActiveApplications());
    assertEquals(2, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertEquals(1, queue.getNumActiveApplications(user_1));
    assertEquals(0, queue.getNumPendingApplications(user_1));
    assertEquals(1, queue.getNumPendingApplications(user_2));

    // Now finish one app of user_1 so app_5 should be activated
    queue.finishApplicationAttempt(app_4, A);
    assertEquals(3, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertEquals(0, queue.getNumActiveApplications(user_1));
    assertEquals(0, queue.getNumPendingApplications(user_1));
    assertEquals(1, queue.getNumActiveApplications(user_2));
    assertEquals(0, queue.getNumPendingApplications(user_2));
    
  }
  
  @Test
  public void testActiveLimitsWithKilledApps() throws Exception {
    final String user_0 = "user_0";

    int APPLICATION_ID = 0;

    // Submit first application
    FiCaSchedulerApp app_0 = getMockApplication(APPLICATION_ID++, user_0,
      Resources.createResource(4 * GB, 0));
    queue.submitApplicationAttempt(app_0, user_0);
    assertEquals(1, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertTrue(queue.getApplications().contains(app_0));

    // Submit second application
    FiCaSchedulerApp app_1 = getMockApplication(APPLICATION_ID++, user_0,
      Resources.createResource(4 * GB, 0));
    queue.submitApplicationAttempt(app_1, user_0);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertTrue(queue.getApplications().contains(app_1));

    // Submit third application, should remain pending
    FiCaSchedulerApp app_2 = getMockApplication(APPLICATION_ID++, user_0,
      Resources.createResource(4 * GB, 0));
    queue.submitApplicationAttempt(app_2, user_0);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertTrue(queue.getPendingApplications().contains(app_2));

    // Submit fourth application, should remain pending
    FiCaSchedulerApp app_3 = getMockApplication(APPLICATION_ID++, user_0,
      Resources.createResource(4 * GB, 0));
    queue.submitApplicationAttempt(app_3, user_0);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(2, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(2, queue.getNumPendingApplications(user_0));
    assertTrue(queue.getPendingApplications().contains(app_3));

    // Kill 3rd pending application
    queue.finishApplicationAttempt(app_2, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertFalse(queue.getPendingApplications().contains(app_2));
    assertFalse(queue.getApplications().contains(app_2));

    // Finish 1st application, app_3 should become active
    queue.finishApplicationAttempt(app_0, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertTrue(queue.getApplications().contains(app_3));
    assertFalse(queue.getPendingApplications().contains(app_3));
    assertFalse(queue.getApplications().contains(app_0));

    // Finish 2nd application
    queue.finishApplicationAttempt(app_1, A);
    assertEquals(1, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertFalse(queue.getApplications().contains(app_1));

    // Finish 4th application
    queue.finishApplicationAttempt(app_3, A);
    assertEquals(0, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(0, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertFalse(queue.getApplications().contains(app_3));
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
    when(csContext.getNonPartitionedQueueComparator()).
        thenReturn(CapacityScheduler.nonPartitionedQueueComparator);
    when(csContext.getResourceCalculator()).thenReturn(resourceCalculator);
    when(csContext.getRMContext()).thenReturn(rmContext);
    
    // Say cluster has 100 nodes of 16G each
    Resource clusterResource = Resources.createResource(100 * 16 * GB);
    when(csContext.getClusterResource()).thenReturn(clusterResource);
    
    Map<String, CSQueue> queues = new HashMap<String, CSQueue>();
    CSQueue rootQueue = CapacityScheduler.parseQueue(csContext, csConf, null,
        "root", queues, queues, TestUtils.spyHook);

    ResourceUsage queueCapacities = rootQueue.getQueueResourceUsage();
    when(csContext.getClusterResourceUsage())
        .thenReturn(queueCapacities);

    // Manipulate queue 'a'
    LeafQueue queue = TestLeafQueue.stubLeafQueue((LeafQueue)queues.get(A));
    queue.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));
    
    String host_0 = "host_0";
    String rack_0 = "rack_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, rack_0, 0, 16*GB);

    final String user_0 = "user_0";
    final String user_1 = "user_1";
    
    RecordFactory recordFactory = 
        RecordFactoryProvider.getRecordFactory(null);
    RMContext rmContext = TestUtils.getMockRMContext();
    RMContext spyRMContext = spy(rmContext);
    
    ConcurrentMap<ApplicationId, RMApp> spyApps = 
        spy(new ConcurrentHashMap<ApplicationId, RMApp>());
    RMApp rmApp = mock(RMApp.class);
    ResourceRequest amResourceRequest = mock(ResourceRequest.class);
    Resource amResource = Resources.createResource(0, 0);
    when(amResourceRequest.getCapability()).thenReturn(amResource);
    when(rmApp.getAMResourceRequest()).thenReturn(amResourceRequest);
    Mockito.doReturn(rmApp).when(spyApps).get((ApplicationId)Matchers.any());
    when(spyRMContext.getRMApps()).thenReturn(spyApps);
    RMAppAttempt rmAppAttempt = mock(RMAppAttempt.class);
    when(rmApp.getRMAppAttempt((ApplicationAttemptId) Matchers.any()))
        .thenReturn(rmAppAttempt);
    when(rmApp.getCurrentAppAttempt()).thenReturn(rmAppAttempt);
    Mockito.doReturn(rmApp).when(spyApps).get((ApplicationId) Matchers.any());
    Mockito.doReturn(true).when(spyApps)
        .containsKey((ApplicationId) Matchers.any());

    Priority priority_1 = TestUtils.createMockPriority(1);

    // Submit first application with some resource-requests from user_0, 
    // and check headroom
    final ApplicationAttemptId appAttemptId_0_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0_0 = new FiCaSchedulerApp(
      appAttemptId_0_0, user_0, queue, 
            queue.getActiveUsersManager(), spyRMContext);
    queue.submitApplicationAttempt(app_0_0, user_0);

    List<ResourceRequest> app_0_0_requests = new ArrayList<ResourceRequest>();
    app_0_0_requests.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 2,
            true, priority_1, recordFactory));
    app_0_0.updateResourceRequests(app_0_0_requests);

    // Schedule to compute 
    queue.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    Resource expectedHeadroom = Resources.createResource(10*16*GB, 1);
    assertEquals(expectedHeadroom, app_0_0.getHeadroom());

    // Submit second application from user_0, check headroom
    final ApplicationAttemptId appAttemptId_0_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_0_1 = new FiCaSchedulerApp(
      appAttemptId_0_1, user_0, queue, 
            queue.getActiveUsersManager(), spyRMContext);
    queue.submitApplicationAttempt(app_0_1, user_0);
    
    List<ResourceRequest> app_0_1_requests = new ArrayList<ResourceRequest>();
    app_0_1_requests.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 2,
            true, priority_1, recordFactory));
    app_0_1.updateResourceRequests(app_0_1_requests);

    // Schedule to compute 
    queue.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY); // Schedule to compute
    assertEquals(expectedHeadroom, app_0_0.getHeadroom());
    assertEquals(expectedHeadroom, app_0_1.getHeadroom());// no change
    
    // Submit first application from user_1, check  for new headroom
    final ApplicationAttemptId appAttemptId_1_0 = 
        TestUtils.getMockApplicationAttemptId(2, 0); 
    FiCaSchedulerApp app_1_0 = new FiCaSchedulerApp(
      appAttemptId_1_0, user_1, queue, 
            queue.getActiveUsersManager(), spyRMContext);
    queue.submitApplicationAttempt(app_1_0, user_1);

    List<ResourceRequest> app_1_0_requests = new ArrayList<ResourceRequest>();
    app_1_0_requests.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 2,
            true, priority_1, recordFactory));
    app_1_0.updateResourceRequests(app_1_0_requests);
    
    // Schedule to compute 
    queue.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY); // Schedule to compute
    expectedHeadroom = Resources.createResource(10*16*GB / 2, 1); // changes
    assertEquals(expectedHeadroom, app_0_0.getHeadroom());
    assertEquals(expectedHeadroom, app_0_1.getHeadroom());
    assertEquals(expectedHeadroom, app_1_0.getHeadroom());

    // Now reduce cluster size and check for the smaller headroom
    clusterResource = Resources.createResource(90*16*GB);
    queue.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY); // Schedule to compute
    expectedHeadroom = Resources.createResource(9*16*GB / 2, 1); // changes
    assertEquals(expectedHeadroom, app_0_0.getHeadroom());
    assertEquals(expectedHeadroom, app_0_1.getHeadroom());
    assertEquals(expectedHeadroom, app_1_0.getHeadroom());
  }
  

  @After
  public void tearDown() {
  
  }
}
