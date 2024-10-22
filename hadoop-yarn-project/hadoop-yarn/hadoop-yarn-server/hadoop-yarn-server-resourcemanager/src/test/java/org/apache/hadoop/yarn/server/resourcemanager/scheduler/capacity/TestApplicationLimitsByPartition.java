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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.resource.TestResourceProfiles;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt.AMState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;

public class TestApplicationLimitsByPartition {
  final static int GB = 1024;
  final static String A1_PATH = CapacitySchedulerConfiguration.ROOT + ".a" + ".a1";
  final static String B1_PATH = CapacitySchedulerConfiguration.ROOT + ".b" + ".b1";
  final static String B2_PATH = CapacitySchedulerConfiguration.ROOT + ".b" + ".b2";
  final static String C1_PATH = CapacitySchedulerConfiguration.ROOT + ".c" + ".c1";
  final static QueuePath ROOT = new QueuePath(CapacitySchedulerConfiguration.ROOT);
  final static QueuePath A1 = new QueuePath(A1_PATH);
  final static QueuePath B1 = new QueuePath(B1_PATH);
  final static QueuePath B2 = new QueuePath(B2_PATH);
  final static QueuePath C1 = new QueuePath(C1_PATH);

  LeafQueue queue;
  RMNodeLabelsManager mgr;
  private YarnConfiguration conf;

  private final ResourceCalculator resourceCalculator =
      new DefaultResourceCalculator();

  @Before
  public void setUp() throws IOException {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }

  private void simpleNodeLabelMappingToManager() throws IOException {
    // set node -> label
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0),
        TestUtils.toSet("x"), NodeId.newInstance("h2", 0),
        TestUtils.toSet("y")));
  }

  private void complexNodeLabelMappingToManager() throws IOException {
    // set node -> label
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y",
        "z"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0),
        TestUtils.toSet("x"), NodeId.newInstance("h2", 0),
        TestUtils.toSet("y"), NodeId.newInstance("h3", 0),
        TestUtils.toSet("y"), NodeId.newInstance("h4", 0),
        TestUtils.toSet("z"), NodeId.newInstance("h5", 0),
        RMNodeLabelsManager.EMPTY_STRING_SET));
  }

  @Test(timeout = 120000)
  public void testAMResourceLimitWithLabels() throws Exception {
    /*
     * Test Case:
     * Verify AM resource limit per partition level and per queue level. So
     * we use 2 queues to verify this case.
     * Queue a1 supports labels (x,y). Configure am-resource-limit as 0.2 (x)
     * Queue c1 supports default label. Configure am-resource-limit as 0.2
     *
     * Queue A1 for label X can only support 2Gb AM resource.
     * Queue C1 (empty label) can support 2Gb AM resource.
     *
     * Verify atleast one AM is launched, and AM resources should not go more
     * than 2GB in each queue.
     */

    simpleNodeLabelMappingToManager();
    CapacitySchedulerConfiguration config = (CapacitySchedulerConfiguration)
        TestUtils.getConfigurationWithQueueLabels(conf);

    // After getting queue conf, configure AM resource percent for Queue A1
    // as 0.2 (Label X) and for Queue C1 as 0.2 (Empty Label)
    config.setMaximumAMResourcePercentPerPartition(A1, "x", 0.2f);
    config.setMaximumApplicationMasterResourcePerQueuePercent(C1, 0.2f);

    // Now inject node label manager with this updated config
    MockRM rm1 = new MockRM(config) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 10 * GB); // label = x
    rm1.registerNode("h2:1234", 10 * GB); // label = y
    MockNM nm3 = rm1.registerNode("h3:1234", 10 * GB); // label = <empty>

    // Submit app1 with 1Gb AM resource to Queue A1 for label X
    MockRMAppSubmissionData data5 =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withAmLabel("x")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data5);

    // Submit app2 with 1Gb AM resource to Queue A1 for label X
    MockRMAppSubmissionData data4 =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withAmLabel("x")
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data4);

    // Submit 3rd app to Queue A1 for label X, and this will be pending as
    // AM limit is already crossed for label X. (2GB)
    MockRMAppSubmissionData data3 =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withAmLabel("x")
            .build();
    RMApp pendingApp = MockRMAppSubmitter.submit(rm1, data3);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    LeafQueue leafQueue = (LeafQueue) cs.getQueue("a1");
    Assert.assertNotNull(leafQueue);

    // Only one AM will be activated here and second AM will be still
    // pending.
    Assert.assertEquals(2, leafQueue.getNumActiveApplications());
    Assert.assertEquals(1, leafQueue.getNumPendingApplications());
    Assert.assertTrue("AM diagnostics not set properly", app1.getDiagnostics()
        .toString().contains(AMState.ACTIVATED.getDiagnosticMessage()));
    Assert.assertTrue("AM diagnostics not set properly", app2.getDiagnostics()
        .toString().contains(AMState.ACTIVATED.getDiagnosticMessage()));
    Assert.assertTrue("AM diagnostics not set properly",
        pendingApp.getDiagnostics().toString()
            .contains(AMState.INACTIVATED.getDiagnosticMessage()));
    Assert.assertTrue("AM diagnostics not set properly",
        pendingApp.getDiagnostics().toString().contains(
            CSAMContainerLaunchDiagnosticsConstants.QUEUE_AM_RESOURCE_LIMIT_EXCEED));

    // Now verify the same test case in Queue C1 where label is not configured.
    // Submit an app to Queue C1 with empty label
    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c1")
            .withUnmanagedAM(false)
            .build();
    RMApp app3 = MockRMAppSubmitter.submit(rm1, data2);
    MockRM.launchAndRegisterAM(app3, rm1, nm3);

    // Submit next app to Queue C1 with empty label
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c1")
            .withUnmanagedAM(false)
            .build();
    RMApp app4 = MockRMAppSubmitter.submit(rm1, data1);
    MockRM.launchAndRegisterAM(app4, rm1, nm3);

    // Submit 3rd app to Queue C1. This will be pending as Queue's am-limit
    // is reached.
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c1")
            .withUnmanagedAM(false)
            .build();
    pendingApp = MockRMAppSubmitter.submit(rm1, data);

    leafQueue = (LeafQueue) cs.getQueue("c1");
    Assert.assertNotNull(leafQueue);

    // 2 apps will be activated, third one will be pending as am-limit
    // is reached.
    Assert.assertEquals(2, leafQueue.getNumActiveApplications());
    Assert.assertEquals(1, leafQueue.getNumPendingApplications());
    Assert.assertTrue("AM diagnostics not set properly",
        pendingApp.getDiagnostics().toString()
            .contains(AMState.INACTIVATED.getDiagnosticMessage()));
    Assert.assertTrue("AM diagnostics not set properly",
        pendingApp.getDiagnostics().toString().contains(
            CSAMContainerLaunchDiagnosticsConstants.QUEUE_AM_RESOURCE_LIMIT_EXCEED));

    rm1.killApp(app3.getApplicationId());
    Thread.sleep(1000);

    // After killing one running app, pending app will also get activated.
    Assert.assertEquals(2, leafQueue.getNumActiveApplications());
    Assert.assertEquals(0, leafQueue.getNumPendingApplications());
    rm1.close();
  }

  @Test(timeout = 120000)
  public void testAtleastOneAMRunPerPartition() throws Exception {
    /*
     * Test Case:
     * Even though am-resource-limit per queue/partition may cross if we
     * activate an app (high am resource demand), we have to activate it
     * since no other apps are running in that Queue/Partition. Here also
     * we run one test case for partition level and one in queue level to
     * ensure no breakage in existing functionality.
     *
     * Queue a1 supports labels (x,y). Configure am-resource-limit as 0.15 (x)
     * Queue c1 supports default label. Configure am-resource-limit as 0.15
     *
     * Queue A1 for label X can only support 1.5Gb AM resource.
     * Queue C1 (empty label) can support 1.5Gb AM resource.
     *
     * Verify atleast one AM is launched in each Queue.
     */
    simpleNodeLabelMappingToManager();
    CapacitySchedulerConfiguration config = (CapacitySchedulerConfiguration)
        TestUtils.getConfigurationWithQueueLabels(conf);

    // After getting queue conf, configure AM resource percent for Queue A1
    // as 0.15 (Label X) and for Queue C1 as 0.15 (Empty Label)
    config.setMaximumAMResourcePercentPerPartition(A1, "x", 0.15f);
    config.setMaximumApplicationMasterResourcePerQueuePercent(C1, 0.15f);
    // inject node label manager
    MockRM rm1 = new MockRM(config) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 10 * GB); // label = x
    rm1.registerNode("h2:1234", 10 * GB); // label = y
    MockNM nm3 = rm1.registerNode("h3:1234", 10 * GB); // label = <empty>

    // Submit app1 (2 GB) to Queue A1 and label X
    MockRMAppSubmissionData data3 =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withAmLabel("x")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data3);
    // This app must be activated eventhough the am-resource per-partition
    // limit is only for 1.5GB.
    MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // Submit 2nd app to label "X" with one GB and it must be pending since
    // am-resource per-partition limit is crossed (1.5 GB was the limit).
    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withAmLabel("x")
            .build();
    MockRMAppSubmitter.submit(rm1, data2);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    LeafQueue leafQueue = (LeafQueue) cs.getQueue("a1");
    Assert.assertNotNull(leafQueue);

    // Only 1 app will be activated as am-limit for partition "x" is 0.15
    Assert.assertEquals(1, leafQueue.getNumActiveApplications());
    Assert.assertEquals(1, leafQueue.getNumPendingApplications());

    // Now verify the same test case in Queue C1 which takes default label
    // to see queue level am-resource-limit is still working as expected.

    // Submit an app to Queue C1 with empty label (2 GB)
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c1")
            .withUnmanagedAM(false)
            .build();
    RMApp app3 = MockRMAppSubmitter.submit(rm1, data1);
    // This app must be activated even though the am-resource per-queue
    // limit is only for 1.5GB
    MockRM.launchAndRegisterAM(app3, rm1, nm3);

    // Submit 2nd app to C1 (Default label, hence am-limit per-queue will be
    // considered).
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c1")
            .withUnmanagedAM(false)
            .build();
    MockRMAppSubmitter.submit(rm1, data);

    leafQueue = (LeafQueue) cs.getQueue("c1");
    Assert.assertNotNull(leafQueue);

    // 1 app will be activated (and it has AM resource more than queue limit)
    Assert.assertEquals(1, leafQueue.getNumActiveApplications());
    Assert.assertEquals(1, leafQueue.getNumPendingApplications());
    rm1.close();
  }

  @Test(timeout = 120000)
  public void testDefaultAMLimitFromQueueForPartition() throws Exception {
    /*
     * Test Case:
     * Configure AM resource limit per queue level. If partition level config
     * is not found, we will be considering per-queue level am-limit. Ensure
     * this is working as expected.
     *
     * Queue A1 am-resource limit to be configured as 0.2 (not for partition x)
     *
     * Eventhough per-partition level config is not done, CS should consider
     * the configuration done for queue level.
     */
    simpleNodeLabelMappingToManager();
    CapacitySchedulerConfiguration config = (CapacitySchedulerConfiguration)
        TestUtils.getConfigurationWithQueueLabels(conf);

    // After getting queue conf, configure AM resource percent for Queue A1
    // as 0.2 (not for partition, rather in queue level)
    config.setMaximumApplicationMasterResourcePerQueuePercent(A1, 0.2f);
    // inject node label manager
    MockRM rm1 = new MockRM(config) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 10 * GB); // label = x
    rm1.registerNode("h2:1234", 10 * GB); // label = y
    rm1.registerNode("h3:1234", 10 * GB); // label = <empty>

    // Submit app1 (2 GB) to Queue A1 and label X
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withAmLabel("x")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data1);

    // Submit 2nd app to label "X" with one GB. Since queue am-limit is 2GB,
    // 2nd app will be pending and first one will get activated.
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withAmLabel("x")
            .build();
    RMApp pendingApp = MockRMAppSubmitter.submit(rm1, data);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    LeafQueue leafQueue = (LeafQueue) cs.getQueue("a1");
    Assert.assertNotNull(leafQueue);

    // Only 1 app will be activated as am-limit for queue is 0.2 and same is
    // used for partition "x" also.
    Assert.assertEquals(1, leafQueue.getNumActiveApplications());
    Assert.assertEquals(1, leafQueue.getNumPendingApplications());
    Assert.assertTrue("AM diagnostics not set properly", app1.getDiagnostics()
        .toString().contains(AMState.ACTIVATED.getDiagnosticMessage()));
    Assert.assertTrue("AM diagnostics not set properly",
        pendingApp.getDiagnostics().toString()
            .contains(AMState.INACTIVATED.getDiagnosticMessage()));
    Assert.assertTrue("AM diagnostics not set properly",
        pendingApp.getDiagnostics().toString()
            .contains(CSAMContainerLaunchDiagnosticsConstants.QUEUE_AM_RESOURCE_LIMIT_EXCEED));
    rm1.close();
  }

  @Test(timeout = 120000)
  public void testUserAMResourceLimitWithLabels() throws Exception {
    /*
     * Test Case:
     * Verify user level AM resource limit. This test case is ran with two
     * users. And per-partition level am-resource-limit will be 0.4, which
     * internally will be 4GB. Hence 2GB will be available for each
     * user for its AM resource.
     *
     * Now this test case will create a scenario where AM resource limit per
     * partition is not met, but user level am-resource limit is reached.
     * Hence app will be pending.
     */

    final String user_0 = "user_0";
    final String user_1 = "user_1";
    simpleNodeLabelMappingToManager();
    CapacitySchedulerConfiguration config = (CapacitySchedulerConfiguration)
        TestUtils.getConfigurationWithQueueLabels(conf);

    // After getting queue conf, configure AM resource percent for Queue A1
    // as 0.4 (Label X). Also set userlimit as 50% for this queue. So when we
    // have two users submitting applications, each user will get 50%  of AM
    // resource which is available in this partition.
    config.setMaximumAMResourcePercentPerPartition(A1, "x", 0.4f);
    config.setUserLimit(A1, 50);

    // Now inject node label manager with this updated config
    MockRM rm1 = new MockRM(config) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 10 * GB); // label = x
    rm1.registerNode("h2:1234", 10 * GB); // label = y
    rm1.registerNode("h3:1234", 10 * GB); // label = <empty>

    // Submit app1 with 1Gb AM resource to Queue A1 for label X for user0
    MockRMAppSubmissionData data3 =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app")
            .withUser(user_0)
            .withAcls(null)
            .withQueue("a1")
            .withAmLabel("x")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data3);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // Place few allocate requests to make it an active application
    am1.allocate("*", 1 * GB, 15, new ArrayList<ContainerId>(), "");

    // Now submit 2nd app to Queue A1 for label X for user1
    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app")
            .withUser(user_1)
            .withAcls(null)
            .withQueue("a1")
            .withAmLabel("x")
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data2);
    MockRM.launchAndRegisterAM(app2, rm1, nm1);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    LeafQueue leafQueue = (LeafQueue) cs.getQueue("a1");
    Assert.assertNotNull(leafQueue);

    // Verify active applications count in this queue.
    Assert.assertEquals(2, leafQueue.getNumActiveApplications());
    Assert.assertEquals(1, leafQueue.getNumActiveApplications(user_0));
    Assert.assertEquals(0, leafQueue.getNumPendingApplications());

    // Submit 3rd app to Queue A1 for label X for user1. Now user1 will have
    // 2 applications (2 GB resource) and user0 will have one app (1GB).
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app")
            .withUser(user_1)
            .withAcls(null)
            .withQueue("a1")
            .withAmLabel("x")
            .build();
    RMApp app3 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am2 = MockRM.launchAndRegisterAM(app3, rm1, nm1);

    // Place few allocate requests to make it an active application. This is
    // to ensure that user1 and user0 are active users.
    am2.allocate("*", 1 * GB, 10, new ArrayList<ContainerId>(), "");

    // Submit final app to Queue A1 for label X. Since we are trying to submit
    // for user1, we need 3Gb resource for AMs.
    // 4Gb -> 40% of label "X" in queue A1
    // Since we have 2 users, 50% of 4Gb will be max for each user. Here user1
    // has already crossed this 2GB limit, hence this app will be pending.
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app")
            .withUser(user_1)
            .withAcls(null)
            .withQueue("a1")
            .withAmLabel("x")
            .build();
    RMApp pendingApp = MockRMAppSubmitter.submit(rm1, data);

    // Verify active applications count per user and also in queue level.
    Assert.assertEquals(3, leafQueue.getNumActiveApplications());
    Assert.assertEquals(1, leafQueue.getNumActiveApplications(user_0));
    Assert.assertEquals(2, leafQueue.getNumActiveApplications(user_1));
    Assert.assertEquals(1, leafQueue.getNumPendingApplications(user_1));
    Assert.assertEquals(1, leafQueue.getNumPendingApplications());

    //verify Diagnostic messages
    Assert.assertTrue("AM diagnostics not set properly",
        pendingApp.getDiagnostics().toString()
            .contains(AMState.INACTIVATED.getDiagnosticMessage()));
    Assert.assertTrue("AM diagnostics not set properly",
        pendingApp.getDiagnostics().toString().contains(
            CSAMContainerLaunchDiagnosticsConstants.USER_AM_RESOURCE_LIMIT_EXCEED));
    rm1.close();
  }

  @Test
  public void testAMResourceLimitForMultipleApplications() throws Exception {
    /*
     * Test Case:
     * In a complex node label setup, verify am-resource-percentage calculation
     * and check whether applications can get activated as per expectation.
     */
    complexNodeLabelMappingToManager();
    CapacitySchedulerConfiguration config = (CapacitySchedulerConfiguration)
        TestUtils.getComplexConfigurationWithQueueLabels(conf);

    /*
     * Queue structure:
     *                      root (*)
     *                  ________________
     *                 /                \
     *               a x(100%), y(50%)   b y(50%), z(100%)
     *               ________________    ______________
     *              /                   /              \
     *             a1 (x,y)         b1(no)              b2(y,z)
     *               100%                          y = 100%, z = 100%
     *
     * Node structure:
     * h1 : x
     * h2 : y
     * h3 : y
     * h4 : z
     * h5 : NO
     *
     * Total resource:
     * x: 10G
     * y: 20G
     * z: 10G
     * *: 10G
     *
     * AM resource percentage config:
     * A1  : 0.25
     * B2  : 0.15
     */
    config.setMaximumAMResourcePercentPerPartition(A1, "y", 0.25f);
    config.setMaximumApplicationMasterResourcePerQueuePercent(B1, 0.15f);

    // Now inject node label manager with this updated config
    MockRM rm1 = new MockRM(config) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    rm1.registerNode("h1:1234", 10 * GB); // label = x
    MockNM nm2 = rm1.registerNode("h2:1234", 10 * GB); // label = y
    MockNM nm3 = rm1.registerNode("h3:1234", 10 * GB); // label = y
    rm1.registerNode("h4:1234", 10 * GB); // label = z
    MockNM nm5 = rm1.registerNode("h5:1234", 10 * GB); // label = <empty>

    // Submit app1 with 2Gb AM resource to Queue A1 for label Y
    MockRMAppSubmissionData data4 =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withAmLabel("y")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data4);
    MockRM.launchAndRegisterAM(app1, rm1, nm2);

    // Submit app2 with 1Gb AM resource to Queue A1 for label Y
    MockRMAppSubmissionData data3 =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withAmLabel("y")
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data3);
    MockRM.launchAndRegisterAM(app2, rm1, nm3);

    // Submit another app with 1Gb AM resource to Queue A1 for label Y
    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withAmLabel("y")
            .build();
    MockRMAppSubmitter.submit(rm1, data2);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    LeafQueue leafQueue = (LeafQueue) cs.getQueue("a1");
    Assert.assertNotNull(leafQueue);

    /*
     *  capacity of queue A  -> 50% for label Y
     *  capacity of queue A1 -> 100% for label Y
     *
     *  Total resources available for label Y -> 20GB (nm2 and nm3)
     *  Hence in queue A1, max resource for label Y is 10GB.
     *
     *  AM resource percent config for queue A1 -> 0.25
     *        ==> 2.5Gb (3 Gb) is max-am-resource-limit
     */
    Assert.assertEquals(2, leafQueue.getNumActiveApplications());
    Assert.assertEquals(1, leafQueue.getNumPendingApplications());

    // Submit app3 with 1Gb AM resource to Queue B1 (no_label)
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b1")
            .withUnmanagedAM(false)
            .build();
    RMApp app3 = MockRMAppSubmitter.submit(rm1, data1);
    MockRM.launchAndRegisterAM(app3, rm1, nm5);

    // Submit another app with 1Gb AM resource to Queue B1 (no_label)
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("b1")
            .withUnmanagedAM(false)
            .build();
    MockRMAppSubmitter.submit(rm1, data);

    leafQueue = (LeafQueue) cs.getQueue("b1");
    Assert.assertNotNull(leafQueue);

    /*
     *  capacity of queue B  -> 90% for queue
     *                       -> and 100% for no-label
     *  capacity of queue B1 -> 50% for no-label/queue
     *
     *  Total resources available for no-label -> 10GB (nm5)
     *  Hence in queue B1, max resource for no-label is 5GB.
     *
     *  AM resource percent config for queue B1 -> 0.15
     *        ==> 1Gb is max-am-resource-limit
     *
     *  Only one app will be activated and all othe will be pending.
     */
    Assert.assertEquals(1, leafQueue.getNumActiveApplications());
    Assert.assertEquals(1, leafQueue.getNumPendingApplications());

    rm1.close();
  }

  @Test
  public void testHeadroom() throws Exception {
    /*
     * Test Case: Verify Headroom calculated is sum of headrooms for each
     * partition requested. So submit a app with requests for default partition
     * and 'x' partition, so the total headroom for the user should be sum of
     * the head room for both labels.
     */

    simpleNodeLabelMappingToManager();
    CapacitySchedulerConfiguration csConf =
        (CapacitySchedulerConfiguration) TestUtils
            .getComplexConfigurationWithQueueLabels(conf);
    csConf.setUserLimit(A1, 25);
    csConf.setUserLimit(B2, 25);

    YarnConfiguration conf = new YarnConfiguration();

    CapacitySchedulerContext csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getConf()).thenReturn(conf);
    when(csContext.getMinimumResourceCapability())
        .thenReturn(Resources.createResource(GB));
    when(csContext.getMaximumResourceCapability())
        .thenReturn(Resources.createResource(16 * GB));
    when(csContext.getResourceCalculator()).thenReturn(resourceCalculator);
    RMContext rmContext = TestUtils.getMockRMContext();
    RMContext spyRMContext = spy(rmContext);
    when(spyRMContext.getNodeLabelManager()).thenReturn(mgr);
    when(csContext.getRMContext()).thenReturn(spyRMContext);
    when(csContext.getPreemptionManager()).thenReturn(new PreemptionManager());
    CapacitySchedulerQueueManager queueManager =
        new CapacitySchedulerQueueManager(csConf, mgr, null);
    when(csContext.getCapacitySchedulerQueueManager()).thenReturn(queueManager);

    // Setup nodelabels
    queueManager.reinitConfiguredNodeLabels(csConf);

    mgr.activateNode(NodeId.newInstance("h0", 0),
        Resource.newInstance(160 * GB, 16)); // default Label
    mgr.activateNode(NodeId.newInstance("h1", 0),
        Resource.newInstance(160 * GB, 16)); // label x
    mgr.activateNode(NodeId.newInstance("h2", 0),
        Resource.newInstance(160 * GB, 16)); // label y

    // Say cluster has 100 nodes of 16G each
    Resource clusterResource = Resources.createResource(160 * GB);
    when(csContext.getClusterResource()).thenReturn(clusterResource);

    CapacitySchedulerQueueContext queueContext = new CapacitySchedulerQueueContext(csContext);

    CSQueueStore queues = new CSQueueStore();
    CSQueue rootQueue = CapacitySchedulerQueueManager.parseQueue(queueContext,
        csConf, null, "root", queues, queues, TestUtils.spyHook);
    queueManager.setRootQueue(rootQueue);
    rootQueue.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    // Manipulate queue 'a'
    LeafQueue queue = TestLeafQueue.stubLeafQueue((LeafQueue) queues.get("b2"));
    queue.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    String rack_0 = "rack_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode("h0", rack_0, 0, 160 * GB);
    FiCaSchedulerNode node_1 = TestUtils.getMockNode("h1", rack_0, 0, 160 * GB);

    final String user_0 = "user_0";
    final String user_1 = "user_1";

    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

    ConcurrentMap<ApplicationId, RMApp> spyApps =
        spy(new ConcurrentHashMap<ApplicationId, RMApp>());
    RMApp rmApp = mock(RMApp.class);
    ResourceRequest amResourceRequest = mock(ResourceRequest.class);
    Resource amResource = Resources.createResource(0, 0);
    when(amResourceRequest.getCapability()).thenReturn(amResource);
    when(rmApp.getAMResourceRequests()).thenReturn(
        Collections.singletonList(amResourceRequest));
    Mockito.doReturn(rmApp)
        .when(spyApps).get(ArgumentMatchers.<ApplicationId>any());
    when(spyRMContext.getRMApps()).thenReturn(spyApps);
    RMAppAttempt rmAppAttempt = mock(RMAppAttempt.class);
    when(rmApp.getRMAppAttempt(any()))
        .thenReturn(rmAppAttempt);
    when(rmApp.getCurrentAppAttempt()).thenReturn(rmAppAttempt);
    Mockito.doReturn(rmApp)
        .when(spyApps).get(ArgumentMatchers.<ApplicationId>any());
    Mockito.doReturn(true).when(spyApps)
        .containsKey(ArgumentMatchers.<ApplicationId>any());

    Priority priority_1 = TestUtils.createMockPriority(1);

    // Submit first application with some resource-requests from user_0,
    // and check headroom
    final ApplicationAttemptId appAttemptId_0_0 =
        TestUtils.getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0_0 = new FiCaSchedulerApp(appAttemptId_0_0, user_0,
        queue, queue.getAbstractUsersManager(), spyRMContext);
    queue.submitApplicationAttempt(app_0_0, user_0);

    List<ResourceRequest> app_0_0_requests = new ArrayList<ResourceRequest>();
    app_0_0_requests.add(TestUtils.createResourceRequest(ResourceRequest.ANY,
        1 * GB, 2, true, priority_1, recordFactory));
    app_0_0.updateResourceRequests(app_0_0_requests);

    // Schedule to compute
    queue.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    //head room = queue capacity = 50 % 90% 160 GB * 0.25 (UL)
    Resource expectedHeadroom =
        Resources.createResource((int) (0.5 * 0.9 * 160 * 0.25) * GB, 1);
    assertEquals(expectedHeadroom, app_0_0.getHeadroom());

    // Submit second application from user_0, check headroom
    final ApplicationAttemptId appAttemptId_0_1 =
        TestUtils.getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_0_1 = new FiCaSchedulerApp(appAttemptId_0_1, user_0,
        queue, queue.getAbstractUsersManager(), spyRMContext);
    queue.submitApplicationAttempt(app_0_1, user_0);

    List<ResourceRequest> app_0_1_requests = new ArrayList<ResourceRequest>();
    app_0_1_requests.add(TestUtils.createResourceRequest(ResourceRequest.ANY,
        1 * GB, 2, true, priority_1, recordFactory));
    app_0_1.updateResourceRequests(app_0_1_requests);

    app_0_1_requests.clear();
    app_0_1_requests.add(TestUtils.createResourceRequest(ResourceRequest.ANY,
        1 * GB, 2, true, priority_1, recordFactory, "y"));
    app_0_1.updateResourceRequests(app_0_1_requests);

    // Schedule to compute
    queue.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY); // Schedule to compute
    queue.assignContainers(clusterResource, node_1,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY); // Schedule to compute
    assertEquals(expectedHeadroom, app_0_0.getHeadroom());// no change
    //head room for default label + head room for y partition
    //head room for y partition = 100% 50%(b queue capacity ) *  160 * GB
    Resource expectedHeadroomWithReqInY = Resources.add(
        Resources.createResource((int) (0.25 * 0.5 * 160) * GB, 1),
        expectedHeadroom);
    assertEquals(expectedHeadroomWithReqInY, app_0_1.getHeadroom());

    // Submit first application from user_1, check for new headroom
    final ApplicationAttemptId appAttemptId_1_0 =
        TestUtils.getMockApplicationAttemptId(2, 0);
    FiCaSchedulerApp app_1_0 = new FiCaSchedulerApp(appAttemptId_1_0, user_1,
        queue, queue.getAbstractUsersManager(), spyRMContext);
    queue.submitApplicationAttempt(app_1_0, user_1);

    List<ResourceRequest> app_1_0_requests = new ArrayList<ResourceRequest>();
    app_1_0_requests.add(TestUtils.createResourceRequest(ResourceRequest.ANY,
        1 * GB, 2, true, priority_1, recordFactory));
    app_1_0.updateResourceRequests(app_1_0_requests);

    app_1_0_requests.clear();
    app_1_0_requests.add(TestUtils.createResourceRequest(ResourceRequest.ANY,
        1 * GB, 2, true, priority_1, recordFactory, "y"));
    app_1_0.updateResourceRequests(app_1_0_requests);

    // Schedule to compute
    queue.assignContainers(clusterResource, node_0,
        new ResourceLimits(clusterResource),
        SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY); // Schedule to compute
    //head room = queue capacity = (50 % 90% 160 GB)/2 (for 2 users)
    expectedHeadroom =
        Resources.createResource((int) (0.5 * 0.9 * 160 * 0.25) * GB, 1);
    //head room for default label + head room for y partition
    //head room for y partition = 100% 50%(b queue capacity ) *  160 * GB
    expectedHeadroomWithReqInY = Resources.add(
        Resources.createResource((int) (0.25 * 0.5 * 160) * GB, 1),
        expectedHeadroom);
    assertEquals(expectedHeadroom, app_0_0.getHeadroom());
    assertEquals(expectedHeadroomWithReqInY, app_0_1.getHeadroom());
    assertEquals(expectedHeadroomWithReqInY, app_1_0.getHeadroom());


  }

  /**
   * {@link LeafQueue#activateApplications()} should validate values of all
   * resourceTypes before activating application.
   *
   * @throws Exception
   */
  @Test
  public void testAMLimitByAllResources() throws Exception {
    CapacitySchedulerConfiguration csconf =
        new CapacitySchedulerConfiguration();
    csconf.setResourceComparator(DominantResourceCalculator.class);
    String queueName = "a1";
    csconf.setQueues(ROOT,
        new String[] {queueName});
    csconf.setCapacity(new QueuePath("root.a1"), 100);

    ResourceInformation res0 = ResourceInformation.newInstance("memory-mb",
        ResourceInformation.MEMORY_MB.getUnits(), GB, Long.MAX_VALUE);
    ResourceInformation res1 = ResourceInformation.newInstance("vcores",
        ResourceInformation.VCORES.getUnits(), 1, Integer.MAX_VALUE);
    ResourceInformation res2 = ResourceInformation.newInstance("gpu",
        ResourceInformation.GPUS.getUnits(), 0, Integer.MAX_VALUE);
    Map<String, ResourceInformation> riMap = new HashMap<>();
    riMap.put(ResourceInformation.MEMORY_URI, res0);
    riMap.put(ResourceInformation.VCORES_URI, res1);
    riMap.put(ResourceInformation.GPU_URI, res2);
    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);

    YarnConfiguration config = new YarnConfiguration(csconf);
    config.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    config.setBoolean(TestResourceProfiles.TEST_CONF_RESET_RESOURCE_TYPES,
        false);

    MockRM rm = new MockRM(config);
    rm.start();

    Map<String, Long> res = new HashMap<>();
    res.put("gpu", 0L);

    Resource clusterResource = Resource.newInstance(16 * GB, 64, res);

    // Cluster Resource - 16GB, 64vcores
    // AMLimit 16384 x .1 mb , 64 x .1 vcore
    // Effective AM limit after normalized to minimum resource 2048,7

    rm.registerNode("127.0.0.1:1234", clusterResource);

    String userName = "user_0";
    ResourceScheduler scheduler = rm.getRMContext().getScheduler();
    LeafQueue queueA = (LeafQueue) ((CapacityScheduler) scheduler)
        .getQueue(queueName);

    Resource amResource = Resource.newInstance(GB, 1);

    MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithResource(amResource, rm)
            .withAppName("app-1")
            .withUser(userName)
            .withAcls(null)
            .withQueue(queueName)
            .build());
    MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithResource(amResource, rm)
            .withAppName("app-2")
            .withUser(userName)
            .withAcls(null)
            .withQueue(queueName)
            .build());

    // app-3 should not be activated as amLimit will be reached
    // for memory
    MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithResource(amResource, rm)
            .withAppName("app-3")
            .withUser(userName)
            .withAcls(null)
            .withQueue(queueName)
            .build());

    Assert.assertEquals("PendingApplications should be 1", 1,
        queueA.getNumPendingApplications());
    Assert.assertEquals("Active applications should be 2", 2,
        queueA.getNumActiveApplications());
    // AMLimit is 2048,7
    Assert.assertEquals(2048,
        queueA.getQueueResourceUsage().getAMLimit().getMemorySize());
    Assert.assertEquals(7,
        queueA.getQueueResourceUsage().getAMLimit().getVirtualCores());
    // Used AM Resource is 2048,2
    Assert.assertEquals(2048,
        queueA.getQueueResourceUsage().getAMUsed().getMemorySize());
    Assert.assertEquals(2,
        queueA.getQueueResourceUsage().getAMUsed().getVirtualCores());

    rm.close();

  }

  @Test(timeout = 120000)
  public void testDiagnosticWhenAMActivated() throws Exception {
    /*
     * Test Case:
     * Verify AM resource limit per partition level and per queue level.
     * Queue a1 supports labels (x,y). Configure am-resource-limit as 0.2 (x)
     * Queue a1 for label X can only support 2GB AM resource.
     *
     * Verify that 'AMResource request info' should be displayed whether AM
     * is in state 'activated' or 'not activated'
     */

    simpleNodeLabelMappingToManager();
    CapacitySchedulerConfiguration config = (CapacitySchedulerConfiguration)
         TestUtils.getConfigurationWithQueueLabels(conf);

    // After getting queue conf, configure AM resource percent for Queue a1
    // as 0.2 (Label X)
    config.setMaximumAMResourcePercentPerPartition(A1, "x", 0.2f);

    // Now inject node label manager with this updated config.
    MockRM rm = new MockRM(config) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();
    rm.registerNode("h1:1234", 10 * GB); // label = x
    rm.registerNode("h2:1234", 10 * GB); // label = y
    rm.registerNode("h3:1234", 10 * GB); // label = <empty>

    // Submit app1 with 1GB AM resource to Queue a1 for label X
    long amMemoryMB = GB;
    MockRMAppSubmissionData data1 =
         MockRMAppSubmissionData.Builder.createWithMemory(amMemoryMB, rm)
             .withAppName("app")
             .withUser("user")
             .withAcls(null)
             .withQueue("a1")
             .withAmLabel("x")
             .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data1);

    // Submit app2 with 1GB AM resource to Queue a1 for label X
    MockRMAppSubmissionData data2 =
         MockRMAppSubmissionData.Builder.createWithMemory(amMemoryMB, rm)
             .withAppName("app")
             .withUser("user")
             .withAcls(null)
             .withQueue("a1")
             .withAmLabel("x")
             .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm, data2);

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    LeafQueue leafQueue = (LeafQueue) cs.getQueue("a1");
    Assert.assertNotNull(leafQueue);

    // Only one AM will be activated here, and second AM will be not activated.
    // The expectation: in either case, should be shown AMResource request info.
    Assert.assertEquals(2, leafQueue.getNumActiveApplications());

    String activatedDiagnostics="AM Resource Request = <memory:" + amMemoryMB;
    Assert.assertTrue("Still doesn't show AMResource " +
        "when application is activated", app1.getDiagnostics()
         .toString().contains(activatedDiagnostics));
    Assert.assertTrue("Doesn't show AMResource " +
        "when application is not activated", app2.getDiagnostics()
         .toString().contains(activatedDiagnostics));
    rm.close();
  }
}
