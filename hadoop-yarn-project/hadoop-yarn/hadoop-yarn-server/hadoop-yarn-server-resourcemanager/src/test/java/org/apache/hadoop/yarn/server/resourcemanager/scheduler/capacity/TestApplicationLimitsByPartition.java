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


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt.AMState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class TestApplicationLimitsByPartition {
  final static int GB = 1024;

  LeafQueue queue;
  RMNodeLabelsManager mgr;
  private YarnConfiguration conf;

  RMContext rmContext = null;

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
    final String A1 = CapacitySchedulerConfiguration.ROOT + ".a" + ".a1";
    final String C1 = CapacitySchedulerConfiguration.ROOT + ".c" + ".c1";
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
    RMApp app1 = rm1.submitApp(GB, "app", "user", null, "a1", "x");

    // Submit app2 with 1Gb AM resource to Queue A1 for label X
    RMApp app2 = rm1.submitApp(GB, "app", "user", null, "a1", "x");

    // Submit 3rd app to Queue A1 for label X, and this will be pending as
    // AM limit is already crossed for label X. (2GB)
    RMApp pendingApp = rm1.submitApp(GB, "app", "user", null, "a1", "x");

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
    RMApp app3 = rm1.submitApp(GB, "app", "user", null, "c1");
    MockRM.launchAndRegisterAM(app3, rm1, nm3);

    // Submit next app to Queue C1 with empty label
    RMApp app4 = rm1.submitApp(GB, "app", "user", null, "c1");
    MockRM.launchAndRegisterAM(app4, rm1, nm3);

    // Submit 3rd app to Queue C1. This will be pending as Queue's am-limit
    // is reached.
    pendingApp = rm1.submitApp(GB, "app", "user", null, "c1");

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
    final String A1 = CapacitySchedulerConfiguration.ROOT + ".a" + ".a1";
    final String C1 = CapacitySchedulerConfiguration.ROOT + ".c" + ".c1";
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
    RMApp app1 = rm1.submitApp(2 * GB, "app", "user", null, "a1", "x");
    // This app must be activated eventhough the am-resource per-partition
    // limit is only for 1.5GB.
    MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // Submit 2nd app to label "X" with one GB and it must be pending since
    // am-resource per-partition limit is crossed (1.5 GB was the limit).
    rm1.submitApp(GB, "app", "user", null, "a1", "x");

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    LeafQueue leafQueue = (LeafQueue) cs.getQueue("a1");
    Assert.assertNotNull(leafQueue);

    // Only 1 app will be activated as am-limit for partition "x" is 0.15
    Assert.assertEquals(1, leafQueue.getNumActiveApplications());
    Assert.assertEquals(1, leafQueue.getNumPendingApplications());

    // Now verify the same test case in Queue C1 which takes default label
    // to see queue level am-resource-limit is still working as expected.

    // Submit an app to Queue C1 with empty label (2 GB)
    RMApp app3 = rm1.submitApp(2 * GB, "app", "user", null, "c1");
    // This app must be activated even though the am-resource per-queue
    // limit is only for 1.5GB
    MockRM.launchAndRegisterAM(app3, rm1, nm3);

    // Submit 2nd app to C1 (Default label, hence am-limit per-queue will be
    // considered).
    rm1.submitApp(GB, "app", "user", null, "c1");

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
    final String A1 = CapacitySchedulerConfiguration.ROOT + ".a" + ".a1";
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
    RMApp app1 = rm1.submitApp(2 * GB, "app", "user", null, "a1", "x");

    // Submit 2nd app to label "X" with one GB. Since queue am-limit is 2GB,
    // 2nd app will be pending and first one will get activated.
    RMApp pendingApp = rm1.submitApp(GB, "app", "user", null, "a1", "x");

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
    final String A1 = CapacitySchedulerConfiguration.ROOT + ".a" + ".a1";
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
    RMApp app1 = rm1.submitApp(GB, "app", user_0, null, "a1", "x");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // Place few allocate requests to make it an active application
    am1.allocate("*", 1 * GB, 15, new ArrayList<ContainerId>(), "");

    // Now submit 2nd app to Queue A1 for label X for user1
    RMApp app2 = rm1.submitApp(GB, "app", user_1, null, "a1", "x");
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
    RMApp app3 = rm1.submitApp(GB, "app", user_1, null, "a1", "x");
    MockAM am2 = MockRM.launchAndRegisterAM(app3, rm1, nm1);

    // Place few allocate requests to make it an active application. This is
    // to ensure that user1 and user0 are active users.
    am2.allocate("*", 1 * GB, 10, new ArrayList<ContainerId>(), "");

    // Submit final app to Queue A1 for label X. Since we are trying to submit
    // for user1, we need 3Gb resource for AMs.
    // 4Gb -> 40% of label "X" in queue A1
    // Since we have 2 users, 50% of 4Gb will be max for each user. Here user1
    // has already crossed this 2GB limit, hence this app will be pending.
    RMApp pendingApp = rm1.submitApp(GB, "app", user_1, null, "a1", "x");

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
    final String A1 = CapacitySchedulerConfiguration.ROOT + ".a" + ".a1";
    final String B1 = CapacitySchedulerConfiguration.ROOT + ".b" + ".b1";
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
    RMApp app1 = rm1.submitApp(2 * GB, "app", "user", null, "a1", "y");
    MockRM.launchAndRegisterAM(app1, rm1, nm2);

    // Submit app2 with 1Gb AM resource to Queue A1 for label Y
    RMApp app2 = rm1.submitApp(GB, "app", "user", null, "a1", "y");
    MockRM.launchAndRegisterAM(app2, rm1, nm3);

    // Submit another app with 1Gb AM resource to Queue A1 for label Y
    rm1.submitApp(GB, "app", "user", null, "a1", "y");

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
    RMApp app3 = rm1.submitApp(GB, "app", "user", null, "b1");
    MockRM.launchAndRegisterAM(app3, rm1, nm5);

    // Submit another app with 1Gb AM resource to Queue B1 (no_label)
    rm1.submitApp(GB, "app", "user", null, "b1");

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
}
