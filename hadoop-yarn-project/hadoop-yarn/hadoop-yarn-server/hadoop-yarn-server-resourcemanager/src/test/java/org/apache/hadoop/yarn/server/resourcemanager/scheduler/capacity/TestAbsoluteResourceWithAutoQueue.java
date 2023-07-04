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

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager.NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueUtils.EPSILON;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.queuemanagement.GuaranteedOrZeroCapacityOverTimePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicy;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAbsoluteResourceWithAutoQueue
    extends TestCapacitySchedulerAutoCreatedQueueBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestAbsoluteResourceWithAutoQueue.class);

  private static final String QUEUEA = "queueA";
  private static final String QUEUEB = "queueB";
  private static final String QUEUEC = "queueC";
  private static final String QUEUED = "queueD";

  private static final String QUEUEA_FULL =
      CapacitySchedulerConfiguration.ROOT + "." + QUEUEA;
  private static final String QUEUEB_FULL =
      CapacitySchedulerConfiguration.ROOT + "." + QUEUEB;
  private static final String QUEUEC_FULL =
      CapacitySchedulerConfiguration.ROOT + "." + QUEUEC;
  private static final String QUEUED_FULL =
      CapacitySchedulerConfiguration.ROOT + "." + QUEUED;

  private static final Resource QUEUE_A_MINRES =
      Resource.newInstance(100 * GB, 10);
  private static final Resource QUEUE_A_MAXRES =
      Resource.newInstance(200 * GB, 30);
  private static final Resource QUEUE_B_MINRES =
      Resource.newInstance(50 * GB, 10);
  private static final Resource QUEUE_B_MAXRES =
      Resource.newInstance(150 * GB, 30);
  private static final Resource QUEUE_C_MINRES =
      Resource.newInstance(25 * GB, 5);
  private static final Resource QUEUE_C_MAXRES =
      Resource.newInstance(150 * GB, 20);
  private static final Resource QUEUE_D_MINRES =
      Resource.newInstance(25 * GB, 5);
  private static final Resource QUEUE_D_MAXRES =
      Resource.newInstance(150 * GB, 20);

  @Before
  public void setUp() throws Exception {

    accessibleNodeLabelsOnC.add(NO_LABEL);
  }

  @After
  public void tearDown() {
    if (mockRM != null) {
      mockRM.stop();
    }
  }

  private CapacitySchedulerConfiguration setupMinMaxResourceConfiguration(
      CapacitySchedulerConfiguration csConf) {
    // Update min/max resource to queueA/B/C
    csConf.setMinimumResourceRequirement("", new QueuePath(QUEUEA_FULL), QUEUE_A_MINRES);
    csConf.setMinimumResourceRequirement("", new QueuePath(QUEUEB_FULL), QUEUE_B_MINRES);
    csConf.setMinimumResourceRequirement("", new QueuePath(QUEUEC_FULL), QUEUE_C_MINRES);
    csConf.setMinimumResourceRequirement("", new QueuePath(QUEUED_FULL), QUEUE_D_MINRES);

    csConf.setMaximumResourceRequirement("", new QueuePath(QUEUEA_FULL), QUEUE_A_MAXRES);
    csConf.setMaximumResourceRequirement("", new QueuePath(QUEUEB_FULL), QUEUE_B_MAXRES);
    csConf.setMaximumResourceRequirement("", new QueuePath(QUEUEC_FULL), QUEUE_C_MAXRES);
    csConf.setMaximumResourceRequirement("", new QueuePath(QUEUED_FULL), QUEUE_D_MAXRES);

    return csConf;
  }

  public static CapacitySchedulerConfiguration setupQueueConfiguration(
      CapacitySchedulerConfiguration conf) {

    return conf;
  }

  private CapacitySchedulerConfiguration setupSimpleQueueConfiguration(
      boolean isCapacityNeeded) {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { QUEUEA, QUEUEB, QUEUEC, QUEUED });

    // Set default capacities like normal configuration.
    if (isCapacityNeeded) {
      csConf.setCapacity(QUEUEA_FULL, 50f);
      csConf.setCapacity(QUEUEB_FULL, 25f);
      csConf.setCapacity(QUEUEC_FULL, 25f);
      csConf.setCapacity(QUEUED_FULL, 25f);
    }

    csConf.setAutoCreateChildQueueEnabled(QUEUEC_FULL, true);

    csConf.setAutoCreatedLeafQueueTemplateCapacityByLabel(QUEUEC_FULL, "",
        QUEUE_C_MINRES);
    csConf.setAutoCreatedLeafQueueTemplateMaxCapacity(QUEUEC_FULL, "",
        QUEUE_C_MAXRES);

    csConf.setAutoCreateChildQueueEnabled(QUEUED_FULL, true);

    // Setup leaf queue template configs
    csConf.setAutoCreatedLeafQueueTemplateCapacityByLabel(QUEUED_FULL, "",
        Resource.newInstance(10 * GB, 2));
    csConf.setAutoCreatedLeafQueueTemplateMaxCapacity(QUEUED_FULL, "",
        QUEUE_D_MAXRES);

    return csConf;
  }

  @Test(timeout = 20000)
  public void testAutoCreateLeafQueueCreation() throws Exception {

    try {

      CapacitySchedulerConfiguration csConf =
          setupSimpleQueueConfiguration(false);
      setupMinMaxResourceConfiguration(csConf);

      csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      csConf.setOverrideWithQueueMappings(true);

      mockRM = new MockRM(csConf);
      cs = (CapacityScheduler) mockRM.getResourceScheduler();

      mockRM.start();
      cs.start();

      // Add few nodes
      mockRM.registerNode("127.0.0.1:1234", 250 * GB, 40);

      setupGroupQueueMappings(QUEUED, cs.getConfiguration(), "%user");
      cs.reinitialize(cs.getConfiguration(), mockRM.getRMContext());

      submitApp(mockRM, cs.getQueue(QUEUED), TEST_GROUPUSER, TEST_GROUPUSER, 1,
          1);
      AutoCreatedLeafQueue autoCreatedLeafQueue =
          (AutoCreatedLeafQueue) cs.getQueue(TEST_GROUPUSER);
      ManagedParentQueue parentQueue = (ManagedParentQueue) cs.getQueue(QUEUED);
      assertEquals(parentQueue, autoCreatedLeafQueue.getParent());

      validateCapacities(autoCreatedLeafQueue, 0.4f, 0.04f, 1f, 0.6f);
      validateCapacitiesByLabel(parentQueue, autoCreatedLeafQueue, NO_LABEL);

      Map<String, Float> expectedChildQueueAbsCapacity =
          new HashMap<String, Float>() {
            {
              put(NO_LABEL, 0.04f);
            }
          };

      validateInitialQueueEntitlement(parentQueue, TEST_GROUPUSER,
          expectedChildQueueAbsCapacity, new HashSet<String>() {
            {
              add(NO_LABEL);
            }
          });

      validateUserAndAppLimits(autoCreatedLeafQueue, 400, 400);
      assertTrue(autoCreatedLeafQueue
          .getOrderingPolicy() instanceof FifoOrderingPolicy);

      ApplicationId user1AppId = submitApp(mockRM, cs.getQueue(QUEUED),
          TEST_GROUPUSER1, TEST_GROUPUSER1, 2, 1);
      AutoCreatedLeafQueue autoCreatedLeafQueue1 =
          (AutoCreatedLeafQueue) cs.getQueue(TEST_GROUPUSER1);

      validateCapacities((AutoCreatedLeafQueue) autoCreatedLeafQueue1, 0.4f,
          0.04f, 1f, 0.6f);
      validateCapacitiesByLabel((ManagedParentQueue) parentQueue,
          (AutoCreatedLeafQueue) autoCreatedLeafQueue1, NO_LABEL);

      assertEquals(parentQueue, autoCreatedLeafQueue1.getParent());

      Map<String, Float> expectedChildQueueAbsCapacity1 =
          new HashMap<String, Float>() {
            {
              put(NO_LABEL, 0.08f);
            }
          };

      validateInitialQueueEntitlement(parentQueue, TEST_GROUPUSER1,
          expectedChildQueueAbsCapacity1, new HashSet<String>() {
            {
              add(NO_LABEL);
            }
          });

      submitApp(mockRM, cs.getQueue(QUEUED), TEST_GROUPUSER2, TEST_GROUPUSER2,
          3, 1);

      final CSQueue autoCreatedLeafQueue2 = cs.getQueue(TEST_GROUPUSER2);
      validateCapacities((AutoCreatedLeafQueue) autoCreatedLeafQueue2, 0.0f,
          0.0f, 1f, 0.6f);
      validateCapacities((AutoCreatedLeafQueue) autoCreatedLeafQueue1, 0.4f,
          0.04f, 1f, 0.6f);
      validateCapacities((AutoCreatedLeafQueue) autoCreatedLeafQueue, 0.4f,
          0.04f, 1f, 0.6f);

      GuaranteedOrZeroCapacityOverTimePolicy autoCreatedQueueManagementPolicy =
          (GuaranteedOrZeroCapacityOverTimePolicy) ((ManagedParentQueue) parentQueue)
              .getAutoCreatedQueueManagementPolicy();

      assertEquals(0.08f, autoCreatedQueueManagementPolicy
          .getAbsoluteActivatedChildQueueCapacity(NO_LABEL), EPSILON);

      cs.killAllAppsInQueue(TEST_GROUPUSER1);
      mockRM.waitForState(user1AppId, RMAppState.KILLED);

      List<QueueManagementChange> queueManagementChanges =
          autoCreatedQueueManagementPolicy.computeQueueManagementChanges();

      ManagedParentQueue managedParentQueue = (ManagedParentQueue) parentQueue;
      managedParentQueue
          .validateAndApplyQueueManagementChanges(queueManagementChanges);

      validateDeactivatedQueueEntitlement(parentQueue, TEST_GROUPUSER1,
          expectedChildQueueAbsCapacity1, queueManagementChanges);

      Set<String> expectedNodeLabelsUpdated = new HashSet<>();
      expectedNodeLabelsUpdated.add(NO_LABEL);

      validateActivatedQueueEntitlement(parentQueue, TEST_GROUPUSER2,
          expectedChildQueueAbsCapacity1, queueManagementChanges,
          expectedNodeLabelsUpdated);

    } finally {
      cleanupQueue(TEST_GROUPUSER);
      cleanupQueue(TEST_GROUPUSER1);
      cleanupQueue(TEST_GROUPUSER2);
    }
  }

  @Test(expected = Exception.class)
  public void testValidateLeafQueueTemplateConfigurations() {

    CapacitySchedulerConfiguration csConf = setupSimpleQueueConfiguration(true);

    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    mockRM = new MockRM(csConf);
    fail("Exception should be thrown as leaf queue template configuration is "
        + "not same as Parent configuration");
  }

  @Test(timeout = 20000)
  public void testApplicationRunningWithDRF() throws Exception {
    CapacitySchedulerConfiguration csConf =
        setupSimpleQueueConfiguration(false);
    setupMinMaxResourceConfiguration(csConf);
    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    // Validate Leaf Queue Template in Absolute Resource with DRF
    csConf.setResourceComparator(DominantResourceCalculator.class);
    setupGroupQueueMappings(QUEUED, csConf, "%user");

    mockRM = new MockRM(csConf);
    mockRM.start();

    MockNM nm1 = mockRM.registerNode("127.0.0.1:1234", 250 * GB, 40);

    // Submit a Application and validate if it is moving to RUNNING state
    RMApp app1 = MockRMAppSubmitter.submit(mockRM,
        MockRMAppSubmissionData.Builder.createWithMemory(1024, mockRM)
            .withAppName("app1")
            .withUser(TEST_GROUPUSER)
            .withAcls(null)
            .build());
    MockAM am1 = MockRM.launchAndRegisterAM(app1, mockRM, nm1);

    cs = (CapacityScheduler) mockRM.getResourceScheduler();
    AutoCreatedLeafQueue autoCreatedLeafQueue =
        (AutoCreatedLeafQueue) cs.getQueue(TEST_GROUPUSER);
    assertNotNull("Auto Creation of Queue failed", autoCreatedLeafQueue);
    ManagedParentQueue parentQueue = (ManagedParentQueue) cs.getQueue(QUEUED);
    assertEquals(parentQueue, autoCreatedLeafQueue.getParent());
  }
}
