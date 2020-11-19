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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test Queue States.
 */
public class TestQueueState {

  private static final String Q1 = "q1";
  private static final String Q2 = "q2";
  private static final String Q3 = "q3";

  private final static String Q1_PATH =
      CapacitySchedulerConfiguration.ROOT + "." + Q1;
  private final static String Q2_PATH =
      Q1_PATH + "." + Q2;
  private final static String Q3_PATH =
      Q1_PATH + "." + Q3;
  private CapacityScheduler cs;
  private YarnConfiguration conf;

  @Test (timeout = 15000)
  public void testQueueState() throws IOException {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {Q1});
    csConf.setQueues(Q1_PATH, new String[] {Q2});

    csConf.setCapacity(Q1_PATH, 100);
    csConf.setCapacity(Q2_PATH, 100);

    conf = new YarnConfiguration(csConf);
    cs = new CapacityScheduler();

    RMContext rmContext = TestUtils.getMockRMContext();
    cs.setConf(conf);
    cs.setRMContext(rmContext);
    cs.init(conf);

    //by default, the state of both queues should be RUNNING
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q1).getState());
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q2).getState());

    // Change the state of Q1 to STOPPED, and re-initiate the CS
    csConf.setState(Q1_PATH, QueueState.STOPPED);
    conf = new YarnConfiguration(csConf);
    cs.reinitialize(conf, rmContext);
    // The state of Q1 and its child: Q2 should be STOPPED
    Assert.assertEquals(QueueState.STOPPED, cs.getQueue(Q1).getState());
    Assert.assertEquals(QueueState.STOPPED, cs.getQueue(Q2).getState());

    // Change the state of Q1 to RUNNING, and change the state of Q2 to STOPPED
    csConf.setState(Q1_PATH, QueueState.RUNNING);
    csConf.setState(Q2_PATH, QueueState.STOPPED);
    conf = new YarnConfiguration(csConf);
    // reinitialize the CS, the operation should be successful
    cs.reinitialize(conf, rmContext);
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q1).getState());
    Assert.assertEquals(QueueState.STOPPED, cs.getQueue(Q2).getState());

    // Change the state of Q1 to STOPPED, and change the state of Q2 to RUNNING
    csConf.setState(Q1_PATH, QueueState.STOPPED);
    csConf.setState(Q2_PATH, QueueState.RUNNING);
    conf = new YarnConfiguration(csConf);
    // reinitialize the CS, the operation should be failed.
    try {
      cs.reinitialize(conf, rmContext);
      Assert.fail("Should throw an Exception.");
    } catch (Exception ex) {
      Assert.assertTrue(ex.getCause().getMessage().contains(
          "The parent queue:root.q1 cannot be STOPPED as the child" +
          " queue:root.q1.q2 is in RUNNING state."));
    }
  }

  @Test(timeout = 15000)
  public void testQueueStateTransit() throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {Q1});
    csConf.setQueues(Q1_PATH, new String[] {Q2, Q3});

    csConf.setCapacity(Q1_PATH, 100);
    csConf.setCapacity(Q2_PATH, 50);
    csConf.setCapacity(Q3_PATH, 50);

    conf = new YarnConfiguration(csConf);
    cs = new CapacityScheduler();

    RMContext rmContext = TestUtils.getMockRMContext();
    cs.setConf(conf);
    cs.setRMContext(rmContext);
    cs.init(conf);

    //by default, the state of ALL queues should be RUNNING
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q1).getState());
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q2).getState());
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q3).getState());

    // submit an application to Q2
    ApplicationId appId = ApplicationId.newInstance(
            System.currentTimeMillis(), 1);
    String userName = "testUser";
    cs.getQueue(Q2).submitApplication(appId, userName, Q2);
    FiCaSchedulerApp app = getMockApplication(appId, userName,
        Resources.createResource(4, 0));
    cs.getQueue(Q2).submitApplicationAttempt(app, userName);

    // set Q2 state to stop and do reinitialize.
    csConf.setState(Q2_PATH, QueueState.STOPPED);
    conf = new YarnConfiguration(csConf);
    cs.reinitialize(conf, rmContext);
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q1).getState());
    Assert.assertEquals(QueueState.DRAINING, cs.getQueue(Q2).getState());
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q3).getState());

    // set Q2 state to RUNNING and do reinitialize.
    // Q2 should transit from DRAINING to RUNNING
    csConf.setState(Q2_PATH, QueueState.RUNNING);
    conf = new YarnConfiguration(csConf);
    cs.reinitialize(conf, rmContext);
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q1).getState());
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q2).getState());
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q3).getState());

    // set Q2 state to stop and do reinitialize.
    csConf.setState(Q2_PATH, QueueState.STOPPED);
    conf = new YarnConfiguration(csConf);
    cs.reinitialize(conf, rmContext);
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q1).getState());
    Assert.assertEquals(QueueState.DRAINING, cs.getQueue(Q2).getState());
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q3).getState());

    // set Q1 state to stop and do reinitialize.
    csConf.setState(Q1_PATH, QueueState.STOPPED);
    conf = new YarnConfiguration(csConf);
    cs.reinitialize(conf, rmContext);
    Assert.assertEquals(QueueState.DRAINING, cs.getQueue(Q1).getState());
    Assert.assertEquals(QueueState.DRAINING, cs.getQueue(Q2).getState());
    Assert.assertEquals(QueueState.STOPPED, cs.getQueue(Q3).getState());

    // Active Q3, should fail
    csConf.setState(Q3_PATH, QueueState.RUNNING);
    conf = new YarnConfiguration(csConf);
    try {
      cs.reinitialize(conf, rmContext);
      Assert.fail("Should throw an Exception.");
    } catch (Exception ex) {
      // Do Nothing
    }

    // stop the app running in q2
    cs.getQueue(Q2).finishApplicationAttempt(app, Q2);
    cs.getQueue(Q2).finishApplication(appId, userName);
    Assert.assertEquals(QueueState.STOPPED, cs.getQueue(Q1).getState());
    Assert.assertEquals(QueueState.STOPPED, cs.getQueue(Q2).getState());
    Assert.assertEquals(QueueState.STOPPED, cs.getQueue(Q3).getState());
    
  }

  private FiCaSchedulerApp getMockApplication(ApplicationId appId, String user,
      Resource amResource) {
    FiCaSchedulerApp application = mock(FiCaSchedulerApp.class);
    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(appId, 0);
    doReturn(applicationAttemptId.getApplicationId()).
        when(application).getApplicationId();
    doReturn(applicationAttemptId).when(application).getApplicationAttemptId();
    doReturn(user).when(application).getUser();
    doReturn(amResource).when(application).getAMResource();
    doReturn(Priority.newInstance(0)).when(application).getPriority();
    doReturn(CommonNodeLabelsManager.NO_LABEL).when(application)
        .getAppAMNodePartitionName();
    doReturn(amResource).when(application).getAMResource(
        CommonNodeLabelsManager.NO_LABEL);
    when(application.compareInputOrderTo(any(FiCaSchedulerApp.class)))
        .thenCallRealMethod();
    when(application.isRunnable()).thenReturn(true);
    return application;
  }

  @Test (timeout = 30000)
  public void testRecoverDrainingStateAfterRMRestart() throws Exception {
    // init conf
    CapacitySchedulerConfiguration newConf =
        new CapacitySchedulerConfiguration();
    newConf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    newConf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED,
        false);
    newConf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    newConf.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 1);
    newConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[]{Q1});
    newConf.setQueues(Q1_PATH, new String[]{Q2});
    newConf.setCapacity(Q1_PATH, 100);
    newConf.setCapacity(Q2_PATH, 100);

    // init state store
    MemoryRMStateStore newMemStore = new MemoryRMStateStore();
    newMemStore.init(newConf);
    // init RM & NMs & Nodes
    MockRM rm = new MockRM(newConf, newMemStore);
    rm.start();
    MockNM nm = rm.registerNode("h1:1234", 204800);

    // submit an app, AM is running on nm1
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
            .withAppName("appname")
            .withUser("appuser")
            .withAcls(null)
            .withQueue(Q2)
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    MockRM.launchAM(app, rm, nm);
    rm.waitForState(app.getApplicationId(), RMAppState.ACCEPTED);
    // update queue state to STOPPED
    newConf.setState(Q1_PATH, QueueState.STOPPED);
    CapacityScheduler capacityScheduler =
        (CapacityScheduler) rm.getRMContext().getScheduler();
    capacityScheduler.reinitialize(newConf, rm.getRMContext());
    // current queue state should be DRAINING
    Assert.assertEquals(QueueState.DRAINING,
        capacityScheduler.getQueue(Q2).getState());
    Assert.assertEquals(QueueState.DRAINING,
        capacityScheduler.getQueue(Q1).getState());

    // RM restart
    rm = new MockRM(newConf, newMemStore);
    rm.start();
    rm.registerNode("h1:1234", 204800);

    // queue state should be DRAINING after app recovered
    rm.waitForState(app.getApplicationId(), RMAppState.ACCEPTED);
    capacityScheduler = (CapacityScheduler) rm.getRMContext().getScheduler();
    Assert.assertEquals(QueueState.DRAINING,
        capacityScheduler.getQueue(Q2).getState());
    Assert.assertEquals(QueueState.DRAINING,
        capacityScheduler.getQueue(Q1).getState());

    // close rm
    rm.close();
  }
}
