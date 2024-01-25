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

import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueStateManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test QueueStateManager.
 *
 */
public class TestQueueStateManager {
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

  @Test
  public void testQueueStateManager() throws AccessControlException,
      YarnException {
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

    @SuppressWarnings("rawtypes")
    QueueStateManager stateManager = cs.getCapacitySchedulerQueueManager()
        .getQueueStateManager();

    //by default, the state of both queues should be RUNNING
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q1).getState());
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q2).getState());
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q3).getState());

    // Stop Q2, and verify that Q2 transmits to STOPPED STATE
    stateManager.stopQueue(Q2);
    Assert.assertEquals(QueueState.STOPPED, cs.getQueue(Q2).getState());

    // Stop Q1, and verify that Q1, as well as its child: Q3,
    // transmits to STOPPED STATE
    stateManager.stopQueue(Q1);
    Assert.assertEquals(QueueState.STOPPED, cs.getQueue(Q1).getState());
    Assert.assertEquals(QueueState.STOPPED, cs.getQueue(Q3).getState());

    Assert.assertTrue(stateManager.canDelete(Q1));
    Assert.assertTrue(stateManager.canDelete(Q2));
    Assert.assertTrue(stateManager.canDelete(Q3));

    // Active Q2, it will fail.
    Assert.assertEquals(QueueState.STOPPED, cs.getQueue(Q2).getState());

    // Now active Q1
    stateManager.activateQueue(Q1);
    // Q1 should be in RUNNING state. Its children: Q2 and Q3
    // should still be in STOPPED state.
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q1).getState());
    Assert.assertEquals(QueueState.STOPPED, cs.getQueue(Q2).getState());
    Assert.assertEquals(QueueState.STOPPED, cs.getQueue(Q3).getState());

    // Now active Q2 and Q3
    stateManager.activateQueue(Q2);
    stateManager.activateQueue(Q3);
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q2).getState());
    Assert.assertEquals(QueueState.RUNNING, cs.getQueue(Q3).getState());

    Assert.assertFalse(stateManager.canDelete(Q1));
    Assert.assertFalse(stateManager.canDelete(Q2));
    Assert.assertFalse(stateManager.canDelete(Q3));

    ApplicationId appId = ApplicationId.newInstance(
        System.currentTimeMillis(), 1);
    String userName = "testUser";
    cs.getQueue(Q2).submitApplication(appId, userName, Q2);
    FiCaSchedulerApp app = getMockApplication(appId, userName,
          Resources.createResource(4, 0));
    cs.getQueue(Q2).submitApplicationAttempt(app, userName);
    stateManager.stopQueue(Q1);

    Assert.assertEquals(QueueState.DRAINING, cs.getQueue(Q1).getState());
    Assert.assertEquals(QueueState.DRAINING, cs.getQueue(Q2).getState());
    Assert.assertEquals(QueueState.STOPPED, cs.getQueue(Q3).getState());

    cs.getQueue(Q2).finishApplicationAttempt(app, Q2);
    cs.getQueue(Q2).finishApplication(appId, userName);

    Assert.assertEquals(QueueState.STOPPED, cs.getQueue(Q1).getState());
    Assert.assertEquals(QueueState.STOPPED, cs.getQueue(Q2).getState());
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
}
