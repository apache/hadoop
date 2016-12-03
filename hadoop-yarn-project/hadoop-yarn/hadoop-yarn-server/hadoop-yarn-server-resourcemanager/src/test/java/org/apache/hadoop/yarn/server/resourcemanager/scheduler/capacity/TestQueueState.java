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
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test Queue States.
 */
public class TestQueueState {

  private static final String Q1 = "q1";
  private static final String Q2 = "q2";

  private final static String Q1_PATH =
      CapacitySchedulerConfiguration.ROOT + "." + Q1;
  private final static String Q2_PATH =
      Q1_PATH + "." + Q2;
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
          "The parent queue:q1 state is STOPPED, "
          + "child queue:q2 state cannot be RUNNING."));
    }
  }
}
