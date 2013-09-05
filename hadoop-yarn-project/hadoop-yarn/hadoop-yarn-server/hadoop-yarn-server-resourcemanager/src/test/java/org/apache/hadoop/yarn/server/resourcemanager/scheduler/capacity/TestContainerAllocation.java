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

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.TestFifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.junit.Test;


public class TestContainerAllocation {

  private static final Log LOG = LogFactory.getLog(TestFifoScheduler.class);

  private final int GB = 1024;

  @Test(timeout = 3000000)
  public void testExcessReservationThanNodeManagerCapacity() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    MockRM rm = new MockRM(conf);
    rm.start();

    // Register node1
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 2 * GB, 4);
    MockNM nm2 = rm.registerNode("127.0.0.1:2234", 3 * GB, 4);

    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);

    // wait..
    int waitCount = 20;
    int size = rm.getRMContext().getRMNodes().size();
    while ((size = rm.getRMContext().getRMNodes().size()) != 2
        && waitCount-- > 0) {
      LOG.info("Waiting for node managers to register : " + size);
      Thread.sleep(100);
    }
    Assert.assertEquals(2, rm.getRMContext().getRMNodes().size());
    // Submit an application
    RMApp app1 = rm.submitApp(128);

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    LOG.info("sending container requests ");
    am1.addRequests(new String[] {"*"}, 3 * GB, 1, 1);
    AllocateResponse alloc1Response = am1.schedule(); // send the request

    // kick the scheduler
    nm1.nodeHeartbeat(true);
    int waitCounter = 20;
    LOG.info("heartbeating nm1");
    while (alloc1Response.getAllocatedContainers().size() < 1
        && waitCounter-- > 0) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(500);
      alloc1Response = am1.schedule();
    }
    LOG.info("received container : "
        + alloc1Response.getAllocatedContainers().size());

    // No container should be allocated.
    // Internally it should not been reserved.
    Assert.assertTrue(alloc1Response.getAllocatedContainers().size() == 0);

    LOG.info("heartbeating nm2");
    waitCounter = 20;
    nm2.nodeHeartbeat(true);
    while (alloc1Response.getAllocatedContainers().size() < 1
        && waitCounter-- > 0) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(500);
      alloc1Response = am1.schedule();
    }
    LOG.info("received container : "
        + alloc1Response.getAllocatedContainers().size());
    Assert.assertTrue(alloc1Response.getAllocatedContainers().size() == 1);

    rm.stop();
  }
}