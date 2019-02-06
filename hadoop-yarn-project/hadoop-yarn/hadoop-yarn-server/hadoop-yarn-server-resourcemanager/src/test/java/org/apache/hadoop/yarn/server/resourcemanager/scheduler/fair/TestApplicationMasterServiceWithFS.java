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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test Application master service using Fair scheduler.
 */
public class TestApplicationMasterServiceWithFS {

  private static final Log LOG =
      LogFactory.getLog(TestApplicationMasterServiceWithFS.class);

  private static final int GB = 1024;
  private static final int MEMORY_ALLOCATION = 3 * GB;
  private static final String TEST_FOLDER = "test-queues";
  private AllocateResponse allocateResponse;
  private static YarnConfiguration configuration;

  @BeforeClass
  public static void setup() throws IOException {
    String allocFile =
        GenericTestUtils.getTestDir(TEST_FOLDER).getAbsolutePath();

    configuration = new YarnConfiguration();
    configuration.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
        ResourceScheduler.class);
    configuration.set(FairSchedulerConfiguration.ALLOCATION_FILE, allocFile);

    PrintWriter out = new PrintWriter(new FileWriter(allocFile));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("  <queue name=\"queueA\">");
    out.println(
        "   <maxContainerAllocation>2048 mb 1 vcores</maxContainerAllocation>");
    out.println("  </queue>");
    out.println("  <queue name=\"queueB\">");
    out.println(
        "   <maxContainerAllocation>3072 mb 1 vcores</maxContainerAllocation>");
    out.println("  </queue>");
    out.println("  <queue name=\"queueC\">");
    out.println("  </queue>");
    out.println("</allocations>");
    out.close();
  }

  @AfterClass
  public static void teardown(){
    File allocFile = GenericTestUtils.getTestDir(TEST_FOLDER);
    allocFile.delete();
  }

  @Test(timeout = 3000000)
  public void testQueueLevelContainerAllocationFail() throws Exception {
    MockRM rm = new MockRM(configuration);
    rm.start();

    // Register node1
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * GB);

    // Submit an application
    RMApp app1 = rm.submitApp(2 * GB, "queueA");

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    am1.addRequests(new String[] { "127.0.0.1" }, MEMORY_ALLOCATION, 1, 1);
    try {
      allocateResponse = am1.schedule(); // send the request
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof InvalidResourceRequestException);
      Assert.assertEquals(
          InvalidResourceRequestException.InvalidResourceType.GREATER_THEN_MAX_ALLOCATION,
          ((InvalidResourceRequestException) e).getInvalidResourceType());

    } finally {
      rm.stop();
    }
  }

  @Test(timeout = 3000000)
  public void testQueueLevelContainerAllocationSuccess() throws Exception {
    testFairSchedulerContainerAllocationSuccess("queueB");
  }

  @Test(timeout = 3000000)
  public void testSchedulerLevelContainerAllocationSuccess() throws Exception {
    testFairSchedulerContainerAllocationSuccess("queueC");
  }

  private void testFairSchedulerContainerAllocationSuccess(String queueName)
      throws Exception {
    MockRM rm = new MockRM(configuration);
    rm.start();

    // Register node1
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * GB);

    // Submit an application
    RMApp app1 = rm.submitApp(2 * GB, queueName);

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    am1.addRequests(new String[] { "127.0.0.1" }, MEMORY_ALLOCATION, 1, 1);

    allocateResponse = am1.schedule(); // send the request
    ((FairScheduler) rm.getResourceScheduler()).update();

    // kick the scheduler
    nm1.nodeHeartbeat(true);
    GenericTestUtils.waitFor(() -> {
      LOG.info("Waiting for containers to be created for app 1");
      try {
        allocateResponse = am1.schedule();
      } catch (Exception e) {
        Assert.fail("Allocation should be successful");
      }
      return allocateResponse.getAllocatedContainers().size() > 0;
    }, 1000, 10000);

    Container allocatedContainer =
        allocateResponse.getAllocatedContainers().get(0);
    Assert.assertEquals(MEMORY_ALLOCATION,
        allocatedContainer.getResource().getMemorySize());
    Assert.assertEquals(1, allocatedContainer.getResource().getVirtualCores());
    rm.stop();
  }
}
