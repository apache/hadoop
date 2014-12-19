/*******************************************************************************
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *******************************************************************************/
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerTestBase;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.mockito.Mockito.when;

public class TestFairReservationSystem extends FairSchedulerTestBase {
  private final static String ALLOC_FILE = new File(TEST_DIR,
      TestFairReservationSystem.class.getName() + ".xml").getAbsolutePath();

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
        ResourceScheduler.class);
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    return conf;
  }

  @Before
  public void setup() throws IOException {
    conf = createConfiguration();
  }

  @After
  public void teardown() {
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
    conf = null;
  }

  @Test
  public void testFairReservationSystemInitialize() throws IOException {
    ReservationSystemTestUtil.setupFSAllocationFile(ALLOC_FILE);

    ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
    
    // Setup
    RMContext mockRMContext = testUtil.createRMContext(conf);
    setupFairScheduler(testUtil, mockRMContext);

    FairReservationSystem reservationSystem = new FairReservationSystem();
    reservationSystem.setRMContext(mockRMContext);

    try {
      reservationSystem.reinitialize(scheduler.getConf(), mockRMContext);
    } catch (YarnException e) {
      Assert.fail(e.getMessage());
    }

    ReservationSystemTestUtil.validateReservationQueue(reservationSystem,
        testUtil.getFullReservationQueueName());
  }

  @Test
  public void testFairReservationSystemReinitialize() throws IOException {
    ReservationSystemTestUtil.setupFSAllocationFile(ALLOC_FILE);

    ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();

    // Setup
    RMContext mockContext = testUtil.createRMContext(conf);
    setupFairScheduler(testUtil, mockContext);

    FairReservationSystem reservationSystem = new FairReservationSystem();
    reservationSystem.setRMContext(mockContext);

    try {
      reservationSystem.reinitialize(scheduler.getConf(), mockContext);
    } catch (YarnException e) {
      Assert.fail(e.getMessage());
    }

    // Assert queue in original config
    final String planQNam = testUtil.getFullReservationQueueName();
    ReservationSystemTestUtil.validateReservationQueue(reservationSystem,
        planQNam);

    // Dynamically add a plan
    ReservationSystemTestUtil.updateFSAllocationFile(ALLOC_FILE);
    scheduler.reinitialize(conf, mockContext);

    try {
      reservationSystem.reinitialize(conf, mockContext);
    } catch (YarnException e) {
      Assert.fail(e.getMessage());
    }

    String newQueue = "root.reservation";
    ReservationSystemTestUtil.validateNewReservationQueue
        (reservationSystem, newQueue);
  }

  private void setupFairScheduler(ReservationSystemTestUtil testUtil,
      RMContext rmContext) throws
      IOException {

    scheduler = new FairScheduler();
    scheduler.setRMContext(rmContext);

    int numContainers = 10;
    when(rmContext.getScheduler()).thenReturn(scheduler);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, rmContext);

    Resource resource = testUtil.calculateClusterResource(numContainers);
    RMNode node1 = MockNodes.newNodeInfo(1, resource, 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
  }
}
