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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerTestBase;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestFairReservationSystem {
  private final static String ALLOC_FILE = new File(FairSchedulerTestBase.
    TEST_DIR,
      TestFairReservationSystem.class.getName() + ".xml").getAbsolutePath();
  private Configuration conf;
  private FairScheduler scheduler;
  private FairSchedulerTestBase testHelper = new FairSchedulerTestBase();

  protected Configuration createConfiguration() {
    Configuration conf = testHelper.createConfiguration();
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
    conf = null;
  }

  @Test
  public void testFairReservationSystemInitialize() throws IOException {
    ReservationSystemTestUtil.setupFSAllocationFile(ALLOC_FILE);

    ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
    
    // Setup
    RMContext mockRMContext = testUtil.createRMContext(conf);
    scheduler = ReservationSystemTestUtil.setupFairScheduler(testUtil,
        mockRMContext, conf, 10);

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
    RMContext mockRMContext = testUtil.createRMContext(conf);
    scheduler = ReservationSystemTestUtil.setupFairScheduler(testUtil,
        mockRMContext, conf, 10);

    FairReservationSystem reservationSystem = new FairReservationSystem();
    reservationSystem.setRMContext(mockRMContext);

    try {
      reservationSystem.reinitialize(scheduler.getConf(), mockRMContext);
    } catch (YarnException e) {
      Assert.fail(e.getMessage());
    }

    // Assert queue in original config
    final String planQNam = testUtil.getFullReservationQueueName();
    ReservationSystemTestUtil.validateReservationQueue(reservationSystem,
        planQNam);

    // Dynamically add a plan
    ReservationSystemTestUtil.updateFSAllocationFile(ALLOC_FILE);
    scheduler.reinitialize(conf, mockRMContext);

    try {
      reservationSystem.reinitialize(conf, mockRMContext);
    } catch (YarnException e) {
      Assert.fail(e.getMessage());
    }

    String newQueue = "root.reservation";
    ReservationSystemTestUtil.validateNewReservationQueue
        (reservationSystem, newQueue);
  }

}
