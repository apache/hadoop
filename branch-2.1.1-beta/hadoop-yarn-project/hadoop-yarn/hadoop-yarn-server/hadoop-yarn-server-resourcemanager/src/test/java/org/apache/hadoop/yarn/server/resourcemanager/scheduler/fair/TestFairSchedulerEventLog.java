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
import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFairSchedulerEventLog {
  private File logFile;
  private FairScheduler scheduler;
  private ResourceManager resourceManager;
  
  @Before
  public void setUp() throws IOException {
    scheduler = new FairScheduler();
    
    Configuration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
        ResourceScheduler.class);
    conf.set("mapred.fairscheduler.eventlog.enabled", "true");

    // All tests assume only one assignment per node update
    conf.set(FairSchedulerConfiguration.ASSIGN_MULTIPLE, "false");
    resourceManager = new ResourceManager();
    resourceManager.init(conf);
    ((AsyncDispatcher)resourceManager.getRMContext().getDispatcher()).start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
  }

  /**
   * Make sure the scheduler creates the event log.
   */
  @Test
  public void testCreateEventLog() throws IOException {
    FairSchedulerEventLog eventLog = scheduler.getEventLog();
    
    logFile = new File(eventLog.getLogFile());
    Assert.assertTrue(logFile.exists());
  }
  
  @After
  public void tearDown() {
    logFile.delete();
    logFile.getParentFile().delete(); // fairscheduler/
    scheduler = null;
    resourceManager = null;
  }
}
