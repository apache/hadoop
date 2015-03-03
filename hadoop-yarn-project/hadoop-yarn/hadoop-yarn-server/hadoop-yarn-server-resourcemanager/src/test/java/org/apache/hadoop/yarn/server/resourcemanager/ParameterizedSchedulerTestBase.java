/*
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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;


import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public abstract class ParameterizedSchedulerTestBase {
  protected final static String TEST_DIR =
      new File(System.getProperty("test.build.data", "/tmp")).getAbsolutePath();
  private final static String FS_ALLOC_FILE =
      new File(TEST_DIR, "test-fs-queues.xml").getAbsolutePath();

  private SchedulerType schedulerType;
  private YarnConfiguration conf = null;

  public enum SchedulerType {
    CAPACITY, FAIR
  }

  public ParameterizedSchedulerTestBase(SchedulerType type) {
    schedulerType = type;
  }

  public YarnConfiguration getConf() {
    return conf;
  }

  @Parameterized.Parameters
  public static Collection<SchedulerType[]> getParameters() {
    return Arrays.asList(new SchedulerType[][]{
        {SchedulerType.CAPACITY}, {SchedulerType.FAIR}});
  }

  @Before
  public void configureScheduler() throws IOException {
    conf = new YarnConfiguration();
    switch (schedulerType) {
      case CAPACITY:
        conf.set(YarnConfiguration.RM_SCHEDULER,
            CapacityScheduler.class.getName());
        break;
      case FAIR:
        configureFairScheduler(conf);
        break;
    }
  }

  private void configureFairScheduler(YarnConfiguration conf) throws IOException {
    // Disable queueMaxAMShare limitation for fair scheduler
    PrintWriter out = new PrintWriter(new FileWriter(FS_ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queueMaxAMShareDefault>-1.0</queueMaxAMShareDefault>");
    out.println("<defaultQueueSchedulingPolicy>fair</defaultQueueSchedulingPolicy>");
    out.println("<queue name=\"root\">");
    out.println("  <schedulingPolicy>drf</schedulingPolicy>");
    out.println("  <weight>1.0</weight>");
    out.println("  <fairSharePreemptionTimeout>100</fairSharePreemptionTimeout>");
    out.println("  <minSharePreemptionTimeout>120</minSharePreemptionTimeout>");
    out.println("  <fairSharePreemptionThreshold>.5</fairSharePreemptionThreshold>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    conf.set(YarnConfiguration.RM_SCHEDULER, FairScheduler.class.getName());
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, FS_ALLOC_FILE);
  }

  public SchedulerType getSchedulerType() {
    return schedulerType;
  }
}
