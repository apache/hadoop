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

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;

import org.junit.After;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public abstract class ParameterizedSchedulerTestBase {
  protected final static String TEST_DIR =
      new File(System.getProperty("test.build.data", "/tmp")).getAbsolutePath();
  private final static String FS_ALLOC_FILE =
      new File(TEST_DIR, "test-fs-queues.xml").getAbsolutePath();

  public enum SchedulerType {
    CAPACITY, FAIR
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> getParameters() {
    return Arrays.stream(SchedulerType.values()).map(
        type -> new Object[]{type}).collect(Collectors.toList());
  }

  private SchedulerType schedulerType;
  private YarnConfiguration conf = null;
  private AbstractYarnScheduler scheduler = null;

  public YarnConfiguration getConf() {
    return conf;
  }

  // Due to parameterization, this gets called before each test method
  public ParameterizedSchedulerTestBase(SchedulerType type)
      throws IOException {
    conf = new YarnConfiguration();

    QueueMetrics.clearQueueMetrics();
    DefaultMetricsSystem.setMiniClusterMode(true);

    schedulerType = type;
    switch (schedulerType) {
      case FAIR:
        configureFairScheduler(conf);
        scheduler = new FairScheduler();
        conf.set(YarnConfiguration.RM_SCHEDULER,
            FairScheduler.class.getName());
        break;
      case CAPACITY:
        scheduler = new CapacityScheduler();
        ((CapacityScheduler)scheduler).setConf(conf);
        conf.set(YarnConfiguration.RM_SCHEDULER,
            CapacityScheduler.class.getName());
        break;
      default:
        throw new IllegalArgumentException("Invalid type: " + type);
    }
  }

  protected void configureFairScheduler(YarnConfiguration conf)
      throws IOException {
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

    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, FS_ALLOC_FILE);
    conf.setLong(FairSchedulerConfiguration.UPDATE_INTERVAL_MS, 10);
  }

  @After
  public void tearDown() {
    if (schedulerType == SchedulerType.FAIR) {
      (new File(FS_ALLOC_FILE)).delete();
    }
  }

  public SchedulerType getSchedulerType() {
    return schedulerType;
  }

  /**
   * Return a scheduler configured by {@code YarnConfiguration.RM_SCHEDULER}
   *
   * <p>The scheduler is configured by
   * {@link #ParameterizedSchedulerTestBase(SchedulerType)}.
   * Client test code can obtain the scheduler with this getter method.
   * Schedulers supported by this class are {@link FairScheduler} or
   * {@link CapacityScheduler}. </p>
   *
   * @return   The scheduler configured by
   *           {@code YarnConfiguration.RM_SCHEDULER}
   */
  public AbstractYarnScheduler getScheduler() {
    return scheduler;
  }
}
