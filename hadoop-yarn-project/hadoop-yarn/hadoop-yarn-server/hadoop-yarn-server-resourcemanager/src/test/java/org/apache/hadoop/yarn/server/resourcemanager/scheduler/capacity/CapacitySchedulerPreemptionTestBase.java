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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.Application;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.Clock;
import org.junit.Assert;
import org.junit.Before;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CapacitySchedulerPreemptionTestBase {

  final int GB = 1024;

  Configuration conf;

  RMNodeLabelsManager mgr;

  Clock clock;

  @Before
  void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    conf.setClass(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
        ProportionalCapacityPreemptionPolicy.class, SchedulingEditPolicy.class);
    conf = TestUtils.getConfigurationWithMultipleQueues(this.conf);

    // Set preemption related configurations
    conf.setInt(CapacitySchedulerConfiguration.PREEMPTION_WAIT_TIME_BEFORE_KILL,
        0);
    conf.setFloat(CapacitySchedulerConfiguration.TOTAL_PREEMPTION_PER_ROUND,
        1.0f);
    conf.setFloat(
        CapacitySchedulerConfiguration.PREEMPTION_NATURAL_TERMINATION_FACTOR,
        1.0f);
    conf.setLong(CapacitySchedulerConfiguration.PREEMPTION_MONITORING_INTERVAL,
        60000L);
    mgr = new NullRMNodeLabelsManager();
    mgr.init(this.conf);
    clock = mock(Clock.class);
    when(clock.getTime()).thenReturn(0L);
  }

  SchedulingEditPolicy getSchedulingEditPolicy(MockRM rm) {
    ResourceManager.RMActiveServices activeServices = rm.getRMActiveService();
    SchedulingMonitor mon = null;
    for (Service service : activeServices.getServices()) {
      if (service instanceof SchedulingMonitor) {
        mon = (SchedulingMonitor) service;
        break;
      }
    }

    if (mon != null) {
      return mon.getSchedulingEditPolicy();
    }
    return null;
  }

  public void waitNumberOfLiveContainersFromApp(FiCaSchedulerApp app,
      int expected) throws InterruptedException {
    int waitNum = 0;

    while (waitNum < 10) {
      System.out.println(app.getLiveContainers().size());
      if (app.getLiveContainers().size() == expected) {
        return;
      }
      Thread.sleep(100);
      waitNum++;
    }

    Assert.fail();
  }

  public void waitNumberOfReservedContainersFromApp(FiCaSchedulerApp app,
      int expected) throws InterruptedException {
    int waitNum = 0;

    while (waitNum < 10) {
      System.out.println(app.getReservedContainers().size());
      if (app.getReservedContainers().size() == expected) {
        return;
      }
      Thread.sleep(100);
      waitNum++;
    }

    Assert.fail();
  }

  public void waitNumberOfLiveContainersOnNodeFromApp(FiCaSchedulerNode node,
      ApplicationAttemptId appId, int expected) throws InterruptedException {
    int waitNum = 0;

    while (waitNum < 500) {
      int total = 0;
      for (RMContainer c : node.getCopiedListOfRunningContainers()) {
        if (c.getApplicationAttemptId().equals(appId)) {
          total++;
        }
      }
      if (total == expected) {
        return;
      }
      Thread.sleep(10);
      waitNum++;
    }

    Assert.fail();
  }
}
