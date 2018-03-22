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

package org.apache.hadoop.yarn.server.resourcemanager.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestSchedulingMonitor {

  @Test(timeout = 10000)
  public void testRMStarts() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    conf.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
        ProportionalCapacityPreemptionPolicy.class.getCanonicalName());

    ResourceManager rm = new MockRM();
    rm.init(conf);
    SchedulingEditPolicy mPolicy = mock(SchedulingEditPolicy.class);
    when(mPolicy.getMonitoringInterval()).thenReturn(1000L);
    SchedulingMonitor monitor = new SchedulingMonitor(rm.getRMContext(),
        mPolicy);
    monitor.serviceInit(conf);
    monitor.serviceStart();
    verify(mPolicy, timeout(10000)).editSchedule();
    monitor.close();
    rm.close();
  }

  @Test(timeout = 10000)
  public void testRMUpdateSchedulingEditPolicy() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    MockRM rm = new MockRM(conf);
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    SchedulingMonitorManager smm = cs.getSchedulingMonitorManager();

    // runningSchedulingMonitors should not be empty when initialize RM
    // scheduler monitor
    cs.reinitialize(conf, rm.getRMContext());
    assertFalse(smm.isRSMEmpty());

    // make sure runningSchedulingPolicies contains all the configured policy
    // in YARNConfiguration
    String[] configuredPolicies = conf.getStrings(
        YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES);
    Set<String> configurePoliciesSet = new HashSet<>();
    for (String s : configuredPolicies) {
      configurePoliciesSet.add(s);
    }
    assertTrue(smm.isSameConfiguredPolicies(configurePoliciesSet));

    // disable RM scheduler monitor
    conf.setBoolean(
        YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ENABLE_MONITORS);
    cs.reinitialize(conf, rm.getRMContext());
    assertTrue(smm.isRSMEmpty());
  }
}
