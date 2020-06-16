/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for FSYarnSiteConverter.
 *
 */
public class TestFSYarnSiteConverter {
  private Configuration yarnConfig;
  private FSYarnSiteConverter converter;
  private Configuration yarnConvertedConfig;

  @Before
  public void setup() {
    yarnConfig = new Configuration(false);
    yarnConvertedConfig = new Configuration(false);
    converter = new FSYarnSiteConverter();
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testSiteContinuousSchedulingConversion() {
    yarnConfig.setBoolean(
        FairSchedulerConfiguration.CONTINUOUS_SCHEDULING_ENABLED, true);
    yarnConfig.setInt(
        FairSchedulerConfiguration.CONTINUOUS_SCHEDULING_SLEEP_MS, 666);

    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
      false);

    assertTrue("Cont. scheduling", yarnConvertedConfig.getBoolean(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, false));
    assertEquals("Scheduling interval", 666,
        yarnConvertedConfig.getInt(
            "yarn.scheduler.capacity.schedule-asynchronously" +
                ".scheduling-interval-ms", -1));
  }

  @Test
  public void testSiteMinimumAllocationIncrementConversion() {
    yarnConfig.setInt("yarn.resource-types.memory-mb.increment-allocation", 11);
    yarnConfig.setInt("yarn.resource-types.vcores.increment-allocation", 5);

    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false, false);

    assertEquals("Memory alloc increment", 11,
        yarnConvertedConfig.getInt("yarn.scheduler.minimum-allocation-mb",
            -1));
    assertEquals("Vcore increment", 5,
        yarnConvertedConfig.getInt("yarn.scheduler.minimum-allocation-vcores",
            -1));
  }

  @Test
  public void testSitePreemptionConversion() {
    yarnConfig.setBoolean(FairSchedulerConfiguration.PREEMPTION, true);
    yarnConfig.setInt(FairSchedulerConfiguration.WAIT_TIME_BEFORE_KILL, 123);
    yarnConfig.setInt(
        FairSchedulerConfiguration.WAIT_TIME_BEFORE_NEXT_STARVATION_CHECK_MS,
          321);

    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
      false);

    assertTrue("Preemption enabled",
        yarnConvertedConfig.getBoolean(
            YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS,
            false));
    assertEquals("Wait time before kill", 123,
        yarnConvertedConfig.getInt(
            CapacitySchedulerConfiguration.PREEMPTION_WAIT_TIME_BEFORE_KILL,
              -1));
    assertEquals("Starvation check wait time", 321,
        yarnConvertedConfig.getInt(
            CapacitySchedulerConfiguration.PREEMPTION_MONITORING_INTERVAL,
              -1));
  }

  @Test
  public void testSiteAssignMultipleConversion() {
    yarnConfig.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, true);

    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
      false);

    assertTrue("Assign multiple",
        yarnConvertedConfig.getBoolean(
            CapacitySchedulerConfiguration.ASSIGN_MULTIPLE_ENABLED,
            false));
  }

  @Test
  public void testSiteMaxAssignConversion() {
    yarnConfig.setInt(FairSchedulerConfiguration.MAX_ASSIGN, 111);

    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
      false);

    assertEquals("Max assign", 111,
        yarnConvertedConfig.getInt(
            CapacitySchedulerConfiguration.MAX_ASSIGN_PER_HEARTBEAT, -1));
  }

  @Test
  public void testSiteLocalityThresholdConversion() {
    yarnConfig.set(FairSchedulerConfiguration.LOCALITY_THRESHOLD_NODE,
        "123.123");
    yarnConfig.set(FairSchedulerConfiguration.LOCALITY_THRESHOLD_RACK,
        "321.321");

    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
      false);

    assertEquals("Locality threshold node", "123.123",
        yarnConvertedConfig.get(
            CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY));
    assertEquals("Locality threshold rack", "321.321",
        yarnConvertedConfig.get(
            CapacitySchedulerConfiguration.RACK_LOCALITY_ADDITIONAL_DELAY));
  }

  @Test
  public void testSiteDrfEnabledConversion() {
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, true,
      false);

    assertEquals("Resource calculator type", DominantResourceCalculator.class,
        yarnConvertedConfig.getClass(
            CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS, null));
  }

  @Test
  public void testSiteDrfDisabledConversion() {
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
      false);

    assertEquals("Resource calculator type", DefaultResourceCalculator.class,
        yarnConvertedConfig.getClass(
            CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
            CapacitySchedulerConfiguration.DEFAULT_RESOURCE_CALCULATOR_CLASS));
  }

  @Test
  public void testAsyncSchedulingEnabledConversion() {
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, true,
            true);

    assertTrue("Asynchronous scheduling", yarnConvertedConfig.getBoolean(
                    CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE,
            CapacitySchedulerConfiguration.DEFAULT_SCHEDULE_ASYNCHRONOUSLY_ENABLE));
  }

  @Test
  public void testAsyncSchedulingDisabledConversion() {
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
            false);

    assertFalse("Asynchronous scheduling", yarnConvertedConfig.getBoolean(
            CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE,
            CapacitySchedulerConfiguration.DEFAULT_SCHEDULE_ASYNCHRONOUSLY_ENABLE));
  }
}
