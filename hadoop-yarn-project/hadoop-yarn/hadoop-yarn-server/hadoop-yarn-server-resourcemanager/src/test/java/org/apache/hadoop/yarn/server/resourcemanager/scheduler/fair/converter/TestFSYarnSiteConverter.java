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
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueDeletionPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueConfigurationAutoRefreshPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Unit tests for FSYarnSiteConverter.
 *
 */
public class TestFSYarnSiteConverter {
  private Configuration yarnConfig;
  private FSYarnSiteConverter converter;
  private Configuration yarnConvertedConfig;
  private static final String DELETION_POLICY_CLASS =
      AutoCreatedQueueDeletionPolicy.class.getCanonicalName();

  @BeforeEach
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
        true, false, null);

    assertTrue(yarnConvertedConfig.getBoolean(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, false),
        "Cont. scheduling");
    assertEquals(666, yarnConvertedConfig.getInt(
        "yarn.scheduler.capacity.schedule-asynchronously" +
        ".scheduling-interval-ms", -1), "Scheduling interval");
  }

  @Test
  public void testSiteQueueConfAutoRefreshConversion() {
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, false, null);
    assertTrue(true, yarnConvertedConfig.get(YarnConfiguration.
        RM_SCHEDULER_ENABLE_MONITORS));
    assertTrue(yarnConvertedConfig.
        get(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES)
        .contains(QueueConfigurationAutoRefreshPolicy.
        class.getCanonicalName()),
        "Scheduling Policies contains queue conf auto refresh");
  }

  @Test
  public void testSitePreemptionConversion() {
    yarnConfig.setBoolean(FairSchedulerConfiguration.PREEMPTION, true);
    yarnConfig.setInt(FairSchedulerConfiguration.WAIT_TIME_BEFORE_KILL, 123);
    yarnConfig.setInt(
        FairSchedulerConfiguration.WAIT_TIME_BEFORE_NEXT_STARVATION_CHECK_MS,
          321);

    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, false, null);

    assertTrue(yarnConvertedConfig.
        getBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, false), "Preemption enabled");
    assertEquals(123, yarnConvertedConfig.getInt(
        CapacitySchedulerConfiguration.PREEMPTION_WAIT_TIME_BEFORE_KILL, -1),
        "Wait time before kill");
    assertEquals(321, yarnConvertedConfig.getInt(
        CapacitySchedulerConfiguration.PREEMPTION_MONITORING_INTERVAL, -1),
        "Starvation check wait time");

    assertFalse(yarnConvertedConfig.getBoolean(CapacitySchedulerConfiguration.
        PREEMPTION_OBSERVE_ONLY, false), "Observe_only should be false");

    assertTrue(yarnConvertedConfig.get(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES).
        contains(ProportionalCapacityPreemptionPolicy.class.getCanonicalName()),
        "Should contain ProportionalCapacityPreemptionPolicy.");
  }

  @Test
  public void testSiteDisabledPreemptionWithNoPolicyConversion() {
    // Default mode is nopolicy
    yarnConfig.setBoolean(FairSchedulerConfiguration.PREEMPTION, false);
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, false,  null);

    assertFalse(yarnConvertedConfig.get(
        YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES).
        contains(ProportionalCapacityPreemptionPolicy.class.getCanonicalName()),
        "Should not contain ProportionalCapacityPreemptionPolicy.");

    yarnConfig.setBoolean(FairSchedulerConfiguration.PREEMPTION, false);
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, false,
        FSConfigToCSConfigConverterParams.PreemptionMode.NO_POLICY);

    assertFalse(yarnConvertedConfig.get(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES).
        contains(ProportionalCapacityPreemptionPolicy.class.getCanonicalName()),
        "Should not contain ProportionalCapacityPreemptionPolicy.");
  }

  @Test
  public void testSiteAssignMultipleConversion() {
    yarnConfig.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, true);

    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, false, null);

    assertTrue(yarnConvertedConfig.getBoolean(
        CapacitySchedulerConfiguration.ASSIGN_MULTIPLE_ENABLED, false),
        "Assign multiple");
  }

  @Test
  public void testSiteMaxAssignConversion() {
    yarnConfig.setInt(FairSchedulerConfiguration.MAX_ASSIGN, 111);

    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, false, null);

    assertEquals(111, yarnConvertedConfig.getInt(
        CapacitySchedulerConfiguration.MAX_ASSIGN_PER_HEARTBEAT, -1),
        "Max assign");
  }

  @Test
  public void testSiteLocalityThresholdConversion() {
    yarnConfig.set(FairSchedulerConfiguration.LOCALITY_THRESHOLD_NODE,
        "123.123");
    yarnConfig.set(FairSchedulerConfiguration.LOCALITY_THRESHOLD_RACK,
        "321.321");

    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, false, null);

    assertEquals("123.123",
        yarnConvertedConfig.get(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY),
        "Locality threshold node");
    assertEquals("321.321", yarnConvertedConfig.get(
        CapacitySchedulerConfiguration.RACK_LOCALITY_ADDITIONAL_DELAY),
        "Locality threshold rack");
  }

  @Test
  public void testSiteDrfEnabledConversion() {
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, true,
        false, false, null);

    assertEquals(DominantResourceCalculator.class, yarnConvertedConfig.getClass(
        CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS, null),
        "Resource calculator type");
  }

  @Test
  public void testSiteDrfDisabledConversion() {
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, false, null);

    assertEquals(DefaultResourceCalculator.class,
        yarnConvertedConfig.getClass(
        CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        CapacitySchedulerConfiguration.DEFAULT_RESOURCE_CALCULATOR_CLASS),
        "Resource calculator type");
  }

  @Test
  public void testAsyncSchedulingEnabledConversion() {
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, true,
            true, false, null);

    assertTrue(yarnConvertedConfig.getBoolean(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE,
        CapacitySchedulerConfiguration.DEFAULT_SCHEDULE_ASYNCHRONOUSLY_ENABLE),
        "Asynchronous scheduling");
  }

  @Test
  public void testAsyncSchedulingDisabledConversion() {
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
            false, false, null);

    assertFalse(yarnConvertedConfig.getBoolean(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE,
        false), "Asynchronous scheduling");
  }

  @Test
  public void testSiteQueueAutoDeletionConversionWithWeightMode() {
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, false, null);
    assertTrue(true, yarnConvertedConfig.get(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS));
    assertTrue(yarnConvertedConfig.get(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES)
        .contains(DELETION_POLICY_CLASS),
        "Scheduling Policies contain auto deletion policy");

    // Test when policy has existed.
    yarnConvertedConfig.
        set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
        "testPolicy");
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, false, null);
    assertTrue(yarnConvertedConfig.get(
        YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES).contains(
        DELETION_POLICY_CLASS),
        "Scheduling Policies contain auto deletion policy");

    assertEquals(10, yarnConvertedConfig.
        getLong(CapacitySchedulerConfiguration.AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME,
        CapacitySchedulerConfiguration.DEFAULT_AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME),
        "Auto deletion policy expired time should be 10s");
  }

  @Test
  public void
      testSiteQueueAutoDeletionConversionDisabledForPercentageMode() {

    // test percentage mode
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, true, null);
    assertTrue(true, yarnConvertedConfig.get(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS));

    assertTrue(yarnConvertedConfig.
        get(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES) == null ||
        !yarnConvertedConfig.get(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES).
        contains(DELETION_POLICY_CLASS), "Scheduling Policies should not" +
        "contain auto deletion policy in percentage mode");

    yarnConvertedConfig.
        set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
            "testPolicy");
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, true, null);
    assertFalse(yarnConvertedConfig.
        get(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES).contains(
        DELETION_POLICY_CLASS), "Scheduling Policies should not " +
        "contain auto deletion policy in percentage mode");

    assertNotEquals(10, yarnConvertedConfig.
        getLong(CapacitySchedulerConfiguration.AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME,
        CapacitySchedulerConfiguration.DEFAULT_AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME),
        "Auto deletion policy expired time should not be set in percentage mode");

  }
}
