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
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotEquals;

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
        false, false, null);

    assertTrue("Cont. scheduling", yarnConvertedConfig.getBoolean(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, false));
    assertEquals("Scheduling interval", 666,
        yarnConvertedConfig.getInt(
            "yarn.scheduler.capacity.schedule-asynchronously" +
                ".scheduling-interval-ms", -1));
  }

  @Test
  public void testSiteQueueConfAutoRefreshConversion() {
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, false, null);
    assertTrue(yarnConvertedConfig.get(YarnConfiguration.
        RM_SCHEDULER_ENABLE_MONITORS), true);
    assertTrue("Scheduling Policies contains queue conf auto refresh",
        yarnConvertedConfig.
            get(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES)
            .contains(QueueConfigurationAutoRefreshPolicy.
                class.getCanonicalName()));
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

    assertFalse("Observe_only should be false",
        yarnConvertedConfig.getBoolean(CapacitySchedulerConfiguration.
                PREEMPTION_OBSERVE_ONLY, false));

    assertTrue("Should contain ProportionalCapacityPreemptionPolicy.",
        yarnConvertedConfig.
            get(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES).
        contains(ProportionalCapacityPreemptionPolicy.
        class.getCanonicalName()));
  }

  @Test
  public void testSiteDisabledPreemptionWithNoPolicyConversion() {
    // Default mode is nopolicy
    yarnConfig.setBoolean(FairSchedulerConfiguration.PREEMPTION, false);
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, false,  null);

    assertFalse("Should not contain ProportionalCapacityPreemptionPolicy.",
        yarnConvertedConfig.
            get(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES).
            contains(ProportionalCapacityPreemptionPolicy.
                class.getCanonicalName()));

    yarnConfig.setBoolean(FairSchedulerConfiguration.PREEMPTION, false);
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, false,
        FSConfigToCSConfigConverterParams.PreemptionMode.NO_POLICY);

    assertFalse("Should not contain ProportionalCapacityPreemptionPolicy.",
        yarnConvertedConfig.
            get(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES).
            contains(ProportionalCapacityPreemptionPolicy.
                class.getCanonicalName()));
  }

  @Test
  public void testSiteAssignMultipleConversion() {
    yarnConfig.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, true);

    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, false, null);

    assertTrue("Assign multiple",
        yarnConvertedConfig.getBoolean(
            CapacitySchedulerConfiguration.ASSIGN_MULTIPLE_ENABLED,
            false));
  }

  @Test
  public void testSiteMaxAssignConversion() {
    yarnConfig.setInt(FairSchedulerConfiguration.MAX_ASSIGN, 111);

    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, false, null);

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
        false, false, null);

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
        false, false, null);

    assertEquals("Resource calculator type", DominantResourceCalculator.class,
        yarnConvertedConfig.getClass(
            CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS, null));
  }

  @Test
  public void testSiteDrfDisabledConversion() {
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, false, null);

    assertEquals("Resource calculator type", DefaultResourceCalculator.class,
        yarnConvertedConfig.getClass(
            CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
            CapacitySchedulerConfiguration.DEFAULT_RESOURCE_CALCULATOR_CLASS));
  }

  @Test
  public void testAsyncSchedulingEnabledConversion() {
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, true,
            true, false, null);

    assertTrue("Asynchronous scheduling", yarnConvertedConfig.getBoolean(
                    CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE,
            CapacitySchedulerConfiguration.DEFAULT_SCHEDULE_ASYNCHRONOUSLY_ENABLE));
  }

  @Test
  public void testAsyncSchedulingDisabledConversion() {
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
            false, false, null);

    assertFalse("Asynchronous scheduling", yarnConvertedConfig.getBoolean(
            CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE,
            CapacitySchedulerConfiguration.DEFAULT_SCHEDULE_ASYNCHRONOUSLY_ENABLE));
  }

  @Test
  public void testSiteQueueAutoDeletionConversionWithWeightMode() {
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, false, null);
    assertTrue(yarnConvertedConfig.get(YarnConfiguration.
        RM_SCHEDULER_ENABLE_MONITORS), true);
    assertTrue("Scheduling Policies contain auto deletion policy",
        yarnConvertedConfig.
            get(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES)
            .contains(DELETION_POLICY_CLASS));

    // Test when policy has existed.
    yarnConvertedConfig.
        set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
        "testPolicy");
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, false, null);
    assertTrue("Scheduling Policies contain auto deletion policy",
        yarnConvertedConfig.
            get(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES)
            .contains(DELETION_POLICY_CLASS));

    assertEquals("Auto deletion policy expired time should be 10s",
        10, yarnConvertedConfig.
            getLong(CapacitySchedulerConfiguration.
                    AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME,
                CapacitySchedulerConfiguration.
                    DEFAULT_AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME));
  }

  @Test
  public void
      testSiteQueueAutoDeletionConversionDisabledForPercentageMode() {

    // test percentage mode
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, true, null);
    assertTrue(yarnConvertedConfig.get(YarnConfiguration.
        RM_SCHEDULER_ENABLE_MONITORS), true);

    assertTrue("Scheduling Policies should not" +
            "contain auto deletion policy in percentage mode",
        yarnConvertedConfig.
            get(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES) == null ||
            !yarnConvertedConfig.
            get(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES).
                contains(DELETION_POLICY_CLASS));

    yarnConvertedConfig.
        set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
            "testPolicy");
    converter.convertSiteProperties(yarnConfig, yarnConvertedConfig, false,
        false, true, null);
    assertFalse("Scheduling Policies should not " +
            "contain auto deletion policy in percentage mode",
        yarnConvertedConfig.
            get(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES)
            .contains(DELETION_POLICY_CLASS));

    assertNotEquals("Auto deletion policy expired time should not " +
            "be set in percentage mode",
        10, yarnConvertedConfig.
            getLong(CapacitySchedulerConfiguration.
                    AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME,
                CapacitySchedulerConfiguration.
                    DEFAULT_AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME));

  }
}
