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
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.impl.LightWeightResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ConfigSnapshot;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CSConfigValidator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.GPU_URI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCapacitySchedulerConfigValidator {
  public static final int NODE_MEMORY = 16;
  public static final int NODE1_VCORES = 8;
  public static final int NODE2_VCORES = 10;
  public static final int NODE3_VCORES = 12;
  public static final Map<String, Long> NODE_GPU = ImmutableMap.of(GPU_URI, 2L);
  public static final int GB = 1024;

  private static final String PARENT_A = "parentA";
  private static final String PARENT_B = "parentB";
  private static final String LEAF_A = "leafA";
  private static final String LEAF_B = "leafB";

  private static final QueuePath PARENT_A_FULL_PATH =
          new QueuePath(CapacitySchedulerConfiguration.ROOT + "." + PARENT_A);
  private static final QueuePath LEAF_A_FULL_PATH =
          new QueuePath(PARENT_A_FULL_PATH + "." + LEAF_A);
  private static final QueuePath PARENT_B_FULL_PATH =
          new QueuePath(CapacitySchedulerConfiguration.ROOT + "." + PARENT_B);
  private static final QueuePath LEAF_B_FULL_PATH =
          new QueuePath(PARENT_B_FULL_PATH + "." + LEAF_B);

  private final Resource A_MINRES = Resource.newInstance(16 * GB, 10);
  private final Resource B_MINRES = Resource.newInstance(32 * GB, 5);
  private final Resource FULL_MAXRES = Resource.newInstance(48 * GB, 30);
  private final Resource PARTIAL_MAXRES = Resource.newInstance(16 * GB, 10);
  private final Resource VCORE_EXCEEDED_MAXRES = Resource.newInstance(16 * GB, 50);
  private Resource A_MINRES_GPU;
  private Resource B_MINRES_GPU;
  private Resource FULL_MAXRES_GPU;
  private Resource PARTIAL_MAXRES_GPU;
  private Resource GPU_EXCEEDED_MAXRES_GPU;

  protected MockRM mockRM = null;
  protected MockNM nm1 = null;
  protected MockNM nm2 = null;
  protected MockNM nm3 = null;
  protected CapacityScheduler cs;

  public static void setupResources(boolean useGpu) {
    Map<String, ResourceInformation> riMap = new HashMap<>();

    ResourceInformation memory = ResourceInformation.newInstance(
        ResourceInformation.MEMORY_MB.getName(),
        ResourceInformation.MEMORY_MB.getUnits(),
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    ResourceInformation vcores = ResourceInformation.newInstance(
        ResourceInformation.VCORES.getName(),
        ResourceInformation.VCORES.getUnits(),
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
    riMap.put(ResourceInformation.MEMORY_URI, memory);
    riMap.put(ResourceInformation.VCORES_URI, vcores);
    if (useGpu) {
      riMap.put(ResourceInformation.GPU_URI,
          ResourceInformation.newInstance(ResourceInformation.GPU_URI, "", 0,
              ResourceTypes.COUNTABLE, 0, 10L));
    }

    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);
  }

  /**
   * Test for the case when the scheduler.minimum-allocation-mb == 0.
   */
  @Test
  public void testValidateMemoryAllocationInvalidMinMem() {
    assertThrows(YarnRuntimeException.class, () -> {
      Map<String, String> configs = new HashMap();
      configs.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, "0");
      Configuration config = CapacitySchedulerConfigGeneratorForTest
          .createConfiguration(configs);
      CapacitySchedulerConfigValidator.validateMemoryAllocation(config);
      fail(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB +
          " should be > 0");
    });
  }

  /**
   * Test for the case when the scheduler.minimum-allocation-mb is greater than
   * scheduler.maximum-allocation-mb.
   */
  @Test
  public void testValidateMemoryAllocationHIgherMinThanMaxMem() {
    assertThrows(YarnRuntimeException.class, () -> {
      Map<String, String> configs = new HashMap();
      configs.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, "8192");
      configs.put(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, "1024");
      Configuration config = CapacitySchedulerConfigGeneratorForTest
          .createConfiguration(configs);
      CapacitySchedulerConfigValidator.validateMemoryAllocation(config);
      fail(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB + " should be > "
          + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    });
  }

  @Test
  public void testValidateMemoryAllocation() {
    Map<String, String> configs = new HashMap();
    configs.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, "1024");
    configs.put(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, "8192");
    Configuration config = CapacitySchedulerConfigGeneratorForTest
            .createConfiguration(configs);
    // there is no need for assertion, since there is no further method call
    // inside the tested code and in case of a valid configuration no exception
    // is thrown
    CapacitySchedulerConfigValidator.validateMemoryAllocation(config);
  }

  /**
   * Test for the case when the scheduler.minimum-allocation-vcores == 0.
   */
  @Test
  public void testValidateVCoresInvalidMinVCore() {
    assertThrows(YarnRuntimeException.class, () -> {
      Map<String, String> configs = new HashMap();
      configs.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, "0");
      Configuration config = CapacitySchedulerConfigGeneratorForTest
          .createConfiguration(configs);
      CapacitySchedulerConfigValidator.validateVCores(config);
      fail(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES
          + " should be > 0");
    });
  }

  /**
   * Test for the case when the scheduler.minimum-allocation-vcores is greater
   * than scheduler.maximum-allocation-vcores.
   */
  @Test
  public void testValidateVCoresHigherMinThanMaxVCore() {
    assertThrows(YarnRuntimeException.class, () -> {
      Map<String, String> configs = new HashMap();
      configs.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, "4");
      configs.put(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, "1");
      Configuration config = CapacitySchedulerConfigGeneratorForTest
          .createConfiguration(configs);
      CapacitySchedulerConfigValidator.validateVCores(config);
      fail(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES +
          " should be > "
          + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    });
  }

  @Test
  public void testValidateVCores() {
    Map<String, String> configs = new HashMap();
    configs.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, "1");
    configs.put(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, "4");
    Configuration config = CapacitySchedulerConfigGeneratorForTest
            .createConfiguration(configs);
    // there is no need for assertion, since there is no further method call
    // inside the tested code and in case of a valid configuration no exception
    // is thrown
    CapacitySchedulerConfigValidator.validateVCores(config);
  }

  @Test
  public void testValidateCSConfigInvalidCapacity() {
    Configuration oldConfig = CapacitySchedulerConfigGeneratorForTest
            .createBasicCSConfiguration();
    Configuration newConfig = new Configuration(oldConfig);
    newConfig
            .set("yarn.scheduler.capacity.root.test1.capacity", "500");
    RMContext rmContext = prepareRMContext();
    try {
      CapacitySchedulerConfigValidator
              .validateCSConfiguration(oldConfig, newConfig, rmContext);
      fail("Invalid capacity");
    } catch (IOException e) {
      assertEquals("Illegal capacity of 500.0 for queue root.test1", e.getCause().getMessage());
    }
  }

  /**
   * The hierarchy transition checks fail on the first live queue in the
   * iteration order of the queue store: root.a (S06) before root.a.a1 (S04).
   */
  @Test
  public void testValidateCSConfigHierarchyCheckOrder() {
    String p = CapacitySchedulerConfiguration.PREFIX + "root";
    Configuration oldConfig = new Configuration(false);
    oldConfig.set(p + ".queues", "a,b");
    oldConfig.set(p + ".a.capacity", "50");
    oldConfig.set(p + ".b.capacity", "50");
    oldConfig.set(p + ".a.queues", "a1");
    oldConfig.set(p + ".a.a1.capacity", "100");
    Configuration newConfig = new Configuration(false);
    newConfig.set(p + ".queues", "a,b");
    newConfig.set(p + ".a.capacity", "50");
    newConfig.set(p + ".b.capacity", "50");
    newConfig.set(p + ".a.auto-create-child-queue.enabled", "true");
    IOException e = assertThrows(IOException.class,
        () -> CapacitySchedulerConfigValidator.validateCSConfiguration(
            oldConfig, newConfig, prepareRMContext()));
    assertEquals("Failed to re-init queues : Can not convert parent queue:"
        + " root.a to auto create enabled parent queue since it could have"
        + " other pre-configured queues which is not supported",
        e.getMessage());
  }

  @Test
  public void testThrowIfInvalidKeepsParseFailureCause() {
    String p = CapacitySchedulerConfiguration.PREFIX + "root";
    Configuration conf = new Configuration(false);
    conf.set(p + ".queues", "a,b");
    conf.set(p + ".a.capacity", "50%");
    conf.set(p + ".b.capacity", "50");
    ConfigSnapshot snapshot = ConfigSnapshot.of(conf);
    ClusterFacts facts = ClusterFacts.empty();
    ValidationResult result = new CSConfigValidator().validate(snapshot,
        facts);
    IOException e = assertThrows(IOException.class,
        () -> CapacitySchedulerConfigValidator.throwIfInvalid(result));
    assertEquals("Failed to re-init queues : For input string: \"50%\"",
        e.getMessage());
    assertTrue(e.getCause() instanceof NumberFormatException,
        String.valueOf(e.getCause()));
  }

  /**
   * A ReservationQueue validates the user weights it inherits from its plan
   * against the user limit of its own path, not the plan's user limit.
   */
  @Test
  public void testReservationQueueUserWeightsUseOwnUserLimit()
      throws Exception {
    YarnConfiguration yarnConf = new YarnConfiguration();
    yarnConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(yarnConf);
    String p = CapacitySchedulerConfiguration.PREFIX;
    conf.set(p + "root.queues", "a,plan");
    conf.set(p + "root.a.capacity", "50");
    conf.set(p + "root.plan.capacity", "50");
    conf.set(p + "root.plan.reservable", "true");
    conf.set(p + "minimum-user-limit-percent", "25");
    conf.set(p + "root.plan.minimum-user-limit-percent", "100");
    conf.set(p + "root.plan.user-settings.alice.weight", "2");

    mockRM = new MockRM(conf);
    try {
      mockRM.start();
      cs = (CapacityScheduler) mockRM.getResourceScheduler();
      assertNotNull(cs.getQueue("root.plan.plan-default"));
      cs.reinitialize(conf, mockRM.getRMContext());

      ValidationResult result = new CSConfigValidator().validate(
          ConfigSnapshot.of(conf), ClusterFacts.capture(cs));
      assertTrue(result.isValid(), result.toString());
      assertTrue(CapacitySchedulerConfigValidator.validateCSConfiguration(
          conf, conf, mockRM.getRMContext()));
    } finally {
      mockRM.stop();
    }
  }

  /**
   * A malformed scheduler allocation fails before the queues are refreshed,
   * with the parse failure itself.
   */
  @Test
  public void testValidateCSConfigMalformedAllocation() {
    Configuration oldConfig = CapacitySchedulerConfigGeneratorForTest
        .createBasicCSConfiguration();
    for (String key : new String[] {
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES}) {
      Configuration newConfig = new Configuration(oldConfig);
      newConfig.set(key, "abc");
      NumberFormatException e = assertThrows(NumberFormatException.class,
          () -> CapacitySchedulerConfigValidator.validateCSConfiguration(
              oldConfig, newConfig, prepareRMContext()), key);
      assertEquals("java.lang.NumberFormatException: For input string:"
          + " \"abc\"", e.toString(), key);
    }
  }

  @Test
  public void testValidateCSConfigDefaultRCAbsoluteModeParentMaxMemoryExceeded()
      throws Exception {
    setUpMockRM(false);
    RMContext rmContext = mockRM.getRMContext();
    CapacitySchedulerConfiguration oldConfiguration = cs.getConfiguration();
    CapacitySchedulerConfiguration newConfiguration =
        new CapacitySchedulerConfiguration(cs.getConfiguration());
    newConfiguration.setMaximumResourceRequirement("",
            LEAF_A_FULL_PATH, FULL_MAXRES);
    try {
      CapacitySchedulerConfigValidator
          .validateCSConfiguration(oldConfiguration, newConfiguration, rmContext);
      fail("Parent maximum capacity exceeded");
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage()
          .startsWith("Max resource configuration"));
    } finally {
      mockRM.stop();
    }
  }

  @Test
  public void testValidateCSConfigDefaultRCAbsoluteModeParentMaxVcoreExceeded() throws Exception {
    setUpMockRM(false);
    RMContext rmContext = mockRM.getRMContext();
    CapacitySchedulerConfiguration oldConfiguration = cs.getConfiguration();
    CapacitySchedulerConfiguration newConfiguration =
        new CapacitySchedulerConfiguration(cs.getConfiguration());
    newConfiguration.setMaximumResourceRequirement("",
            LEAF_A_FULL_PATH, VCORE_EXCEEDED_MAXRES);
    try {
      CapacitySchedulerConfigValidator
          .validateCSConfiguration(oldConfiguration, newConfiguration, rmContext);
    } catch (IOException e) {
      fail("In DefaultResourceCalculator vcore limits are not enforced");
    } finally {
      mockRM.stop();
    }
  }

  @Test
  public void testValidateCSConfigDominantRCAbsoluteModeParentMaxMemoryExceeded()
      throws Exception {
    setUpMockRM(true);
    RMContext rmContext = mockRM.getRMContext();
    CapacitySchedulerConfiguration oldConfiguration = cs.getConfiguration();
    CapacitySchedulerConfiguration newConfiguration =
        new CapacitySchedulerConfiguration(cs.getConfiguration());
    newConfiguration.setMaximumResourceRequirement("",
            LEAF_A_FULL_PATH, FULL_MAXRES);
    try {
      CapacitySchedulerConfigValidator
          .validateCSConfiguration(oldConfiguration, newConfiguration, rmContext);
      fail("Parent maximum capacity exceeded");
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage()
          .startsWith("Max resource configuration"));
    } finally {
      mockRM.stop();
    }
  }

  @Test
  public void testValidateCSConfigDominantRCAbsoluteModeParentMaxVcoreExceeded() throws Exception {
    setUpMockRM(true);
    RMContext rmContext = mockRM.getRMContext();
    CapacitySchedulerConfiguration oldConfiguration = cs.getConfiguration();
    CapacitySchedulerConfiguration newConfiguration =
        new CapacitySchedulerConfiguration(cs.getConfiguration());
    newConfiguration.setMaximumResourceRequirement("",
            LEAF_A_FULL_PATH, VCORE_EXCEEDED_MAXRES);
    try {
      CapacitySchedulerConfigValidator
          .validateCSConfiguration(oldConfiguration, newConfiguration, rmContext);
      fail("Parent maximum capacity exceeded");
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage()
          .startsWith("Max resource configuration"));
    } finally {
      mockRM.stop();
    }
  }

  @Test
  public void testValidateCSConfigDominantRCAbsoluteModeParentMaxGPUExceeded() throws Exception {
    setUpMockRM(true);
    RMContext rmContext = mockRM.getRMContext();
    CapacitySchedulerConfiguration oldConfiguration = cs.getConfiguration();
    CapacitySchedulerConfiguration newConfiguration =
        new CapacitySchedulerConfiguration(cs.getConfiguration());
    newConfiguration.setMaximumResourceRequirement("",
            LEAF_A_FULL_PATH, GPU_EXCEEDED_MAXRES_GPU);
    try {
      CapacitySchedulerConfigValidator
          .validateCSConfiguration(oldConfiguration, newConfiguration, rmContext);
      fail("Parent maximum capacity exceeded");
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage()
          .startsWith("Max resource configuration"));
    } finally {
      mockRM.stop();
    }
  }

  @Test
  public void testValidateCSConfigStopALeafQueue() throws IOException {
    Configuration oldConfig = CapacitySchedulerConfigGeneratorForTest
            .createBasicCSConfiguration();
    Configuration newConfig = new Configuration(oldConfig);
    newConfig
            .set("yarn.scheduler.capacity.root.test1.state", "STOPPED");
    RMContext rmContext = prepareRMContext();
    boolean isValidConfig = CapacitySchedulerConfigValidator
            .validateCSConfiguration(oldConfig, newConfig, rmContext);
    assertTrue(isValidConfig);
  }

  /**
   * Stop the root queue if there are running child queues.
   */
  @Test
  public void testValidateCSConfigStopANonLeafQueueInvalid() {
    Configuration oldConfig = CapacitySchedulerConfigGeneratorForTest
            .createBasicCSConfiguration();
    Configuration newConfig = new Configuration(oldConfig);
    newConfig
            .set("yarn.scheduler.capacity.root.state", "STOPPED");
    RMContext rmContext = prepareRMContext();
    try {
      CapacitySchedulerConfigValidator
              .validateCSConfiguration(oldConfig, newConfig, rmContext);
      fail("There are child queues in running state");
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage()
              .contains("The parent queue:root cannot be STOPPED"));
    }
  }

  @Test
  public void testValidateCSConfigStopANonLeafQueue() throws IOException {
    Configuration oldConfig = CapacitySchedulerConfigGeneratorForTest
            .createBasicCSConfiguration();
    Configuration newConfig = new Configuration(oldConfig);
    newConfig
            .set("yarn.scheduler.capacity.root.state", "STOPPED");
    newConfig
            .set("yarn.scheduler.capacity.root.test1.state", "STOPPED");
    newConfig
            .set("yarn.scheduler.capacity.root.test2.state", "STOPPED");
    RMContext rmContext = prepareRMContext();
    Boolean isValidConfig = CapacitySchedulerConfigValidator
            .validateCSConfiguration(oldConfig, newConfig, rmContext);
    assertTrue(isValidConfig);

  }

  /**
   * Add a leaf queue without modifying the capacity of other leaf queues
   * so the total capacity != 100.
   */
  @Test
  public void testValidateCSConfigAddALeafQueueInvalid() {
    Configuration oldConfig = CapacitySchedulerConfigGeneratorForTest
            .createBasicCSConfiguration();
    CapacitySchedulerConfiguration newConfig = new CapacitySchedulerConfiguration(oldConfig);
    newConfig
            .set("yarn.scheduler.capacity.root.queues", "test1, test2, test3");
    newConfig
            .set("yarn.scheduler.capacity.root.test3.state", "RUNNING");
    newConfig
            .set("yarn.scheduler.capacity.root.test3.capacity", "30");

    RMContext rmContext = prepareRMContext();
    try {
      CapacitySchedulerConfigValidator
              .validateCSConfiguration(oldConfig, newConfig, rmContext);
      if (newConfig.isLegacyQueueMode()) {
        fail("Invalid capacity for children of queue root");
      }
    } catch (IOException e) {
      assertEquals("Illegal capacity sum of 1.3 for children of queue root for label=."
          + " It should be either 0 or 1.0", e.getCause().getMessage());
    }
  }

  /**
   * Add a leaf queue by modifying the capacity of other leaf queues
   * and adjust the capacities of other leaf queues, so total capacity = 100.
   */
  @Test
  public void testValidateCSConfigAddALeafQueueValid() throws IOException {
    Configuration oldConfig = CapacitySchedulerConfigGeneratorForTest
            .createBasicCSConfiguration();
    Configuration newConfig = new Configuration(oldConfig);
    newConfig
            .set("yarn.scheduler.capacity.root.queues", "test1, test2, test3");
    newConfig
            .set("yarn.scheduler.capacity.root.test3.state", "RUNNING");
    newConfig
            .set("yarn.scheduler.capacity.root.test3.capacity", "30");
    newConfig
            .set("yarn.scheduler.capacity.root.test1.capacity", "20");

    RMContext rmContext = prepareRMContext();
    Boolean isValidConfig = CapacitySchedulerConfigValidator
            .validateCSConfiguration(oldConfig, newConfig, rmContext);
    assertTrue(isValidConfig);
  }

  @Test
  public void testValidateDoesNotModifyTheDefaultMetricsSystem() throws Exception {
    try {
      YarnConfiguration conf = new YarnConfiguration(CapacitySchedulerConfigGeneratorForTest
          .createBasicCSConfiguration());
      conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      mockRM = new MockRM(conf);

      cs = (CapacityScheduler) mockRM.getResourceScheduler();
      mockRM.start();
      cs.start();
      RMContext rmContext = mockRM.getRMContext();
      Configuration oldConfig = cs.getConfig();

      final Map<String, QueueMetrics> cache = QueueMetrics.getQueueMetrics();
      final MetricsSystem ms = DefaultMetricsSystem.instance();

      QueueMetrics origQM1 = cache.get("root.test1");
      QueueMetrics origQM2 = cache.get("root.test2");
      assertNotNull(origQM1, "Original queues should be found in the cache");
      assertNotNull(origQM2, "Original queues should be found in the cache");

      QueueMetrics origPQM1 = cache.get("default.root.test1");
      QueueMetrics origPQM2 = cache.get("default.root.test2");
      assertNotNull(origPQM1,
          "Original queues should be found in the cache (PartitionQueueMetrics)");
      assertNotNull(origPQM2,
          "Original queues should be found in the cache (PartitionQueueMetrics)");

      MetricsSource origMS1 =
          ms.getSource("QueueMetrics,q0=root,q1=test1");
      MetricsSource origMS2 =
          ms.getSource("QueueMetrics,q0=root,q1=test2");
      assertNotNull(origMS1,
          "Original queues should be found in the Metrics System");
      assertNotNull(origMS2,
          "Original queues should be found in the Metrics System");

      MetricsSource origPMS1 = ms
          .getSource("PartitionQueueMetrics,partition=,q0=root,q1=test1");
      MetricsSource origPMS2 = ms
          .getSource("PartitionQueueMetrics,partition=,q0=root,q1=test2");
      assertNotNull(origPMS1,
          "Original queues should be found in Metrics System (PartitionQueueMetrics)");
      assertNotNull(origPMS2,
          "Original queues should be found in Metrics System (PartitionQueueMetrics)");

      Configuration newConfig = new Configuration(oldConfig);
      newConfig
          .set("yarn.scheduler.capacity.root.queues", "test1, test2, test3");
      newConfig
          .set("yarn.scheduler.capacity.root.test3.state", "RUNNING");
      newConfig
          .set("yarn.scheduler.capacity.root.test3.capacity", "30");
      newConfig
          .set("yarn.scheduler.capacity.root.test1.capacity", "20");

      boolean isValidConfig = CapacitySchedulerConfigValidator
          .validateCSConfiguration(oldConfig, newConfig, rmContext);
      assertTrue(isValidConfig);

      assertFalse(cache.containsKey("root.test3"),
          "Validated new queue should not be in the cache");
      assertFalse(cache.containsKey("default.root.test3"),
          "Validated new queue should not be in the cache (PartitionQueueMetrics)");
      assertNull(ms.getSource("QueueMetrics,q0=root,q1=test3"),
          "Validated new queue should not be in the Metrics System");
      assertNull(ms.getSource("PartitionQueueMetrics,partition=,q0=root,q1=test3"),
          "Validated new queue should not be in Metrics System (PartitionQueueMetrics)");

      // Config validation should not change the existing
      // objects in the cache and the metrics system
      assertEquals(origQM1, cache.get("root.test1"));
      assertEquals(origQM2, cache.get("root.test2"));
      assertEquals(origPQM1, cache.get("default.root.test1"));
      assertEquals(origPQM1, cache.get("default.root.test1"));
      assertEquals(origMS1,
          ms.getSource("QueueMetrics,q0=root,q1=test1"));
      assertEquals(origMS2,
          ms.getSource("QueueMetrics,q0=root,q1=test2"));
      assertEquals(origPMS1,
          ms.getSource("PartitionQueueMetrics,partition=,q0=root,q1=test1"));
      assertEquals(origPMS2,
          ms.getSource("PartitionQueueMetrics,partition=,q0=root,q1=test2"));
    } finally {
      mockRM.stop();
    }
  }

  /**
   * Delete a running queue.
   */
  @Test
  public void testValidateCSConfigInvalidQueueDeletion() {
    Configuration oldConfig = CapacitySchedulerConfigGeneratorForTest
            .createBasicCSConfiguration();
    Configuration newConfig = new Configuration(oldConfig);
    newConfig.set("yarn.scheduler.capacity.root.queues", "test1");
    newConfig.set("yarn.scheduler.capacity.root.test1.capacity", "100");
    newConfig.unset("yarn.scheduler.capacity.root.test2.maximum-capacity");
    newConfig.unset("yarn.scheduler.capacity.root.test2.state");
    newConfig.set("yarn.scheduler.capacity.queue-mappings",
            "u:test1:test1");
    RMContext rmContext = prepareRMContext();
    try {
      CapacitySchedulerConfigValidator
              .validateCSConfiguration(oldConfig, newConfig, rmContext);
      fail("Invalid capacity for children of queue root");
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage()
              .contains("root.test2 cannot be deleted"));
      assertTrue(e.getCause().getMessage()
              .contains("the queue is not yet in stopped state"));
    }
  }

  @Test
  public void testValidateCSConfigDeletionReadsStateCaseSensitively() {
    Configuration oldConfig = CapacitySchedulerConfigGeneratorForTest
            .createBasicCSConfiguration();
    Configuration newConfig = new Configuration(oldConfig);
    newConfig.set("yarn.scheduler.capacity.root.queues", "test1");
    newConfig.set("yarn.scheduler.capacity.root.test1.capacity", "100");
    // Queue parsing accepts a lowercase state, the deletion check does not.
    newConfig.set("yarn.scheduler.capacity.root.test2.state", "stopped");
    newConfig.set("yarn.scheduler.capacity.queue-mappings", "u:test1:test1");
    RMContext rmContext = prepareRMContext();
    IOException e = assertThrows(IOException.class, () -> CapacitySchedulerConfigValidator
        .validateCSConfiguration(oldConfig, newConfig, rmContext));
    assertEquals("root.test2 cannot be deleted from the capacity scheduler configuration,"
        + " as the queue is not yet in stopped state. Current State : RUNNING",
        e.getCause().getMessage());
  }

  @Test
  public void testValidatePlacementRulesRejectsDuplicates() {
    IOException e = assertThrows(IOException.class, () -> CapacitySchedulerConfigValidator
        .validatePlacementRules(Arrays.asList("user-group", "user-group")));
    assertEquals("Invalid PlacementRule inputs which contains duplicate rule strings",
        e.getMessage());
  }

  /**
   * Delete a queue and not adjust capacities.
   */
  @Test
  public void testValidateCSConfigInvalidQueueDeletion2() {
    Configuration oldConfig = CapacitySchedulerConfigGeneratorForTest
            .createBasicCSConfiguration();
    oldConfig.set("yarn.scheduler.capacity.root.test2.state", "STOPPED");
    CapacitySchedulerConfiguration newConfig = new CapacitySchedulerConfiguration(oldConfig);
    newConfig.set("yarn.scheduler.capacity.root.queues", "test1");
    newConfig.unset("yarn.scheduler.capacity.root.test2.maximum-capacity");
    newConfig.unset("yarn.scheduler.capacity.root.test2.state");
    newConfig.set("yarn.scheduler.capacity.queue-mappings",
            "u:test1:test1");
    RMContext rmContext = prepareRMContext();
    try {
      CapacitySchedulerConfigValidator
              .validateCSConfiguration(oldConfig, newConfig, rmContext);
      if (newConfig.isLegacyQueueMode()) {
        fail("Invalid capacity for children of queue root");
      }
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage()
              .contains("Illegal capacity"));
    }
  }

  /**
   * Delete a queue and adjust capacities to have total capacity = 100.
   */
  @Test
  public void testValidateCSConfigValidQueueDeletion() throws IOException {
    Configuration oldConfig = CapacitySchedulerConfigGeneratorForTest
            .createBasicCSConfiguration();
    oldConfig.set("yarn.scheduler.capacity.root.test2.state", "STOPPED");
    Configuration newConfig = new Configuration(oldConfig);
    newConfig.set("yarn.scheduler.capacity.root.queues", "test1");
    newConfig.set("yarn.scheduler.capacity.root.test1.capacity", "100");
    newConfig.unset("yarn.scheduler.capacity.root.test2.maximum-capacity");
    newConfig.unset("yarn.scheduler.capacity.root.test2.state");
    newConfig.set("yarn.scheduler.capacity.queue-mappings",
            "u:test1:test1");
    RMContext rmContext = prepareRMContext();
    boolean isValidConfig = CapacitySchedulerConfigValidator
              .validateCSConfiguration(oldConfig, newConfig, rmContext);
    assertTrue(isValidConfig);

  }

  @Test
  public void testAddQueueToALeafQueue() throws IOException {
    Configuration oldConfig = CapacitySchedulerConfigGeneratorForTest
            .createBasicCSConfiguration();
    oldConfig.set("yarn.scheduler.capacity.root.test1.state", "STOPPED");
    Configuration newConfig = new Configuration(oldConfig);
    newConfig.set("yarn.scheduler.capacity.root.test1.queues", "newQueue");
    newConfig
            .set("yarn.scheduler.capacity.root.test1.newQueue.capacity", "100");
    newConfig.set("yarn.scheduler.capacity.queue-mappings",
            "u:test1:test2");
    RMContext rmContext = prepareRMContext();
    boolean isValidConfig = CapacitySchedulerConfigValidator
            .validateCSConfiguration(oldConfig, newConfig, rmContext);
    assertTrue(isValidConfig);
  }

  public static RMContext prepareRMContext() {
    setupResources(false);
    RMContext rmContext = mock(RMContext.class);
    CapacityScheduler mockCs = mock(CapacityScheduler.class);
    when(rmContext.getScheduler()).thenReturn(mockCs);
    LocalConfigurationProvider configProvider = mock(LocalConfigurationProvider.class);
    when(rmContext.getConfigurationProvider())
            .thenReturn(configProvider);
    RMNodeLabelsManager nodeLabelsManager = mock(RMNodeLabelsManager.class);
    when(rmContext.getNodeLabelManager()).thenReturn(nodeLabelsManager);
    LightWeightResource partitionResource = mock(LightWeightResource.class);
    when(nodeLabelsManager
        .getResourceByLabel(any(), any()))
        .thenReturn(partitionResource);
    PlacementManager queuePlacementManager = mock(PlacementManager.class);
    when(rmContext.getQueuePlacementManager())
        .thenReturn(queuePlacementManager);
    return rmContext;
  }

  private void setUpMockRM(boolean useDominantRC) throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    setupResources(useDominantRC);
    CapacitySchedulerConfiguration csConf = setupCSConfiguration(conf, useDominantRC);

    mockRM = new MockRM(csConf);

    cs = (CapacityScheduler) mockRM.getResourceScheduler();
    mockRM.start();
    cs.start();

    setupNodes(mockRM);
  }

  private void setupNodes(MockRM newMockRM) throws Exception {
      nm1 = new MockNM("h1:1234",
          Resource.newInstance(NODE_MEMORY * GB, NODE1_VCORES, NODE_GPU),
          newMockRM.getResourceTrackerService(),
          YarnVersionInfo.getVersion());

      nm1.registerNode();

      nm2 = new MockNM("h2:1234",
          Resource.newInstance(NODE_MEMORY * GB, NODE2_VCORES, NODE_GPU),
          newMockRM.getResourceTrackerService(),
          YarnVersionInfo.getVersion());
      nm2.registerNode();

      nm3 = new MockNM("h3:1234",
          Resource.newInstance(NODE_MEMORY * GB, NODE3_VCORES, NODE_GPU),
          newMockRM.getResourceTrackerService(),
          YarnVersionInfo.getVersion());
      nm3.registerNode();
  }

  private void setupGpuResourceValues() {
    A_MINRES_GPU = Resource.newInstance(A_MINRES.getMemorySize(), A_MINRES.getVirtualCores(),
        ImmutableMap.of(GPU_URI, 2L));
    B_MINRES_GPU =  Resource.newInstance(B_MINRES.getMemorySize(), B_MINRES.getVirtualCores(),
        ImmutableMap.of(GPU_URI, 2L));
    FULL_MAXRES_GPU = Resource.newInstance(FULL_MAXRES.getMemorySize(),
        FULL_MAXRES.getVirtualCores(), ImmutableMap.of(GPU_URI, 6L));
    PARTIAL_MAXRES_GPU = Resource.newInstance(PARTIAL_MAXRES.getMemorySize(),
        PARTIAL_MAXRES.getVirtualCores(), ImmutableMap.of(GPU_URI, 4L));
    GPU_EXCEEDED_MAXRES_GPU = Resource.newInstance(PARTIAL_MAXRES.getMemorySize(),
        PARTIAL_MAXRES.getVirtualCores(), ImmutableMap.of(GPU_URI, 50L));
  }

  private CapacitySchedulerConfiguration setupCSConfiguration(YarnConfiguration configuration,
                                                              boolean useDominantRC) {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(configuration);
    if (useDominantRC) {
      csConf.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
          DominantResourceCalculator.class.getName());
      csConf.set(YarnConfiguration.RESOURCE_TYPES, ResourceInformation.GPU_URI);
    }

    csConf.setQueues(new QueuePath(CapacitySchedulerConfiguration.ROOT),
        new String[]{PARENT_A, PARENT_B});
    csConf.setQueues(PARENT_A_FULL_PATH, new String[]{LEAF_A});
    csConf.setQueues(PARENT_B_FULL_PATH, new String[]{LEAF_B});

    if (useDominantRC) {
      setupGpuResourceValues();
      csConf.setMinimumResourceRequirement("", PARENT_A_FULL_PATH, A_MINRES_GPU);
      csConf.setMinimumResourceRequirement("", PARENT_B_FULL_PATH, B_MINRES_GPU);
      csConf.setMinimumResourceRequirement("", LEAF_A_FULL_PATH, A_MINRES_GPU);
      csConf.setMinimumResourceRequirement("", LEAF_B_FULL_PATH, B_MINRES_GPU);

      csConf.setMaximumResourceRequirement("", PARENT_A_FULL_PATH, PARTIAL_MAXRES_GPU);
      csConf.setMaximumResourceRequirement("", PARENT_B_FULL_PATH, FULL_MAXRES_GPU);
      csConf.setMaximumResourceRequirement("", LEAF_A_FULL_PATH, PARTIAL_MAXRES_GPU);
      csConf.setMaximumResourceRequirement("", LEAF_B_FULL_PATH, FULL_MAXRES_GPU);
    } else {
      csConf.setMinimumResourceRequirement("", PARENT_A_FULL_PATH, A_MINRES);
      csConf.setMinimumResourceRequirement("", PARENT_B_FULL_PATH, B_MINRES);
      csConf.setMinimumResourceRequirement("", LEAF_A_FULL_PATH, A_MINRES);
      csConf.setMinimumResourceRequirement("", LEAF_B_FULL_PATH, B_MINRES);

      csConf.setMaximumResourceRequirement("", PARENT_A_FULL_PATH, PARTIAL_MAXRES);
      csConf.setMaximumResourceRequirement("", PARENT_B_FULL_PATH, FULL_MAXRES);
      csConf.setMaximumResourceRequirement("", LEAF_A_FULL_PATH, PARTIAL_MAXRES);
      csConf.setMaximumResourceRequirement("", LEAF_B_FULL_PATH, FULL_MAXRES);
    }

    return csConf;
  }
}
