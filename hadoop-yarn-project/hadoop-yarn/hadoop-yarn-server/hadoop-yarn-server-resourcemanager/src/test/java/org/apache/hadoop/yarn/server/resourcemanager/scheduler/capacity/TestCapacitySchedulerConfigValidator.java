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
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.records.impl.LightWeightResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

public class TestCapacitySchedulerConfigValidator {

  /**
   * Test for the case when the scheduler.minimum-allocation-mb == 0.
   */
  @Test (expected = YarnRuntimeException.class)
  public void testValidateMemoryAllocationInvalidMinMem() {
    Map<String, String> configs = new HashMap();
    configs.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, "0");
    Configuration config = CapacitySchedulerConfigGeneratorForTest
            .createConfiguration(configs);
    CapacitySchedulerConfigValidator.validateMemoryAllocation(config);
    fail(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB +
            " should be > 0");
  }

  /**
   * Test for the case when the scheduler.minimum-allocation-mb is greater than
   * scheduler.maximum-allocation-mb.
   */
  @Test (expected = YarnRuntimeException.class)
  public void testValidateMemoryAllocationHIgherMinThanMaxMem() {
    Map<String, String> configs = new HashMap();
    configs.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, "8192");
    configs.put(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, "1024");
    Configuration config = CapacitySchedulerConfigGeneratorForTest
            .createConfiguration(configs);
    CapacitySchedulerConfigValidator.validateMemoryAllocation(config);
    fail(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB + " should be > "
            + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

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
  @Test (expected = YarnRuntimeException.class)
  public void testValidateVCoresInvalidMinVCore() {
    Map<String, String> configs = new HashMap();
    configs.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, "0");
    Configuration config = CapacitySchedulerConfigGeneratorForTest
            .createConfiguration(configs);
    CapacitySchedulerConfigValidator.validateVCores(config);
    fail(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES
            + " should be > 0");
  }

  /**
   * Test for the case when the scheduler.minimum-allocation-vcores is greater
   * than scheduler.maximum-allocation-vcores.
   */
  @Test (expected = YarnRuntimeException.class)
  public void testValidateVCoresHigherMinThanMaxVCore() {
    Map<String, String> configs = new HashMap();
    configs.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, "4");
    configs.put(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, "1");
    Configuration config = CapacitySchedulerConfigGeneratorForTest
            .createConfiguration(configs);
    CapacitySchedulerConfigValidator.validateVCores(config);
    fail(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES +
            " should be > "
            + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

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
      Assert.assertTrue(e.getCause().getMessage()
              .startsWith("Illegal capacity"));
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
    Boolean isValidConfig = CapacitySchedulerConfigValidator
            .validateCSConfiguration(oldConfig, newConfig, rmContext);
    Assert.assertTrue(isValidConfig);
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
      Assert.assertTrue(e.getCause().getMessage()
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
    Assert.assertTrue(isValidConfig);

  }

  /**
   * Add a leaf queue without modifying the capacity of other leaf queues
   * so the total capacity != 100.
   */
  @Test
  public void testValidateCSConfigAddALeafQueueInvalid() {
    Configuration oldConfig = CapacitySchedulerConfigGeneratorForTest
            .createBasicCSConfiguration();
    Configuration newConfig = new Configuration(oldConfig);
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
      fail("Invalid capacity for children of queue root");
    } catch (IOException e) {
      Assert.assertTrue(e.getCause().getMessage()
              .startsWith("Illegal capacity"));
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
    Assert.assertTrue(isValidConfig);
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
      Assert.assertTrue(e.getCause().getMessage()
              .contains("root.test2 cannot be deleted"));
      Assert.assertTrue(e.getCause().getMessage()
              .contains("the queue is not yet in stopped state"));
    }
  }

  /**
   * Delete a queue and not adjust capacities.
   */
  @Test
  public void testValidateCSConfigInvalidQueueDeletion2() {
    Configuration oldConfig = CapacitySchedulerConfigGeneratorForTest
            .createBasicCSConfiguration();
    Configuration newConfig = new Configuration(oldConfig);
    newConfig.set("yarn.scheduler.capacity.root.queues", "test1");
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
      Assert.assertTrue(e.getCause().getMessage()
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
    Assert.assertTrue(isValidConfig);

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
    Assert.assertTrue(isValidConfig);
  }


  public static RMContext prepareRMContext() {
    RMContext rmContext = Mockito.mock(RMContext.class);
    LocalConfigurationProvider configProvider = Mockito
            .mock(LocalConfigurationProvider.class);
    Mockito.when(rmContext.getConfigurationProvider())
            .thenReturn(configProvider);
    RMNodeLabelsManager nodeLabelsManager = Mockito
            .mock(RMNodeLabelsManager.class);
    Mockito.when(rmContext.getNodeLabelManager()).thenReturn(nodeLabelsManager);
    LightWeightResource partitionResource = Mockito
            .mock(LightWeightResource.class);
    Mockito.when(nodeLabelsManager
            .getResourceByLabel(Mockito.any(), Mockito.any()))
            .thenReturn(partitionResource);
    PlacementManager queuePlacementManager = Mockito
            .mock(PlacementManager.class);
    Mockito.when(rmContext.getQueuePlacementManager())
            .thenReturn(queuePlacementManager);
    return rmContext;
  }
}
