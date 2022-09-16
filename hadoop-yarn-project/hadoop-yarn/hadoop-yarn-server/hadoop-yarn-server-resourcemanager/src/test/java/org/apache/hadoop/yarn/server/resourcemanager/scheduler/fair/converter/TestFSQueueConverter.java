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

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for FSQueueConverter.
 *
 */
@ExtendWith(MockitoExtension.class)
public class TestFSQueueConverter {
  private static final float MAX_AM_SHARE_DEFAULT = 0.16f;
  private static final int MAX_APPS_DEFAULT = 15;
  private static final Resource CLUSTER_RESOURCE =
      Resource.newInstance(16384, 16);
  private final static Set<String> ALL_QUEUES =
      Sets.newHashSet("root",
          "root.default",
          "root.admins",
          "root.users",
          "root.admins.alice",
          "root.admins.bob",
          "root.users.joe",
          "root.users.john",
          "root.misc",
          "root.misc.a",
          "root.misc.b");

  private static final String FILE_PREFIX = "file:";
  private static final String FAIR_SCHEDULER_XML =
      prepareFileName("fair-scheduler-conversion.xml");

  private static String prepareFileName(String f) {
    return FILE_PREFIX + new File("src/test/resources/" + f).getAbsolutePath();
  }

  private FSQueueConverter converter;
  private Configuration yarnConfig;
  private Configuration csConfig;
  private FairScheduler fs;
  private FSQueue rootQueue;
  private ConversionOptions conversionOptions;
  private DryRunResultHolder dryRunResultHolder;
  private FSQueueConverterBuilder builder;

  @Mock
  private FSConfigToCSConfigRuleHandler ruleHandler;

  @BeforeEach
  public void setup() {
    yarnConfig = new Configuration(false);
    yarnConfig.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        FAIR_SCHEDULER_XML);
    yarnConfig.setBoolean(FairSchedulerConfiguration.MIGRATION_MODE, true);
    csConfig = new Configuration(false);
    dryRunResultHolder = new DryRunResultHolder();
    conversionOptions =
        new ConversionOptions(dryRunResultHolder, false);

    fs = createFairScheduler();
    createBuilder();

    rootQueue = fs.getQueueManager().getRootQueue();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (fs != null) {
      fs.close();
    }
  }

  private FairScheduler createFairScheduler() {
    RMContext ctx = new RMContextImpl();
    PlacementManager placementManager = new PlacementManager();
    ctx.setQueuePlacementManager(placementManager);

    FairScheduler fairScheduler = new FairScheduler();
    fairScheduler.setRMContext(ctx);
    fairScheduler.init(yarnConfig);

    return fairScheduler;
  }

  private void createBuilder() {
    builder = FSQueueConverterBuilder.create()
        .withRuleHandler(ruleHandler)
        .withCapacitySchedulerConfig(csConfig)
        .withPreemptionEnabled(false)
        .withSizeBasedWeight(false)
        .withClusterResource(CLUSTER_RESOURCE)
        .withQueueMaxAMShareDefault(MAX_AM_SHARE_DEFAULT)
        .withQueueMaxAppsDefault(MAX_APPS_DEFAULT)
        .withConversionOptions(conversionOptions);
  }

  @Test
  void testConvertQueueHierarchy() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    // root children
    assertEquals("admins,users,misc,default",
        csConfig.get(PREFIX + "root.queues"),"root children");

    // root.admins children
    assertEquals("bob,alice",
        csConfig.get(PREFIX + "root.admins.queues"),"root.admins children");

    // root.default children - none
    assertNull(csConfig.get(PREFIX + "root.default" +
        ".queues"), "root.default children");

    // root.users children
    assertEquals("john, joe",
        csConfig.get(PREFIX + "root.users.queues"),"root.users children");

    Set<String> leafs = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root",
            "root.default",
            "root.admins",
            "root.users",
            "root.misc"));

    assertNoValueForQueues(leafs, ".queues", csConfig);
  }

  @Test
  void testQueueMaxAMShare() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    // root.admins.bob
    assertEquals("1.0", csConfig.get(PREFIX + "root.admins.bob.maximum-am-resource-percent"),
        "root.admins.bob AM share");

    // root.admins.alice
    assertEquals("0.15", csConfig.get(PREFIX +
            "root.admins.alice.maximum-am-resource-percent"),
        "root.admins.alice AM share");

    Set<String> remaining = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.admins.bob", "root.admins.alice"));
    assertNoValueForQueues(remaining, ".maximum-am-resource-percent",
        csConfig);
  }

  @Test
  void testQueueMaxParallelApps() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    assertEquals(2, csConfig.getInt(PREFIX + "root.admins.alice.max-parallel-apps",
            -1),
        "root.admins.alice max apps");

    Set<String> remaining = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.admins.alice"));
    assertNoValueForQueues(remaining, ".max-parallel-apps", csConfig);
  }

  @Test
  void testQueueMaxAllocations() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    // root.admins vcores + mb
    assertEquals(3, csConfig.getInt(PREFIX + "root.admins.maximum-allocation-vcores", -1),
        "root.admins max vcores");
    assertEquals(4096, csConfig.getInt(PREFIX + "root.admins.maximum-allocation-mb", -1),
        "root.admins max memory");

    // root.users.john max vcores + mb
    assertEquals(2, csConfig.getInt(PREFIX + "root.users.john.maximum-allocation-vcores",
            -1),
        "root.users.john max vcores");
    assertEquals(8192, csConfig.getInt(PREFIX + "root.users.john.maximum-allocation-mb", -1),
        "root.users.john max memory");

    Set<String> remaining = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.admins", "root.users.john"));
    assertNoValueForQueues(remaining, ".maximum-allocation-vcores", csConfig);
    assertNoValueForQueues(remaining, ".maximum-allocation-mb", csConfig);
  }

  @Test
  void testQueuePreemptionDisabled() {
    converter = builder.withPreemptionEnabled(true).build();

    converter.convertQueueHierarchy(rootQueue);

    assertTrue(csConfig.getBoolean(PREFIX + "root.admins.alice.disable_preemption",
            false),
        "root.admins.alice preemption setting");
    assertTrue(csConfig.getBoolean(PREFIX + "root.users.joe.disable_preemption",
            false),
        "root.users.joe preemption setting");

    Set<String> remaining = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.admins.alice", "root.users.joe"));
    assertNoValueForQueues(remaining, ".disable_preemption", csConfig);
  }

  @Test
  void testQueuePreemptionDisabledWhenGlobalPreemptionDisabled() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    assertNoValueForQueues(ALL_QUEUES, ".disable_preemption", csConfig);
  }

  @Test
  void testChildCapacityInCapacityMode() {
    converter = builder.withPercentages(true).build();

    converter.convertQueueHierarchy(rootQueue);

    // root
    assertEquals("33.333", csConfig.get(PREFIX + "root.default.capacity"),
        "root.default capacity");
    assertEquals("33.333", csConfig.get(PREFIX + "root.admins.capacity"),
        "root.admins capacity");
    assertEquals("33.334", csConfig.get(PREFIX + "root.users.capacity"),
        "root.users capacity");

    // root.users
    assertEquals("25.000", csConfig.get(PREFIX + "root.users.john.capacity"),
        "root.users.john capacity");
    assertEquals("75.000", csConfig.get(PREFIX + "root.users.joe.capacity"),
        "root.users.joe capacity");

    // root.admins
    assertEquals("75.000", csConfig.get(PREFIX + "root.admins.alice.capacity"),
        "root.admins.alice capacity");
    assertEquals("25.000", csConfig.get(PREFIX + "root.admins.bob.capacity"),
        "root.admins.bob capacity");

    // root.misc
    assertEquals("0.000", csConfig.get(PREFIX + "root.misc.capacity"),
        "root.misc capacity");
    assertEquals("0.000", csConfig.get(PREFIX + "root.misc.a.capacity"),
        "root.misc.a capacity");
    assertEquals("0.000", csConfig.get(PREFIX + "root.misc.b.capacity"),
        "root.misc.b capacity");
  }

  @Test
  void testChildCapacityInWeightMode() {
    converter = builder.withPercentages(false).build();

    converter.convertQueueHierarchy(rootQueue);

    // root
    assertEquals("1.0w", csConfig.get(PREFIX + "root.default.capacity"),
        "root.default weight");
    assertEquals("1.0w", csConfig.get(PREFIX + "root.admins.capacity"),
        "root.admins weight");
    assertEquals("1.0w", csConfig.get(PREFIX + "root.users.capacity"),
        "root.users weight");

    // root.users
    assertEquals("1.0w", csConfig.get(PREFIX + "root.users.john.capacity"),
        "root.users.john weight");
    assertEquals("3.0w", csConfig.get(PREFIX + "root.users.joe.capacity"),
        "root.users.joe weight");

    // root.admins
    assertEquals("3.0w", csConfig.get(PREFIX + "root.admins.alice.capacity"),
        "root.admins.alice weight");
    assertEquals("1.0w", csConfig.get(PREFIX + "root.admins.bob.capacity"),
        "root.admins.bob weight");

    // root.misc
    assertEquals("0.0w", csConfig.get(PREFIX + "root.misc.capacity"),
        "root.misc weight");
    assertEquals("0.0w", csConfig.get(PREFIX + "root.misc.a.capacity"),
        "root.misc.a weight");
    assertEquals("0.0w", csConfig.get(PREFIX + "root.misc.b.capacity"),
        "root.misc.b weight");
  }

  @Test
  void testAutoCreateV2FlagsInWeightMode() {
    converter = builder.withPercentages(false).build();

    converter.convertQueueHierarchy(rootQueue);

    assertTrue(csConfig.getBoolean(
            PREFIX + "root.auto-queue-creation-v2.enabled", false),
        "root autocreate v2 flag");
    assertTrue(csConfig.getBoolean(
            PREFIX + "root.admins.auto-queue-creation-v2.enabled", false),
        "root.admins autocreate v2 flag");
    assertTrue(csConfig.getBoolean(
            PREFIX + "root.users.auto-queue-creation-v2.enabled", false),
        "root.users autocreate v2 flag");
    assertTrue(csConfig.getBoolean(
            PREFIX + "root.misc.auto-queue-creation-v2.enabled", false),
        "root.misc autocreate v2 flag");

    Set<String> leafs = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root",
            "root.default",
            "root.admins",
            "root.users",
            "root.misc"));
    assertNoValueForQueues(leafs, "auto-queue-creation-v2.enabled",
        csConfig);
  }

  @Test
  void testZeroSumCapacityValidation() {
    converter = builder.withPercentages(true).build();

    converter.convertQueueHierarchy(rootQueue);

    Set<String> noZeroSumAllowedQueues = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.misc"));
    assertNoValueForQueues(noZeroSumAllowedQueues, ".allow-zero-capacity-sum",
        csConfig);

    assertTrue(csConfig.getBoolean(
        PREFIX + "root.misc.allow-zero-capacity-sum", false), "root.misc allow zero capacities");
  }

  @Test
  void testQueueMaximumCapacity() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    assertValueForQueues(ALL_QUEUES, ".maximum-capacity", csConfig, "100");
    verify(ruleHandler, times(3)).handleMaxResources();
  }

  @Test
  void testQueueMinimumCapacity() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    verify(ruleHandler, times(2)).handleMinResources();
  }

  @Test
  void testQueueWithNoAutoCreateChildQueue() {
    converter = builder
        .withCapacitySchedulerConfig(csConfig)
        .build();

    converter.convertQueueHierarchy(rootQueue);

    assertNoValueForQueues(ALL_QUEUES, ".auto-create-child-queue.enabled",
        csConfig);
  }

  @Test
  void testQueueSizeBasedWeightEnabled() {
    converter = builder.withSizeBasedWeight(true).build();

    converter.convertQueueHierarchy(rootQueue);

    assertTrueForQueues(ALL_QUEUES,
        ".ordering-policy.fair.enable-size-based-weight", csConfig);
  }

  @Test
  void testQueueSizeBasedWeightDisabled() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    assertNoValueForQueues(ALL_QUEUES,
        ".ordering-policy.fair.enable-size-based-weight", csConfig);
  }

  @Test
  void testQueueOrderingPolicy() throws Exception {
    converter = builder.build();
    String absolutePath =
        new File("src/test/resources/fair-scheduler-orderingpolicy.xml")
          .getAbsolutePath();
    yarnConfig.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        FILE_PREFIX + absolutePath);
    fs.close();
    fs = createFairScheduler();
    rootQueue = fs.getQueueManager().getRootQueue();

    converter.convertQueueHierarchy(rootQueue);

    // root
    assertEquals(null, csConfig.get(PREFIX + "root.ordering-policy"),
        "root ordering policy");
    assertEquals("fair", csConfig.get(PREFIX + "root.default.ordering-policy"),
        "root.default ordering policy");
    assertEquals(null, csConfig.get(PREFIX + "root.admins.ordering-policy"),
        "root.admins ordering policy");
    assertEquals(null, csConfig.get(PREFIX + "root.users.ordering-policy"),
        "root.users ordering policy");

    // root.users
    assertEquals("fair", csConfig.get(PREFIX + "root.users.joe.ordering-policy"),
        "root.users.joe ordering policy");
    assertEquals("fifo", csConfig.get(PREFIX + "root.users.john.ordering-policy"),
        "root.users.john ordering policy");

    // root.admins
    assertEquals("fifo", csConfig.get(PREFIX + "root.admins.alice.ordering-policy"),
        "root.admins.alice ordering policy");
    assertEquals("fair", csConfig.get(PREFIX + "root.admins.bob.ordering-policy"),
        "root.admins.bob ordering policy");
  }

  @Test
  void testQueueUnsupportedMixedOrderingPolicy() throws IOException {
    converter = builder.withDrfUsed(true).build();
    String absolutePath =
        new File("src/test/resources/fair-scheduler-orderingpolicy-mixed.xml")
          .getAbsolutePath();
    yarnConfig.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        FILE_PREFIX + absolutePath);
    fs.close();
    fs = createFairScheduler();
    rootQueue = fs.getQueueManager().getRootQueue();

    converter.convertQueueHierarchy(rootQueue);

    verify(ruleHandler, times(5)).handleFairAsDrf(anyString());
  }

  @Test
  void testQueueMaxChildCapacityNotSupported() {
    converter = builder.build();
    Mockito.doThrow(new UnsupportedPropertyException("test"))
      .when(ruleHandler).handleMaxChildCapacity();

    final Exception ex = Assertions.assertThrows(
        UnsupportedPropertyException.class,
        () -> converter.convertQueueHierarchy(rootQueue)
    );
    Assertions.assertEquals(ex.getMessage(), "test");
  }

  @Test
  void testReservationSystemNotSupported() {
    converter = builder.build();

    Mockito.doThrow(new UnsupportedPropertyException("maxCapacity"))
      .when(ruleHandler).handleMaxChildCapacity();
    yarnConfig.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE,
        true);

    final Exception ex = Assertions.assertThrows(
        UnsupportedPropertyException.class,
        () -> converter.convertQueueHierarchy(rootQueue)
    );
    Assertions.assertEquals(ex.getMessage(), "maxCapacity");
  }

  private void assertNoValueForQueues(Set<String> queues, String postfix,
      Configuration config) {
    for (String queue : queues) {
      String key = PREFIX + queue + postfix;
      assertNull(config.get(key),
          "Key " + key + " has value, but it should be null");
    }
  }

  private void assertValueForQueues(Set<String> queues, String postfix,
      Configuration config, String expectedValue) {
    for (String queue : queues) {
      String key = PREFIX + queue + postfix;
      assertEquals(expectedValue, config.get(key),
          "Key " + key + " has different value");
    }
  }

  private void assertTrueForQueues(Set<String> queues, String postfix,
      Configuration config) {
    for (String queue : queues) {
      String key = PREFIX + queue + postfix;
      assertTrue(config.getBoolean(key, false),
          "Key " + key + " is false, should be true");
    }
  }
}