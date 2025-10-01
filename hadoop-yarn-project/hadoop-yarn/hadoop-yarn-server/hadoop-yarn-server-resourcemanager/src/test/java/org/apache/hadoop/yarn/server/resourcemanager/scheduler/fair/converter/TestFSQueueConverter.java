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

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DEFAULT_MAX_PARALLEL_APPLICATIONS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
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
  private CapacitySchedulerConfiguration csConfig;
  private FairScheduler fs;
  private FSQueue rootQueue;
  private ConversionOptions conversionOptions;
  private DryRunResultHolder dryRunResultHolder;
  private FSQueueConverterBuilder builder;
  private String key;

  private static final QueuePath ROOT = new QueuePath("root");
  private static final QueuePath DEFAULT = new QueuePath("root.default");
  private static final QueuePath USERS = new QueuePath("root.users");
  private static final QueuePath USERS_JOE = new QueuePath("root.users.joe");
  private static final QueuePath USERS_JOHN = new QueuePath("root.users.john");
  private static final QueuePath ADMINS = new QueuePath("root.admins");
  private static final QueuePath ADMINS_ALICE = new QueuePath("root.admins.alice");
  private static final QueuePath ADMINS_BOB = new QueuePath("root.admins.bob");
  private static final QueuePath MISC = new QueuePath("root.misc");
  private static final QueuePath MISC_A = new QueuePath("root.misc.a");
  private static final QueuePath MISC_B = new QueuePath("root.misc.b");

  @Mock
  private FSConfigToCSConfigRuleHandler ruleHandler;

  @BeforeEach
  public void setup() {
    yarnConfig = new Configuration(false);
    yarnConfig.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        FAIR_SCHEDULER_XML);
    yarnConfig.setBoolean(FairSchedulerConfiguration.MIGRATION_MODE, true);
    csConfig = new CapacitySchedulerConfiguration(
        new Configuration(false));
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
  public void testConvertQueueHierarchy() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    // root children
    assertEquals(Arrays.asList("admins", "users", "misc", "default"),
        csConfig.getQueues(ROOT), "root children");

    // root.admins children
    assertEquals(Arrays.asList("bob", "alice"),
        csConfig.getQueues(ADMINS), "root.admins children");

    // root.default children - none
    assertTrue(csConfig.getQueues(DEFAULT).isEmpty(), "root.default children");

    // root.users children
    assertEquals(Arrays.asList("john", "joe"),
        csConfig.getQueues(USERS), "root.users children");

    Set<String> leafs = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root",
            "root.default",
            "root.admins",
            "root.users",
            "root.misc"));

    for (String queue : leafs) {
      key = PREFIX + queue + ".queues";
      assertTrue(csConfig.getQueues(new QueuePath(queue)).isEmpty(),
          "Key " + key + " has value, but it should be empty");
    }

  }

  @Test
  public void testQueueMaxAMShare() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    // root.admins.bob
    assertEquals(1.0f,
        csConfig.getMaximumApplicationMasterResourcePerQueuePercent(
            ADMINS_BOB), 0.0f, "root.admins.bob AM share");

    // root.admins.alice
    assertEquals(0.15f,
        csConfig.getMaximumApplicationMasterResourcePerQueuePercent(
            ADMINS_ALICE), 0.0f, "root.admins.alice AM share");

    Set<String> remaining = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.admins.bob", "root.admins.alice"));

    for (String queue : remaining) {
      key = PREFIX + queue + ".maximum-am-resource-percent";
      assertEquals(0.1f,
          csConfig.getMaximumApplicationMasterResourcePerQueuePercent(new QueuePath(queue)), 0.0f,
          "Key " + key + " has different value");
    }
  }

  @Test
  public void testQueueMaxParallelApps() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    assertEquals(2, csConfig.getMaxParallelAppsForQueue(ADMINS_ALICE), 0,
        "root.admins.alice max apps");

    Set<String> remaining = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.admins.alice"));

    for (String queue : remaining) {
      key = PREFIX + queue + ".max-parallel-apps";
      assertEquals(DEFAULT_MAX_PARALLEL_APPLICATIONS,
          csConfig.getMaxParallelAppsForQueue(new QueuePath(queue)), 0,
          "Key " + key + " has different value");
    }
  }

  @Test
  public void testQueueMaxAllocations() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    // root.admins vcores + mb
    assertEquals(3, csConfig.getQueueMaximumAllocationVcores(ADMINS),
        "root.admins max vcores");
    assertEquals(4096, csConfig.getQueueMaximumAllocationMb(ADMINS),
        "root.admins max memory");

    // root.users.john max vcores + mb
    assertEquals(2, csConfig.getQueueMaximumAllocationVcores(USERS_JOHN),
        "root.users.john max vcores");
    assertEquals(8192, csConfig.getQueueMaximumAllocationMb(USERS_JOHN),
        "root.users.john max memory");

    Set<String> remaining = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.admins", "root.users.john"));

    for (String queue : remaining) {
      key = PREFIX + queue + ".maximum-allocation-vcores";
      assertEquals(-1.0,
          csConfig.getQueueMaximumAllocationVcores(new QueuePath(queue)), 0.0f,
          "Key " + key + " has different value");

      key = PREFIX + queue + ".maximum-allocation-mb";
      assertEquals(-1.0,
          csConfig.getQueueMaximumAllocationMb(new QueuePath(queue)), 0.0f,
          "Key " + key + " has different value");
    }
  }

  @Test
  public void testQueuePreemptionDisabled() {
    converter = builder.withPreemptionEnabled(true).build();

    converter.convertQueueHierarchy(rootQueue);

    assertTrue(csConfig.getPreemptionDisabled(
        ADMINS_ALICE, false), "root.admins.alice preemption setting");
    assertTrue(csConfig.getPreemptionDisabled(
        USERS_JOE, false), "root.users.joe preemption setting");

    Set<String> remaining = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.admins.alice", "root.users.joe"));

    for (String queue : remaining) {
      key = PREFIX + queue + ".disable_preemption";
      assertEquals(false, csConfig.getPreemptionDisabled(new QueuePath(queue), false),
          "Key " + key + " has different value");
    }
  }

  @Test
  public void testQueuePreemptionDisabledWhenGlobalPreemptionDisabled() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    for (String queue : ALL_QUEUES) {
      key = PREFIX + queue + ".disable_preemption";
      assertEquals(false, csConfig.getPreemptionDisabled(new QueuePath(queue), false),
          "Key " + key + " has different value");
    }
  }

  @Test
  public void testChildCapacityInCapacityMode() {
    converter = builder.withPercentages(true).build();

    converter.convertQueueHierarchy(rootQueue);

    // root
    assertEquals(33.333f, csConfig.getNonLabeledQueueCapacity(DEFAULT), 0.0f,
        "root.default capacity");
    assertEquals(33.333f, csConfig.getNonLabeledQueueCapacity(ADMINS), 0.0f,
        "root.admins capacity");
    assertEquals(33.334f, csConfig.getNonLabeledQueueCapacity(USERS), 0.0f,
        "root.users capacity");

    // root.users
    assertEquals(25.000f, csConfig.getNonLabeledQueueCapacity(USERS_JOHN), 0.0f,
        "root.users.john capacity");
    assertEquals(75.000f, csConfig.getNonLabeledQueueCapacity(USERS_JOE), 0.0f,
        "root.users.joe capacity");

    // root.admins
    assertEquals(75.000f, csConfig.getNonLabeledQueueCapacity(ADMINS_ALICE), 0.0f,
        "root.admins.alice capacity");
    assertEquals(25.000f, csConfig.getNonLabeledQueueCapacity(ADMINS_BOB), 0.0f,
        "root.admins.bob capacity");

    // root.misc
    assertEquals(0.000f, csConfig.getNonLabeledQueueCapacity(MISC), 0.000f,
        "root.misc capacity");
    assertEquals(0.000f, csConfig.getNonLabeledQueueCapacity(MISC_A), 0.000f,
        "root.misc.a capacity");
    assertEquals(0.000f, csConfig.getNonLabeledQueueCapacity(MISC_B), 0.000f,
        "root.misc.b capacity");
  }

  @Test
  public void testChildCapacityInWeightMode() {
    converter = builder.withPercentages(false).build();

    converter.convertQueueHierarchy(rootQueue);

    // root
    assertEquals(1.0f, csConfig.getNonLabeledQueueWeight(DEFAULT), 0.01f,
        "root.default weight");
    assertEquals(1.0f, csConfig.getNonLabeledQueueWeight(ADMINS), 0.01f,
        "root.admins weight");
    assertEquals(1.0f, csConfig.getNonLabeledQueueWeight(USERS), 0.01f,
        "root.users weight");

    // root.users
    assertEquals(1.0f, csConfig.getNonLabeledQueueWeight(USERS_JOHN), 0.01f,
        "root.users.john weight");
    assertEquals(3.0f, csConfig.getNonLabeledQueueWeight(USERS_JOE), 0.01f,
        "root.users.joe weight");

    // root.admins
    assertEquals(3.0f, csConfig.getNonLabeledQueueWeight(ADMINS_ALICE), 0.01f,
        "root.admins.alice weight");
    assertEquals(1.0f, csConfig.getNonLabeledQueueWeight(ADMINS_BOB), 0.01f,
        "root.admins.bob weight");

    // root.misc
    assertEquals(0.0f, csConfig.getNonLabeledQueueWeight(MISC), 0.00f,
        "root.misc weight");
    assertEquals(0.0f, csConfig.getNonLabeledQueueWeight(MISC_A), 0.00f,
        "root.misc.a weight");
    assertEquals(0.0f, csConfig.getNonLabeledQueueWeight(MISC_B), 0.00f,
        "root.misc.b weight");
  }

  @Test
  public void testAutoCreateV2FlagsInWeightMode() {
    converter = builder.withPercentages(false).build();

    converter.convertQueueHierarchy(rootQueue);

    assertTrue(csConfig.isAutoQueueCreationV2Enabled(ROOT),
        "root autocreate v2 flag");
    assertTrue(csConfig.isAutoQueueCreationV2Enabled(ADMINS),
        "root.admins autocreate v2 flag");
    assertTrue(csConfig.isAutoQueueCreationV2Enabled(ADMINS_ALICE),
        "root.admins.alice autocreate v2 flag");
    assertTrue(csConfig.isAutoQueueCreationV2Enabled(USERS),
        "root.users autocreate v2 flag");
    assertTrue(csConfig.isAutoQueueCreationV2Enabled(MISC),
        "root.misc autocreate v2 flag");

    //leaf queue root.admins.alice is removed from the below list
    //adding reservation to a leaf, it's queueType changes to FSParentQueue
    Set<String> leafs = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root",
            "root.admins",
            "root.users",
            "root.misc",
            "root.admins.alice"));

    for (String queue : leafs) {
      key = PREFIX + queue + ".auto-queue-creation-v2.enabled";
      assertEquals(false, csConfig.isAutoQueueCreationV2Enabled(new QueuePath(queue)),
          "Key " + key + " has different value");
    }

  }

  @Test
  public void testZeroSumCapacityValidation() {
    converter = builder.withPercentages(true).build();

    converter.convertQueueHierarchy(rootQueue);

    Set<String> noZeroSumAllowedQueues = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.misc"));

    for (String queue : noZeroSumAllowedQueues) {
      key = PREFIX + queue + ".allow-zero-capacity-sum";
      assertEquals(false, csConfig.getAllowZeroCapacitySum(new QueuePath(queue)),
          "Key " + key + " has different value");
    }

    assertTrue(csConfig.getAllowZeroCapacitySum(MISC), "root.misc allow zero capacities");
  }

  @Test
  public void testQueueMaximumCapacity() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    for (String queue : ALL_QUEUES) {
      key = PREFIX + queue + ".maximum-capacity";
      assertEquals(100.0, csConfig.getNonLabeledQueueMaximumCapacity(new QueuePath(queue)), 0.0f,
          "Key " + key + " has different value");
    }
    verify(ruleHandler, times(3)).handleMaxResources();
  }

  @Test
  public void testQueueMinimumCapacity() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    verify(ruleHandler, times(2)).handleMinResources();
  }

  @Test
  public void testQueueWithNoAutoCreateChildQueue() {
    converter = builder
        .withCapacitySchedulerConfig(csConfig)
        .build();

    converter.convertQueueHierarchy(rootQueue);

    for (String queue : ALL_QUEUES) {
      key = PREFIX + queue + ".auto-create-child-queue.enabled";
      assertEquals(false, csConfig.isAutoCreateChildQueueEnabled(new QueuePath(queue)),
          "Key " + key + " has different value");
    }
  }

  @Test
  public void testQueueSizeBasedWeightEnabled() {
    converter = builder.withSizeBasedWeight(true).build();

    converter.convertQueueHierarchy(rootQueue);

    for (String queue : ALL_QUEUES) {
      key = PREFIX + queue + ".ordering-policy.fair.enable-size-based-weight";
      assertTrue(csConfig.getBoolean(key, false), "Key " + key + " has different value");
    }
  }

  @Test
  public void testQueueSizeBasedWeightDisabled() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    for (String queue : ALL_QUEUES) {
      key = PREFIX + queue + ".ordering-policy.fair.enable-size-based-weight";
      assertNull(csConfig.get(key), "Key " + key + " has different value");
    }
  }

  @Test
  public void testQueueOrderingPolicy() throws Exception {
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
    assertEquals("fifo", csConfig.getAppOrderingPolicy(ROOT).getConfigName(),
        "root ordering policy");
    assertEquals("fair", csConfig.getAppOrderingPolicy(DEFAULT).getConfigName(),
        "root.default ordering policy");
    assertEquals("fifo", csConfig.getAppOrderingPolicy(ADMINS).getConfigName(),
        "root.admins ordering policy");
    assertEquals("fifo", csConfig.getAppOrderingPolicy(USERS).getConfigName(),
        "root.users ordering policy");

    // root.users
    assertEquals("fair", csConfig.getAppOrderingPolicy(USERS_JOE).getConfigName(),
        "root.users.joe ordering policy");
    assertEquals("fifo", csConfig.getAppOrderingPolicy(USERS_JOHN).getConfigName(),
        "root.users.john ordering policy");

    // root.admins
    assertEquals("fifo", csConfig.getAppOrderingPolicy(ADMINS_ALICE).getConfigName(),
        "root.admins.alice ordering policy");
    assertEquals("fair", csConfig.getAppOrderingPolicy(ADMINS_BOB).getConfigName(),
        "root.admins.bob ordering policy");
  }

  @Test
  public void testQueueUnsupportedMixedOrderingPolicy() throws IOException {
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
  public void testQueueMaxChildCapacityNotSupported() {
    UnsupportedPropertyException exception =
        assertThrows(UnsupportedPropertyException.class, () -> {
          converter = builder.build();
          doThrow(new UnsupportedPropertyException("test"))
              .when(ruleHandler).handleMaxChildCapacity();
          converter.convertQueueHierarchy(rootQueue);
        });
    assertThat(exception.getMessage()).contains("test");
  }

  @Test
  public void testReservationSystemNotSupported() {
    assertThrows(UnsupportedPropertyException.class, () -> {
      converter = builder.build();
      doThrow(new UnsupportedPropertyException("maxCapacity"))
           .when(ruleHandler).handleMaxChildCapacity();
      yarnConfig.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE,
           true);
      converter.convertQueueHierarchy(rootQueue);
    });
  }
}