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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Unit tests for FSQueueConverter.
 *
 */
@RunWith(MockitoJUnitRunner.class)
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

  @Mock
  private FSConfigToCSConfigRuleHandler ruleHandler;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
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

  @After
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
    assertArrayEquals("root children", new String[]{"admins", "users", "misc", "default"},
        csConfig.getQueues("root"));

    // root.admins children
    assertArrayEquals("root.admins children", new String[]{"bob", "alice"},
        csConfig.getQueues("root.admins"));

    // root.default children - none
    assertNull("root.default children",
        csConfig.getQueues("root.default"));

    // root.users children
    assertArrayEquals("root.users children", new String[]{"john", "joe"},
        csConfig.getQueues("root.users"));

    Set<String> leafs = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root",
            "root.default",
            "root.admins",
            "root.users",
            "root.misc"));

    for (String queue : leafs) {
      key = PREFIX + queue + ".queues";
      assertNull("Key " + key + " has value, but it should be null",
          csConfig.getQueues(queue));
    }

  }

  @Test
  public void testQueueMaxAMShare() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    // root.admins.bob
    assertEquals("root.admins.bob AM share", 1.0f,
        csConfig.getMaximumApplicationMasterResourcePerQueuePercent(
            "root.admins.bob"), 0.0f);

    // root.admins.alice
    assertEquals("root.admins.alice AM share", 0.15f,
        csConfig.getMaximumApplicationMasterResourcePerQueuePercent(
            "root.admins.alice"), 0.0f);

    Set<String> remaining = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.admins.bob", "root.admins.alice"));

    for (String queue : remaining) {
      key = PREFIX + queue + ".maximum-am-resource-percent";
      assertEquals("Key " + key + " has different value",
          0.1f, csConfig
              .getMaximumApplicationMasterResourcePerQueuePercent(queue), 0.0f);
    }
  }

  @Test
  public void testQueueMaxParallelApps() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    assertEquals("root.admins.alice max apps", 2,
        csConfig.getMaxParallelAppsForQueue("root.admins.alice"), 0);

    Set<String> remaining = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.admins.alice"));

    for (String queue : remaining) {
      key = PREFIX + queue + ".max-parallel-apps";
      assertEquals("Key " + key + " has different value",
          DEFAULT_MAX_PARALLEL_APPLICATIONS, csConfig
              .getMaxParallelAppsForQueue(queue), 0);
    }
  }

  @Test
  public void testQueueMaxAllocations() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    // root.admins vcores + mb
    assertEquals("root.admins max vcores", 3,
        csConfig.getQueueMaximumAllocationVcores("root.admins"));
    assertEquals("root.admins max memory", 4096,
        csConfig.getQueueMaximumAllocationMb("root.admins"));

    // root.users.john max vcores + mb
    assertEquals("root.users.john max vcores", 2,
        csConfig.getQueueMaximumAllocationVcores("root.users.john"));
    assertEquals("root.users.john max memory", 8192,
        csConfig.getQueueMaximumAllocationMb("root.users.john"));

    Set<String> remaining = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.admins", "root.users.john"));

    for (String queue : remaining) {
      key = PREFIX + queue + ".maximum-allocation-vcores";
      assertEquals("Key " + key + " has different value",
          -1.0, csConfig
              .getQueueMaximumAllocationVcores(queue), 0.0f);

      key = PREFIX + queue + ".maximum-allocation-mb";
      assertEquals("Key " + key + " has different value",
          -1.0, csConfig
              .getQueueMaximumAllocationMb(queue), 0.0f);
    }
  }

  @Test
  public void testQueuePreemptionDisabled() {
    converter = builder.withPreemptionEnabled(true).build();

    converter.convertQueueHierarchy(rootQueue);

    assertTrue("root.admins.alice preemption setting",
        csConfig.getPreemptionDisabled(
            "root.admins.alice", false));
    assertTrue("root.users.joe preemption setting",
        csConfig.getPreemptionDisabled(
            "root.users.joe", false));

    Set<String> remaining = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.admins.alice", "root.users.joe"));

    for (String queue : remaining) {
      key = PREFIX + queue + ".disable_preemption";
      assertEquals("Key " + key + " has different value",
          false, csConfig.getPreemptionDisabled(queue, false));
    }
  }

  @Test
  public void testQueuePreemptionDisabledWhenGlobalPreemptionDisabled() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    for (String queue : ALL_QUEUES) {
      key = PREFIX + queue + ".disable_preemption";
      assertEquals("Key " + key + " has different value",
          false, csConfig.getPreemptionDisabled(queue, false));
    }
  }

  @Test
  public void testChildCapacityInCapacityMode() {
    converter = builder.withPercentages(true).build();

    converter.convertQueueHierarchy(rootQueue);

    // root
    assertEquals("root.default capacity", 33.333f,
        csConfig.getNonLabeledQueueCapacity(
            new QueuePath("root.default")), 0.0f);
    assertEquals("root.admins capacity", 33.333f,
        csConfig.getNonLabeledQueueCapacity(
            new QueuePath("root.admins")), 0.0f);
    assertEquals("root.users capacity", 33.334f,
        csConfig.getNonLabeledQueueCapacity(
            new QueuePath("root.users")), 0.0f);

    // root.users
    assertEquals("root.users.john capacity", 25.000f,
        csConfig.getNonLabeledQueueCapacity(
            new QueuePath("root.users.john")), 0.0f);
    assertEquals("root.users.joe capacity", 75.000f,
         csConfig.getNonLabeledQueueCapacity(
            new QueuePath("root.users.joe")), 0.0f);

    // root.admins
    assertEquals("root.admins.alice capacity", 75.000f,
        csConfig.getNonLabeledQueueCapacity(
            new QueuePath("root.admins.alice")), 0.0f);
    assertEquals("root.admins.bob capacity", 25.000f,
        csConfig.getNonLabeledQueueCapacity(
            new QueuePath("root.admins.bob")), 0.0f);

    // root.misc
    assertEquals("root.misc capacity", 0.000f,
        csConfig.getNonLabeledQueueCapacity(
            new QueuePath("root.misc")), 0.000f);
    assertEquals("root.misc.a capacity", 0.000f,
        csConfig.getNonLabeledQueueCapacity(
            new QueuePath("root.misc.a")), 0.000f);
    assertEquals("root.misc.b capacity", 0.000f,
        csConfig.getNonLabeledQueueCapacity(
            new QueuePath("root.misc.b")), 0.000f);
  }

  @Test
  public void testChildCapacityInWeightMode() {
    converter = builder.withPercentages(false).build();

    converter.convertQueueHierarchy(rootQueue);

    // root
    assertEquals("root.default weight", 1.0f,
        csConfig.getNonLabeledQueueWeight("root.default"), 0.01f);
    assertEquals("root.admins weight", 1.0f,
        csConfig.getNonLabeledQueueWeight("root.admins"), 0.01f);
    assertEquals("root.users weight", 1.0f,
        csConfig.getNonLabeledQueueWeight("root.users"), 0.01f);

    // root.users
    assertEquals("root.users.john weight", 1.0f,
        csConfig.getNonLabeledQueueWeight("root.users.john"), 0.01f);
    assertEquals("root.users.joe weight", 3.0f,
        csConfig.getNonLabeledQueueWeight("root.users.joe"), 0.01f);

    // root.admins
    assertEquals("root.admins.alice weight", 3.0f,
        csConfig.getNonLabeledQueueWeight("root.admins.alice"), 0.01f);
    assertEquals("root.admins.bob weight", 1.0f,
        csConfig.getNonLabeledQueueWeight("root.admins.bob"), 0.01f);

    // root.misc
    assertEquals("root.misc weight", 0.0f,
        csConfig.getNonLabeledQueueWeight("root.misc"), 0.00f);
    assertEquals("root.misc.a weight", 0.0f,
        csConfig.getNonLabeledQueueWeight("root.misc.a"), 0.00f);
    assertEquals("root.misc.b weight", 0.0f,
        csConfig.getNonLabeledQueueWeight("root.misc.b"), 0.00f);
  }

  @Test
  public void testAutoCreateV2FlagsInWeightMode() {
    converter = builder.withPercentages(false).build();

    converter.convertQueueHierarchy(rootQueue);

    assertTrue("root autocreate v2 flag",
        csConfig.isAutoQueueCreationV2Enabled("root"));
    assertTrue("root.admins autocreate v2 flag",
        csConfig.isAutoQueueCreationV2Enabled("root.admins"));
    assertTrue("root.admins.alice autocreate v2 flag",
        csConfig.isAutoQueueCreationV2Enabled("root.admins.alice"));
    assertTrue("root.users autocreate v2 flag",
        csConfig.isAutoQueueCreationV2Enabled("root.users"));
    assertTrue("root.misc autocreate v2 flag",
        csConfig.isAutoQueueCreationV2Enabled("root.misc"));

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
      assertEquals("Key " + key + " has different value",
          false, csConfig
              .isAutoQueueCreationV2Enabled(queue));
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
      assertEquals("Key " + key + " has different value",
          false, csConfig
              .getAllowZeroCapacitySum(queue));
    }

    assertTrue("root.misc allow zero capacities",
        csConfig.getAllowZeroCapacitySum("root.misc"));
  }

  @Test
  public void testQueueMaximumCapacity() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    for (String queue : ALL_QUEUES) {
      key = PREFIX + queue + ".maximum-capacity";
      assertEquals("Key " + key + " has different value",
          100.0, csConfig
              .getNonLabeledQueueMaximumCapacity(new QueuePath(queue)), 0.0f);
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
      assertEquals("Key " + key + " has different value",
          false, csConfig.isAutoCreateChildQueueEnabled(queue));
    }
  }

  @Test
  public void testQueueSizeBasedWeightEnabled() {
    converter = builder.withSizeBasedWeight(true).build();

    converter.convertQueueHierarchy(rootQueue);

    for (String queue : ALL_QUEUES) {
      key = PREFIX + queue + ".ordering-policy.fair.enable-size-based-weight";
      assertTrue("Key " + key + " has different value",
          csConfig.getBoolean(key, false));
    }
  }

  @Test
  public void testQueueSizeBasedWeightDisabled() {
    converter = builder.build();

    converter.convertQueueHierarchy(rootQueue);

    for (String queue : ALL_QUEUES) {
      key = PREFIX + queue + ".ordering-policy.fair.enable-size-based-weight";
      assertNull("Key " + key + " has different value",
          csConfig.get(key));
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
    assertEquals("root ordering policy", "fifo",
        csConfig.getAppOrderingPolicy("root").getConfigName());
    assertEquals("root.default ordering policy", "fair",
        csConfig.getAppOrderingPolicy("root.default").getConfigName());
    assertEquals("root.admins ordering policy", "fifo",
        csConfig.getAppOrderingPolicy("root.admins").getConfigName());
    assertEquals("root.users ordering policy", "fifo",
        csConfig.getAppOrderingPolicy("root.users").getConfigName());

    // root.users
    assertEquals("root.users.joe ordering policy", "fair",
        csConfig.getAppOrderingPolicy("root.users.joe").getConfigName());
    assertEquals("root.users.john ordering policy", "fifo",
        csConfig.getAppOrderingPolicy("root.users.john").getConfigName());

    // root.admins
    assertEquals("root.admins.alice ordering policy", "fifo",
        csConfig.getAppOrderingPolicy("root.admins.alice.").getConfigName());
    assertEquals("root.admins.bob ordering policy", "fair",
        csConfig.getAppOrderingPolicy("root.admins.bob").getConfigName());
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
    converter = builder.build();
    expectedException.expect(UnsupportedPropertyException.class);
    expectedException.expectMessage("test");

    Mockito.doThrow(new UnsupportedPropertyException("test"))
      .when(ruleHandler).handleMaxChildCapacity();

    converter.convertQueueHierarchy(rootQueue);
  }

  @Test
  public void testReservationSystemNotSupported() {
    converter = builder.build();
    expectedException.expect(UnsupportedPropertyException.class);
    expectedException.expectMessage("maxCapacity");

    Mockito.doThrow(new UnsupportedPropertyException("maxCapacity"))
      .when(ruleHandler).handleMaxChildCapacity();
    yarnConfig.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE,
        true);

    converter.convertQueueHierarchy(rootQueue);
  }
}