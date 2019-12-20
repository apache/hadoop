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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
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

import com.google.common.collect.Sets;


/**
 * Unit tests for FSQueueConverter.
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class TestFSQueueConverter {
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
          "root.users.john");

  private static final String FILE_PREFIX = "file:";
  private static final String FAIR_SCHEDULER_XML =
      prepareFileName("fair-scheduler-conversion.xml");

  private static String prepareFileName(String f) {
    return FILE_PREFIX + new File("src/test/resources/" + f).getAbsolutePath();
  }

  private FSQueueConverter converter;
  private Configuration config;
  private Configuration csConfig;
  private FairScheduler fs;
  private FSQueue rootQueue;

  @Mock
  private FSConfigToCSConfigRuleHandler ruleHandler;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup() {
    config = new Configuration(false);
    config.set(FairSchedulerConfiguration.ALLOCATION_FILE, FAIR_SCHEDULER_XML);
    config.setBoolean(FairSchedulerConfiguration.MIGRATION_MODE, true);
    csConfig = new Configuration(false);

    fs = createFairScheduler();

    createConverter();
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
    fairScheduler.init(config);

    return fairScheduler;
  }

  private void createConverter() {
    converter = new FSQueueConverter(ruleHandler,
        csConfig,
        false,
        false,
        false,
        CLUSTER_RESOURCE,
        0.16f,
        15);
  }

  @Test
  public void testConvertQueueHierarchy() {
    converter.convertQueueHierarchy(rootQueue);

    // root children
    assertEquals("root children", "default,admins,users",
        csConfig.get(PREFIX + "root.queues"));

    // root.admins children
    assertEquals("root.admins children", "bob,alice",
        csConfig.get(PREFIX + "root.admins.queues"));

    // root.default children - none
    assertNull("root.default children", csConfig.get(PREFIX + "root.default" +
        ".queues"));

    // root.users children
    assertEquals("root.users children", "john,joe",
        csConfig.get(PREFIX + "root.users.queues"));

    Set<String> leafs = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root",
            "root.default",
            "root.admins",
            "root.users"));

    assertNoValueForQueues(leafs, ".queues", csConfig);
  }

  @Test
  public void testConvertQueueHierarchyWithSameLeafQueues() throws Exception {
    expectedException.expect(ConversionException.class);
    expectedException.expectMessage("Leaf queues must be unique");

    String absolutePath =
        new File("src/test/resources/fair-scheduler-sameleafqueue.xml")
          .getAbsolutePath();
    config.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        FILE_PREFIX + absolutePath);
    fs.close();
    fs = createFairScheduler();
    rootQueue = fs.getQueueManager().getRootQueue();

    converter.convertQueueHierarchy(rootQueue);
  }

  @Test
  public void testQueueMaxAMShare() {
    converter.convertQueueHierarchy(rootQueue);

    // root.admins.bob
    assertEquals("root.admins.bob AM share", "1.0",
        csConfig.get(PREFIX + "root.admins.bob.maximum-am-resource-percent"));

    // root.admins.alice
    assertEquals("root.admins.alice AM share", "0.15",
        csConfig.get(PREFIX +
            "root.admins.alice.maximum-am-resource-percent"));

    Set<String> remaining = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.admins.bob", "root.admins.alice"));
    assertNoValueForQueues(remaining, ".maximum-am-resource-percent",
        csConfig);
  }

  @Test
  public void testQueueMaxRunningApps() {
    converter.convertQueueHierarchy(rootQueue);

    assertEquals("root.admins.alice max apps", 2,
        csConfig.getInt(PREFIX + "root.admins.alice.maximum-applications",
            -1));

    Set<String> remaining = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.admins.alice"));
    assertNoValueForQueues(remaining, ".maximum-applications", csConfig);
  }

  @Test
  public void testQueueMaxAllocations() {
    converter.convertQueueHierarchy(rootQueue);

    // root.admins vcores + mb
    assertEquals("root.admins max vcores", 3,
        csConfig.getInt(PREFIX + "root.admins.maximum-allocation-vcores", -1));
    assertEquals("root.admins max memory", 4096,
        csConfig.getInt(PREFIX + "root.admins.maximum-allocation-mb", -1));

    // root.users.john max vcores + mb
    assertEquals("root.users.john max vcores", 2,
        csConfig.getInt(PREFIX + "root.users.john.maximum-allocation-vcores",
            -1));
    assertEquals("root.users.john max memory", 8192,
        csConfig.getInt(PREFIX + "root.users.john.maximum-allocation-mb", -1));

    Set<String> remaining = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.admins", "root.users.john"));
    assertNoValueForQueues(remaining, ".maximum-allocation-vcores", csConfig);
    assertNoValueForQueues(remaining, ".maximum-allocation-mb", csConfig);
  }

  @Test
  public void testQueuePreemptionDisabled() {
    converter = new FSQueueConverter(ruleHandler,
        csConfig,
        true,
        false,
        false,
        CLUSTER_RESOURCE,
        0.16f,
        15);

    converter.convertQueueHierarchy(rootQueue);

    assertTrue("root.admins.alice preemption setting",
        csConfig.getBoolean(PREFIX + "root.admins.alice.disable_preemption",
            false));
    assertTrue("root.users.joe preemption setting",
        csConfig.getBoolean(PREFIX + "root.users.joe.disable_preemption",
            false));

    Set<String> remaining = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.admins.alice", "root.users.joe"));
    assertNoValueForQueues(remaining, ".disable_preemption", csConfig);
  }

  @Test
  public void testQueuePreemptionDisabledWhenGlobalPreemptionDisabled() {
    converter.convertQueueHierarchy(rootQueue);

    assertNoValueForQueues(ALL_QUEUES, ".disable_preemption", csConfig);
  }

  @Test
  public void testChildCapacity() {
    converter.convertQueueHierarchy(rootQueue);

    // root
    assertEquals("root.default capacity", "33.333",
        csConfig.get(PREFIX + "root.default.capacity"));
    assertEquals("root.admins capacity", "33.333",
        csConfig.get(PREFIX + "root.admins.capacity"));
    assertEquals("root.users capacity", "66.667",
        csConfig.get(PREFIX + "root.users.capacity"));

    // root.users
    assertEquals("root.users.john capacity", "25.000",
        csConfig.get(PREFIX + "root.users.john.capacity"));
    assertEquals("root.users.joe capacity", "75.000",
        csConfig.get(PREFIX + "root.users.joe.capacity"));

    // root.admins
    assertEquals("root.admins.alice capacity", "75.000",
        csConfig.get(PREFIX + "root.admins.alice.capacity"));
    assertEquals("root.admins.bob capacity", "25.000",
        csConfig.get(PREFIX + "root.admins.bob.capacity"));
  }

  @Test
  public void testQueueMaximumCapacity() {
    converter.convertQueueHierarchy(rootQueue);

    assertEquals("root.users.joe maximum capacity", "[memory=8192, vcores=8]",
        csConfig.get(PREFIX + "root.users.joe.maximum-capacity"));
    assertEquals("root.admins.bob maximum capacity", "[memory=8192, vcores=2]",
        csConfig.get(PREFIX + "root.admins.bob.maximum-capacity"));
    assertEquals("root.admins.alice maximum capacity",
        "[memory=16384, vcores=4]",
        csConfig.get(PREFIX + "root.admins.alice.maximum-capacity"));

    Set<String> remaining = Sets.difference(ALL_QUEUES,
        Sets.newHashSet("root.users.joe",
            "root.admins.bob",
            "root.admins.alice"));
    assertNoValueForQueues(remaining, ".maximum-capacity", csConfig);
  }

  @Test
  public void testQueueAutoCreateChildQueue() {
    config.setBoolean(FairSchedulerConfiguration.ALLOW_UNDECLARED_POOLS, true);
    converter = new FSQueueConverter(ruleHandler,
        csConfig,
        false,
        false,
        true,
        CLUSTER_RESOURCE,
        0.16f,
        15);

    converter.convertQueueHierarchy(rootQueue);

    assertTrueForQueues(ALL_QUEUES, ".auto-create-child-queue.enabled",
        csConfig);
  }

  @Test
  public void testQueueSizeBasedWeightEnabled() {
    converter = new FSQueueConverter(ruleHandler,
        csConfig,
        false,
        true,
        false,
        CLUSTER_RESOURCE,
        0.16f,
        15);

    converter.convertQueueHierarchy(rootQueue);

    assertTrueForQueues(ALL_QUEUES,
        ".ordering-policy.fair.enable-size-based-weight", csConfig);
  }

  @Test
  public void testQueueSizeBasedWeightDisabled() {
    converter.convertQueueHierarchy(rootQueue);

    assertNoValueForQueues(ALL_QUEUES,
        ".ordering-policy.fair.enable-size-based-weight", csConfig);
  }

  @Test
  public void testQueueOrderingPolicy() throws Exception {
    String absolutePath =
        new File("src/test/resources/fair-scheduler-orderingpolicy.xml")
          .getAbsolutePath();
    config.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        FILE_PREFIX + absolutePath);
    fs.close();
    fs = createFairScheduler();
    rootQueue = fs.getQueueManager().getRootQueue();

    converter.convertQueueHierarchy(rootQueue);

    // root
    assertEquals("root ordering policy", "fair",
        csConfig.get(PREFIX + "root.ordering-policy"));
    assertEquals("root.default ordering policy", "fair",
        csConfig.get(PREFIX + "root.default.ordering-policy"));
    assertEquals("root.admins ordering policy", "fair",
        csConfig.get(PREFIX + "root.admins.ordering-policy"));
    assertEquals("root.users ordering policy", "fair",
        csConfig.get(PREFIX + "root.users.ordering-policy"));

    // root.users
    assertEquals("root.users.joe ordering policy", "fair",
        csConfig.get(PREFIX + "root.users.joe.ordering-policy"));
    assertEquals("root.users.john ordering policy", "FIFO",
        csConfig.get(PREFIX + "root.users.john.ordering-policy"));

    // root.admins
    assertEquals("root.admins.alice ordering policy", "FIFO",
        csConfig.get(PREFIX + "root.admins.alice.ordering-policy"));
    assertEquals("root.admins.bob ordering policy", "fair",
        csConfig.get(PREFIX + "root.admins.bob.ordering-policy"));
  }

  @Test
  public void testQueueMaxChildCapacityNotSupported() {
    expectedException.expect(UnsupportedPropertyException.class);
    expectedException.expectMessage("test");

    Mockito.doThrow(new UnsupportedPropertyException("test"))
      .when(ruleHandler).handleMaxChildCapacity();

    converter.convertQueueHierarchy(rootQueue);
  }

  @Test
  public void testReservationSystemNotSupported() {
    expectedException.expect(UnsupportedPropertyException.class);
    expectedException.expectMessage("maxCapacity");

    Mockito.doThrow(new UnsupportedPropertyException("maxCapacity"))
      .when(ruleHandler).handleMaxChildCapacity();
    config.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);

    converter.convertQueueHierarchy(rootQueue);
  }

  private void assertNoValueForQueues(Set<String> queues, String postfix,
      Configuration config) {
    for (String queue : queues) {
      String key = PREFIX + queue + postfix;
      assertNull("Key " + key + " has value, but it should be null",
          config.get(key));
    }
  }

  private void assertTrueForQueues(Set<String> queues, String postfix,
      Configuration config) {
    for (String queue : queues) {
      String key = PREFIX + queue + postfix;
      assertTrue("Key " + key + " is false, should be true",
          config.getBoolean(key, false));
    }
  }
}