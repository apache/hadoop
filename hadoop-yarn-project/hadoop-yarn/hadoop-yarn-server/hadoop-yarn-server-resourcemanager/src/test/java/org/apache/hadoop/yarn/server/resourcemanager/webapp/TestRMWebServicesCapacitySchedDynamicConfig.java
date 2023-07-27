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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfigGeneratorForTest.createConfiguration;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.assertJsonResponse;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.createMutableRM;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.createWebAppDescriptor;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.getExpectedResourceFile;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.reinitialize;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.runTest;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.sendRequest;
import static org.assertj.core.api.Assertions.fail;


/*
 *                                         EffectiveMin (32GB 32VCores)     AbsoluteCapacity
 *     root.default              4/32      [memory=4096,  vcores=4]       12.5%
 *     root.test_1              16/32      [memory=16384, vcores=16]
 *     root.test_2              12/32      [memory=12288, vcores=12]      37.5%
 *     root.test_1.test_1_1      2/16      [memory=2048,  vcores=2]       6.25%
 *     root.test_1.test_1_2      2/16      [memory=2048,  vcores=2]       6.25%
 *     root.test_1.test_1_3     12/16      [memory=12288, vcores=12]      37.5%
 */
@RunWith(Parameterized.class)
public class TestRMWebServicesCapacitySchedDynamicConfig extends JerseyTestBase {

  private final boolean legacyQueueMode;

  @Parameterized.Parameters(name = "{index}: legacy-queue-mode={0}")
  public static Collection<Boolean> getParameters() {
    return Arrays.asList(true, false);
  }

  private static final String EXPECTED_FILE_TMPL = "webapp/dynamic-%s-%s.json";

  public TestRMWebServicesCapacitySchedDynamicConfig(boolean legacyQueueMode) {
    super(createWebAppDescriptor());
    this.legacyQueueMode = legacyQueueMode;
  }

  @Test
  public void testPercentageMode() throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.put("yarn.scheduler.capacity.legacy-queue-mode.enabled", String.valueOf(legacyQueueMode));
    conf.put("yarn.scheduler.capacity.root.queues", "default, test1, test2");
    conf.put("yarn.scheduler.capacity.root.test1.queues", "test1_1, test1_2, test1_3");
    conf.put("yarn.scheduler.capacity.root.default.capacity", "12.5");
    conf.put("yarn.scheduler.capacity.root.test1.capacity", "50");
    conf.put("yarn.scheduler.capacity.root.test2.capacity", "37.5");
    conf.put("yarn.scheduler.capacity.root.test1.test1_1.capacity", "12.5");
    conf.put("yarn.scheduler.capacity.root.test1.test1_2.capacity", "12.5");
    conf.put("yarn.scheduler.capacity.root.test1.test1_3.capacity", "75");
    try (MockRM rm = createMutableRM(createConfiguration(conf))) {
      runTest(EXPECTED_FILE_TMPL, "testPercentageMode", rm, resource());
    }
  }
  @Test
  public void testAbsoluteMode() throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.put("yarn.scheduler.capacity.legacy-queue-mode.enabled", String.valueOf(legacyQueueMode));
    conf.put("yarn.scheduler.capacity.root.queues", "default, test1, test2");
    conf.put("yarn.scheduler.capacity.root.test1.queues", "test1_1, test1_2, test1_3");
    conf.put("yarn.scheduler.capacity.root.default.capacity", "[memory=4096,vcores=4]");
    conf.put("yarn.scheduler.capacity.root.test1.capacity", "[memory=16384,vcores=16]");
    conf.put("yarn.scheduler.capacity.root.test2.capacity", "[memory=12288,vcores=12]");
    conf.put("yarn.scheduler.capacity.root.test1.test1_1.capacity", "[memory=2048,vcores=2]");
    conf.put("yarn.scheduler.capacity.root.test1.test1_2.capacity", "[memory=2048,vcores=2]");
    conf.put("yarn.scheduler.capacity.root.test1.test1_3.capacity", "[memory=12288,vcores=12]");
    try (MockRM rm = createMutableRM(createConfiguration(conf))) {
      runTest(EXPECTED_FILE_TMPL, "testAbsoluteMode", rm, resource());
    }
  }

  @Test
  public void testWeightMode() throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.put("yarn.scheduler.capacity.legacy-queue-mode.enabled", String.valueOf(legacyQueueMode));
    conf.put("yarn.scheduler.capacity.root.queues", "default, test1, test2");
    conf.put("yarn.scheduler.capacity.root.test1.queues", "test1_1, test1_2, test1_3");
    conf.put("yarn.scheduler.capacity.root.default.capacity", "4w");
    conf.put("yarn.scheduler.capacity.root.test1.capacity", "16w");
    conf.put("yarn.scheduler.capacity.root.test2.capacity", "12w");
    conf.put("yarn.scheduler.capacity.root.test1.test1_1.capacity", "2w");
    conf.put("yarn.scheduler.capacity.root.test1.test1_2.capacity", "2w");
    conf.put("yarn.scheduler.capacity.root.test1.test1_3.capacity", "12w");
    try (MockRM rm = createMutableRM(createConfiguration(conf))) {
      // capacity and normalizedWeight are set differently between legacy/non-legacy queue mode
      runTest(EXPECTED_FILE_TMPL, "testWeightMode", rm, resource());
    }
  }

  @Test
  public void testWeightModeFlexibleAQC() throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.put("yarn.scheduler.capacity.legacy-queue-mode.enabled", String.valueOf(legacyQueueMode));
    conf.put("yarn.scheduler.capacity.root.queues", "default, test1, test2");
    conf.put("yarn.scheduler.capacity.root.test1.queues", "test1_1, test1_2, test1_3");
    conf.put("yarn.scheduler.capacity.root.default.capacity", "4w");
    conf.put("yarn.scheduler.capacity.root.test1.capacity", "16w");
    conf.put("yarn.scheduler.capacity.root.test2.capacity", "12w");
    conf.put("yarn.scheduler.capacity.root.test1.test1_1.capacity", "2w");
    conf.put("yarn.scheduler.capacity.root.test1.test1_2.capacity", "2w");
    conf.put("yarn.scheduler.capacity.root.test1.test1_3.capacity", "12w");

    Configuration config = createConfiguration(conf);
    setupAQC(config, "yarn.scheduler.capacity.root.test2.");
    try (MockRM rm = createMutableRM(config)) {
      // capacity and normalizedWeight are set differently between legacy/non-legacy queue mode
      rm.registerNode("h1:1234", 32 * GB, 32);
      assertJsonResponse(sendRequest(resource()),
          getExpectedResourceFile(EXPECTED_FILE_TMPL, "testWeightMode",
              "before-aqc", legacyQueueMode));
      createDynamicQueues(rm, "test2");
      reinitialize(rm, config);
      assertJsonResponse(sendRequest(resource()),
          getExpectedResourceFile(EXPECTED_FILE_TMPL, "testWeightMode",
              "after-aqc", legacyQueueMode));
    }
  }

  private void setupAQC(Configuration config, String queueWithConfigPrefix) {
    config.set(queueWithConfigPrefix + "auto-queue-creation-v2.enabled", "true");
    config.set(queueWithConfigPrefix + "auto-queue-creation-v2.maximum-queue-depth", "10");
    config.set(queueWithConfigPrefix + "auto-queue-creation-v2.leaf-template.capacity", "10w");
    config.set(queueWithConfigPrefix + "auto-queue-creation-v2.parent-template.acl_submit_applications",
        "parentUser");
    config.set(queueWithConfigPrefix + "auto-queue-creation-v2.parent-template.acl_administer_queue",
        "parentAdmin");
    config.set(queueWithConfigPrefix + "autoParent1.auto-queue-creation-v2.leaf-template.acl_submit_applications",
        "ap1User");
    config.set(queueWithConfigPrefix + "autoParent1.auto-queue-creation-v2.leaf-template.acl_administer_queue",
        "ap1Admin");
    config.set(queueWithConfigPrefix + "*.auto-queue-creation-v2.leaf-template.acl_submit_applications",
        "leafUser");
    config.set(queueWithConfigPrefix + "*.auto-queue-creation-v2.leaf-template.acl_administer_queue",
        "leafAdmin");
    config.set(queueWithConfigPrefix + "parent.*.auto-queue-creation-v2.leaf-template.acl_submit_applications",
        "pLeafUser");
    config.set(queueWithConfigPrefix + "parent.*.auto-queue-creation-v2.leaf-template.acl_administer_queue",
        "pLeafAdmin");
    config.set(queueWithConfigPrefix + "autoParent1.auto-queue-creation-v2.template.maximum-applications", "300");
  }
  private void createDynamicQueues(MockRM rm, String queueName) {
    try {
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      CapacitySchedulerQueueManager autoQueueHandler = cs.getCapacitySchedulerQueueManager();
      autoQueueHandler.createQueue(new QueuePath("root." + queueName + ".auto1"));
      autoQueueHandler.createQueue(new QueuePath("root." + queueName + ".auto2"));
      autoQueueHandler.createQueue(new QueuePath("root." + queueName + ".autoParent1.auto3"));
      autoQueueHandler.createQueue(new QueuePath("root." + queueName + ".autoParent1.auto4"));
      autoQueueHandler.createQueue(new QueuePath("root." + queueName + ".autoParent2.auto5"));
      autoQueueHandler.createQueue(new QueuePath("root." + queueName + ".parent.autoParent2.auto6"));
      autoQueueHandler.createQueue(new QueuePath("root." + queueName + ".parent2.auto7"));
    } catch (YarnException | IOException e) {
      fail("Can not auto create queues under " + queueName, e);
    }
  }
}