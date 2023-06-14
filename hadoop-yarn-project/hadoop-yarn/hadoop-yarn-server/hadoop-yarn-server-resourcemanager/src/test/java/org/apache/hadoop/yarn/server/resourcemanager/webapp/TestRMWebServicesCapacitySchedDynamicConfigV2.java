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
import java.util.List;
import javax.ws.rs.core.MediaType;

import com.google.inject.Guice;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestRMWebServicesCapacitySched.assertJsonResponse;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestRMWebServicesCapacitySched.createWebAppDescriptor;
import static org.assertj.core.api.Assertions.fail;

public class TestRMWebServicesCapacitySchedDynamicConfigV2 extends
    JerseyTestBase {
  private MockRM rm;

  public TestRMWebServicesCapacitySchedDynamicConfigV2() {
    super(createWebAppDescriptor());
  }

  private static final String[] SETUP = new String[] {
      "yarn.scheduler.capacity.root.queues = default, test1, test2",
      "yarn.scheduler.capacity.root.test1.queues = test1_1, test1_2, test1_3"
  };

  /*
   *                                         EffectiveMin (32GB 32VCores)     AbsoluteCapacity
   *     root.default              4/32      [memory=4096,  vcores=4]       12.5%
   *     root.test_1              16/32      [memory=16384, vcores=16]      50%
   *     root.test_2              12/32      [memory=12288, vcores=12]      37.5%
   *     root.test_1.test_1_1      2/16      [memory=2048,  vcores=2]       6.25%
   *     root.test_1.test_1_2      2/16      [memory=2048,  vcores=2]       6.25%
   *     root.test_1.test_1_3     12/16      [memory=12288, vcores=12]      37.5%
   */

  @Test
  public void testPercentageMode() throws Exception {
    runTest("webapp/V2/percentage",
        "yarn.scheduler.capacity.root.default.capacity = 12.5",
        "yarn.scheduler.capacity.root.test1.capacity =                 50",
        "yarn.scheduler.capacity.root.test2.capacity =                 37.5",
        "yarn.scheduler.capacity.root.test1.test1_1.capacity =         12.5",
        "yarn.scheduler.capacity.root.test1.test1_2.capacity =         12.5",
        "yarn.scheduler.capacity.root.test1.test1_3.capacity =         75"
    );
  }

  @Test
  public void testAbsoluteMode() throws Exception {
    runTest("webapp/V2/absolute",
        "yarn.scheduler.capacity.root.default.capacity = [memory=4096,vcores=4]",
        "yarn.scheduler.capacity.root.test1.capacity = [memory=16384,vcores=16]",
        "yarn.scheduler.capacity.root.test2.capacity = [memory=12288,vcores=12]",
        "yarn.scheduler.capacity.root.test1.test1_1.capacity = [memory=2048,vcores=2]",
        "yarn.scheduler.capacity.root.test1.test1_2.capacity = [memory=2048,vcores=2]",
        "yarn.scheduler.capacity.root.test1.test1_3.capacity = [memory=12288,vcores=12]"
    );
  }

  @Test
  public void testWeightMode() throws Exception {
    String[] capacity = new String[] {
        "yarn.scheduler.capacity.root.default.capacity =       4w",
        "yarn.scheduler.capacity.root.test1.capacity =         16w",
        "yarn.scheduler.capacity.root.test2.capacity =         12w",
        "yarn.scheduler.capacity.root.test1.test1_1.capacity = 2w",
        "yarn.scheduler.capacity.root.test1.test1_2.capacity = 2w",
        "yarn.scheduler.capacity.root.test1.test1_3.capacity = 12w"
    };
    runTest("webapp/V2/weight", capacity);
    runAQCTest("webapp/V2/weight", capacity);
  }

  private void runTest(
      String expectedResourceFilename,
      String... capacitySetup
  ) throws Exception {
    Configuration config = new Configuration();
    enrichConfig(config, SETUP);
    enrichConfig(config, capacitySetup);
    createRM(config);
    MockNM nm1 = rm.registerNode("h1:1234", 8192, 8);
    MockNM nm2 = rm.registerNode("h2:1234", 8192, 8);
    reinitialize(config);
    assertJsonResponse(sendRequest(), expectedResourceFilename + "/2node.json");
    MockNM nm3 = rm.registerNode("h3:1234", 8192, 8);
    MockNM nm4 = rm.registerNode("h4:1234", 8192, 8);
    reinitialize(config);
    assertJsonResponse(sendRequest(), expectedResourceFilename+ "/4node.json");
    rm.unRegisterNode(nm1);
    rm.unRegisterNode(nm4);
    reinitialize(config);
    assertJsonResponse(sendRequest(), expectedResourceFilename + "/2node.json");
  }

  private void runAQCTest(
      String expectedResourceFilename,
      String... capacitySetup
  ) throws Exception {
    Configuration config = new Configuration();
    enrichConfig(config, SETUP);
    enrichConfig(config, capacitySetup);
    setupAQC(config, "yarn.scheduler.capacity.root.test1.");
    setupAQC(config, "yarn.scheduler.capacity.root.test1.test1_1.");
    createRM(config);
    createAQC("test1");
    createAQC("test1.test1_1");
    rm.registerNode("h1:1234", 32768, 32);
    reinitialize(config);
    assertJsonResponse(sendRequest(), expectedResourceFilename+ "/aqc.json");
  }

  private void setupAQC(Configuration config, String queue) {
    config.set(queue + "auto-queue-creation-v2.enabled", "true");
    config.set(queue + "auto-queue-creation-v2.maximum-queue-depth", "10");
    config.set(queue + "auto-queue-creation-v2.parent-template.acl_submit_applications", "u1");
    config.set(queue + "auto-queue-creation-v2.parent-template.acl_administer_queue", "u2");
    config.set(queue + "autoParent1.auto-queue-creation-v2.leaf-template.acl_submit_applications", "u3");
    config.set(queue + "autoParent1.auto-queue-creation-v2.leaf-template.acl_administer_queue", "u4");
    config.set(queue + "*.auto-queue-creation-v2.leaf-template.acl_submit_applications", "u5");
    config.set(queue + "*.auto-queue-creation-v2.leaf-template.acl_administer_queue", "u6");
    config.set(queue + "parent.*.auto-queue-creation-v2.leaf-template.acl_submit_applications", "u7");
    config.set(queue + "parent.*.auto-queue-creation-v2.leaf-template.acl_administer_queue", "u8");
    config.set(queue + "autoParent1.auto-queue-creation-v2.template.maximum-applications", "300");
  }

  private void createAQC(String queue) {
    try {
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      CapacitySchedulerQueueManager autoQueueHandler = cs.getCapacitySchedulerQueueManager();
      autoQueueHandler.createQueue(new QueuePath("root." + queue + ".auto1"));
      autoQueueHandler.createQueue(new QueuePath("root." + queue + ".auto2"));
      autoQueueHandler.createQueue(new QueuePath("root." + queue + ".auto3"));
      autoQueueHandler.createQueue(new QueuePath("root." + queue + ".autoParent1.auto4"));
      autoQueueHandler.createQueue(new QueuePath("root." + queue + ".autoParent2.auto5"));
      autoQueueHandler.createQueue(new QueuePath("root." + queue + ".parent.autoParent3.auto6"));
    } catch (Throwable e) {
      fail("Can not auto create queues under " + queue, e);
    }
  }

  private ClientResponse sendRequest() {
    return resource().path("ws").path("v1").path("cluster")
        .path("scheduler").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
  }

  private void reinitialize(Configuration conf) throws IOException {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    cs.reinitialize(conf, rm.getRMContext(), true);
  }

  private void createRM(Configuration config) throws IOException {
    config.setClass(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class, ResourceScheduler.class);
    config.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    config.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);
    rm = new MockRM(config);
    GuiceServletConfig.setInjector(Guice.createInjector(new TestRMWebServicesCapacitySched.WebServletModule(rm)));
    rm.start();
    reinitialize(config);
  }

  private void enrichConfig(Configuration config, String[] newValues) {
    for (String newValue : newValues) {
      int index = newValue.indexOf("=");
      config.set(
          newValue.substring(0, index).trim(),
          newValue.substring(index + 1).trim()
      );
    }
  }
}
