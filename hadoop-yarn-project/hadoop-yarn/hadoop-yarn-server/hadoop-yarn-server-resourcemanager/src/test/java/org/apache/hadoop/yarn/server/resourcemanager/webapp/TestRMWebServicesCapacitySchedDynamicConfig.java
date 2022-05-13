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

import com.google.inject.Guice;
import com.sun.jersey.api.client.ClientResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueTemplate;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.junit.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestRMWebServicesCapacitySched.assertJsonResponse;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestRMWebServicesCapacitySched.createMockRM;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestRMWebServicesCapacitySched.createWebAppDescriptor;

public class TestRMWebServicesCapacitySchedDynamicConfig extends
    JerseyTestBase {
  private MockRM rm;

  private CapacitySchedulerQueueManager autoQueueHandler;

  public TestRMWebServicesCapacitySchedDynamicConfig() {
    super(createWebAppDescriptor());
  }

  @Test
  public void testSchedulerResponsePercentageMode()
      throws Exception {
    Configuration config = CSConfigGenerator
        .createPercentageConfig();

    initResourceManager(config);

    /*
     * mode: percentage
     * autoCreationEligibility: off
     * weight: -1, normalizedWeight: 0
     * root.queueType: parent, others.queueType: leaf
     */
    assertJsonResponse(sendRequest(), "webapp/scheduler-response-PercentageMode.json");
  }

  @Test
  public void testSchedulerResponsePercentageModeLegacyAutoCreation()
      throws Exception {
    Configuration config = CSConfigGenerator
        .createPercentageConfigLegacyAutoCreation();

    initResourceManager(config);

    /*
     * mode: percentage
     * managedtest2.autoCreationEligibility: legacy, others.autoCreationEligibility: off
     * weight: -1, normalizedWeight: 0
     * root.queueType: parent, others.queueType: leaf
     */
    assertJsonResponse(sendRequest(),
        "webapp/scheduler-response-PercentageModeLegacyAutoCreation.json");
  }

  @Test
  public void testSchedulerResponseAbsoluteModeLegacyAutoCreation()
      throws Exception {
    Configuration config = CSConfigGenerator
        .createAbsoluteConfigLegacyAutoCreation();

    initResourceManager(config);
    initAutoQueueHandler(8192 * GB);
    createQueue("root.managed.queue1");

    assertJsonResponse(sendRequest(),
        "webapp/scheduler-response-AbsoluteModeLegacyAutoCreation.json");
  }

  @Test
  public void testSchedulerResponseAbsoluteMode()
      throws Exception {
    Configuration config = CSConfigGenerator
        .createAbsoluteConfig();

    initResourceManager(config);

    /*
     * mode: absolute
     * autoCreationEligibility: off
     * weight: -1, normalizedWeight: 0
     * root.queueType: parent, others.queueType: leaf
     */
    assertJsonResponse(sendRequest(), "webapp/scheduler-response-AbsoluteMode.json");
  }

  @Test
  public void testSchedulerResponseWeightMode()
      throws Exception {
    Configuration config = CSConfigGenerator
        .createWeightConfig();

    initResourceManager(config);

    /*
     * mode: weight
     * autoCreationEligibility: off
     *                   root   default  test1  test2
     * weight:            1       10       4     6
     * normalizedWeight:  1       0.5      0.2   0.3
     * root.queueType: parent, others.queueType: leaf
     */
    assertJsonResponse(sendRequest(), "webapp/scheduler-response-WeightMode.json");
  }

  @Test
  public void testSchedulerResponseWeightModeWithAutoCreatedQueues()
      throws Exception {
    Configuration config = CSConfigGenerator
        .createWeightConfigWithAutoQueueCreationEnabled();
    config.setInt(CapacitySchedulerConfiguration
        .getQueuePrefix("root.autoParent1") +
        AutoCreatedQueueTemplate.AUTO_QUEUE_TEMPLATE_PREFIX +
        "maximum-applications", 300);

    initResourceManager(config);
    initAutoQueueHandler(1200 * GB);

    // same as webapp/scheduler-response-WeightMode.json, but with effective resources filled in
    assertJsonResponse(sendRequest(),
        "webapp/scheduler-response-WeightModeWithAutoCreatedQueues-Before.json");

    //Now create some auto created queues
    createQueue("root.auto1");
    createQueue("root.auto2");
    createQueue("root.auto3");
    createQueue("root.autoParent1.auto4");
    createQueue("root.autoParent2.auto5");
    createQueue("root.parent.autoParent3.auto6");

    assertJsonResponse(sendRequest(),
        "webapp/scheduler-response-WeightModeWithAutoCreatedQueues-After.json");
  }

  private void initAutoQueueHandler(int nodeMemory) throws Exception {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    autoQueueHandler = cs.getCapacitySchedulerQueueManager();
    rm.registerNode("h1:1234", nodeMemory); // label = x
  }

  private void createQueue(String queuePath) throws YarnException,
      IOException {
    autoQueueHandler.createQueue(new QueuePath(queuePath));
  }

  private ClientResponse sendRequest() {
    return resource().path("ws").path("v1").path("cluster")
        .path("scheduler").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
  }

  private static class CSConfigGenerator {
    public static Configuration createPercentageConfig() {
      Map<String, String> conf = new HashMap<>();
      conf.put("yarn.scheduler.capacity.root.queues", "default, test1, test2");
      conf.put("yarn.scheduler.capacity.root.test1.capacity", "50");
      conf.put("yarn.scheduler.capacity.root.test2.capacity", "50");
      conf.put("yarn.scheduler.capacity.root.test1.maximum-capacity", "100");
      conf.put("yarn.scheduler.capacity.root.test1.state", "RUNNING");
      conf.put("yarn.scheduler.capacity.root.test2.state", "RUNNING");
      return createConfiguration(conf);
    }

    public static Configuration createPercentageConfigLegacyAutoCreation() {
      Map<String, String> conf = new HashMap<>();
      conf.put("yarn.scheduler.capacity.root.queues", "default, test1, " +
          "managedtest2");
      conf.put("yarn.scheduler.capacity.root.test1.capacity", "50");
      conf.put("yarn.scheduler.capacity.root.managedtest2.capacity", "50");
      conf.put("yarn.scheduler.capacity.root.test1.maximum-capacity", "100");
      conf.put("yarn.scheduler.capacity.root.test1.state", "RUNNING");
      conf.put("yarn.scheduler.capacity.root.managedtest2.state", "RUNNING");
      conf.put("yarn.scheduler.capacity.root.managedtest2." +
          "auto-create-child-queue.enabled", "true");
      return createConfiguration(conf);
    }

    public static Configuration createAbsoluteConfigLegacyAutoCreation() {
      Map<String, String> conf = new HashMap<>();
      conf.put("yarn.scheduler.capacity.root.queues", "default, managed");
      conf.put("yarn.scheduler.capacity.root.default.state", "STOPPED");
      conf.put("yarn.scheduler.capacity.root.managed.capacity", "[memory=4096,vcores=4]");
      conf.put("yarn.scheduler.capacity.root.managed.leaf-queue-template.capacity",
          "[memory=2048,vcores=2]");
      conf.put("yarn.scheduler.capacity.root.managed.state", "RUNNING");
      conf.put("yarn.scheduler.capacity.root.managed." +
          "auto-create-child-queue.enabled", "true");
      conf.put("yarn.scheduler.capacity.root.managed.leaf-queue-template.acl_submit_applications",
          "user");
      conf.put("yarn.scheduler.capacity.root.managed.leaf-queue-template.acl_administer_queue",
          "admin");
      return createConfiguration(conf);
    }

    public static Configuration createAbsoluteConfig() {
      Map<String, String> conf = new HashMap<>();
      conf.put("yarn.scheduler.capacity.root.queues", "default, test1, test2");
      conf.put("yarn.scheduler.capacity.root.capacity",
          "[memory=6136,vcores=30]");
      conf.put("yarn.scheduler.capacity.root.default.capacity",
          "[memory=3064,vcores=15]");
      conf.put("yarn.scheduler.capacity.root.test1.capacity",
          "[memory=2048,vcores=10]");
      conf.put("yarn.scheduler.capacity.root.test2.capacity",
          "[memory=1024,vcores=5]");
      conf.put("yarn.scheduler.capacity.root.test1.state", "RUNNING");
      conf.put("yarn.scheduler.capacity.root.test2.state", "RUNNING");
      return createConfiguration(conf);
    }

    public static Configuration createWeightConfig() {
      return createWeightConfigInternal(false);
    }

    public static Configuration createWeightConfigWithAutoQueueCreationEnabled() {
      return createWeightConfigInternal(true);
    }

    private static Configuration createWeightConfigInternal(boolean enableAqc) {
      Map<String, String> conf = new HashMap<>();
      conf.put("yarn.scheduler.capacity.root.queues", "default, test1, test2, parent");
      conf.put("yarn.scheduler.capacity.root.capacity", "1w");
      conf.put("yarn.scheduler.capacity.root.default.capacity", "10w");
      conf.put("yarn.scheduler.capacity.root.test1.capacity", "5w");
      conf.put("yarn.scheduler.capacity.root.test2.capacity", "10w");
      conf.put("yarn.scheduler.capacity.root.parent.capacity", "20w");
      conf.put("yarn.scheduler.capacity.root.test1.state", "RUNNING");
      conf.put("yarn.scheduler.capacity.root.test2.state", "RUNNING");

      if (enableAqc) {
        final String root = "yarn.scheduler.capacity.root.";
        conf.put(root +  "auto-queue-creation-v2.enabled", "true");

        conf.put(root + "auto-queue-creation-v2.parent-template.acl_submit_applications",
            "parentUser1");
        conf.put(root + "auto-queue-creation-v2.parent-template.acl_administer_queue",
            "parentAdmin1");

        conf.put(root + "autoParent1.auto-queue-creation-v2.leaf-template.acl_submit_applications",
            "user1");
        conf.put(root + "autoParent1.auto-queue-creation-v2.leaf-template.acl_administer_queue",
            "admin1");

        conf.put(root + "*.auto-queue-creation-v2.leaf-template.acl_submit_applications",
            "wildUser1");
        conf.put(root + "*.auto-queue-creation-v2.leaf-template.acl_administer_queue",
            "wildAdmin1");


        conf.put(root + "parent.auto-queue-creation-v2.enabled", "true");
        conf.put(root + "parent.auto-queue-creation-v2.parent-template.acl_submit_applications",
            "parentUser2");
        conf.put(root + "parent.auto-queue-creation-v2.parent-template.acl_administer_queue",
            "parentAdmin2");

        conf.put(root + "parent.*.auto-queue-creation-v2.leaf-template.acl_submit_applications",
            "wildUser2");
        conf.put(root + "parent.*.auto-queue-creation-v2.leaf-template.acl_administer_queue",
            "wildAdmin2");
      }
      return createConfiguration(conf);
    }

    public static Configuration createConfiguration(
        Map<String, String> configs) {
      Configuration config = new Configuration();

      for (Map.Entry<String, String> entry : configs.entrySet()) {
        config.set(entry.getKey(), entry.getValue());
      }

      config.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
          YarnConfiguration.MEMORY_CONFIGURATION_STORE);

      return config;
    }
  }

  private void initResourceManager(Configuration conf) throws IOException {
    rm = createMockRM(new CapacitySchedulerConfiguration(conf));
    GuiceServletConfig.setInjector(
        Guice.createInjector(new TestRMWebServicesCapacitySched.WebServletModule(rm)));
    rm.start();
    //Need to call reinitialize as
    //MutableCSConfigurationProvider with InMemoryConfigurationStore
    //somehow does not load the queues properly and falls back to default config.
    //Therefore CS will think there's only the default queue there.
    ((CapacityScheduler) rm.getResourceScheduler()).reinitialize(conf,
        rm.getRMContext(), true);
  }
}
