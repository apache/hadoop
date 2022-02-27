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
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueTemplate;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.junit.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestRMWebServicesCapacitySched.assertJsonResponse;

public class TestRMWebServicesCapacitySchedDynamicConfig extends
    JerseyTestBase {
  private static final int GB = 1024;
  private static MockRM rm;

  private CapacitySchedulerQueueManager autoQueueHandler;

  private static class WebServletModule extends ServletModule {
    private final Configuration conf;

    WebServletModule(Configuration conf) {
      this.conf = conf;
    }

    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
          YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
      rm = new MockRM(conf);
      bind(ResourceManager.class).toInstance(rm);
      serve("/*").with(GuiceContainer.class);
    }
  }

  private void initResourceManager(Configuration conf) throws IOException {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule(conf)));
    rm.start();
    //Need to call reinitialize as
    //MutableCSConfigurationProvider with InMemoryConfigurationStore
    //somehow does not load the queues properly and falls back to default config.
    //Therefore CS will think there's only the default queue there.
    ((CapacityScheduler) rm.getResourceScheduler()).reinitialize(conf,
        rm.getRMContext(), true);
  }

  public TestRMWebServicesCapacitySchedDynamicConfig() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
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

    /*
     *                         root   default  test1  test2  autoParent1  auto1  auto2  auto3  auto4
     * weight:                  1        10      4     6         1          1      1     1      1
     * normalizedWeight:        1        0.41    0.16  0.25      1          0.04   0.04  0.04   0.04
     * autoCreationEligibility: flexible off     off   off     flexible     off    off   off    off
     * queueType:               parent   leaf    leaf  leaf    parent       leaf   leaf  leaf   leaf
     */
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
      conf.put("yarn.scheduler.capacity.root.queues", "default, test1, test2");
      conf.put("yarn.scheduler.capacity.root.capacity", "1w");
      conf.put("yarn.scheduler.capacity.root.default.capacity", "10w");
      conf.put("yarn.scheduler.capacity.root.test1.capacity", "4w");
      conf.put("yarn.scheduler.capacity.root.test2.capacity", "6w");
      conf.put("yarn.scheduler.capacity.root.test1.state", "RUNNING");
      conf.put("yarn.scheduler.capacity.root.test2.state", "RUNNING");

      if (enableAqc) {
        conf.put("yarn.scheduler.capacity.root.auto-queue-creation-v2.enabled",
            "true");
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
}
