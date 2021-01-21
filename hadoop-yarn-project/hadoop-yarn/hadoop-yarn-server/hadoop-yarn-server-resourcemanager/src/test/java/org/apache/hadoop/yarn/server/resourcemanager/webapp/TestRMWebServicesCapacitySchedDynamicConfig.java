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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

public class TestRMWebServicesCapacitySchedDynamicConfig extends
    JerseyTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRMWebServicesCapacitySchedDynamicConfig.class);
  private static final float EXP_WEIGHT_NON_WEIGHT_MODE = -1.0F;
  private static final float EXP_NORM_WEIGHT_NON_WEIGHT_MODE = 0.0F;
  private static final float EXP_ROOT_WEIGHT_IN_WEIGHT_MODE = 1.0F;
  private static final double DELTA = 0.00001;

  protected static MockRM rm;

  private static class ExpectedQueueWithProperties {
    private String path;
    public final float weight;
    public final float normalizedWeight;

    public ExpectedQueueWithProperties(String path, float weight,
        float normalizedWeight) {
      this.path = path;
      this.weight = weight;
      this.normalizedWeight = normalizedWeight;
    }
  }

  private static class WebServletModule extends ServletModule {
    private final Configuration conf;

    public WebServletModule(Configuration conf) {
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

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  private void initResourceManager(Configuration conf) throws IOException {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule(conf)));
    rm.start();
    //Need to call reinitialize as
    //MutableCSConfigurationProvider with InMemoryConfigurationStore
    //somehow does not load the queues properly and falls back to default config.
    //Therefore CS will think there's only the default queue there.
    ((CapacityScheduler)rm.getResourceScheduler()).reinitialize(conf,
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
    config.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);

    initResourceManager(config);
    JSONObject json = sendRequestToSchedulerEndpoint();
    validateSchedulerInfo(json, "percentage",
        new ExpectedQueueWithProperties("root",
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE),
        new ExpectedQueueWithProperties("root.default",
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE),
        new ExpectedQueueWithProperties("root.test1",
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE),
        new ExpectedQueueWithProperties("root.test2",
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE));
  }

  @Test
  public void testSchedulerResponseAbsoluteMode()
      throws Exception {
    Configuration config = CSConfigGenerator
        .createAbsoluteConfig();
    config.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);

    initResourceManager(config);
    JSONObject json = sendRequestToSchedulerEndpoint();
    validateSchedulerInfo(json, "absolute",
        new ExpectedQueueWithProperties("root",
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE),
        new ExpectedQueueWithProperties("root.default",
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE),
        new ExpectedQueueWithProperties("root.test1",
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE),
        new ExpectedQueueWithProperties("root.test2",
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE));
  }

  @Test
  public void testSchedulerResponseWeightMode()
      throws Exception {
    Configuration config = CSConfigGenerator
        .createWeightConfig();
    config.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);

    initResourceManager(config);
    JSONObject json = sendRequestToSchedulerEndpoint();
    validateSchedulerInfo(json, "weight",
        new ExpectedQueueWithProperties("root",
            EXP_ROOT_WEIGHT_IN_WEIGHT_MODE, EXP_ROOT_WEIGHT_IN_WEIGHT_MODE),
        new ExpectedQueueWithProperties("root.default", 10.0f, 0.5f),
        new ExpectedQueueWithProperties("root.test1", 4.0f, 0.2f),
        new ExpectedQueueWithProperties("root.test2", 6.0f, 0.3f));
  }

  private JSONObject sendRequestToSchedulerEndpoint() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    String jsonString = response.getEntity(String.class);
    LOG.debug("Received JSON response: " + jsonString);
    return new JSONObject(jsonString);
  }

  private void validateSchedulerInfo(JSONObject json, String expectedMode,
      ExpectedQueueWithProperties rootQueue,
      ExpectedQueueWithProperties... expectedQueues) throws JSONException {
    Map<String, ExpectedQueueWithProperties> queuesMap = new HashMap<>();
    for (ExpectedQueueWithProperties expectedQueue : expectedQueues) {
      queuesMap.put(expectedQueue.path, expectedQueue);
    }


    int expectedQSize = expectedQueues.length;
    Assert.assertNotNull("SchedulerTypeInfo should not be null", json);
    assertEquals("incorrect number of elements in: " + json, 1, json.length());

    JSONObject info = json.getJSONObject("scheduler");
    Assert.assertNotNull("Scheduler object should not be null", json);
    assertEquals("incorrect number of elements in: " + info, 1, info.length());

    //Validate if root queue has the expected mode and weight values
    info = info.getJSONObject("schedulerInfo");
    Assert.assertNotNull("SchedulerInfo should not be null", info);
    Assert.assertEquals("Expected Queue mode " +expectedMode, expectedMode,
        info.getString("mode"));
    Assert.assertEquals(rootQueue.weight,
        Float.parseFloat(info.getString("weight")), DELTA);
    Assert.assertEquals(rootQueue.normalizedWeight,
        Float.parseFloat(info.getString("normalizedWeight")), DELTA);

    JSONObject queuesObj = info.getJSONObject("queues");
    Assert.assertNotNull("QueueInfoList should not be null", queuesObj);

    JSONArray queueArray = queuesObj.getJSONArray("queue");
    Assert.assertNotNull("Queue list should not be null", queueArray);
    assertEquals("QueueInfoList should be size of " + expectedQSize,
        expectedQSize, queueArray.length());

    // Create mapping of queue path -> mode
    Map<String, String> modesMap = new HashMap<>();
    for (int i = 0; i < queueArray.length(); i++) {
      JSONObject obj = queueArray.getJSONObject(i);
      String queuePath = CapacitySchedulerConfiguration.ROOT + "." +
          obj.getString("queueName");
      String mode = obj.getString("mode");
      modesMap.put(queuePath, mode);

      //validate weights of all other queues
      ExpectedQueueWithProperties expectedQueue = queuesMap.get(queuePath);
      Assert.assertNotNull("Queue not found in expectedQueueMap with path: " +
          queuePath, expectedQueue);
      Assert.assertEquals("Weight value does not match",
          expectedQueue.weight, Float.parseFloat(obj.getString("weight")),
          DELTA);
      Assert.assertEquals("Normalized weight value does not match",
          expectedQueue.normalizedWeight,
          Float.parseFloat(obj.getString("normalizedWeight")), DELTA);
    }

    //Validate queue paths and modes
    List<String> sortedExpectedPaths = Arrays.stream(expectedQueues)
        .map(eq -> eq.path)
        .sorted(Comparator.comparing(String::toLowerCase))
        .collect(Collectors.toList());

    List<String> sortedActualPaths = modesMap.keySet().stream()
        .sorted(Comparator.comparing(String::toLowerCase))
        .collect(Collectors.toList());
    Assert.assertEquals("Expected Queue paths: " + sortedExpectedPaths,
        sortedExpectedPaths, sortedActualPaths);

    // Validate if we have a single "mode" for all queues
    Set<String> modesSet = new HashSet<>(modesMap.values());
    Assert.assertEquals("Expected a single Queue mode for all queues: " +
        expectedMode + ", got: " + modesMap, 1, modesSet.size());
    Assert.assertEquals("Expected Queue mode " + expectedMode,
        expectedMode, modesSet.iterator().next());
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
      Map<String, String> conf = new HashMap<>();
      conf.put("yarn.scheduler.capacity.root.queues", "default, test1, test2");
      conf.put("yarn.scheduler.capacity.root.capacity", "1w");
      conf.put("yarn.scheduler.capacity.root.default.capacity", "10w");
      conf.put("yarn.scheduler.capacity.root.test1.capacity", "4w");
      conf.put("yarn.scheduler.capacity.root.test2.capacity", "6w");
      conf.put("yarn.scheduler.capacity.root.test1.state", "RUNNING");
      conf.put("yarn.scheduler.capacity.root.test2.state", "RUNNING");
      return createConfiguration(conf);
    }

    public static Configuration createConfiguration(
        Map<String, String> configs) {
      Configuration config = new Configuration();

      for (Map.Entry<String, String> entry: configs.entrySet()) {
        config.set(entry.getKey(), entry.getValue());
      }
      return config;
    }
  }
}
