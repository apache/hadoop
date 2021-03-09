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
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerAutoQueueHandler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
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
  private static final float EXP_DEFAULT_WEIGHT_IN_WEIGHT_MODE = 1.0F;
  private static final double DELTA = 0.00001;
  private static final String PARENT_QUEUE = "parent";
  private static final String LEAF_QUEUE = "leaf";
  private static final String STATIC_QUEUE = "static";
  private static final String FLEXIBLE_DYNAMIC_QUEUE = "dynamicFlexible";
  private static final String AUTO_CREATION_OFF = "off";
  private static final String AUTO_CREATION_LEGACY = "legacy";
  private static final String AUTO_CREATION_FLEXIBLE = "flexible";
  private static final int GB = 1024;
  protected static MockRM RM;

  private CapacitySchedulerAutoQueueHandler autoQueueHandler;
  private CapacitySchedulerConfiguration csConf;

  private static class ExpectedQueueWithProperties {
    private String path;
    public final float weight;
    public final float normalizedWeight;
    private String queueType;
    private String creationMethod;
    private String autoCreationEligibility;

    public ExpectedQueueWithProperties(String path, float weight,
        float normalizedWeight, String queueType, String creationMethod,
        String autoCreationEligibility) {
      this.path = path;
      this.weight = weight;
      this.normalizedWeight = normalizedWeight;
      this.queueType = queueType;
      this.creationMethod = creationMethod;
      this.autoCreationEligibility = autoCreationEligibility;
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
      RM = new MockRM(conf);
      bind(ResourceManager.class).toInstance(RM);
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
    RM.start();
    //Need to call reinitialize as
    //MutableCSConfigurationProvider with InMemoryConfigurationStore
    //somehow does not load the queues properly and falls back to default config.
    //Therefore CS will think there's only the default queue there.
    ((CapacityScheduler) RM.getResourceScheduler()).reinitialize(conf,
        RM.getRMContext(), true);
    CapacityScheduler cs = (CapacityScheduler) RM.getResourceScheduler();
    csConf = cs.getConfiguration();
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
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE,
            PARENT_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.default",
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE,
            LEAF_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.test1",
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE,
            LEAF_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.test2",
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE,
            LEAF_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF));
  }

  @Test
  public void testSchedulerResponsePercentageModeLegacyAutoCreation()
      throws Exception {
    Configuration config = CSConfigGenerator
        .createPercentageConfigLegacyAutoCreation();
    config.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);

    initResourceManager(config);
    JSONObject json = sendRequestToSchedulerEndpoint();
    validateSchedulerInfo(json, "percentage",
        new ExpectedQueueWithProperties("root",
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE,
            PARENT_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.default",
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE,
            LEAF_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.test1",
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE,
            LEAF_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.managedtest2",
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE,
            PARENT_QUEUE, STATIC_QUEUE, AUTO_CREATION_LEGACY));
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
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE,
            PARENT_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.default",
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE,
            LEAF_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.test1",
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE,
            LEAF_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.test2",
            EXP_WEIGHT_NON_WEIGHT_MODE, EXP_NORM_WEIGHT_NON_WEIGHT_MODE,
            LEAF_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF));
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
            EXP_ROOT_WEIGHT_IN_WEIGHT_MODE, EXP_ROOT_WEIGHT_IN_WEIGHT_MODE,
            PARENT_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.default", 10.0f, 0.5f,
            LEAF_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.test1", 4.0f, 0.2f,
            LEAF_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.test2", 6.0f, 0.3f,
            LEAF_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF));
  }

  @Test
  public void testSchedulerResponseWeightModeWithAutoCreatedQueues()
      throws Exception {
    Configuration config = CSConfigGenerator
        .createWeightConfigWithAutoQueueCreationEnabled();
    config.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);

    initResourceManager(config);
    initAutoQueueHandler();
    JSONObject json = sendRequestToSchedulerEndpoint();
    validateSchedulerInfo(json, "weight",
        new ExpectedQueueWithProperties("root",
            EXP_ROOT_WEIGHT_IN_WEIGHT_MODE, EXP_ROOT_WEIGHT_IN_WEIGHT_MODE,
            PARENT_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.default", 10.0f, 0.5f,
            LEAF_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.test1", 4.0f, 0.2f,
            LEAF_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.test2", 6.0f, 0.3f,
            LEAF_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF));

    //Now create some auto created queues
    createQueue("root.auto1");
    createQueue("root.auto2");
    createQueue("root.auto3");
    createQueue("root.autoParent1.auto4");

    json = sendRequestToSchedulerEndpoint();
    //root.auto1=1w, root.auto2=1w, root.auto3=1w
    //root.default=10w, root.test1=4w, root.test2=6w
    //root.autoparent1=1w
    int sumOfWeights = 24;
    ExpectedQueueWithProperties expectedRootQ =
        new ExpectedQueueWithProperties("root",
            EXP_ROOT_WEIGHT_IN_WEIGHT_MODE, EXP_ROOT_WEIGHT_IN_WEIGHT_MODE,
            PARENT_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF);
    validateSchedulerInfo(json, "weight",
        expectedRootQ,
        new ExpectedQueueWithProperties("root.auto1",
            EXP_DEFAULT_WEIGHT_IN_WEIGHT_MODE,
            EXP_DEFAULT_WEIGHT_IN_WEIGHT_MODE / sumOfWeights,
            LEAF_QUEUE, FLEXIBLE_DYNAMIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.auto2",
            EXP_DEFAULT_WEIGHT_IN_WEIGHT_MODE,
            EXP_DEFAULT_WEIGHT_IN_WEIGHT_MODE / sumOfWeights,
            LEAF_QUEUE, FLEXIBLE_DYNAMIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.auto3",
            EXP_DEFAULT_WEIGHT_IN_WEIGHT_MODE,
            EXP_DEFAULT_WEIGHT_IN_WEIGHT_MODE / sumOfWeights,
            LEAF_QUEUE, FLEXIBLE_DYNAMIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.autoParent1",
            EXP_DEFAULT_WEIGHT_IN_WEIGHT_MODE,
            EXP_DEFAULT_WEIGHT_IN_WEIGHT_MODE / sumOfWeights,
            PARENT_QUEUE, FLEXIBLE_DYNAMIC_QUEUE, AUTO_CREATION_FLEXIBLE),
        new ExpectedQueueWithProperties("root.default", 10.0f,
            10.0f / sumOfWeights,
            LEAF_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.test1", 4.0f,
            4.0f / sumOfWeights,
            LEAF_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF),
        new ExpectedQueueWithProperties("root.test2", 6.0f,
            6.0f / sumOfWeights,
            LEAF_QUEUE, STATIC_QUEUE, AUTO_CREATION_OFF));

    validateChildrenOfParent(json, "root.autoParent1", "weight",
        expectedRootQ,
        new ExpectedQueueWithProperties("root.autoParent1.auto4",
            EXP_DEFAULT_WEIGHT_IN_WEIGHT_MODE,
            EXP_DEFAULT_WEIGHT_IN_WEIGHT_MODE,
            LEAF_QUEUE, FLEXIBLE_DYNAMIC_QUEUE, AUTO_CREATION_OFF));
  }

  private void initAutoQueueHandler() throws Exception {
    CapacityScheduler cs = (CapacityScheduler) RM.getResourceScheduler();
    autoQueueHandler = new CapacitySchedulerAutoQueueHandler(
        cs.getCapacitySchedulerQueueManager());
    MockNM nm1 = RM.registerNode("h1:1234", 1200 * GB); // label = x
  }

  private LeafQueue createQueue(String queuePath) throws YarnException {
    return autoQueueHandler.autoCreateQueue(
        CSQueueUtils.extractQueuePath(queuePath));
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
    Assert.assertNotNull("SchedulerTypeInfo should not be null", json);
    assertEquals("incorrect number of elements in: " + json, 1, json.length());

    JSONObject info = verifySchedulerJSONObject(json);
    info = verifySchedulerInfoJSONObject(expectedMode, rootQueue, info);
    JSONArray queueArray = verifyQueueJSONListObject(info,
        expectedQueues.length);
    verifyQueues(CapacitySchedulerConfiguration.ROOT, expectedMode,
        queueArray, expectedQueues);
  }

  private void validateChildrenOfParent(JSONObject json,
      String parentPath, String expectedMode,
      ExpectedQueueWithProperties rootQueue,
      ExpectedQueueWithProperties... expectedLeafQueues) throws JSONException {
    Assert.assertNotNull("SchedulerTypeInfo should not be null", json);
    assertEquals("incorrect number of elements in: " + json, 1, json.length());

    JSONObject info = verifySchedulerJSONObject(json);
    info = verifySchedulerInfoJSONObject(expectedMode, rootQueue, info);
    JSONArray queueArray = getQueuesJSONListObject(info);

    Set<String> verifiedQueues = new HashSet<>();
    for (int i = 0; i < queueArray.length(); i++) {
      JSONObject childQueueObj = queueArray.getJSONObject(i);
      String queuePath = CapacitySchedulerConfiguration.ROOT + "." +
          childQueueObj.getString("queueName");
      if (queuePath.equals(parentPath)) {
        JSONArray childQueueArray = verifyQueueJSONListObject(childQueueObj,
            expectedLeafQueues.length);
        verifyQueues(parentPath, expectedMode, childQueueArray,
            expectedLeafQueues);
        verifiedQueues.add(queuePath);
      }
    }

    Assert.assertEquals("Not all child queues were found. " +
            String.format("Found queues: %s, All queues: %s", verifiedQueues,
                Arrays.stream(expectedLeafQueues).map(lq -> lq.path)
                    .collect(Collectors.toList())),
        expectedLeafQueues.length, verifiedQueues.size());
  }

  private JSONObject verifySchedulerJSONObject(JSONObject json)
      throws JSONException {
    JSONObject info = json.getJSONObject("scheduler");
    Assert.assertNotNull("Scheduler object should not be null", json);
    assertEquals("incorrect number of elements in: " + info, 1, info.length());
    return info;
  }

  private JSONObject verifySchedulerInfoJSONObject(String expectedMode,
      ExpectedQueueWithProperties rootQueue, JSONObject info)
      throws JSONException {
    //Validate if root queue has the expected mode and weight values
    info = info.getJSONObject("schedulerInfo");
    Assert.assertNotNull("SchedulerInfo should not be null", info);
    Assert.assertEquals("Expected Queue mode " + expectedMode, expectedMode,
        info.getString("mode"));
    Assert.assertEquals(rootQueue.weight,
        Float.parseFloat(info.getString("weight")), DELTA);
    Assert.assertEquals(rootQueue.normalizedWeight,
        Float.parseFloat(info.getString("normalizedWeight")), DELTA);
    return info;
  }

  private JSONArray verifyQueueJSONListObject(JSONObject info,
      int expectedQSize) throws JSONException {
    JSONArray queueArray = getQueuesJSONListObject(info);
    assertEquals("QueueInfoList should be size of " + expectedQSize,
        expectedQSize, queueArray.length());
    return queueArray;
  }

  private JSONArray getQueuesJSONListObject(JSONObject info)
      throws JSONException {
    JSONObject queuesObj = info.getJSONObject("queues");
    Assert.assertNotNull("QueueInfoList should not be null", queuesObj);

    JSONArray queueArray = queuesObj.getJSONArray("queue");
    Assert.assertNotNull("Queue list should not be null", queueArray);
    return queueArray;
  }

  private void verifyQueues(String parentPath, String expectedMode,
      JSONArray queueArray, ExpectedQueueWithProperties[] expectedQueues)
      throws JSONException {
    Map<String, ExpectedQueueWithProperties> queuesMap = new HashMap<>();
    for (ExpectedQueueWithProperties expectedQueue : expectedQueues) {
      queuesMap.put(expectedQueue.path, expectedQueue);
    }

    // Create mapping of queue path -> mode
    Map<String, String> modesMap = new HashMap<>();
    for (int i = 0; i < queueArray.length(); i++) {
      JSONObject obj = queueArray.getJSONObject(i);
      String queuePath = parentPath + "." + obj.getString("queueName");
      String mode = obj.getString("mode");
      modesMap.put(queuePath, mode);

      //validate weights of all other queues
      ExpectedQueueWithProperties expectedQueue = queuesMap.get(queuePath);
      Assert.assertNotNull("Queue not found in expectedQueueMap with path: " +
          queuePath, expectedQueue);
      Assert.assertEquals("Weight value does not match",
          expectedQueue.weight, Float.parseFloat(obj.getString("weight")),
          DELTA);
      Assert.assertEquals("Normalized weight value does not match for queue " +
              queuePath,
          expectedQueue.normalizedWeight,
          Float.parseFloat(obj.getString("normalizedWeight")), DELTA);

      //validate queue creation type
      Assert.assertEquals("Queue type does not match for queue " +
              queuePath,
          expectedQueue.queueType, obj.getString("queueType"));

      Assert.assertEquals("Queue creation type does not match for queue " +
              queuePath,
          expectedQueue.creationMethod, obj.getString("creationMethod"));

      Assert.assertEquals("Queue auto creation eligibility does not " +
              "match for queue " + queuePath,
          expectedQueue.autoCreationEligibility,
          obj.getString("autoCreationEligibility"));
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

      for (Map.Entry<String, String> entry: configs.entrySet()) {
        config.set(entry.getKey(), entry.getValue());
      }
      return config;
    }
  }
}
