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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.reader.ApplicationSubmissionContextInfoReader;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.writer.ApplicationSubmissionContextInfoWriter;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Application;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfigGeneratorForTest.createConfiguration;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.assertJsonResponse;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.backupSchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.createMutableRM;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.getExpectedResourceFile;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.reinitialize;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.restoreSchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.sendRequest;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/*
 *                                         EffectiveMin (32GB 32VCores)     AbsoluteCapacity
 *     root.default              4/32      [memory=4096,  vcores=4]       12.5%
 *     root.test_1              16/32      [memory=16384, vcores=16]
 *     root.test_2              12/32      [memory=12288, vcores=12]      37.5%
 *     root.test_1.test_1_1      2/16      [memory=2048,  vcores=2]       6.25%
 *     root.test_1.test_1_2      2/16      [memory=2048,  vcores=2]       6.25%
 *     root.test_1.test_1_3     12/16      [memory=12288, vcores=12]      37.5%
 */
public class TestRMWebServicesCapacitySchedDynamicConfigWeightModeDQC extends JerseyTestBase {

  private boolean legacyQueueMode;

  private MockRM rm;

  public static Collection<Boolean> getParameters() {
    return Arrays.asList(true, false);
  }

  private static final String EXPECTED_FILE_TMPL = "webapp/dynamic-%s-%s.json";

  private Configuration conf;

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public void initTestRMWebServicesCapacitySchedDynamicConfigWeightModeDQC(
      boolean pLegacyQueueMode) throws Exception {
    this.legacyQueueMode = pLegacyQueueMode;
    backupSchedulerConfigFileInTarget();
    setUp();
  }

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(RMWebServices.class);
    config.register(new JerseyBinder());
    config.register(GenericExceptionHandler.class);
    config.register(ApplicationSubmissionContextInfoWriter.class);
    config.register(ApplicationSubmissionContextInfoReader.class);
    config.register(TestRMWebServicesAppsModification.TestRMCustomAuthFilter.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    return config;
  }

  private class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      Map<String, String> configMap = new HashMap<>();
      configMap.put("yarn.scheduler.capacity.legacy-queue-mode.enabled",
          String.valueOf(legacyQueueMode));
      configMap.put("yarn.scheduler.capacity.root.queues", "default, test1, test2");
      configMap.put("yarn.scheduler.capacity.root.test1.queues", "test1_1, test1_2, test1_3");
      configMap.put("yarn.scheduler.capacity.root.default.capacity", "4w");
      configMap.put("yarn.scheduler.capacity.root.test1.capacity", "16w");
      configMap.put("yarn.scheduler.capacity.root.test2.capacity", "12w");
      configMap.put("yarn.scheduler.capacity.root.test1.test1_1.capacity", "2w");
      configMap.put("yarn.scheduler.capacity.root.test1.test1_2.capacity", "2w");
      configMap.put("yarn.scheduler.capacity.root.test1.test1_3.capacity", "12w");

      conf = createConfiguration(configMap);
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);

      setupAQC(conf, "yarn.scheduler.capacity.root.test2.");
      rm = createMutableRM(conf, false);
      final HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getScheme()).thenReturn("http");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(conf).to(Configuration.class).named("conf");
      bind(request).to(HttpServletRequest.class);
      bind(response).to(HttpServletResponse.class);
    }
  }

  @AfterAll
  public static void afterClass() {
    restoreSchedulerConfigFileInTarget();
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{index}: legacy-queue-mode={0}")
  public void testWeightModeFlexibleAQC(boolean pLegacyQueueMode) throws Exception {
    initTestRMWebServicesCapacitySchedDynamicConfigWeightModeDQC(pLegacyQueueMode);
    // capacity and normalizedWeight are set differently between legacy/non-legacy queue mode
    rm.registerNode("h1:1234", 32 * GB, 32);
    assertJsonResponse(sendRequest(target()),
        getExpectedResourceFile(EXPECTED_FILE_TMPL, "testWeightMode",
        "before-aqc", legacyQueueMode));
    createDynamicQueues(rm, "test2");
    reinitialize(rm, conf);
    assertJsonResponse(sendRequest(target()),
        getExpectedResourceFile(EXPECTED_FILE_TMPL, "testWeightMode",
        "after-aqc", legacyQueueMode));
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

  private void createDynamicQueues(MockRM mockRM, String queueName) {
    try {
      CapacityScheduler cs = (CapacityScheduler) mockRM.getResourceScheduler();
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