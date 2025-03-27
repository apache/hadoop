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
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Application;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfigGeneratorForTest.createConfiguration;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.backupSchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.createRM;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.restoreSchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.runTest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The queues are configured in each test so that the effectiveMinResource is the same.
 * This makes it possible to compare the JSONs among the tests.
 *                                         EffectiveMin (32GB 32VCores)     AbsoluteCapacity
 *     root.default              4/32      [memory=4096,    vcores=4]       12.5%
 *     root.test_1              16/32      [memory=16384,   vcores=16]
 *     root.test_1.test_1_1        2/16      [memory=2048,  vcores=2]       6.25%
 *     root.test_1.test_1_2        2/16      [memory=2048,  vcores=2]       6.25%
 *     root.test_1.test_1_3       12/16      [memory=12288, vcores=12]      37.5%
 *     root.test_2              12/32      [memory=12288,   vcores=12]      37.5%
 */
public class TestRMWebServicesCapacitySchedulerMixedModeAbsoluteAndWeight
    extends JerseyTestBase {

  private static final String EXPECTED_FILE_TMPL = "webapp/mixed-%s-%s.json";

  public TestRMWebServicesCapacitySchedulerMixedModeAbsoluteAndWeight() {
    backupSchedulerConfigFileInTarget();
  }

  private MockRM rm;
  private Configuration conf;
  private RMWebServices rmWebServices;

  @AfterAll
  public static void afterClass() {
    restoreSchedulerConfigFileInTarget();
  }

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(RMWebServices.class);
    config.register(new JerseyBinder());
    config.register(GenericExceptionHandler.class);
    config.register(TestRMWebServicesAppsModification.TestRMCustomAuthFilter.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    return config;
  }

  private class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      Map<String, String> configMap = new HashMap<>();
      configMap.put("yarn.scheduler.capacity.legacy-queue-mode.enabled", "false");
      configMap.put("yarn.scheduler.capacity.root.queues", "default, test_1, test_2");
      configMap.put("yarn.scheduler.capacity.root.test_1.queues", "test_1_1, test_1_2, test_1_3");
      configMap.put("yarn.scheduler.capacity.root.default.capacity", "1w");
      configMap.put("yarn.scheduler.capacity.root.test_1.capacity", "[memory=16384, vcores=16]");
      configMap.put("yarn.scheduler.capacity.root.test_2.capacity", "3w");
      configMap.put("yarn.scheduler.capacity.root.test_1.test_1_1.capacity",
          "[memory=2048, vcores=2]");
      configMap.put("yarn.scheduler.capacity.root.test_1.test_1_2.capacity",
          "[memory=2048, vcores=2]");
      configMap.put("yarn.scheduler.capacity.root.test_1.test_1_3.capacity", "1w");
      conf = createConfiguration(configMap);

      rm = createRM(createConfiguration(configMap));
      final HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getScheme()).thenReturn("http");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      rmWebServices = new RMWebServices(rm, conf);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(conf).to(Configuration.class).named("conf");
      bind(rmWebServices).to(RMWebServices.class);
      bind(request).to(HttpServletRequest.class);
      rmWebServices.setResponse(response);
      bind(response).to(HttpServletResponse.class);
    }
  }

  @Test
  public void testSchedulerAbsoluteAndWeight() throws Exception {
    runTest(EXPECTED_FILE_TMPL, "testSchedulerAbsoluteAndWeight", rm, target());
  }
}
