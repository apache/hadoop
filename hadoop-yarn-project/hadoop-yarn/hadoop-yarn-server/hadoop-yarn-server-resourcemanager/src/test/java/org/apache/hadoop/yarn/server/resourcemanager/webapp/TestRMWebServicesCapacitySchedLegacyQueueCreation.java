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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.ClientResponse;
import org.junit.Test;

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

@RunWith(Parameterized.class)
public class TestRMWebServicesCapacitySchedLegacyQueueCreation extends
    JerseyTestBase {

  private final boolean legacyQueueMode;

  @Parameterized.Parameters(name = "{index}: legacy-queue-mode={0}")
  public static Collection<Boolean> getParameters() {
    return Arrays.asList(true, false);
  }

  public TestRMWebServicesCapacitySchedLegacyQueueCreation(boolean legacyQueueMode) {
    super(createWebAppDescriptor());
    this.legacyQueueMode = legacyQueueMode;
  }

  @Test
  public void testSchedulerResponsePercentageModeLegacyAutoCreation()
      throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.put("yarn.scheduler.capacity.legacy-queue-mode.enabled", String.valueOf(legacyQueueMode));
    conf.put("yarn.scheduler.capacity.root.queues", "default, managed");
    conf.put("yarn.scheduler.capacity.root.default.capacity", "25");
    conf.put("yarn.scheduler.capacity.root.managed.capacity", "75");
    conf.put("yarn.scheduler.capacity.root.managed." +
        "auto-create-child-queue.enabled", "true");
    try (MockRM rm = createMutableRM(createConfiguration(conf))) {
      rm.registerNode("h1:1234", 32 * GB, 32);
      assertJsonResponse(sendRequest(),
          "webapp/scheduler-response-PercentageModeLegacyAutoCreation.json");
    }
  }

  @Test
  public void testSchedulerResponseAbsoluteModeLegacyAutoCreation()
      throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.put("yarn.scheduler.capacity.legacy-queue-mode.enabled", String.valueOf(legacyQueueMode));
    conf.put("yarn.scheduler.capacity.root.queues", "default, managed");
    conf.put("yarn.scheduler.capacity.root.default.capacity", "[memory=28672,vcores=28]");
    conf.put("yarn.scheduler.capacity.root.managed.capacity", "[memory=4096,vcores=4]");
    conf.put("yarn.scheduler.capacity.root.managed.leaf-queue-template.capacity",
        "[memory=2048,vcores=2]");
    conf.put("yarn.scheduler.capacity.root.managed." +
        "auto-create-child-queue.enabled", "true");
    conf.put("yarn.scheduler.capacity.root.managed.leaf-queue-template.acl_submit_applications",
        "user");
    conf.put("yarn.scheduler.capacity.root.managed.leaf-queue-template.acl_administer_queue",
        "admin");
    try (MockRM rm = createMutableRM(createConfiguration(conf))) {
      rm.registerNode("h1:1234", 32 * GB, 32);
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      CapacitySchedulerQueueManager autoQueueHandler = cs.getCapacitySchedulerQueueManager();
      autoQueueHandler.createQueue(new QueuePath("root.managed.queue1"));
      assertJsonResponse(sendRequest(),
          "webapp/scheduler-response-AbsoluteModeLegacyAutoCreation.json");
    }
  }

  private ClientResponse sendRequest() {
    return resource().path("ws").path("v1").path("cluster")
        .path("scheduler").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
  }

}