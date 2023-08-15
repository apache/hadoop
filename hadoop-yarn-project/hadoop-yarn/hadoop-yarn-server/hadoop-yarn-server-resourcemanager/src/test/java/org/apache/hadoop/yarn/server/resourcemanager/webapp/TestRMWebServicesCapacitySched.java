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

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.ClientResponse;

import java.util.Arrays;
import java.util.Collection;

import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.assertJsonResponse;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.assertJsonType;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.assertXmlResponse;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.createRM;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.createWebAppDescriptor;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestRMWebServicesCapacitySched extends JerseyTestBase {

  private final boolean legacyQueueMode;

  @Parameterized.Parameters(name = "{index}: legacy-queue-mode={0}")
  public static Collection<Boolean> getParameters() {
    return Arrays.asList(true, false);
  }

  public TestRMWebServicesCapacitySched(boolean legacyQueueMode) {
    super(createWebAppDescriptor());
    this.legacyQueueMode = legacyQueueMode;
  }

  @Test
  public void testClusterScheduler() throws Exception {
    try (MockRM rm = createRM(createConfig())){
      rm.registerNode("h1:1234", 32 * GB, 32);
      assertJsonResponse(resource().path("ws/v1/cluster/scheduler")
              .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class),
          "webapp/scheduler-response.json");
      assertJsonResponse(resource().path("ws/v1/cluster/scheduler/")
              .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class),
          "webapp/scheduler-response.json");
      assertJsonResponse(resource().path("ws/v1/cluster/scheduler").get(ClientResponse.class),
          "webapp/scheduler-response.json");
      assertXmlResponse(resource().path("ws/v1/cluster/scheduler/")
              .accept(MediaType.APPLICATION_XML).get(ClientResponse.class),
          "webapp/scheduler-response.xml");
    }
  }

  @Test
  public void testPerUserResources() throws Exception {
    try (MockRM rm = createRM(createConfig())){
      rm.registerNode("h1:1234", 32 * GB, 32);
      MockRMAppSubmitter.submit(rm, MockRMAppSubmissionData.Builder
          .createWithMemory(32, rm)
          .withAppName("app1")
          .withUser("user1")
          .withAcls(null)
          .withQueue("a")
          .withUnmanagedAM(false)
          .build()
      );
      MockRMAppSubmitter.submit(rm, MockRMAppSubmissionData.Builder
          .createWithMemory(64, rm)
          .withAppName("app2")
          .withUser("user2")
          .withAcls(null)
          .withQueue("b")
          .withUnmanagedAM(false)
          .build()
      );
      assertXmlResponse(resource().path("ws/v1/cluster/scheduler")
              .accept(MediaType.APPLICATION_XML).get(ClientResponse.class),
          "webapp/scheduler-response-PerUserResources.xml");
      assertJsonResponse(resource().path("ws/v1/cluster/scheduler")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class),
          "webapp/scheduler-response-PerUserResources.json");

    }
  }

  @Test
  public void testNodeLabelDefaultAPI() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(createConfig());
    conf.setDefaultNodeLabelExpression("root", "ROOT-INHERITED");
    conf.setDefaultNodeLabelExpression("root.a", "root-a-default-label");
    try (MockRM rm = createRM(conf)) {
      rm.registerNode("h1:1234", 32 * GB, 32);
      ClientResponse response = resource().path("ws/v1/cluster/scheduler")
          .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
      assertXmlResponse(response, "webapp/scheduler-response-NodeLabelDefaultAPI.xml");
    }
  }
  @Test
  public void testClusterSchedulerOverviewCapacity() throws Exception {
    try (MockRM rm = createRM(createConfig())) {
      rm.registerNode("h1:1234", 32 * GB, 32);
      ClientResponse response = resource().path("ws/v1/cluster/scheduler-overview")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertJsonType(response);
      JSONObject json = response.getEntity(JSONObject.class);
      TestRMWebServices.verifyClusterSchedulerOverView(json, "Capacity Scheduler");
    }
  }

  @Test
  public void testResourceInfo() {
    Resource res = Resources.createResource(10, 1);
    // If we add a new resource (e.g. disks), then
    // CapacitySchedulerPage and these RM WebServices + docs need to be updated
    // e.g. ResourceInfo
    assertEquals("<memory:10, vCores:1>", res.toString());
  }

  private Configuration createConfig() {
    Configuration conf = new Configuration();
    conf.set("yarn.scheduler.capacity.legacy-queue-mode.enabled", String.valueOf(legacyQueueMode));
    conf.set("yarn.scheduler.capacity.root.queues", "a, b, c");
    conf.set("yarn.scheduler.capacity.root.a.capacity", "12.5");
    conf.set("yarn.scheduler.capacity.root.a.maximum-capacity", "50");
    conf.set("yarn.scheduler.capacity.root.a.max-parallel-app", "42");
    conf.set("yarn.scheduler.capacity.root.b.capacity", "50");
    conf.set("yarn.scheduler.capacity.root.c.capacity", "37.5");
    return conf;
  }
}
