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
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * Tests partition resource usage per application.
 *
 */
public class TestRMWebServiceAppsNodelabel extends JerseyTestBase {

  private static final int AM_CONTAINER_MB = 1024;

  private static RMNodeLabelsManager nodeLabelManager;

  private static MockRM rm;
  private static CapacitySchedulerConfiguration csConf;
  private static YarnConfiguration conf;

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(new JerseyBinder());
    config.register(RMWebServices.class);
    config.register(GenericExceptionHandler.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    forceSet(TestProperties.CONTAINER_PORT, JERSEY_RANDOM_PORT);
    return config;
  }

  private static class JerseyBinder extends AbstractBinder {
    private static final String LABEL_X = "X";
    @Override
    protected void configure() {
      csConf = new CapacitySchedulerConfiguration();
      setupQueueConfiguration(csConf);
      conf = new YarnConfiguration(csConf);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
              ResourceScheduler.class);
      rm = new MockRM(conf);
      Set<NodeLabel> labels = new HashSet<>();
      labels.add(NodeLabel.newInstance(LABEL_X));
      try {
        nodeLabelManager = rm.getRMContext().getNodeLabelManager();
        nodeLabelManager.addToCluserNodeLabels(labels);
      } catch (Exception e) {
        Assert.fail();
      }
      final HttpServletRequest request = mock(HttpServletRequest.class);
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(conf).to(Configuration.class).named("conf");
      bind(request).to(HttpServletRequest.class);
      bind(response).to(HttpServletResponse.class);
    }
  }

  public TestRMWebServiceAppsNodelabel() {
  }

  private static void setupQueueConfiguration(
      CapacitySchedulerConfiguration config) {

    // Define top-level queues
    QueuePath root = new QueuePath(CapacitySchedulerConfiguration.ROOT);
    QueuePath queueA = root.createNewLeaf("a");
    QueuePath defaultQueue = root.createNewLeaf("default");

    config.setQueues(root,
        new String[]{"a", "default"});

    config.setCapacity(queueA, 50f);
    config.setMaximumCapacity(queueA, 50);

    config.setCapacity(defaultQueue, 50f);
    config.setCapacityByLabel(root, "X", 100);
    config.setMaximumCapacityByLabel(root, "X", 100);
    // set for default queue
    config.setCapacityByLabel(defaultQueue, "X", 100);
    config.setMaximumCapacityByLabel(defaultQueue, "X", 100);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testAppsFinished() throws Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    RMApp killedApp = MockRMAppSubmitter.submitWithMemory(AM_CONTAINER_MB, rm);
    rm.killApp(killedApp.getApplicationId());
    WebTarget r = target();
    Response response =
        r.path("ws").path("v1").path("cluster").path("apps")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    try {
      apps.getJSONArray("app").getJSONObject(0).getJSONObject("resourceInfo");
      fail("resourceInfo object shouldn't be available for finished apps");
    } catch (Exception e) {
      assertTrue("resourceInfo shouldn't be available for finished apps",
          true);
    }
    rm.stop();
  }

  @Test
  public void testAppsRunning() throws Exception {
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 2048);
    MockNM nm2 = rm.registerNode("h2:1235", 2048);

    nodeLabelManager.addLabelsToNode(
        ImmutableMap.of(NodeId.newInstance("h2", 1235), toSet("X")));

    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(AM_CONTAINER_MB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);
    nm1.nodeHeartbeat(true);

    // AM request for resource in partition X
    am1.allocate("*", 1024, 1, new ArrayList<>(), "X");
    nm2.nodeHeartbeat(true);

    WebTarget r = target();

    Response response = r.path("ws").path("v1").path("cluster").path("apps")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);

    // Verify apps resource
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONObject jsonObject = apps.getJSONObject("app").getJSONObject("resourceInfo");
    JSONArray jsonArray = jsonObject.getJSONArray("resourceUsagesByPartition");
    assertEquals("Partition expected is 2", 2, jsonArray.length());

    // Default partition resource
    JSONObject defaultPartition = jsonArray.getJSONObject(0);
    verifyResource(defaultPartition, "", getResource(1024, 1),
        getResource(1024, 1), getResource(0, 0));
    // verify resource used for partition x
    JSONObject partitionX = jsonArray.getJSONObject(1);
    verifyResource(partitionX, "X", getResource(0, 0), getResource(1024, 1),
        getResource(0, 0));
    rm.stop();
  }

  private String getResource(int memory, int vcore) {
    return "{\"memory\":" + memory + ",\"vCores\":" + vcore + "}";
  }

  private void verifyResource(JSONObject partition, String partitionName,
      String amused, String used, String reserved) throws JSONException {
    JSONObject amusedObject = (JSONObject) partition.get("amUsed");
    JSONObject usedObject = (JSONObject) partition.get("used");
    JSONObject reservedObject = (JSONObject) partition.get("reserved");
    assertEquals("Partition expected", partitionName,
        partition.get("partitionName"));
    assertEquals("partition amused", amused, getResource(
        (int) amusedObject.get("memory"), (int) amusedObject.get("vCores")));
    assertEquals("partition used", used, getResource(
        (int) usedObject.get("memory"), (int) usedObject.get("vCores")));
    assertEquals("partition reserved", reserved,
        getResource((int) reservedObject.get("memory"),
            (int) reservedObject.get("vCores")));
  }

  @SuppressWarnings("unchecked")
  private <E> Set<E> toSet(E... elements) {
    return Sets.newHashSet(elements);
  }
}
