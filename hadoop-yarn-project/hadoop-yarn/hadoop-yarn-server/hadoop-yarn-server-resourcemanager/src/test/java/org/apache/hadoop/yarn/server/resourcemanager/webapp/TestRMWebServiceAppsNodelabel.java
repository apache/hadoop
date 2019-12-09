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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

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

  private static class WebServletModule extends ServletModule {

    private static final String LABEL_X = "X";

    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      csConf = new CapacitySchedulerConfiguration();
      setupQueueConfiguration(csConf);
      conf = new YarnConfiguration(csConf);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      rm = new MockRM(conf);
      Set<NodeLabel> labels = new HashSet<NodeLabel>();
      labels.add(NodeLabel.newInstance(LABEL_X));
      try {
        nodeLabelManager = rm.getRMContext().getNodeLabelManager();
        nodeLabelManager.addToCluserNodeLabels(labels);
      } catch (Exception e) {
        Assert.fail();
      }
      bind(ResourceManager.class).toInstance(rm);
      serve("/*").with(GuiceContainer.class);
    }
  };

  public TestRMWebServiceAppsNodelabel() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
            .contextListenerClass(GuiceServletConfig.class)
            .filterClass(com.google.inject.servlet.GuiceFilter.class)
            .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  private static void setupQueueConfiguration(
      CapacitySchedulerConfiguration config) {

    // Define top-level queues
    config.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{"a", "default"});

    final String queueA = CapacitySchedulerConfiguration.ROOT + ".a";
    config.setCapacity(queueA, 50f);
    config.setMaximumCapacity(queueA, 50);

    final String defaultQueue =
        CapacitySchedulerConfiguration.ROOT + ".default";
    config.setCapacity(defaultQueue, 50f);
    config.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "X", 100);
    config.setMaximumCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "X",
        100);
    // set for default queue
    config.setCapacityByLabel(defaultQueue, "X", 100);
    config.setMaximumCapacityByLabel(defaultQueue, "X", 100);
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    GuiceServletConfig
        .setInjector(Guice.createInjector(new WebServletModule()));
  }

  @Test
  public void testAppsFinished() throws JSONException, Exception {
    rm.start();
    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    RMApp killedApp = rm.submitApp(AM_CONTAINER_MB);
    rm.killApp(killedApp.getApplicationId());
    WebResource r = resource();
    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("apps")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    JSONObject json = response.getEntity(JSONObject.class);
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
  public void testAppsRunning() throws JSONException, Exception {
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 2048);
    MockNM nm2 = rm.registerNode("h2:1235", 2048);

    nodeLabelManager.addLabelsToNode(
        ImmutableMap.of(NodeId.newInstance("h2", 1235), toSet("X")));

    RMApp app1 = rm.submitApp(AM_CONTAINER_MB, "app", "user", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);
    nm1.nodeHeartbeat(true);

    // AM request for resource in partition X
    am1.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "X");
    nm2.nodeHeartbeat(true);

    WebResource r = resource();

    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("apps")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    JSONObject json = response.getEntity(JSONObject.class);

    // Verify apps resource
    JSONObject apps = json.getJSONObject("apps");
    assertEquals("incorrect number of elements", 1, apps.length());
    JSONObject jsonObject =
        apps.getJSONArray("app").getJSONObject(0).getJSONObject("resourceInfo");
    JSONArray jsonArray = jsonObject.getJSONArray("resourceUsagesByPartition");
    assertEquals("Partition expected is 2", 2, jsonArray.length());

    // Default partition resource
    JSONObject defaultPartition = jsonArray.getJSONObject(0);
    verifyResource(defaultPartition, "", getResource(1024, 1),
        getResource(1024, 1), getResource(0, 0));
    // verify resource used for parition x
    JSONObject paritionX = jsonArray.getJSONObject(1);
    verifyResource(paritionX, "X", getResource(0, 0), getResource(1024, 1),
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
    Set<E> set = Sets.newHashSet(elements);
    return set;
  }
}
