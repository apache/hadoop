/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp.fairscheduler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueueManager;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.JAXBContextResolver;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebServices;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.TestRMWebServices;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Tests RM Webservices fair scheduler resources.
 */
public class TestRMWebServicesFairScheduler extends JerseyTestBase {
  private static MockRM rm;
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
    @Override
    protected void configure() {
      conf = new YarnConfiguration();
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
          ResourceScheduler.class);
      rm = new MockRM(conf);

      final HttpServletRequest request = mock(HttpServletRequest.class);
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(conf).to(Configuration.class).named("conf");
      bind(request).to(HttpServletRequest.class);
      bind(response).to(HttpServletResponse.class);
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public TestRMWebServicesFairScheduler() {
  }

  @Test
  public void testClusterScheduler() throws JSONException {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster").path("scheduler")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    verifyClusterScheduler(json);
  }

  @Test
  public void testClusterSchedulerSlash() throws JSONException {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster").path("scheduler/")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    verifyClusterScheduler(json);
  }

  @Test
  public void testClusterSchedulerWithSubQueues()
      throws JSONException {
    FairScheduler scheduler = (FairScheduler) rm.getResourceScheduler();
    QueueManager queueManager = scheduler.getQueueManager();
    // create LeafQueue
    queueManager.getLeafQueue("root.q.subqueue1", true);
    queueManager.getLeafQueue("root.q.subqueue2", true);

    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster").path("scheduler/")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    JSONArray subQueueInfo = json.getJSONObject("scheduler")
        .getJSONObject("schedulerInfo").getJSONObject("rootQueue")
        .getJSONObject("childQueues").getJSONObject("queue")
        .getJSONObject("childQueues").getJSONArray("queue");
    // subQueueInfo is consist of subqueue1 and subqueue2 info
    assertEquals(2, subQueueInfo.length());

    // Verify 'childQueues' field is omitted from FairSchedulerLeafQueueInfo.
    try {
      subQueueInfo.getJSONObject(1).getJSONObject("childQueues");
      fail("FairSchedulerQueueInfo should omit field 'childQueues'"
          + "if child queue is empty.");
    } catch (JSONException je) {
      assertEquals("JSONObject[\"childQueues\"] not found.", je.getMessage());
    }
  }

  private void verifyClusterScheduler(JSONObject json) throws JSONException {
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject info = json.getJSONObject("scheduler");
    assertEquals("incorrect number of elements", 1, info.length());
    info = info.getJSONObject("schedulerInfo");
    assertEquals("incorrect number of elements", 2, info.length());
    JSONObject rootQueue = info.getJSONObject("rootQueue");
    assertEquals("root", rootQueue.getString("queueName"));
  }

  @Test
  public void testClusterSchedulerOverviewFair() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("cluster")
        .path("scheduler-overview").request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String entity = response.readEntity(String.class);
    JSONObject json = new JSONObject(entity);
    JSONObject scheduler = json.getJSONObject("scheduler");
    TestRMWebServices.verifyClusterSchedulerOverView(scheduler, "Fair Scheduler");
  }
}
