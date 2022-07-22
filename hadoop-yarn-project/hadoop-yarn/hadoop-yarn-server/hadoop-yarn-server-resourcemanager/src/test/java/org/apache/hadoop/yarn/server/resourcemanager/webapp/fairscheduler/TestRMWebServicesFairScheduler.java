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

import com.google.inject.Guice;
import com.google.inject.Scopes;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.ServletModule;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueueManager;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.JAXBContextResolver;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebServices;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests RM Webservices fair scheduler resources.
 */
public class TestRMWebServicesFairScheduler extends JerseyTest {
  private static MockRM rm;
  private static YarnConfiguration conf;

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      conf = new YarnConfiguration();
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
          ResourceScheduler.class);
      rm = new MockRM(conf);
      bind(ResourceManager.class).toInstance(rm);
      bind(GuiceFilter.class).in(Scopes.SINGLETON);
    }
  }

  static {
    GuiceServletConfig
        .setInjector(Guice.createInjector(new WebServletModule()));
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    GuiceServletConfig
        .setInjector(Guice.createInjector(new WebServletModule()));
  }

  public TestRMWebServicesFairScheduler() {
  }

  @Test
  public void testClusterScheduler() throws JSONException {
    WebTarget r = target();
    Response response =
        r.path("ws").path("v1").path("cluster").path("scheduler")
            .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    verifyClusterScheduler(json);
  }

  @Test
  public void testClusterSchedulerSlash() throws JSONException {
    WebTarget r = target();
    Response response =
        r.path("ws").path("v1").path("cluster").path("scheduler/")
            .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
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
    Response response =
        r.path("ws").path("v1").path("cluster").path("scheduler")
            .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    JSONArray subQueueInfo = json.getJSONObject("scheduler")
        .getJSONObject("schedulerInfo").getJSONObject("rootQueue")
        .getJSONObject("childQueues").getJSONArray("queue").getJSONObject(0)
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

}
