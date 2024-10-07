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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.fairscheduler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.JAXBContextResolver;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebServices;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.helper.BufferedClientResponse;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.helper.JsonCustomResourceTypeTestcase;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.helper.XmlCustomResourceTypeTestCase;
import org.apache.hadoop.yarn.util.resource.CustomResourceTypesConfigurationProvider;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Element;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * This class is to test response representations of queue resources,
 * explicitly setting custom resource types. with the help of
 * {@link CustomResourceTypesConfigurationProvider}
 */
public class TestRMWebServicesFairSchedulerCustomResourceTypes extends JerseyTestBase {

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
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class, ResourceScheduler.class);
      initResourceTypes(conf);
      rm = new MockRM(conf);

      final HttpServletRequest request = mock(HttpServletRequest.class);
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(conf).to(Configuration.class).named("conf");
      bind(request).to(HttpServletRequest.class);
      bind(response).to(HttpServletResponse.class);
    }

    private void initResourceTypes(YarnConfiguration conf) {
      conf.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
          CustomResourceTypesConfigurationProvider.class.getName());
      ResourceUtils.resetResourceTypes(conf);
    }
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  public void tearDown() {
    ResourceUtils.resetResourceTypes(new Configuration());
  }

  @After
  public void teardown() {
    CustomResourceTypesConfigurationProvider.reset();
  }

  public TestRMWebServicesFairSchedulerCustomResourceTypes() {
  }

  @Test
  public void testClusterSchedulerWithCustomResourceTypesJson() throws JSONException {
    FairScheduler scheduler = (FairScheduler) rm.getResourceScheduler();
    QueueManager queueManager = scheduler.getQueueManager();
    // create LeafQueues
    queueManager.getLeafQueue("root.q.subqueue1", true);
    queueManager.getLeafQueue("root.q.subqueue2", true);

    FSLeafQueue subqueue1 =
        queueManager.getLeafQueue("root.q.subqueue1", false);
    incrementUsedResourcesOnQueue(subqueue1, 33L);

    WebTarget path =
        target().path("ws").path("v1").path("cluster").path("scheduler");
    Response response =
        path.request(MediaType.APPLICATION_JSON).get(Response.class);

    verifyJsonResponse(path, response,
        CustomResourceTypesConfigurationProvider.getCustomResourceTypes());
  }

  @Test
  public void testClusterSchedulerWithCustomResourceTypesXml() {
    FairScheduler scheduler = (FairScheduler) rm.getResourceScheduler();
    QueueManager queueManager = scheduler.getQueueManager();
    // create LeafQueues
    queueManager.getLeafQueue("root.q.subqueue1", true);
    queueManager.getLeafQueue("root.q.subqueue2", true);

    FSLeafQueue subqueue1 =
        queueManager.getLeafQueue("root.q.subqueue1", false);
    incrementUsedResourcesOnQueue(subqueue1, 33L);

    WebTarget path =
        target().path("ws").path("v1").path("cluster").path("scheduler");
    Response response =
        path.request(MediaType.APPLICATION_XML).get(Response.class);

    verifyXmlResponse(path, response,
        CustomResourceTypesConfigurationProvider.getCustomResourceTypes());
  }

  @Test
  public void testClusterSchedulerWithElevenCustomResourceTypesXml() {
    CustomResourceTypesConfigurationProvider.setResourceTypes(2, "k");

    FairScheduler scheduler = (FairScheduler) rm.getResourceScheduler();
    QueueManager queueManager = scheduler.getQueueManager();
    // create LeafQueues
    queueManager.getLeafQueue("root.q.subqueue1", true);
    queueManager.getLeafQueue("root.q.subqueue2", true);

    FSLeafQueue subqueue1 =
        queueManager.getLeafQueue("root.q.subqueue1", false);
    incrementUsedResourcesOnQueue(subqueue1, 33L);

    WebTarget path = target().path("ws").path("v1").path("cluster").path("scheduler");
    Response response = path.request(MediaType.APPLICATION_XML).get(Response.class);

    verifyXmlResponse(path, response,
        CustomResourceTypesConfigurationProvider.getCustomResourceTypes());
  }

  @Test
  public void testClusterSchedulerElevenWithCustomResourceTypesJson() throws JSONException {
    CustomResourceTypesConfigurationProvider.setResourceTypes(2, "k");

    FairScheduler scheduler = (FairScheduler) rm.getResourceScheduler();
    QueueManager queueManager = scheduler.getQueueManager();
    // create LeafQueues
    queueManager.getLeafQueue("root.q.subqueue1", true);
    queueManager.getLeafQueue("root.q.subqueue2", true);

    FSLeafQueue subqueue1 =
        queueManager.getLeafQueue("root.q.subqueue1", false);
    incrementUsedResourcesOnQueue(subqueue1, 33L);

    WebTarget path =
        target().path("ws").path("v1").path("cluster").path("scheduler");
    Response response =
        path.request(MediaType.APPLICATION_JSON).get(Response.class);

    verifyJsonResponse(path, response,
        CustomResourceTypesConfigurationProvider.getCustomResourceTypes());
  }

  private void verifyJsonResponse(WebTarget path, Response response,
      List<String> customResourceTypes) throws JSONException {
    JsonCustomResourceTypeTestcase testCase = new JsonCustomResourceTypeTestcase(path,
        new BufferedClientResponse(response));

    testCase.verify(json -> {
      try {
        JSONObject queue = json.getJSONObject("scheduler")
            .getJSONObject("schedulerInfo").getJSONObject("rootQueue")
            .getJSONObject("childQueues").getJSONObject("queue");
        JSONArray queues = new JSONArray();
        queues.put(queue);

        assertEquals(1, queues.length());

        // firstChildQueue info contains subqueue1 and subqueue2 info
        JSONObject firstChildQueue = queues.getJSONObject(0);
        new FairSchedulerJsonVerifications(customResourceTypes)
            .verify(firstChildQueue);
      } catch (JSONException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private void verifyXmlResponse(WebTarget path, Response response,
      List<String> customResourceTypes) {
    XmlCustomResourceTypeTestCase testCase = new XmlCustomResourceTypeTestCase(
        path, new BufferedClientResponse(response));

    testCase.verify(xml -> {
      Element scheduler =
          (Element) xml.getElementsByTagName("scheduler").item(0);
      Element schedulerInfo =
          (Element) scheduler.getElementsByTagName("schedulerInfo").item(0);
      Element rootQueue =
          (Element) schedulerInfo.getElementsByTagName("rootQueue").item(0);

      Element childQueues =
          (Element) rootQueue.getElementsByTagName("childQueues").item(0);
      Element queue =
          (Element) childQueues.getElementsByTagName("queue").item(0);
      new FairSchedulerXmlVerifications(customResourceTypes).verify(queue);
    });
  }

  private void incrementUsedResourcesOnQueue(final FSLeafQueue queue,
      final long value) {
    try {

      Map<String, Long> customResources =
          CustomResourceTypesConfigurationProvider.getCustomResourceTypes()
          .stream()
          .collect(Collectors.toMap(Function.identity(), v -> value));

      System.out.println(customResources.size());

      queue.incUsedResource(Resource.newInstance(20, 30, customResources));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
