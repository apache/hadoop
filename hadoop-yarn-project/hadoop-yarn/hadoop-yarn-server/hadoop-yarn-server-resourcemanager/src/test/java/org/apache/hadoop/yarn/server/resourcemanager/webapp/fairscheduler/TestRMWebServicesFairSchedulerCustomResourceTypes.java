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

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;
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
import org.apache.hadoop.yarn.server.resourcemanager.webapp.helper.*;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Element;
import javax.ws.rs.core.MediaType;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * This class is to test response representations of queue resources,
 * explicitly setting custom resource types. with the help of
 * {@link CustomResourceTypesConfigurationProvider}
 */
public class TestRMWebServicesFairSchedulerCustomResourceTypes
    extends JerseyTestBase {
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
      initResourceTypes(conf);
      rm = new MockRM(conf);
      bind(ResourceManager.class).toInstance(rm);
      serve("/*").with(GuiceContainer.class);
    }

    private void initResourceTypes(YarnConfiguration conf) {
      conf.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
          CustomResourceTypesConfigurationProvider.class.getName());
      ResourceUtils.resetResourceTypes(conf);
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    createInjectorForWebServletModule();
  }

  @After
  public void tearDown() {
    ResourceUtils.resetResourceTypes(new Configuration());
  }

  private void createInjectorForWebServletModule() {
    GuiceServletConfig
        .setInjector(Guice.createInjector(new WebServletModule()));
  }

  @After
  public void teardown() {
    CustomResourceTypesConfigurationProvider.reset();
  }

  public TestRMWebServicesFairSchedulerCustomResourceTypes() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
            .contextListenerClass(GuiceServletConfig.class)
            .filterClass(com.google.inject.servlet.GuiceFilter.class)
            .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testClusterSchedulerWithCustomResourceTypesJson() {
    FairScheduler scheduler = (FairScheduler) rm.getResourceScheduler();
    QueueManager queueManager = scheduler.getQueueManager();
    // create LeafQueues
    queueManager.getLeafQueue("root.q.subqueue1", true);
    queueManager.getLeafQueue("root.q.subqueue2", true);

    FSLeafQueue subqueue1 =
        queueManager.getLeafQueue("root.q.subqueue1", false);
    incrementUsedResourcesOnQueue(subqueue1, 33L);

    WebResource path =
        resource().path("ws").path("v1").path("cluster").path("scheduler");
    ClientResponse response =
        path.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

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

    WebResource path =
        resource().path("ws").path("v1").path("cluster").path("scheduler");
    ClientResponse response =
        path.accept(MediaType.APPLICATION_XML).get(ClientResponse.class);

    verifyXmlResponse(path, response,
        CustomResourceTypesConfigurationProvider.getCustomResourceTypes());
  }

  @Test
  public void testClusterSchedulerWithElevenCustomResourceTypesXml() {
    CustomResourceTypesConfigurationProvider.setNumberOfResourceTypes(11);
    createInjectorForWebServletModule();

    FairScheduler scheduler = (FairScheduler) rm.getResourceScheduler();
    QueueManager queueManager = scheduler.getQueueManager();
    // create LeafQueues
    queueManager.getLeafQueue("root.q.subqueue1", true);
    queueManager.getLeafQueue("root.q.subqueue2", true);

    FSLeafQueue subqueue1 =
        queueManager.getLeafQueue("root.q.subqueue1", false);
    incrementUsedResourcesOnQueue(subqueue1, 33L);

    WebResource path =
        resource().path("ws").path("v1").path("cluster").path("scheduler");
    ClientResponse response =
        path.accept(MediaType.APPLICATION_XML).get(ClientResponse.class);

    verifyXmlResponse(path, response,
        CustomResourceTypesConfigurationProvider.getCustomResourceTypes());
  }

  @Test
  public void testClusterSchedulerElevenWithCustomResourceTypesJson() {
    CustomResourceTypesConfigurationProvider.setNumberOfResourceTypes(11);
    createInjectorForWebServletModule();

    FairScheduler scheduler = (FairScheduler) rm.getResourceScheduler();
    QueueManager queueManager = scheduler.getQueueManager();
    // create LeafQueues
    queueManager.getLeafQueue("root.q.subqueue1", true);
    queueManager.getLeafQueue("root.q.subqueue2", true);

    FSLeafQueue subqueue1 =
        queueManager.getLeafQueue("root.q.subqueue1", false);
    incrementUsedResourcesOnQueue(subqueue1, 33L);

    WebResource path =
        resource().path("ws").path("v1").path("cluster").path("scheduler");
    ClientResponse response =
        path.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    verifyJsonResponse(path, response,
        CustomResourceTypesConfigurationProvider.getCustomResourceTypes());
  }

  private void verifyJsonResponse(WebResource path, ClientResponse response,
      List<String> customResourceTypes) {
    JsonCustomResourceTypeTestcase testCase =
        new JsonCustomResourceTypeTestcase(path,
            new BufferedClientResponse(response));
    testCase.verify(json -> {
      try {
        JSONArray queues = json.getJSONObject("scheduler")
            .getJSONObject("schedulerInfo").getJSONObject("rootQueue")
            .getJSONObject("childQueues").getJSONArray("queue");

        // childQueueInfo consists of subqueue1 and subqueue2 info
        assertEquals(2, queues.length());
        JSONObject firstChildQueue = queues.getJSONObject(0);
        new FairSchedulerJsonVerifications(customResourceTypes)
            .verify(firstChildQueue);
      } catch (JSONException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private void verifyXmlResponse(WebResource path, ClientResponse response,
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
      Method incUsedResourceMethod = queue.getClass().getSuperclass()
          .getDeclaredMethod("incUsedResource", Resource.class);
      incUsedResourceMethod.setAccessible(true);

      Map<String, Long> customResources =
          CustomResourceTypesConfigurationProvider.getCustomResourceTypes()
              .stream()
              .collect(Collectors.toMap(Function.identity(), v -> value));

      incUsedResourceMethod.invoke(queue,
          Resource.newInstance(20, 30, customResources));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
