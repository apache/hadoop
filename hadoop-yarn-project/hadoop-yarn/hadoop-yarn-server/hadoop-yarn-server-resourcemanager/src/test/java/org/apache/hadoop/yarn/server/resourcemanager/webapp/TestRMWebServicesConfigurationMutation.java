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

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.apache.hadoop.yarn.webapp.util.YarnWebServiceUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

/**
 * Test scheduler configuration mutation via REST API.
 */
public class TestRMWebServicesConfigurationMutation extends JerseyTestBase {
  private static final Logger LOG = LoggerFactory
          .getLogger(TestRMWebServicesConfigurationMutation.class);

  private static final File CONF_FILE = new File(new File("target",
      "test-classes"), YarnConfiguration.CS_CONFIGURATION_FILE);
  private static final File OLD_CONF_FILE = new File(new File("target",
      "test-classes"), YarnConfiguration.CS_CONFIGURATION_FILE + ".tmp");

  private static MockRM rm;
  private static String userName;
  private static CapacitySchedulerConfiguration csConf;
  private static YarnConfiguration conf;

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      try {
        userName = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException ioe) {
        throw new RuntimeException("Unable to get current user name "
            + ioe.getMessage(), ioe);
      }
      csConf = new CapacitySchedulerConfiguration(new Configuration(false),
          false);
      setupQueueConfiguration(csConf);
      conf = new YarnConfiguration();
      conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
          YarnConfiguration.MEMORY_CONFIGURATION_STORE);
      conf.set(YarnConfiguration.YARN_ADMIN_ACL, userName);
      try {
        if (CONF_FILE.exists()) {
          if (!CONF_FILE.renameTo(OLD_CONF_FILE)) {
            throw new RuntimeException("Failed to rename conf file");
          }
        }
        FileOutputStream out = new FileOutputStream(CONF_FILE);
        csConf.writeXml(out);
        out.close();
      } catch (IOException e) {
        throw new RuntimeException("Failed to write XML file", e);
      }
      rm = new MockRM(conf);
      bind(ResourceManager.class).toInstance(rm);
      serve("/*").with(GuiceContainer.class);
      filter("/*").through(TestRMWebServicesAppsModification
          .TestRMCustomAuthFilter.class);
    }
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  private static void setupQueueConfiguration(
      CapacitySchedulerConfiguration config) {
    config.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{"a", "b", "c"});

    final String a = CapacitySchedulerConfiguration.ROOT + ".a";
    config.setCapacity(a, 25f);
    config.setMaximumCapacity(a, 50f);

    final String a1 = a + ".a1";
    final String a2 = a + ".a2";
    config.setQueues(a, new String[]{"a1", "a2"});
    config.setCapacity(a1, 100f);
    config.setCapacity(a2, 0f);

    final String b = CapacitySchedulerConfiguration.ROOT + ".b";
    config.setCapacity(b, 75f);

    final String c = CapacitySchedulerConfiguration.ROOT + ".c";
    config.setCapacity(c, 0f);

    final String c1 = c + ".c1";
    config.setQueues(c, new String[] {"c1"});
    config.setCapacity(c1, 0f);
  }

  public TestRMWebServicesConfigurationMutation() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  private CapacitySchedulerConfiguration getSchedulerConf()
      throws JSONException {
    WebResource r = resource();
    ClientResponse response =
        r.path("ws").path("v1").path("cluster")
            .queryParam("user.name", userName).path("scheduler-conf")
            .accept(MediaType.APPLICATION_JSON)
            .get(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    JSONObject json = response.getEntity(JSONObject.class);
    JSONArray items = (JSONArray) json.get("property");
    CapacitySchedulerConfiguration parsedConf =
        new CapacitySchedulerConfiguration();
    for (int i=0; i<items.length(); i++) {
      JSONObject obj = (JSONObject) items.get(i);
      parsedConf.set(obj.get("name").toString(),
          obj.get("value").toString());
    }
    return parsedConf;
  }

  @Test
  public void testGetSchedulerConf() throws Exception {
    CapacitySchedulerConfiguration orgConf = getSchedulerConf();
    assertNotNull(orgConf);
    assertEquals(3, orgConf.getQueues("root").length);
  }

  @Test
  public void testAddNestedQueue() throws Exception {
    CapacitySchedulerConfiguration orgConf = getSchedulerConf();
    assertNotNull(orgConf);
    assertEquals(3, orgConf.getQueues("root").length);

    WebResource r = resource();

    ClientResponse response;

    // Add parent queue root.d with two children d1 and d2.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> d1Capacity = new HashMap<>();
    d1Capacity.put(CapacitySchedulerConfiguration.CAPACITY, "25");
    d1Capacity.put(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY, "25");
    Map<String, String> nearEmptyCapacity = new HashMap<>();
    nearEmptyCapacity.put(CapacitySchedulerConfiguration.CAPACITY, "1E-4");
    nearEmptyCapacity.put(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY,
        "1E-4");
    Map<String, String> d2Capacity = new HashMap<>();
    d2Capacity.put(CapacitySchedulerConfiguration.CAPACITY, "75");
    d2Capacity.put(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY, "75");
    QueueConfigInfo d1 = new QueueConfigInfo("root.d.d1", d1Capacity);
    QueueConfigInfo d2 = new QueueConfigInfo("root.d.d2", d2Capacity);
    QueueConfigInfo d = new QueueConfigInfo("root.d", nearEmptyCapacity);
    updateInfo.getAddQueueInfo().add(d1);
    updateInfo.getAddQueueInfo().add(d2);
    updateInfo.getAddQueueInfo().add(d);
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(YarnWebServiceUtils.toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(4, newCSConf.getQueues("root").length);
    assertEquals(2, newCSConf.getQueues("root.d").length);
    assertEquals(25.0f, newCSConf.getNonLabeledQueueCapacity("root.d.d1"),
        0.01f);
    assertEquals(75.0f, newCSConf.getNonLabeledQueueCapacity("root.d.d2"),
        0.01f);

    CapacitySchedulerConfiguration newConf = getSchedulerConf();
    assertNotNull(newConf);
    assertEquals(4, newConf.getQueues("root").length);
  }

  @Test
  public void testAddWithUpdate() throws Exception {
    WebResource r = resource();

    ClientResponse response;

    // Add root.d with capacity 25, reducing root.b capacity from 75 to 50.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> dCapacity = new HashMap<>();
    dCapacity.put(CapacitySchedulerConfiguration.CAPACITY, "25");
    Map<String, String> bCapacity = new HashMap<>();
    bCapacity.put(CapacitySchedulerConfiguration.CAPACITY, "50");
    QueueConfigInfo d = new QueueConfigInfo("root.d", dCapacity);
    QueueConfigInfo b = new QueueConfigInfo("root.b", bCapacity);
    updateInfo.getAddQueueInfo().add(d);
    updateInfo.getUpdateQueueInfo().add(b);
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(YarnWebServiceUtils.toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(4, newCSConf.getQueues("root").length);
    assertEquals(25.0f, newCSConf.getNonLabeledQueueCapacity("root.d"), 0.01f);
    assertEquals(50.0f, newCSConf.getNonLabeledQueueCapacity("root.b"), 0.01f);
  }

  @Test
  public void testRemoveQueue() throws Exception {
    WebResource r = resource();

    ClientResponse response;

    stopQueue("root.a.a2");
    // Remove root.a.a2
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getRemoveQueueInfo().add("root.a.a2");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(YarnWebServiceUtils.toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(1, newCSConf.getQueues("root.a").length);
    assertEquals("a1", newCSConf.getQueues("root.a")[0]);
  }

  @Test
  public void testRemoveParentQueue() throws Exception {
    WebResource r = resource();

    ClientResponse response;

    stopQueue("root.c", "root.c.c1");
    // Remove root.c (parent queue)
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getRemoveQueueInfo().add("root.c");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(YarnWebServiceUtils.toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(2, newCSConf.getQueues("root").length);
    assertNull(newCSConf.getQueues("root.c"));
  }

  @Test
  public void testRemoveParentQueueWithCapacity() throws Exception {
    WebResource r = resource();

    ClientResponse response;

    stopQueue("root.a", "root.a.a1", "root.a.a2");
    // Remove root.a (parent queue) with capacity 25
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getRemoveQueueInfo().add("root.a");

    // Set root.b capacity to 100
    Map<String, String> bCapacity = new HashMap<>();
    bCapacity.put(CapacitySchedulerConfiguration.CAPACITY, "100");
    QueueConfigInfo b = new QueueConfigInfo("root.b", bCapacity);
    updateInfo.getUpdateQueueInfo().add(b);
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(YarnWebServiceUtils.toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(2, newCSConf.getQueues("root").length);
    assertEquals(100.0f, newCSConf.getNonLabeledQueueCapacity("root.b"),
        0.01f);
  }

  @Test
  public void testRemoveMultipleQueues() throws Exception {
    WebResource r = resource();

    ClientResponse response;

    stopQueue("root.b", "root.c", "root.c.c1");
    // Remove root.b and root.c
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getRemoveQueueInfo().add("root.b");
    updateInfo.getRemoveQueueInfo().add("root.c");
    Map<String, String> aCapacity = new HashMap<>();
    aCapacity.put(CapacitySchedulerConfiguration.CAPACITY, "100");
    aCapacity.put(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY, "100");
    QueueConfigInfo configInfo = new QueueConfigInfo("root.a", aCapacity);
    updateInfo.getUpdateQueueInfo().add(configInfo);
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(YarnWebServiceUtils.toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(1, newCSConf.getQueues("root").length);
  }

  private void stopQueue(String... queuePaths) throws Exception {
    WebResource r = resource();

    ClientResponse response;

    // Set state of queues to STOPPED.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> stoppedParam = new HashMap<>();
    stoppedParam.put(CapacitySchedulerConfiguration.STATE,
        QueueState.STOPPED.toString());
    for (String queue : queuePaths) {
      QueueConfigInfo stoppedInfo = new QueueConfigInfo(queue, stoppedParam);
      updateInfo.getUpdateQueueInfo().add(stoppedInfo);
    }
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(YarnWebServiceUtils.toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    for (String queue : queuePaths) {
      assertEquals(QueueState.STOPPED, newCSConf.getState(queue));
    }
  }

  @Test
  public void testUpdateQueue() throws Exception {
    WebResource r = resource();

    ClientResponse response;

    // Update config value.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> updateParam = new HashMap<>();
    updateParam.put(CapacitySchedulerConfiguration.MAXIMUM_AM_RESOURCE_SUFFIX,
        "0.2");
    QueueConfigInfo aUpdateInfo = new QueueConfigInfo("root.a", updateParam);
    updateInfo.getUpdateQueueInfo().add(aUpdateInfo);
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    assertEquals(CapacitySchedulerConfiguration
            .DEFAULT_MAXIMUM_APPLICATIONMASTERS_RESOURCE_PERCENT,
        cs.getConfiguration()
            .getMaximumApplicationMasterResourcePerQueuePercent("root.a"),
        0.001f);
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(YarnWebServiceUtils.toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);
    LOG.debug("Response headers: " + response.getHeaders());
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf = cs.getConfiguration();
    assertEquals(0.2f, newCSConf
        .getMaximumApplicationMasterResourcePerQueuePercent("root.a"), 0.001f);

    // Remove config. Config value should be reverted to default.
    updateParam.put(CapacitySchedulerConfiguration.MAXIMUM_AM_RESOURCE_SUFFIX,
        null);
    aUpdateInfo = new QueueConfigInfo("root.a", updateParam);
    updateInfo.getUpdateQueueInfo().clear();
    updateInfo.getUpdateQueueInfo().add(aUpdateInfo);
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(YarnWebServiceUtils.toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    newCSConf = cs.getConfiguration();
    assertEquals(CapacitySchedulerConfiguration
        .DEFAULT_MAXIMUM_APPLICATIONMASTERS_RESOURCE_PERCENT, newCSConf
            .getMaximumApplicationMasterResourcePerQueuePercent("root.a"),
        0.001f);
  }

  @Test
  public void testUpdateQueueCapacity() throws Exception {
    WebResource r = resource();

    ClientResponse response;

    // Update root.a and root.b capacity to 50.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> updateParam = new HashMap<>();
    updateParam.put(CapacitySchedulerConfiguration.CAPACITY, "50");
    QueueConfigInfo aUpdateInfo = new QueueConfigInfo("root.a", updateParam);
    QueueConfigInfo bUpdateInfo = new QueueConfigInfo("root.b", updateParam);
    updateInfo.getUpdateQueueInfo().add(aUpdateInfo);
    updateInfo.getUpdateQueueInfo().add(bUpdateInfo);

    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(YarnWebServiceUtils.toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(50.0f, newCSConf.getNonLabeledQueueCapacity("root.a"), 0.01f);
    assertEquals(50.0f, newCSConf.getNonLabeledQueueCapacity("root.b"), 0.01f);
  }

  @Test
  public void testGlobalConfChange() throws Exception {
    WebResource r = resource();

    ClientResponse response;

    // Set maximum-applications to 30000.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getGlobalParams().put(CapacitySchedulerConfiguration.PREFIX +
        "maximum-applications", "30000");

    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(YarnWebServiceUtils.toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(30000, newCSConf.getMaximumSystemApplications());

    updateInfo.getGlobalParams().put(CapacitySchedulerConfiguration.PREFIX +
        "maximum-applications", null);
    // Unset maximum-applications. Should be set to default.
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(YarnWebServiceUtils.toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(CapacitySchedulerConfiguration
        .DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS,
        newCSConf.getMaximumSystemApplications());
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if (rm != null) {
      rm.stop();
    }
    CONF_FILE.delete();
    if (!OLD_CONF_FILE.renameTo(CONF_FILE)) {
      throw new RuntimeException("Failed to re-copy old configuration file");
    }
    super.tearDown();
  }
}
