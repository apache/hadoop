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
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.MAXIMUM_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.getCapacitySchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.backupSchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.restoreSchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.webapp.util.YarnWebServiceUtils.toJson;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ORDERING_POLICY;

/**
 * Test scheduler configuration mutation via REST API.
 */
public class TestRMWebServicesConfigurationMutation extends JerseyTestBase {
  private static final Logger LOG = LoggerFactory
          .getLogger(TestRMWebServicesConfigurationMutation.class);
  private static final String LABEL_1 = "label1";
  public static final QueuePath ROOT = new QueuePath("root");
  public static final QueuePath ROOT_A = new QueuePath("root", "a");
  public static final QueuePath ROOT_A_A1 = QueuePath.createFromQueues("root", "a", "a1");
  public static final QueuePath ROOT_A_A2 = QueuePath.createFromQueues("root", "a", "a2");
  public static final QueuePath ROOT_B = new QueuePath("root", "b");
  public static final QueuePath ROOT_C = new QueuePath("root", "c");
  public static final QueuePath ROOT_C_C1 = QueuePath.createFromQueues("root", "c", "c1");
  public static final QueuePath ROOT_D = new QueuePath("root", "d");
  private static MockRM rm;
  private static String userName;
  private static CapacitySchedulerConfiguration csConf;
  private static YarnConfiguration conf;

  @BeforeClass
  public static void beforeClass() {
    backupSchedulerConfigFileInTarget();
  }

  @AfterClass
  public static void afterClass() {
    restoreSchedulerConfigFileInTarget();
  }

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
        FileOutputStream out = new FileOutputStream(getCapacitySchedulerConfigFileInTarget());
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
    config.setQueues(ROOT, new String[]{"a", "b", "c", "mappedqueue"});

    config.setCapacity(ROOT_A, 25f);
    config.setMaximumCapacity(ROOT_A, 50f);

    config.setQueues(ROOT_A, new String[]{"a1", "a2"});
    config.setCapacity(ROOT_A_A1, 100f);
    config.setCapacity(ROOT_A_A2, 0f);

    config.setCapacity(ROOT_B, 75f);

    config.setCapacity(ROOT_C, 0f);

    config.setQueues(ROOT_C, new String[] {"c1"});
    config.setCapacity(ROOT_C_C1, 0f);

    config.setCapacity(ROOT_D, 0f);
    config.set(CapacitySchedulerConfiguration.QUEUE_MAPPING,
        "g:hadoop:mappedqueue");
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
    assertEquals(4, orgConf.getQueues(ROOT).size());
  }

  @Test
  public void testFormatSchedulerConf() throws Exception {
    CapacitySchedulerConfiguration newConf = getSchedulerConf();
    assertNotNull(newConf);
    assertEquals(4, newConf.getQueues(ROOT).size());

    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> nearEmptyCapacity = new HashMap<>();
    nearEmptyCapacity.put(CapacitySchedulerConfiguration.CAPACITY, "1E-4");
    QueueConfigInfo d = new QueueConfigInfo("root.formattest",
        nearEmptyCapacity);
    updateInfo.getAddQueueInfo().add(d);

    Map<String, String> stoppedParam = new HashMap<>();
    stoppedParam.put(CapacitySchedulerConfiguration.STATE,
        QueueState.STOPPED.toString());
    QueueConfigInfo stoppedInfo = new QueueConfigInfo("root.formattest",
        stoppedParam);
    updateInfo.getUpdateQueueInfo().add(stoppedInfo);

    // Add a queue root.formattest to the existing three queues
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler-conf").queryParam("user.name", userName)
        .accept(MediaType.APPLICATION_JSON)
        .entity(toJson(updateInfo,
        SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
        .put(ClientResponse.class);
    newConf = getSchedulerConf();
    assertNotNull(newConf);
    assertEquals(5, newConf.getQueues(ROOT).size());

    // Format the scheduler config and validate root.formattest is not present
    response = r.path("ws").path("v1").path("cluster")
        .queryParam("user.name", userName)
        .path(RMWSConsts.FORMAT_SCHEDULER_CONF)
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    newConf = getSchedulerConf();
    assertEquals(4, newConf.getQueues(ROOT).size());
  }

  private long getConfigVersion() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .queryParam("user.name", userName)
        .path(RMWSConsts.SCHEDULER_CONF_VERSION)
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    JSONObject json = response.getEntity(JSONObject.class);
    return Long.parseLong(json.get("versionID").toString());
  }

  @Test
  public void testSchedulerConfigVersion() throws Exception {
    assertEquals(1, getConfigVersion());
    testAddNestedQueue();
    assertEquals(2, getConfigVersion());
  }

  @Test
  public void testAddNestedQueue() throws Exception {
    CapacitySchedulerConfiguration orgConf = getSchedulerConf();
    assertNotNull(orgConf);
    assertEquals(4, orgConf.getQueues(ROOT).size());

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
            .entity(toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(5, newCSConf.getQueues(ROOT).size());
    assertEquals(2, newCSConf.getQueues(ROOT_D).size());
    assertEquals(25.0f, newCSConf.getNonLabeledQueueCapacity(new QueuePath("root.d.d1")),
        0.01f);
    assertEquals(75.0f, newCSConf.getNonLabeledQueueCapacity(new QueuePath("root.d.d2")),
        0.01f);

    CapacitySchedulerConfiguration newConf = getSchedulerConf();
    assertNotNull(newConf);
    assertEquals(5, newConf.getQueues(ROOT).size());
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
            .entity(toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(5, newCSConf.getQueues(ROOT).size());
    assertEquals(25.0f, newCSConf.getNonLabeledQueueCapacity(new QueuePath("root.d")), 0.01f);
    assertEquals(50.0f, newCSConf.getNonLabeledQueueCapacity(new QueuePath("root.b")), 0.01f);
  }

  @Test
  public void testUnsetParentQueueOrderingPolicy() throws Exception {
    WebResource r = resource();
    ClientResponse response;

    // Update ordering policy of Leaf Queue root.b to fair
    SchedConfUpdateInfo updateInfo1 = new SchedConfUpdateInfo();
    Map<String, String> updateParam = new HashMap<>();
    updateParam.put(CapacitySchedulerConfiguration.ORDERING_POLICY,
        "fair");
    QueueConfigInfo aUpdateInfo = new QueueConfigInfo("root.b", updateParam);
    updateInfo1.getUpdateQueueInfo().add(aUpdateInfo);
    response = r.path("ws").path("v1").path("cluster")
        .path("scheduler-conf").queryParam("user.name", userName)
        .accept(MediaType.APPLICATION_JSON)
        .entity(toJson(updateInfo1,
        SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
        .put(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    String bOrderingPolicy = CapacitySchedulerConfiguration.PREFIX
        + "root.b" + CapacitySchedulerConfiguration.DOT + ORDERING_POLICY;
    assertEquals("fair", newCSConf.get(bOrderingPolicy));

    stopQueue(ROOT_B);

    // Add root.b.b1 which makes root.b a Parent Queue
    SchedConfUpdateInfo updateInfo2 = new SchedConfUpdateInfo();
    Map<String, String> capacity = new HashMap<>();
    capacity.put(CapacitySchedulerConfiguration.CAPACITY, "100");
    QueueConfigInfo b1 = new QueueConfigInfo("root.b.b1", capacity);
    updateInfo2.getAddQueueInfo().add(b1);
    response = r.path("ws").path("v1").path("cluster")
        .path("scheduler-conf").queryParam("user.name", userName)
        .accept(MediaType.APPLICATION_JSON)
        .entity(toJson(updateInfo2,
        SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
        .put(ClientResponse.class);

    // Validate unset ordering policy of root.b after converted to
    // Parent Queue
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    newCSConf = ((CapacityScheduler) rm.getResourceScheduler())
        .getConfiguration();
    bOrderingPolicy = CapacitySchedulerConfiguration.PREFIX
        + "root.b" + CapacitySchedulerConfiguration.DOT + ORDERING_POLICY;
    assertNull("Failed to unset Parent Queue OrderingPolicy",
        newCSConf.get(bOrderingPolicy));
  }

  @Test
  public void testUnsetLeafQueueOrderingPolicy() throws Exception {
    WebResource r = resource();
    ClientResponse response;

    // Update ordering policy of Parent Queue root.c to priority-utilization
    SchedConfUpdateInfo updateInfo1 = new SchedConfUpdateInfo();
    Map<String, String> updateParam = new HashMap<>();
    updateParam.put(CapacitySchedulerConfiguration.ORDERING_POLICY,
        "priority-utilization");
    QueueConfigInfo aUpdateInfo = new QueueConfigInfo("root.c", updateParam);
    updateInfo1.getUpdateQueueInfo().add(aUpdateInfo);
    response = r.path("ws").path("v1").path("cluster")
        .path("scheduler-conf").queryParam("user.name", userName)
        .accept(MediaType.APPLICATION_JSON)
        .entity(toJson(updateInfo1,
        SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
        .put(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    String cOrderingPolicy = CapacitySchedulerConfiguration.PREFIX
        + "root.c" + CapacitySchedulerConfiguration.DOT + ORDERING_POLICY;
    assertEquals("priority-utilization", newCSConf.get(cOrderingPolicy));

    stopQueue(ROOT_C_C1);

    // Remove root.c.c1 which makes root.c a Leaf Queue
    SchedConfUpdateInfo updateInfo2 = new SchedConfUpdateInfo();
    updateInfo2.getRemoveQueueInfo().add("root.c.c1");
    response = r.path("ws").path("v1").path("cluster")
        .path("scheduler-conf").queryParam("user.name", userName)
        .accept(MediaType.APPLICATION_JSON)
        .entity(toJson(updateInfo2,
        SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
        .put(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    // Validate unset ordering policy of root.c after converted to
    // Leaf Queue
    newCSConf = ((CapacityScheduler) rm.getResourceScheduler())
        .getConfiguration();
    cOrderingPolicy = CapacitySchedulerConfiguration.PREFIX
        + "root.c" + CapacitySchedulerConfiguration.DOT + ORDERING_POLICY;
    assertNull("Failed to unset Leaf Queue OrderingPolicy",
        newCSConf.get(cOrderingPolicy));
  }

  @Test
  public void testRemoveQueue() throws Exception {
    WebResource r = resource();

    ClientResponse response;

    stopQueue(ROOT_A_A2);
    // Remove root.a.a2
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getRemoveQueueInfo().add("root.a.a2");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals("Failed to remove the queue",
        1, newCSConf.getQueues(ROOT_A).size());
    assertEquals("Failed to remove the right queue",
        "a1", newCSConf.getQueues(ROOT_A).get(0));
  }

  @Test
  public void testStopWithRemoveQueue() throws Exception {
    WebResource r = resource();

    ClientResponse response;

    // Set state of queues to STOPPED.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> stoppedParam = new HashMap<>();
    stoppedParam.put(CapacitySchedulerConfiguration.STATE,
        QueueState.STOPPED.toString());
    QueueConfigInfo stoppedInfo = new QueueConfigInfo("root.a.a2",
        stoppedParam);
    updateInfo.getUpdateQueueInfo().add(stoppedInfo);

    updateInfo.getRemoveQueueInfo().add("root.a.a2");
    response = r.path("ws").path("v1").path("cluster")
        .path("scheduler-conf").queryParam("user.name", userName)
        .accept(MediaType.APPLICATION_JSON)
        .entity(toJson(updateInfo,
        SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
        .put(ClientResponse.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(1, newCSConf.getQueues(ROOT_A).size());
    assertEquals("a1", newCSConf.getQueues(ROOT_A).get(0));
  }

  @Test
  public void testRemoveQueueWhichHasQueueMapping() throws Exception {
    WebResource r = resource();

    ClientResponse response;
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    // Validate Queue 'mappedqueue' exists before deletion
    assertNotNull("Failed to setup CapacityScheduler Configuration",
        cs.getQueue("mappedqueue"));

    // Set state of queue 'mappedqueue' to STOPPED.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> stoppedParam = new HashMap<>();
    stoppedParam.put(CapacitySchedulerConfiguration.STATE, QueueState.STOPPED.toString());
    QueueConfigInfo stoppedInfo = new QueueConfigInfo("root.mappedqueue", stoppedParam);
    updateInfo.getUpdateQueueInfo().add(stoppedInfo);

    // Remove queue 'mappedqueue' using update scheduler-conf
    updateInfo.getRemoveQueueInfo().add("root.mappedqueue");
    response = r.path("ws").path("v1").path("cluster").path("scheduler-conf")
        .queryParam("user.name", userName).accept(MediaType.APPLICATION_JSON)
        .entity(YarnWebServiceUtils.toJson(updateInfo, SchedConfUpdateInfo.class),
            MediaType.APPLICATION_JSON).put(ClientResponse.class);
    String responseText = response.getEntity(String.class);

    // Queue 'mappedqueue' deletion will fail as there is queue mapping present
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertTrue(responseText.contains(
        "Failed to re-init queues : " + "org.apache.hadoop.yarn.exceptions.YarnException:"
            + " Path root 'mappedqueue' does not exist. Path 'mappedqueue' is invalid"));

    // Validate queue 'mappedqueue' exists after above failure
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(4, newCSConf.getQueues(ROOT).size());
    assertNotNull("CapacityScheduler Configuration is corrupt",
        cs.getQueue("mappedqueue"));
  }

  @Test
  public void testStopWithConvertLeafToParentQueue() throws Exception {
    WebResource r = resource();
    ClientResponse response;

    // Set state of queues to STOPPED.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> stoppedParam = new HashMap<>();
    stoppedParam.put(CapacitySchedulerConfiguration.STATE,
        QueueState.STOPPED.toString());
    QueueConfigInfo stoppedInfo = new QueueConfigInfo("root.b",
        stoppedParam);
    updateInfo.getUpdateQueueInfo().add(stoppedInfo);

    Map<String, String> b1Capacity = new HashMap<>();
    b1Capacity.put(CapacitySchedulerConfiguration.CAPACITY, "100");
    QueueConfigInfo b1 = new QueueConfigInfo("root.b.b1", b1Capacity);
    updateInfo.getAddQueueInfo().add(b1);

    response = r.path("ws").path("v1").path("cluster")
        .path("scheduler-conf").queryParam("user.name", userName)
        .accept(MediaType.APPLICATION_JSON)
        .entity(toJson(updateInfo,
            SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
        .put(ClientResponse.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(1, newCSConf.getQueues(ROOT_B).size());
    assertEquals("b1", newCSConf.getQueues(ROOT_B).get(0));
  }

  @Test
  public void testRemoveParentQueue() throws Exception {
    WebResource r = resource();

    ClientResponse response;

    stopQueue(ROOT_C, ROOT_C_C1);
    // Remove root.c (parent queue)
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getRemoveQueueInfo().add("root.c");
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(3, newCSConf.getQueues(ROOT).size());
    assertEquals(0, newCSConf.getQueues(ROOT_C).size());
  }

  @Test
  public void testRemoveParentQueueWithCapacity() throws Exception {
    WebResource r = resource();

    ClientResponse response;

    stopQueue(ROOT_A, ROOT_A_A1, ROOT_A_A2);
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
            .entity(toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(3, newCSConf.getQueues(ROOT).size());
    assertEquals(100.0f, newCSConf.getNonLabeledQueueCapacity(new QueuePath("root.b")),
        0.01f);
  }

  @Test
  public void testRemoveMultipleQueues() throws Exception {
    WebResource r = resource();

    ClientResponse response;

    stopQueue(ROOT_B, ROOT_C, ROOT_C_C1);
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
            .entity(toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(2, newCSConf.getQueues(ROOT).size());
  }

  private void stopQueue(QueuePath... queuePaths) throws Exception {
    WebResource r = resource();

    ClientResponse response;

    // Set state of queues to STOPPED.
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> stoppedParam = new HashMap<>();
    stoppedParam.put(CapacitySchedulerConfiguration.STATE,
        QueueState.STOPPED.toString());
    for (QueuePath queue : queuePaths) {
      QueueConfigInfo stoppedInfo = new QueueConfigInfo(queue.getFullPath(), stoppedParam);
      updateInfo.getUpdateQueueInfo().add(stoppedInfo);
    }
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    for (QueuePath queue : queuePaths) {
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
            .getMaximumApplicationMasterResourcePerQueuePercent(ROOT_A),
        0.001f);
    response =
        r.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);
    LOG.debug("Response headers: " + response.getHeaders());
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf = cs.getConfiguration();
    assertEquals(0.2f, newCSConf
        .getMaximumApplicationMasterResourcePerQueuePercent(ROOT_A), 0.001f);

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
            .entity(toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    newCSConf = cs.getConfiguration();
    assertEquals(CapacitySchedulerConfiguration
        .DEFAULT_MAXIMUM_APPLICATIONMASTERS_RESOURCE_PERCENT, newCSConf
            .getMaximumApplicationMasterResourcePerQueuePercent(ROOT_A),
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
            .entity(toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    CapacitySchedulerConfiguration newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(50.0f, newCSConf.getNonLabeledQueueCapacity(new QueuePath("root.a")), 0.01f);
    assertEquals(50.0f, newCSConf.getNonLabeledQueueCapacity(new QueuePath("root.b")), 0.01f);
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
            .entity(toJson(updateInfo,
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
            .entity(toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    newCSConf =
        ((CapacityScheduler) rm.getResourceScheduler()).getConfiguration();
    assertEquals(CapacitySchedulerConfiguration
        .DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS,
        newCSConf.getMaximumSystemApplications());
  }

  @Test
  public void testNodeLabelRemovalResidualConfigsAreCleared() throws Exception {
    WebResource r = resource();
    ClientResponse response;

    // 1. Create Node Label: label1
    NodeLabelsInfo nodeLabelsInfo = new NodeLabelsInfo();
    nodeLabelsInfo.getNodeLabelsInfo().add(new NodeLabelInfo(LABEL_1));
    WebResource addNodeLabelsResource = r.path("ws").path("v1").path("cluster")
        .path("add-node-labels");
    WebResource getNodeLabelsResource = r.path("ws").path("v1").path("cluster")
        .path("get-node-labels");
    WebResource removeNodeLabelsResource = r.path("ws").path("v1").path("cluster")
        .path("remove-node-labels");
    WebResource schedulerConfResource = r.path("ws").path("v1").path("cluster")
        .path(RMWSConsts.SCHEDULER_CONF);
    response =
        addNodeLabelsResource.queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(logAndReturnJson(addNodeLabelsResource,
                    toJson(nodeLabelsInfo, NodeLabelsInfo.class)),
                MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);

    // 2. Verify new Node Label
    response =
        getNodeLabelsResource.queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    nodeLabelsInfo = response.getEntity(NodeLabelsInfo.class);
    assertEquals(1, nodeLabelsInfo.getNodeLabels().size());
    for (NodeLabelInfo nl : nodeLabelsInfo.getNodeLabelsInfo()) {
      assertEquals(LABEL_1, nl.getName());
      assertTrue(nl.getExclusivity());
    }

    // 3. Assign 'label1' to root.a
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> updateForRoot = new HashMap<>();
    updateForRoot.put(CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS, "*");
    QueueConfigInfo rootUpdateInfo = new QueueConfigInfo(ROOT.getFullPath(), updateForRoot);

    Map<String, String> updateForRootA = new HashMap<>();
    updateForRootA.put(CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS, LABEL_1);
    QueueConfigInfo rootAUpdateInfo = new QueueConfigInfo(ROOT_A.getFullPath(), updateForRootA);

    updateInfo.getUpdateQueueInfo().add(rootUpdateInfo);
    updateInfo.getUpdateQueueInfo().add(rootAUpdateInfo);

    response =
        schedulerConfResource
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(logAndReturnJson(schedulerConfResource, toJson(updateInfo,
                SchedConfUpdateInfo.class)), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    assertEquals(Sets.newHashSet("*"),
        cs.getConfiguration().getAccessibleNodeLabels(ROOT));
    assertEquals(Sets.newHashSet(LABEL_1),
        cs.getConfiguration().getAccessibleNodeLabels(ROOT_A));

    // 4. Set partition capacities to queues as below
    updateInfo = new SchedConfUpdateInfo();
    updateForRoot = new HashMap<>();
    updateForRoot.put(getAccessibleNodeLabelsCapacityPropertyName(LABEL_1), "100");
    updateForRoot.put(getAccessibleNodeLabelsMaxCapacityPropertyName(LABEL_1), "100");
    rootUpdateInfo = new QueueConfigInfo(ROOT.getFullPath(), updateForRoot);

    updateForRootA = new HashMap<>();
    updateForRootA.put(getAccessibleNodeLabelsCapacityPropertyName(LABEL_1), "100");
    updateForRootA.put(getAccessibleNodeLabelsMaxCapacityPropertyName(LABEL_1), "100");
    rootAUpdateInfo = new QueueConfigInfo(ROOT_A.getFullPath(), updateForRootA);

    // Avoid the following exception by adding some capacities to root.a.a1 and root.a.a2 to label1
    // Illegal capacity sum of 0.0 for children of queue a for label=label1.
    // It is set to 0, but parent percent != 0, and doesn't allow children capacity to set to 0
    Map<String, String> updateForRootA_A1 = new HashMap<>();
    updateForRootA_A1.put(getAccessibleNodeLabelsCapacityPropertyName(LABEL_1), "20");
    updateForRootA_A1.put(getAccessibleNodeLabelsMaxCapacityPropertyName(LABEL_1), "20");
    QueueConfigInfo rootA_A1UpdateInfo = new QueueConfigInfo(ROOT_A_A1.getFullPath(),
        updateForRootA_A1);

    Map<String, String> updateForRootA_A2 = new HashMap<>();
    updateForRootA_A2.put(getAccessibleNodeLabelsCapacityPropertyName(LABEL_1), "80");
    updateForRootA_A2.put(getAccessibleNodeLabelsMaxCapacityPropertyName(LABEL_1), "80");
    QueueConfigInfo rootA_A2UpdateInfo = new QueueConfigInfo(ROOT_A_A2.getFullPath(),
        updateForRootA_A2);


    updateInfo.getUpdateQueueInfo().add(rootUpdateInfo);
    updateInfo.getUpdateQueueInfo().add(rootAUpdateInfo);
    updateInfo.getUpdateQueueInfo().add(rootA_A1UpdateInfo);
    updateInfo.getUpdateQueueInfo().add(rootA_A2UpdateInfo);

    response =
        schedulerConfResource
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(logAndReturnJson(schedulerConfResource, toJson(updateInfo,
                SchedConfUpdateInfo.class)), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    assertEquals(100.0, cs.getConfiguration().getLabeledQueueCapacity(ROOT, LABEL_1), 0.001f);
    assertEquals(100.0, cs.getConfiguration().getLabeledQueueMaximumCapacity(ROOT, LABEL_1),
        0.001f);
    assertEquals(100.0, cs.getConfiguration().getLabeledQueueCapacity(ROOT_A, LABEL_1), 0.001f);
    assertEquals(100.0, cs.getConfiguration().getLabeledQueueMaximumCapacity(ROOT_A, LABEL_1),
        0.001f);
    assertEquals(20.0, cs.getConfiguration().getLabeledQueueCapacity(ROOT_A_A1, LABEL_1), 0.001f);
    assertEquals(20.0, cs.getConfiguration().getLabeledQueueMaximumCapacity(ROOT_A_A1, LABEL_1),
        0.001f);
    assertEquals(80.0, cs.getConfiguration().getLabeledQueueCapacity(ROOT_A_A2, LABEL_1), 0.001f);
    assertEquals(80.0, cs.getConfiguration().getLabeledQueueMaximumCapacity(ROOT_A_A2, LABEL_1),
        0.001f);

    //5. De-assign node label: "label1" + Remove residual properties
    updateInfo = new SchedConfUpdateInfo();
    updateForRoot = new HashMap<>();
    updateForRoot.put(CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS, "*");
    updateForRoot.put(getAccessibleNodeLabelsCapacityPropertyName(LABEL_1), "");
    updateForRoot.put(getAccessibleNodeLabelsMaxCapacityPropertyName(LABEL_1), "");
    rootUpdateInfo = new QueueConfigInfo(ROOT.getFullPath(), updateForRoot);

    updateForRootA = new HashMap<>();
    updateForRootA.put(CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS, "");
    updateForRootA.put(getAccessibleNodeLabelsCapacityPropertyName(LABEL_1), "");
    updateForRootA.put(getAccessibleNodeLabelsMaxCapacityPropertyName(LABEL_1), "");
    rootAUpdateInfo = new QueueConfigInfo(ROOT_A.getFullPath(), updateForRootA);

    updateForRootA_A1 = new HashMap<>();
    updateForRootA_A1.put(CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS, "");
    updateForRootA_A1.put(getAccessibleNodeLabelsCapacityPropertyName(LABEL_1), "");
    updateForRootA_A1.put(getAccessibleNodeLabelsMaxCapacityPropertyName(LABEL_1), "");
    rootA_A1UpdateInfo = new QueueConfigInfo(ROOT_A_A1.getFullPath(), updateForRootA_A1);

    updateForRootA_A2 = new HashMap<>();
    updateForRootA_A2.put(CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS, "");
    updateForRootA_A2.put(getAccessibleNodeLabelsCapacityPropertyName(LABEL_1), "");
    updateForRootA_A2.put(getAccessibleNodeLabelsMaxCapacityPropertyName(LABEL_1), "");
    rootA_A2UpdateInfo = new QueueConfigInfo(ROOT_A_A2.getFullPath(), updateForRootA_A2);

    updateInfo.getUpdateQueueInfo().add(rootUpdateInfo);
    updateInfo.getUpdateQueueInfo().add(rootAUpdateInfo);
    updateInfo.getUpdateQueueInfo().add(rootA_A1UpdateInfo);
    updateInfo.getUpdateQueueInfo().add(rootA_A2UpdateInfo);

    response =
        schedulerConfResource
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(logAndReturnJson(schedulerConfResource, toJson(updateInfo,
                SchedConfUpdateInfo.class)), MediaType.APPLICATION_JSON)
            .put(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertEquals(Sets.newHashSet("*"),
        cs.getConfiguration().getAccessibleNodeLabels(ROOT));
    assertNull(cs.getConfiguration().getAccessibleNodeLabels(ROOT_A));

    //6. Remove node label 'label1'
    MultivaluedMapImpl params = new MultivaluedMapImpl();
    params.add("labels", LABEL_1);
    response =
        removeNodeLabelsResource
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);

    // Verify
    response =
        getNodeLabelsResource.queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    nodeLabelsInfo = response.getEntity(NodeLabelsInfo.class);
    assertEquals(0, nodeLabelsInfo.getNodeLabels().size());

    //6. Check residual configs
    assertNull(getConfValueForQueueAndLabelAndType(cs, ROOT, LABEL_1, CAPACITY));
    assertNull(getConfValueForQueueAndLabelAndType(cs, ROOT, LABEL_1, MAXIMUM_CAPACITY));
    assertNull(getConfValueForQueueAndLabelAndType(cs, ROOT_A, LABEL_1, CAPACITY));
    assertNull(getConfValueForQueueAndLabelAndType(cs, ROOT_A, LABEL_1, MAXIMUM_CAPACITY));
    assertNull(getConfValueForQueueAndLabelAndType(cs, ROOT_A_A1, LABEL_1, CAPACITY));
    assertNull(getConfValueForQueueAndLabelAndType(cs, ROOT_A_A1, LABEL_1, MAXIMUM_CAPACITY));
    assertNull(getConfValueForQueueAndLabelAndType(cs, ROOT_A_A2, LABEL_1, CAPACITY));
    assertNull(getConfValueForQueueAndLabelAndType(cs, ROOT_A_A2, LABEL_1, MAXIMUM_CAPACITY));
  }

  private String getConfValueForQueueAndLabelAndType(CapacityScheduler cs,
      QueuePath queuePath, String label, String type) {
    return cs.getConfiguration().get(
            QueuePrefixes.getNodeLabelPrefix(
            queuePath, label) + type);
  }

  private Object logAndReturnJson(WebResource ws, String json) {
    LOG.info("Sending to web resource: {}, json: {}", ws, json);
    return json;
  }

  private String getAccessibleNodeLabelsCapacityPropertyName(String label) {
    return String.format("%s.%s.%s", ACCESSIBLE_NODE_LABELS, label, CAPACITY);
  }

  private String getAccessibleNodeLabelsMaxCapacityPropertyName(String label) {
    return String.format("%s.%s.%s", ACCESSIBLE_NODE_LABELS, label, MAXIMUM_CAPACITY);
  }

  @Test
  public void testValidateWithClusterMaxAllocation() throws Exception {
    WebResource r = resource();
    int clusterMax = YarnConfiguration.
        DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB * 2;
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        clusterMax);

    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> updateParam = new HashMap<>();
    updateParam.put(CapacitySchedulerConfiguration.MAXIMUM_APPLICATIONS_SUFFIX,
        "100");
    QueueConfigInfo aUpdateInfo = new QueueConfigInfo("root.a", updateParam);
    updateInfo.getUpdateQueueInfo().add(aUpdateInfo);

    ClientResponse response =
        r.path("ws").path("v1").path("cluster")
            .path(RMWSConsts.SCHEDULER_CONF_VALIDATE)
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(updateInfo,
                SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if (rm != null) {
      rm.stop();
    }
    super.tearDown();
  }
}
