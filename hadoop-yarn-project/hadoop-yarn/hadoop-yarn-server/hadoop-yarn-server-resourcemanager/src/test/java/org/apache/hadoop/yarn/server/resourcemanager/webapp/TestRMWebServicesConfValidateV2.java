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

import java.io.FileOutputStream;
import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider.ExcludeRootJSONProvider;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider.IncludeRootJSONProvider;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider.JsonProviderFeature;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.backupSchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.getCapacitySchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.restoreSchedulerConfigFileInTarget;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@code POST /ws/v1/cluster/scheduler-conf/validate/v2} over HTTP.
 * Authorization and the non-mutable scheduler response are covered by
 * {@code TestSchedulerConfValidateV2Endpoint}.
 */
public class TestRMWebServicesConfValidateV2 extends JerseyTestBase {
  private static final QueuePath ROOT = new QueuePath("root");
  private static final QueuePath ROOT_A = new QueuePath("root", "a");
  private static final QueuePath ROOT_A_A1 =
      QueuePath.createFromQueues("root", "a", "a1");
  private static final QueuePath ROOT_B = new QueuePath("root", "b");

  private static MockRM rm;
  private static String userName;

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(RMWebServices.class);
    config.register(new JerseyBinder());
    config.register(GenericExceptionHandler.class);
    config.register(TestRMWebServicesAppsModification.TestRMCustomAuthFilter.class);
    config.register(JsonProviderFeature.class);
    config.register(JAXBContextResolver.class);
    return config;
  }

  private final class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      try {
        userName = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException ioe) {
        throw new RuntimeException("Unable to get current user name "
            + ioe.getMessage(), ioe);
      }
      CapacitySchedulerConfiguration csConf =
          new CapacitySchedulerConfiguration(new Configuration(false), false);
      csConf.setQueues(ROOT, new String[] {"a", "b"});
      csConf.setCapacity(ROOT_A, 50f);
      csConf.setQueues(ROOT_A, new String[] {"a1"});
      csConf.setCapacity(ROOT_A_A1, 100f);
      csConf.setCapacity(ROOT_B, 50f);
      YarnConfiguration conf = new YarnConfiguration();
      conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
          YarnConfiguration.MEMORY_CONFIGURATION_STORE);
      conf.set(YarnConfiguration.YARN_ADMIN_ACL, userName);
      try (FileOutputStream out =
          new FileOutputStream(getCapacitySchedulerConfigFileInTarget())) {
        csConf.writeXml(out);
      } catch (IOException e) {
        throw new RuntimeException("Failed to write XML file", e);
      }
      rm = new MockRM(conf);

      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getScheme()).thenReturn("http");
      Principal principal = () -> userName;
      when(request.getUserPrincipal()).thenReturn(principal);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(csConf).to(Configuration.class).named("conf");
      bind(request).to(HttpServletRequest.class);
      bind(mock(HttpServletResponse.class)).to(HttpServletResponse.class);
    }
  }

  @BeforeAll
  public static void beforeClass() {
    backupSchedulerConfigFileInTarget();
  }

  @AfterAll
  public static void afterClass() {
    restoreSchedulerConfigFileInTarget();
  }

  @Override
  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  @AfterEach
  public void tearDown() throws Exception {
    if (rm != null) {
      rm.stop();
    }
    super.tearDown();
  }

  private WebTarget validateTarget(String path) {
    return target()
        .register(new IncludeRootJSONProvider())
        .register(new ExcludeRootJSONProvider())
        .path("ws").path("v1").path("cluster").path(path)
        .queryParam("user.name", userName);
  }

  private static SchedConfUpdateInfo updateMaximumCapacity() {
    SchedConfUpdateInfo info = new SchedConfUpdateInfo();
    Map<String, String> params = new HashMap<>();
    params.put(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY, "80");
    info.getUpdateQueueInfo().add(new QueueConfigInfo("root.a", params));
    return info;
  }

  private JSONObject postJson(SchedConfUpdateInfo info, String explain)
      throws Exception {
    WebTarget target = validateTarget(RMWSConsts.SCHEDULER_CONF_VALIDATE_V2);
    if (explain != null) {
      target = target.queryParam(RMWSConsts.EXPLAIN, explain);
    }
    Response response = target.request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(info, MediaType.APPLICATION_JSON), Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertTrue(response.getMediaType().toString()
        .startsWith(MediaType.APPLICATION_JSON));
    JSONObject json = new JSONObject(response.readEntity(String.class));
    assertEquals(1, json.length());
    return json.getJSONObject("validationResult");
  }

  private static void assertNoErrors(JSONObject result) throws Exception {
    assertTrue(result.getBoolean("valid"));
    JSONArray issues = result.getJSONObject("issues").getJSONArray("issue");
    for (int i = 0; i < issues.length(); i++) {
      assertNotEquals("ERROR",
          issues.getJSONObject(i).getString("severity"));
    }
  }

  @Test
  public void testValidMutation() throws Exception {
    JSONObject result = postJson(updateMaximumCapacity(), null);
    assertNoErrors(result);
    assertFalse(result.has("explain"));
  }

  @Test
  public void testInvalidMutationIsReportedAsIssue() throws Exception {
    SchedConfUpdateInfo info = new SchedConfUpdateInfo();
    info.getRemoveQueueInfo().add("root.missing");
    JSONObject result = postJson(info, null);
    assertFalse(result.getBoolean("valid"));
    JSONArray issues = result.getJSONObject("issues").getJSONArray("issue");
    assertEquals(1, issues.length());
    JSONObject issue = issues.getJSONObject(0);
    assertEquals("invalid-mutation", issue.getString("ruleId"));
    assertEquals("ERROR", issue.getString("severity"));
    assertEquals("Queue root.missing not found", issue.getString("message"));
    assertFalse(issue.has("queuePath"));
    assertFalse(issue.has("propertyKey"));
  }

  @Test
  public void testExplainAffected() throws Exception {
    JSONObject result = postJson(updateMaximumCapacity(), "affected");
    assertNoErrors(result);
    JSONArray queues = result.getJSONObject("explain").getJSONArray("queue");
    for (int i = 0; i < queues.length(); i++) {
      String path = queues.getJSONObject(i).getString("queuePath");
      assertTrue(path.equals("root.a") || path.equals("root.a.a1"), path);
    }
  }

  @Test
  public void testExplainQueueList() throws Exception {
    JSONObject result = postJson(updateMaximumCapacity(), "root.b, root.a");
    JSONArray queues = result.getJSONObject("explain").getJSONArray("queue");
    for (int i = 0; i < queues.length(); i++) {
      String path = queues.getJSONObject(i).getString("queuePath");
      assertTrue(path.equals("root.a") || path.equals("root.b"), path);
    }
  }

  @Test
  public void testXmlRequestAndResponse() throws Exception {
    String body = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<sched-conf><update-queue><queue-name>root.a</queue-name>"
        + "<params><entry><key>maximum-capacity</key><value>80</value>"
        + "</entry></params></update-queue></sched-conf>";
    Response response = validateTarget(RMWSConsts.SCHEDULER_CONF_VALIDATE_V2)
        .request(MediaType.APPLICATION_XML)
        .post(Entity.entity(body, MediaType.APPLICATION_XML), Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertTrue(response.getMediaType().toString()
        .startsWith(MediaType.APPLICATION_XML));
    String xml = response.readEntity(String.class);
    assertTrue(xml.contains("<validationResult><valid>true</valid>"), xml);
    assertFalse(xml.contains("<explain"), xml);
  }

  @Test
  public void testUninterpretableBodyIsBadRequest() throws Exception {
    Response response = validateTarget(RMWSConsts.SCHEDULER_CONF_VALIDATE_V2)
        .request(MediaType.APPLICATION_XML)
        .post(Entity.entity("<sched-conf><update-queue>",
            MediaType.APPLICATION_XML), Response.class);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void testLegacyValidateStillReturnsProposedConfiguration()
      throws Exception {
    Response response = validateTarget(RMWSConsts.SCHEDULER_CONF_VALIDATE)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(updateMaximumCapacity(),
            MediaType.APPLICATION_JSON), Response.class);
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    JSONObject json = new JSONObject(response.readEntity(String.class));
    assertFalse(json.has("validationResult"));
    assertTrue(json.has("property"));
  }
}
