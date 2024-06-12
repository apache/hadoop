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
import javax.ws.rs.core.Response;

import com.sun.jersey.api.client.ClientResponse;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.assertJsonResponse;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.createMutableRM;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.createWebAppDescriptor;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.backupSchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.getCapacitySchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.getExpectedResourceFile;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.restoreSchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.webapp.util.YarnWebServiceUtils.toJson;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestRMWebServicesCapacitySchedulerConfigMutation extends JerseyTestBase {
  private static final String EXPECTED_FILE_TMPL = "webapp/configmutation-%s-%s.json";
  private final boolean legacyQueueMode;
  private final String userName;

  @Parameterized.Parameters(name = "{index}: legacy-queue-mode={0}")
  public static Collection<Boolean> getParameters() {
    return Arrays.asList(true, false);
  }

  @BeforeClass
  public static void beforeClass() {
    backupSchedulerConfigFileInTarget();
  }

  @AfterClass
  public static void afterClass() {
    restoreSchedulerConfigFileInTarget();
  }

  public TestRMWebServicesCapacitySchedulerConfigMutation(boolean legacyQueueMode)
      throws IOException {
    super(createWebAppDescriptor());
    this.legacyQueueMode = legacyQueueMode;
    userName = UserGroupInformation.getCurrentUser().getShortUserName();
  }

  @Test
  public void testUpdateAbsoluteHierarchyWithZeroCapacities() throws Exception {
    Configuration absoluteConfig = createAbsoluteConfig();
    FileOutputStream out = new FileOutputStream(getCapacitySchedulerConfigFileInTarget());
    absoluteConfig.writeXml(out);
    out.close();

    try (MockRM rm = createMutableRM(absoluteConfig, true)){
      rm.registerNode("h1:1234", 32 * GB, 32);

      assertJsonResponse(resource().path("ws/v1/cluster/scheduler")
              .queryParam("user.name", userName)
              .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class),
          getExpectedResourceFile(EXPECTED_FILE_TMPL, "absolute-hierarchy", "before-update",
              legacyQueueMode));

      SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
      Map<String, String> capacityChange = new HashMap<>();
      capacityChange.put(CapacitySchedulerConfiguration.CAPACITY,
          "[memory=4096, vcores=4]");
      capacityChange.put(CapacitySchedulerConfiguration.MAXIMUM_CAPACITY,
          "[memory=32768, vcores=32]");
      QueueConfigInfo b = new QueueConfigInfo("root.a", capacityChange);
      updateInfo.getUpdateQueueInfo().add(b);

      ClientResponse response = resource().path("ws/v1/cluster/scheduler-conf")
          .queryParam("user.name", userName)
          .accept(MediaType.APPLICATION_JSON)
          .entity(toJson(updateInfo, SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON)
          .put(ClientResponse.class);

      // HTTP 400 - Bad Request is encountered, check the logs for the failure
      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

      assertJsonResponse(resource().path("ws/v1/cluster/scheduler")
              .queryParam("user.name", userName)
              .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class),
          getExpectedResourceFile(EXPECTED_FILE_TMPL, "absolute-hierarchy", "after-update",
              legacyQueueMode));
    }
  }

  private Configuration createAbsoluteConfig() {
    Configuration conf = new Configuration(false);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, userName);
    conf.set("yarn.scheduler.capacity.legacy-queue-mode.enabled", String.valueOf(legacyQueueMode));
    conf.set("yarn.scheduler.capacity.root.capacity", "[memory=32768, vcores=32]");
    conf.set("yarn.scheduler.capacity.root.queues", "default, a");
    conf.set("yarn.scheduler.capacity.root.default.capacity", "[memory=1024, vcores=1]");
    conf.set("yarn.scheduler.capacity.root.a.capacity", "[memory=0, vcores=0]");
    conf.set("yarn.scheduler.capacity.root.a.max-capacity", "[memory=32768, vcores=32]");
    conf.set("yarn.scheduler.capacity.root.a.queues", "b, c");
    conf.set("yarn.scheduler.capacity.root.a.b.capacity", "[memory=0, vcores=0]");
    conf.set("yarn.scheduler.capacity.root.a.b.max-capacity", "[memory=32768, vcores=32]");
    conf.set("yarn.scheduler.capacity.root.a.c.capacity", "[memory=0, vcores=0]");
    conf.set("yarn.scheduler.capacity.root.a.c.max-capacity", "[memory=32768, vcores=32]");
    return conf;
  }
}
