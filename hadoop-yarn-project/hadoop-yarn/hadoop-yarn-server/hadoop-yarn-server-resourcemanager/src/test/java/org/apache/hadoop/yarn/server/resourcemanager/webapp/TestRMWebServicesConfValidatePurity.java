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
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AccessRequest;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.apache.hadoop.yarn.security.PrivilegedEntity.EntityType;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.NodeAttributeTestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ValidationResultInfo;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.backupSchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.getCapacitySchedulerConfigFileInTarget;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.restoreSchedulerConfigFileInTarget;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Validating a proposed scheduler configuration must not change the running
 * scheduler: placement rules, queue ACLs, the node label manager's queue map
 * and queue metrics stay as they were, for both {@code validate/v2} and the
 * legacy {@code validate} endpoint. On trunk the legacy endpoint built a
 * throwaway scheduler on the live RMContext, which replaced all of them with
 * the proposed configuration.
 *
 * <p>The endpoints are called directly, but the test rewrites
 * {@code target/test-classes/capacity-scheduler.xml}, so it must not run in
 * parallel with the Jersey tests of this module.</p>
 */
public class TestRMWebServicesConfValidatePurity {
  private static final String PREFIX = CapacitySchedulerConfiguration.PREFIX;

  private MockRM rm;
  private RMWebServices webServices;
  private HttpServletRequest request;

  @BeforeEach
  public void setUp() throws Exception {
    backupSchedulerConfigFileInTarget();
    Configuration csConf = new Configuration(false);
    csConf.set(PREFIX + "root.queues", "a,b");
    csConf.set(PREFIX + "root.a.capacity", "50");
    csConf.set(PREFIX + "root.b.capacity", "50");
    csConf.set(PREFIX + "root.acl_submit_applications", " ");
    csConf.set(PREFIX + "root.acl_administer_queue", " ");
    csConf.set(PREFIX + "root.a.acl_submit_applications", "alice");
    csConf.set(PREFIX + "root.a.acl_administer_queue", " ");
    csConf.set(PREFIX + "queue-mappings", "u:alice:a");
    try (FileOutputStream out =
        new FileOutputStream(getCapacitySchedulerConfigFileInTarget())) {
      csConf.writeXml(out);
    }

    String userName = UserGroupInformation.getCurrentUser().getShortUserName();
    YarnConfiguration conf = NodeAttributeTestUtils.getRandomDirConf(null);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, userName);
    rm = new MockRM(conf);
    rm.start();
    rm.registerNode("h1:1234", 8 * 1024, 8);

    request = mock(HttpServletRequest.class);
    Principal principal = () -> userName;
    when(request.getUserPrincipal()).thenReturn(principal);
    webServices = new RMWebServices(rm, conf, mock(HttpServletResponse.class));
  }

  @AfterEach
  public void tearDown() {
    if (rm != null) {
      rm.stop();
    }
    restoreSchedulerConfigFileInTarget();
  }

  @Test
  public void testValidateV2LeavesLiveStateUnchanged() throws Exception {
    LiveState before = new LiveState();
    Response response = webServices.validateSchedulerConfigurationV2(
        proposal(), null, request);
    assertEquals(200, response.getStatus());
    assertTrue(((ValidationResultInfo) response.getEntity()).isValid());
    before.assertUnchanged();
  }

  @Test
  public void testLegacyValidateLeavesLiveStateUnchanged() throws Exception {
    LiveState before = new LiveState();
    Response response = webServices.validateAndGetSchedulerConfiguration(
        proposal(), request);
    assertEquals(200, response.getStatus(),
        String.valueOf(response.getEntity()));
    before.assertUnchanged();
  }

  @Test
  public void testEmptyMutationValidatesCleanly() throws Exception {
    Response response = webServices.validateSchedulerConfigurationV2(
        new SchedConfUpdateInfo(), null, request);
    assertEquals(200, response.getStatus());
    ValidationResultInfo result = (ValidationResultInfo) response.getEntity();
    assertTrue(result.isValid());
    assertTrue(result.getIssues().isEmpty(), result.getIssues().toString());
  }

  @Test
  public void testStoppedStateKeyOfRemovedQueueIsAccepted() throws Exception {
    SchedConfUpdateInfo info = new SchedConfUpdateInfo();
    info.getRemoveQueueInfo().add("root.b");
    info.getUpdateQueueInfo().add(new QueueConfigInfo("root.a",
        Collections.singletonMap("capacity", "100")));
    info.getGlobalParams().put(PREFIX + "root.b.state", "STOPPED");
    Response response = webServices.validateSchedulerConfigurationV2(info,
        null, request);
    assertEquals(200, response.getStatus());
    ValidationResultInfo result = (ValidationResultInfo) response.getEntity();
    assertTrue(result.isValid());
    assertTrue(result.getIssues().isEmpty(), result.getIssues().toString());
    assertEquals(200, webServices.validateAndGetSchedulerConfiguration(info,
        request).getStatus());

    info.getGlobalParams().clear();
    result = (ValidationResultInfo) webServices
        .validateSchedulerConfigurationV2(info, null, request).getEntity();
    assertFalse(result.isValid());
    assertEquals("queue-removal-not-stopped",
        result.getIssues().get(0).getRuleId());
    Response legacy = webServices.validateAndGetSchedulerConfiguration(info,
        request);
    assertEquals(400, legacy.getStatus());
    assertEquals("CapacityScheduler configuration validation failed:"
        + "java.io.IOException: Failed to re-init queues : root.b cannot be"
        + " deleted from the capacity scheduler configuration, as the queue"
        + " is not yet in stopped state. Current State : RUNNING",
        legacy.getEntity());
  }

  /**
   * Removing a DRAINING queue that still has applications: the legacy
   * endpoint judges the removal by the configured state, as trunk did, and
   * accepts it; validate/v2 sees the live DRAINING state and rejects it, as
   * a refresh does.
   */
  @Test
  public void testRemovingADrainingQueueWithApplications() throws Exception {
    MockRMAppSubmitter.submit(rm, MockRMAppSubmissionData.Builder
        .createWithMemory(1024, rm).withQueue("b").build());
    SchedConfUpdateInfo stop = new SchedConfUpdateInfo();
    stop.getUpdateQueueInfo().add(new QueueConfigInfo("root.b",
        Collections.singletonMap("state", "STOPPED")));
    assertEquals(200, webServices.updateSchedulerConfiguration(stop, request)
        .getStatus());
    assertEquals(QueueState.DRAINING, ((CapacityScheduler)
        rm.getResourceScheduler()).getQueue("root.b").getState());

    SchedConfUpdateInfo remove = new SchedConfUpdateInfo();
    remove.getRemoveQueueInfo().add("root.b");
    remove.getUpdateQueueInfo().add(new QueueConfigInfo("root.a",
        Collections.singletonMap("capacity", "100")));
    Response legacy = webServices.validateAndGetSchedulerConfiguration(remove,
        request);
    assertEquals(200, legacy.getStatus(), String.valueOf(legacy.getEntity()));

    ValidationResultInfo result = (ValidationResultInfo) webServices
        .validateSchedulerConfigurationV2(remove, null, request).getEntity();
    assertFalse(result.isValid());
    assertEquals("queue-removal-not-stopped",
        result.getIssues().get(0).getRuleId());
    assertEquals("root.b cannot be deleted from the capacity scheduler"
        + " configuration, as the queue is not yet in stopped state. Current"
        + " State : DRAINING", result.getIssues().get(0).getMessage());
  }

  /** Moves capacity from root.b to root.a and lets bob submit to root.a. */
  private static SchedConfUpdateInfo proposal() {
    SchedConfUpdateInfo info = new SchedConfUpdateInfo();
    Map<String, String> a = new HashMap<>();
    a.put("capacity", "30");
    a.put("acl_submit_applications", "alice,bob");
    info.getUpdateQueueInfo().add(new QueueConfigInfo("root.a", a));
    info.getUpdateQueueInfo().add(new QueueConfigInfo("root.b",
        Collections.singletonMap("capacity", "70")));
    return info;
  }

  /** What validation must leave alone, captured before it runs. */
  private final class LiveState {
    private final List<PlacementRule> placementRules;
    private final Resource queueLabelResource;
    private final float guaranteedCapacity;
    private final float maximumCapacity;

    private LiveState() throws Exception {
      placementRules = new ArrayList<>(
          rm.getRMContext().getQueuePlacementManager().getPlacementRules());
      assertFalse(placementRules.isEmpty());
      queueLabelResource = queueLabelResource();
      guaranteedCapacity = metrics().getGuaranteedCapacity();
      maximumCapacity = metrics().getMaxCapacity();
      assertEquals(0.5f, guaranteedCapacity, 1e-6);
      assertFalse(bobCanSubmitToA());
    }

    private void assertUnchanged() {
      List<PlacementRule> rules =
          rm.getRMContext().getQueuePlacementManager().getPlacementRules();
      assertAll(
          () -> assertEquals(placementRules, rules, "placement rules"),
          () -> {
            for (int i = 0; i < rules.size(); i++) {
              assertSame(placementRules.get(i), rules.get(i),
                  "placement rule " + i);
            }
          },
          () -> assertSame(queueLabelResource, queueLabelResource(),
              "queue map of the node label manager"),
          () -> assertEquals(guaranteedCapacity,
              metrics().getGuaranteedCapacity(), 0f, "guaranteed capacity"),
          () -> assertEquals(maximumCapacity, metrics().getMaxCapacity(), 0f,
              "maximum capacity"),
          () -> assertFalse(bobCanSubmitToA(), "queue ACLs"));
    }
  }

  private Resource queueLabelResource() {
    return rm.getRMContext().getNodeLabelManager().getQueueResource("root.a",
        Collections.<String>emptySet(), Resource.newInstance(0, 0));
  }

  private CSQueueMetrics metrics() {
    return (CSQueueMetrics) ((CapacityScheduler) rm.getResourceScheduler())
        .getQueue("root.a").getMetrics();
  }

  private boolean bobCanSubmitToA() {
    YarnAuthorizationProvider authorizer =
        YarnAuthorizationProvider.getInstance(rm.getConfig());
    return authorizer.checkPermission(new AccessRequest(
        new PrivilegedEntity(EntityType.QUEUE, "root.a"),
        UserGroupInformation.createRemoteUser("bob"),
        AccessType.SUBMIT_APP, null, null, null, null));
  }
}
