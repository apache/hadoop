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

import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.MutableCSConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ValidationIssueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ValidationResultInfo;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Calls {@code RMWebServices#validateSchedulerConfigurationV2} directly for
 * the paths that end before validation runs: authorization, scheduler
 * capability, body checks and mutation assembly. The Jersey test
 * {@code TestRMWebServicesConfValidateV2} covers the HTTP layer.
 */
public class TestSchedulerConfValidateV2Endpoint {

  private static final String EXPLAIN_AFFECTED = "affected";

  private ResourceManager rm;
  private ApplicationACLsManager aclsManager;
  private HttpServletRequest request;
  private RMWebServices webServices;

  @BeforeEach
  public void setUp() throws Exception {
    rm = mock(ResourceManager.class);
    aclsManager = mock(ApplicationACLsManager.class);
    when(rm.getApplicationACLsManager()).thenReturn(aclsManager);
    request = mock(HttpServletRequest.class);
    Principal principal = () -> "alice";
    when(request.getUserPrincipal()).thenReturn(principal);
    webServices = new RMWebServices(rm, new YarnConfiguration(),
        mock(HttpServletResponse.class));
  }

  private void useMutableCapacityScheduler() throws Exception {
    CapacitySchedulerConfiguration current =
        new CapacitySchedulerConfiguration(new Configuration(false), false);
    current.set("yarn.scheduler.capacity.root.queues", "a,b");
    current.set("yarn.scheduler.capacity.root.a.capacity", "50");
    current.set("yarn.scheduler.capacity.root.b.capacity", "50");

    MutableConfigurationProvider provider =
        mock(MutableConfigurationProvider.class);
    when(provider.getConfiguration()).thenReturn(new Configuration(current));
    MutableCSConfigurationProvider assembler =
        new MutableCSConfigurationProvider(null);
    when(provider.applyChanges(any(), any())).thenAnswer(invocation ->
        assembler.applyChanges(invocation.getArgument(0),
            invocation.getArgument(1)));

    CapacityScheduler scheduler = mock(CapacityScheduler.class);
    when(scheduler.isConfigurationMutable()).thenReturn(true);
    when(scheduler.getMutableConfProvider()).thenReturn(provider);
    when(scheduler.getConf()).thenReturn(new YarnConfiguration());
    when(rm.getResourceScheduler()).thenReturn(scheduler);
  }

  private ValidationIssueInfo singleInvalidMutation(SchedConfUpdateInfo info)
      throws Exception {
    Response response = webServices.validateSchedulerConfigurationV2(info,
        EXPLAIN_AFFECTED, request);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    ValidationResultInfo result = (ValidationResultInfo) response.getEntity();
    assertFalse(result.isValid());
    assertNull(result.getExplain());
    List<ValidationIssueInfo> issues = result.getIssues();
    assertEquals(1, issues.size());
    ValidationIssueInfo issue = issues.get(0);
    assertEquals("invalid-mutation", issue.getRuleId());
    assertEquals("ERROR", issue.getSeverity());
    assertNull(issue.getQueuePath());
    assertNull(issue.getPropertyKey());
    return issue;
  }

  @Test
  public void testNonAdminIsForbiddenLikeLegacyValidate() throws Exception {
    useMutableCapacityScheduler();
    when(aclsManager.areACLsEnabled()).thenReturn(true);
    when(aclsManager.isAdmin(any(UserGroupInformation.class)))
        .thenReturn(false);
    SchedConfUpdateInfo info = new SchedConfUpdateInfo();

    ForbiddenException v2 = assertThrows(ForbiddenException.class,
        () -> webServices.validateSchedulerConfigurationV2(info, null,
            request));
    ForbiddenException legacy = assertThrows(ForbiddenException.class,
        () -> webServices.validateAndGetSchedulerConfiguration(info,
            request));
    assertEquals(legacy.getMessage(), v2.getMessage());
  }

  @Test
  public void testNotMutableMatchesLegacyValidate() throws Exception {
    when(rm.getResourceScheduler()).thenReturn(mock(FifoScheduler.class));
    SchedConfUpdateInfo info = new SchedConfUpdateInfo();

    Response v2 = webServices.validateSchedulerConfigurationV2(info, null,
        request);
    Response legacy = webServices.validateAndGetSchedulerConfiguration(info,
        request);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), v2.getStatus());
    assertEquals(legacy.getStatus(), v2.getStatus());
    assertEquals("Configuration change validation only supported by "
        + "MutableConfScheduler.", v2.getEntity());
    assertEquals(legacy.getEntity(), v2.getEntity());

    CapacityScheduler immutable = mock(CapacityScheduler.class);
    when(immutable.isConfigurationMutable()).thenReturn(false);
    when(rm.getResourceScheduler()).thenReturn(immutable);
    v2 = webServices.validateSchedulerConfigurationV2(info, null, request);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), v2.getStatus());
    assertEquals(webServices.validateAndGetSchedulerConfiguration(info,
        request).getEntity(), v2.getEntity());
  }

  @Test
  public void testMissingBodyIsBadRequest() throws Exception {
    useMutableCapacityScheduler();
    Response response = webServices.validateSchedulerConfigurationV2(null,
        null, request);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(),
        response.getStatus());
  }

  @Test
  public void testRemovingMissingQueueIsInvalidMutation() throws Exception {
    useMutableCapacityScheduler();
    SchedConfUpdateInfo info = new SchedConfUpdateInfo();
    info.getRemoveQueueInfo().add("root.missing");
    assertEquals("Queue root.missing not found",
        singleInvalidMutation(info).getMessage());
  }

  @Test
  public void testAddingExistingQueueIsInvalidMutation() throws Exception {
    useMutableCapacityScheduler();
    SchedConfUpdateInfo info = new SchedConfUpdateInfo();
    Map<String, String> params = new HashMap<>();
    info.getAddQueueInfo().add(new QueueConfigInfo("root.a", params));
    assertEquals("Can't add existing queue root.a",
        singleInvalidMutation(info).getMessage());
  }

  @Test
  public void testRemovingRootIsInvalidMutation() throws Exception {
    useMutableCapacityScheduler();
    SchedConfUpdateInfo info = new SchedConfUpdateInfo();
    info.getRemoveQueueInfo().add("root");
    assertEquals("Can't remove queue root",
        singleInvalidMutation(info).getMessage());
  }
}
