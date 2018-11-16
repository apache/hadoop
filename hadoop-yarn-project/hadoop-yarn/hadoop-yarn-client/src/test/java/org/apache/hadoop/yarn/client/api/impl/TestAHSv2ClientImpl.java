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

package org.apache.hadoop.yarn.client.api.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.client.api.TimelineReaderClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class is to test class {@link AHSv2ClientImpl).
 */
public class TestAHSv2ClientImpl {

  private AHSv2ClientImpl client;
  private TimelineReaderClient spyTimelineReaderClient;
  @Before
  public void setup() {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
    client = new AHSv2ClientImpl();
    spyTimelineReaderClient = mock(TimelineReaderClient.class);
    client.setReaderClient(spyTimelineReaderClient);
  }

  @Test
  public void testGetContainerReport() throws IOException, YarnException {
    final ApplicationId appId = ApplicationId.newInstance(0, 1);
    final ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    final ContainerId containerId = ContainerId.newContainerId(appAttemptId, 1);
    when(spyTimelineReaderClient.getContainerEntity(containerId, "ALL", null))
        .thenReturn(createContainerEntity(containerId));
    ContainerReport report = client.getContainerReport(containerId);
    Assert.assertEquals(report.getContainerId(), containerId);
    Assert.assertEquals(report.getAssignedNode().getHost(), "test host");
    Assert.assertEquals(report.getAssignedNode().getPort(), 100);
    Assert.assertEquals(report.getAllocatedResource().getVirtualCores(), 8);
  }

  @Test
  public void testGetAppAttemptReport() throws IOException, YarnException {
    final ApplicationId appId = ApplicationId.newInstance(0, 1);
    final ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    when(spyTimelineReaderClient.getApplicationAttemptEntity(appAttemptId,
        "ALL", null))
        .thenReturn(createAppAttemptTimelineEntity(appAttemptId));
    ApplicationAttemptReport report =
        client.getApplicationAttemptReport(appAttemptId);
    Assert.assertEquals(report.getApplicationAttemptId(), appAttemptId);
    Assert.assertEquals(report.getFinishTime(), Integer.MAX_VALUE + 2L);
    Assert.assertEquals(report.getOriginalTrackingUrl(),
        "test original tracking url");
  }

  @Test
  public void testGetAppReport() throws IOException, YarnException {
    final ApplicationId appId = ApplicationId.newInstance(0, 1);
    when(spyTimelineReaderClient.getApplicationEntity(appId, "ALL", null))
        .thenReturn(createApplicationTimelineEntity(appId, false, false));
    ApplicationReport report = client.getApplicationReport(appId);
    Assert.assertEquals(report.getApplicationId(), appId);
    Assert.assertEquals(report.getAppNodeLabelExpression(), "test_node_label");
    Assert.assertTrue(report.getApplicationTags().contains("Test_APP_TAGS_1"));
    Assert.assertEquals(report.getYarnApplicationState(),
        YarnApplicationState.FINISHED);
  }

  private static TimelineEntity createApplicationTimelineEntity(
      ApplicationId appId, boolean emptyACLs,
      boolean wrongAppId) {
    TimelineEntity entity = new TimelineEntity();
    entity.setType(ApplicationMetricsConstants.ENTITY_TYPE);
    if (wrongAppId) {
      entity.setId("wrong_app_id");
    } else {
      entity.setId(appId.toString());
    }

    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ApplicationMetricsConstants.NAME_ENTITY_INFO, "test app");
    entityInfo.put(ApplicationMetricsConstants.TYPE_ENTITY_INFO,
        "test app type");
    entityInfo.put(ApplicationMetricsConstants.USER_ENTITY_INFO, "user1");
    entityInfo.put(ApplicationMetricsConstants.QUEUE_ENTITY_INFO,
          "test queue");
    entityInfo.put(
        ApplicationMetricsConstants.UNMANAGED_APPLICATION_ENTITY_INFO, "false");
    entityInfo.put(ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO,
        Priority.newInstance(0));
    entityInfo.put(ApplicationMetricsConstants.SUBMITTED_TIME_ENTITY_INFO,
        Integer.MAX_VALUE + 1L);
    entityInfo.put(ApplicationMetricsConstants.APP_MEM_METRICS, 123);
    entityInfo.put(ApplicationMetricsConstants.APP_CPU_METRICS, 345);

    entityInfo.put(ApplicationMetricsConstants.APP_MEM_PREEMPT_METRICS, 456);
    entityInfo.put(ApplicationMetricsConstants.APP_CPU_PREEMPT_METRICS, 789);

    if (emptyACLs) {
      entityInfo.put(ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO, "");
    } else {
      entityInfo.put(ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO,
          "user2");
    }

    Set<String> appTags = new HashSet<String>();
    appTags.add("Test_APP_TAGS_1");
    appTags.add("Test_APP_TAGS_2");
    entityInfo.put(ApplicationMetricsConstants.APP_TAGS_INFO, appTags);
    entity.setInfo(entityInfo);

    Map<String, String> configs = new HashMap<>();
    configs.put(ApplicationMetricsConstants.APP_NODE_LABEL_EXPRESSION,
        "test_node_label");
    entity.setConfigs(configs);

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(Integer.MAX_VALUE + 1L + appId.getId());
    entity.addEvent(tEvent);

    // send a YARN_APPLICATION_STATE_UPDATED event
    // after YARN_APPLICATION_FINISHED
    // The final YarnApplicationState should not be changed
    tEvent = new TimelineEvent();
    tEvent.setId(
        ApplicationMetricsConstants.STATE_UPDATED_EVENT_TYPE);
    tEvent.setTimestamp(Integer.MAX_VALUE + 2L + appId.getId());
    Map<String, Object> eventInfo = new HashMap<>();
    eventInfo.put(ApplicationMetricsConstants.STATE_EVENT_INFO,
        YarnApplicationState.KILLED);
    tEvent.setInfo(eventInfo);
    entity.addEvent(tEvent);

    return entity;
  }

  private static TimelineEntity createAppAttemptTimelineEntity(
      ApplicationAttemptId appAttemptId) {
    TimelineEntity entity = new TimelineEntity();
    entity.setType(AppAttemptMetricsConstants.ENTITY_TYPE);
    entity.setId(appAttemptId.toString());

    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(AppAttemptMetricsConstants.TRACKING_URL_INFO,
        "test tracking url");
    entityInfo.put(AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_INFO,
        "test original tracking url");
    entityInfo.put(AppAttemptMetricsConstants.HOST_INFO, "test host");
    entityInfo.put(AppAttemptMetricsConstants.RPC_PORT_INFO, 100);
    entityInfo.put(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO,
        ContainerId.newContainerId(appAttemptId, 1));
    entity.setInfo(entityInfo);

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(AppAttemptMetricsConstants.REGISTERED_EVENT_TYPE);
    tEvent.setTimestamp(Integer.MAX_VALUE + 1L);
    entity.addEvent(tEvent);

    tEvent = new TimelineEvent();
    tEvent.setId(AppAttemptMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(Integer.MAX_VALUE + 2L);
    entity.addEvent(tEvent);

    return entity;
  }

  private static TimelineEntity createContainerEntity(ContainerId containerId) {
    TimelineEntity entity = new TimelineEntity();
    entity.setType(ContainerMetricsConstants.ENTITY_TYPE);
    entity.setId(containerId.toString());
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_MEMORY_INFO, 1024);
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_VCORE_INFO, 8);
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_HOST_INFO,
        "test host");
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_PORT_INFO, 100);
    entityInfo
        .put(ContainerMetricsConstants.ALLOCATED_PRIORITY_INFO, -1);
    entityInfo.put(ContainerMetricsConstants
        .ALLOCATED_HOST_HTTP_ADDRESS_INFO, "http://test:1234");
    entityInfo.put(ContainerMetricsConstants.DIAGNOSTICS_INFO,
        "test diagnostics info");
    entityInfo.put(ContainerMetricsConstants.EXIT_STATUS_INFO, -1);
    entityInfo.put(ContainerMetricsConstants.STATE_INFO,
        ContainerState.COMPLETE.toString());
    entity.setInfo(entityInfo);

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(ContainerMetricsConstants.CREATED_IN_RM_EVENT_TYPE);
    tEvent.setTimestamp(123456);
    entity.addEvent(tEvent);

    return entity;
  }
}
