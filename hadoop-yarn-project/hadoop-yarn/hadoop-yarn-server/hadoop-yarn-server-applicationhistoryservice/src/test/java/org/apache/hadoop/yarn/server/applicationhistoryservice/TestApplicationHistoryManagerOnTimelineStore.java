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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.timeline.MemoryTimelineStore;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestApplicationHistoryManagerOnTimelineStore {

  private static final int SCALE = 5;
  private static TimelineStore store;

  private ApplicationHistoryManagerOnTimelineStore historyManager;
  private UserGroupInformation callerUGI;
  private Configuration conf;

  @BeforeAll
  public static void prepareStore() throws Exception {
    store = createStore(SCALE);
    TimelineEntities entities = new TimelineEntities();
    entities.addEntity(
        createApplicationTimelineEntity(ApplicationId.newInstance(0, SCALE + 1), true, true, false,
            false, YarnApplicationState.FINISHED));
    entities.addEntity(
        createApplicationTimelineEntity(ApplicationId.newInstance(0, SCALE + 2), true, false, true,
            false, YarnApplicationState.FINISHED));
    store.put(entities);
  }

  public static TimelineStore createStore(int scale) throws Exception {
    store = new MemoryTimelineStore();
    prepareTimelineStore(scale);
    return store;
  }

  @AfterEach
  public void tearDown() {
    if (historyManager != null) {
      historyManager.stop();
    }
  }

  public static Collection<Object[]> callers() {
    // user1 is the owner
    // user2 is the authorized user
    // user3 is the unauthorized user
    // admin is the admin acl
    return Arrays.asList(new Object[][] {{""}, {"user1"}, {"user2"}, {"user3"}, {"admin"}});
  }

  public void initTestApplicationHistoryManagerOnTimelineStore(String caller) {
    conf = new YarnConfiguration();
    if (!caller.equals("")) {
      callerUGI = UserGroupInformation.createRemoteUser(caller, AuthMethod.SIMPLE);
      conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
      conf.set(YarnConfiguration.YARN_ADMIN_ACL, "admin");
    }
    TimelineACLsManager aclsManager = new TimelineACLsManager(new YarnConfiguration());
    aclsManager.setTimelineStore(store);
    TimelineDataManager dataManager = new TimelineDataManager(store, aclsManager);
    dataManager.init(conf);
    ApplicationACLsManager appAclsManager = new ApplicationACLsManager(conf);
    historyManager = new ApplicationHistoryManagerOnTimelineStore(dataManager, appAclsManager);
    historyManager.init(conf);
    historyManager.start();
  }

  private static void prepareTimelineStore(int scale) throws Exception {
    for (int i = 1; i <= scale; ++i) {
      TimelineEntities entities = new TimelineEntities();
      ApplicationId appId = ApplicationId.newInstance(0, i);
      if (i == 2) {
        entities.addEntity(createApplicationTimelineEntity(appId, true, false, false, true,
            YarnApplicationState.FINISHED));
      } else if (i == 3) {
        entities.addEntity(createApplicationTimelineEntity(appId, false, false, false, false,
            YarnApplicationState.FINISHED, true, false));
      } else if (i == SCALE + 1) {
        entities.addEntity(createApplicationTimelineEntity(appId, false, false, false, false,
            YarnApplicationState.FINISHED, false, true));
      } else {
        entities.addEntity(createApplicationTimelineEntity(appId, false, false, false, false,
            YarnApplicationState.FINISHED));
      }
      store.put(entities);
      for (int j = 1; j <= scale; ++j) {
        entities = new TimelineEntities();
        ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, j);
        entities.addEntity(createAppAttemptTimelineEntity(appAttemptId));
        store.put(entities);
        for (int k = 1; k <= scale; ++k) {
          entities = new TimelineEntities();
          ContainerId containerId = ContainerId.newContainerId(appAttemptId, k);
          entities.addEntity(createContainerEntity(containerId));
          store.put(entities);
        }
      }
    }
    TimelineEntities entities = new TimelineEntities();
    ApplicationId appId = ApplicationId.newInstance(1234, 1);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 1);
    entities.addEntity(createApplicationTimelineEntity(appId, true, false, false, false,
        YarnApplicationState.RUNNING));
    entities.addEntity(createAppAttemptTimelineEntity(appAttemptId));
    entities.addEntity(createContainerEntity(containerId));
    store.put(entities);
  }

  @MethodSource("callers")
  @ParameterizedTest
  void testGetApplicationReport(String caller) throws Exception {
    initTestApplicationHistoryManagerOnTimelineStore(caller);
    for (int i = 1; i <= 3; ++i) {
      final ApplicationId appId = ApplicationId.newInstance(0, i);
      ApplicationReport app;
      if (callerUGI == null) {
        app = historyManager.getApplication(appId);
      } else {
        app = callerUGI.doAs(new PrivilegedExceptionAction<ApplicationReport>() {
          @Override
          public ApplicationReport run() throws Exception {
            return historyManager.getApplication(appId);
          }
        });
      }
      assertNotNull(app);
      assertEquals(appId, app.getApplicationId());
      assertEquals("test app", app.getName());
      assertEquals("test app type", app.getApplicationType());
      assertEquals("user1", app.getUser());
      if (i == 2) {
        // Change event is fired only in case of app with ID 2, hence verify
        // with updated changes. And make sure last updated change is accepted.
        assertEquals("changed queue1", app.getQueue());
        assertEquals(Priority.newInstance(6), app.getPriority());
      } else {
        assertEquals("test queue", app.getQueue());
        assertEquals(Priority.newInstance(0), app.getPriority());
      }
      assertEquals(Integer.MAX_VALUE + 2L + app.getApplicationId().getId(), app.getStartTime());
      assertEquals(Integer.MAX_VALUE + 1L, app.getSubmitTime());
      assertEquals(Integer.MAX_VALUE + 3L + +app.getApplicationId().getId(), app.getFinishTime());
      assertTrue(Math.abs(app.getProgress() - 1.0F) < 0.0001);
      assertEquals(2, app.getApplicationTags().size());
      assertTrue(app.getApplicationTags().contains("Test_APP_TAGS_1"));
      assertTrue(app.getApplicationTags().contains("Test_APP_TAGS_2"));
      // App 2 doesn't have the ACLs, such that the default ACLs " " will be used.
      // Nobody except admin and owner has access to the details of the app.
      if ((i != 2 && callerUGI != null && callerUGI.getShortUserName().equals("user3")) || (i == 2
          && callerUGI != null && (callerUGI.getShortUserName().equals("user2")
          || callerUGI.getShortUserName().equals("user3")))) {
        assertEquals(ApplicationAttemptId.newInstance(appId, -1),
            app.getCurrentApplicationAttemptId());
        assertEquals(ApplicationHistoryManagerOnTimelineStore.UNAVAILABLE, app.getHost());
        assertEquals(-1, app.getRpcPort());
        assertEquals(ApplicationHistoryManagerOnTimelineStore.UNAVAILABLE, app.getTrackingUrl());
        assertEquals(ApplicationHistoryManagerOnTimelineStore.UNAVAILABLE,
            app.getOriginalTrackingUrl());
        assertEquals("", app.getDiagnostics());
      } else {
        assertEquals(ApplicationAttemptId.newInstance(appId, 1),
            app.getCurrentApplicationAttemptId());
        assertEquals("test host", app.getHost());
        assertEquals(100, app.getRpcPort());
        assertEquals("test tracking url", app.getTrackingUrl());
        assertEquals("test original tracking url", app.getOriginalTrackingUrl());
        assertEquals("test diagnostics info", app.getDiagnostics());
      }
      ApplicationResourceUsageReport applicationResourceUsageReport =
          app.getApplicationResourceUsageReport();
      assertEquals(123, applicationResourceUsageReport.getMemorySeconds());
      assertEquals(345, applicationResourceUsageReport.getVcoreSeconds());
      long expectedPreemptMemSecs = 456;
      long expectedPreemptVcoreSecs = 789;
      if (i == 3) {
        expectedPreemptMemSecs = 0;
        expectedPreemptVcoreSecs = 0;
      }
      assertEquals(expectedPreemptMemSecs,
          applicationResourceUsageReport.getPreemptedMemorySeconds());
      assertEquals(expectedPreemptVcoreSecs,
          applicationResourceUsageReport.getPreemptedVcoreSeconds());
      assertEquals(FinalApplicationStatus.UNDEFINED, app.getFinalApplicationStatus());
      assertEquals(YarnApplicationState.FINISHED, app.getYarnApplicationState());
    }
  }

  @MethodSource("callers")
  @ParameterizedTest
  void testGetApplicationReportWithNotAttempt(String caller) throws Exception {
    initTestApplicationHistoryManagerOnTimelineStore(caller);
    final ApplicationId appId = ApplicationId.newInstance(0, SCALE + 1);
    ApplicationReport app;
    if (callerUGI == null) {
      app = historyManager.getApplication(appId);
    } else {
      app = callerUGI.doAs(new PrivilegedExceptionAction<ApplicationReport>() {
        @Override
        public ApplicationReport run() throws Exception {
          return historyManager.getApplication(appId);
        }
      });
    }
    assertNotNull(app);
    assertEquals(appId, app.getApplicationId());
    assertEquals(ApplicationAttemptId.newInstance(appId, -1), app.getCurrentApplicationAttemptId());
  }

  @MethodSource("callers")
  @ParameterizedTest
  void testGetApplicationAttemptReport(String caller) throws Exception {
    initTestApplicationHistoryManagerOnTimelineStore(caller);
    final ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 1), 1);
    ApplicationAttemptReport appAttempt;
    if (callerUGI == null) {
      appAttempt = historyManager.getApplicationAttempt(appAttemptId);
    } else {
      try {
        appAttempt = callerUGI.doAs(new PrivilegedExceptionAction<ApplicationAttemptReport>() {
          @Override
          public ApplicationAttemptReport run() throws Exception {
            return historyManager.getApplicationAttempt(appAttemptId);
          }
        });
        if (callerUGI != null && callerUGI.getShortUserName().equals("user3")) {
          // The exception is expected
          fail();
        }
      } catch (AuthorizationException e) {
        if (callerUGI != null && callerUGI.getShortUserName().equals("user3")) {
          // The exception is expected
          return;
        }
        throw e;
      }
    }
    assertNotNull(appAttempt);
    assertEquals(appAttemptId, appAttempt.getApplicationAttemptId());
    assertEquals(ContainerId.newContainerId(appAttemptId, 1), appAttempt.getAMContainerId());
    assertEquals("test host", appAttempt.getHost());
    assertEquals(100, appAttempt.getRpcPort());
    assertEquals("test tracking url", appAttempt.getTrackingUrl());
    assertEquals("test original tracking url", appAttempt.getOriginalTrackingUrl());
    assertEquals("test diagnostics info", appAttempt.getDiagnostics());
    assertEquals(YarnApplicationAttemptState.FINISHED, appAttempt.getYarnApplicationAttemptState());
  }

  @MethodSource("callers")
  @ParameterizedTest
  void testGetContainerReport(String caller) throws Exception {
    initTestApplicationHistoryManagerOnTimelineStore(caller);
    final ContainerId containerId = ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 1), 1), 1);
    ContainerReport container;
    if (callerUGI == null) {
      container = historyManager.getContainer(containerId);
    } else {
      try {
        container = callerUGI.doAs(new PrivilegedExceptionAction<ContainerReport>() {
          @Override
          public ContainerReport run() throws Exception {
            return historyManager.getContainer(containerId);
          }
        });
        if (callerUGI != null && callerUGI.getShortUserName().equals("user3")) {
          // The exception is expected
          fail();
        }
      } catch (AuthorizationException e) {
        if (callerUGI != null && callerUGI.getShortUserName().equals("user3")) {
          // The exception is expected
          return;
        }
        throw e;
      }
    }
    assertNotNull(container);
    assertEquals(Integer.MAX_VALUE + 1L, container.getCreationTime());
    assertEquals(Integer.MAX_VALUE + 2L, container.getFinishTime());
    assertEquals(Resource.newInstance(-1, -1), container.getAllocatedResource());
    assertEquals(NodeId.newInstance("test host", 100), container.getAssignedNode());
    assertEquals(Priority.UNDEFINED, container.getPriority());
    assertEquals("test diagnostics info", container.getDiagnosticsInfo());
    assertEquals(ContainerState.COMPLETE, container.getContainerState());
    assertEquals(-1, container.getContainerExitStatus());
    assertEquals(
        "http://0.0.0.0:8188/applicationhistory/logs/" + "test host:100/container_0_0001_01_000001/"
            + "container_0_0001_01_000001/user1", container.getLogUrl());
  }

  @MethodSource("callers")
  @ParameterizedTest
  void testGetApplications(String caller) throws Exception {
    initTestApplicationHistoryManagerOnTimelineStore(caller);
    Collection<ApplicationReport> apps =
        historyManager.getApplications(Long.MAX_VALUE, 0L, Long.MAX_VALUE).values();
    assertNotNull(apps);
    assertEquals(SCALE + 2, apps.size());
    ApplicationId ignoredAppId = ApplicationId.newInstance(0, SCALE + 2);
    for (ApplicationReport app : apps) {
      assertNotEquals(ignoredAppId, app.getApplicationId());
    }

    // Get apps by given appStartedTime period
    apps = historyManager.getApplications(Long.MAX_VALUE, 2147483653L, Long.MAX_VALUE).values();
    assertNotNull(apps);
    assertEquals(2, apps.size());
  }

  @MethodSource("callers")
  @ParameterizedTest
  void testGetApplicationAttempts(String caller) throws Exception {
    initTestApplicationHistoryManagerOnTimelineStore(caller);
    final ApplicationId appId = ApplicationId.newInstance(0, 1);
    Collection<ApplicationAttemptReport> appAttempts;
    if (callerUGI == null) {
      appAttempts = historyManager.getApplicationAttempts(appId).values();
    } else {
      try {
        appAttempts =
            callerUGI.doAs(new PrivilegedExceptionAction<Collection<ApplicationAttemptReport>>() {
              @Override
              public Collection<ApplicationAttemptReport> run() throws Exception {
                return historyManager.getApplicationAttempts(appId).values();
              }
            });
        if (callerUGI != null && callerUGI.getShortUserName().equals("user3")) {
          // The exception is expected
          fail();
        }
      } catch (AuthorizationException e) {
        if (callerUGI != null && callerUGI.getShortUserName().equals("user3")) {
          // The exception is expected
          return;
        }
        throw e;
      }
    }
    assertNotNull(appAttempts);
    assertEquals(SCALE, appAttempts.size());
  }

  @MethodSource("callers")
  @ParameterizedTest
  void testGetContainers(String caller) throws Exception {
    initTestApplicationHistoryManagerOnTimelineStore(caller);
    final ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 1), 1);
    Collection<ContainerReport> containers;
    if (callerUGI == null) {
      containers = historyManager.getContainers(appAttemptId).values();
    } else {
      try {
        containers = callerUGI.doAs(new PrivilegedExceptionAction<Collection<ContainerReport>>() {
          @Override
          public Collection<ContainerReport> run() throws Exception {
            return historyManager.getContainers(appAttemptId).values();
          }
        });
        if (callerUGI != null && callerUGI.getShortUserName().equals("user3")) {
          // The exception is expected
          fail();
        }
      } catch (AuthorizationException e) {
        if (callerUGI != null && callerUGI.getShortUserName().equals("user3")) {
          // The exception is expected
          return;
        }
        throw e;
      }
    }
    assertNotNull(containers);
    assertEquals(SCALE, containers.size());
  }

  @MethodSource("callers")
  @ParameterizedTest
  void testGetAMContainer(String caller) throws Exception {
    initTestApplicationHistoryManagerOnTimelineStore(caller);
    final ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 1), 1);
    ContainerReport container;
    if (callerUGI == null) {
      container = historyManager.getAMContainer(appAttemptId);
    } else {
      try {
        container = callerUGI.doAs(new PrivilegedExceptionAction<ContainerReport>() {
          @Override
          public ContainerReport run() throws Exception {
            return historyManager.getAMContainer(appAttemptId);
          }
        });
        if (callerUGI != null && callerUGI.getShortUserName().equals("user3")) {
          // The exception is expected
          fail();
        }
      } catch (AuthorizationException e) {
        if (callerUGI != null && callerUGI.getShortUserName().equals("user3")) {
          // The exception is expected
          return;
        }
        throw e;
      }
    }
    assertNotNull(container);
    assertEquals(appAttemptId, container.getContainerId().getApplicationAttemptId());
  }

  private static TimelineEntity createApplicationTimelineEntity(ApplicationId appId,
      boolean emptyACLs, boolean noAttemptId, boolean wrongAppId, boolean enableUpdateEvent,
      YarnApplicationState state) {
    return createApplicationTimelineEntity(appId, emptyACLs, noAttemptId, wrongAppId,
        enableUpdateEvent, state, false, false);
  }

  private static TimelineEntity createApplicationTimelineEntity(ApplicationId appId,
      boolean emptyACLs, boolean noAttemptId, boolean wrongAppId, boolean enableUpdateEvent,
      YarnApplicationState state, boolean missingPreemptMetrics, boolean missingQueue) {
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityType(ApplicationMetricsConstants.ENTITY_TYPE);
    if (wrongAppId) {
      entity.setEntityId("wrong_app_id");
    } else {
      entity.setEntityId(appId.toString());
    }
    entity.setDomainId(TimelineDataManager.DEFAULT_DOMAIN_ID);
    entity.addPrimaryFilter(TimelineStore.SystemFilter.ENTITY_OWNER.toString(), "yarn");
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ApplicationMetricsConstants.NAME_ENTITY_INFO, "test app");
    entityInfo.put(ApplicationMetricsConstants.TYPE_ENTITY_INFO, "test app type");
    entityInfo.put(ApplicationMetricsConstants.USER_ENTITY_INFO, "user1");
    if (!missingQueue) {
      entityInfo.put(ApplicationMetricsConstants.QUEUE_ENTITY_INFO, "test queue");
    }
    entityInfo.put(ApplicationMetricsConstants.UNMANAGED_APPLICATION_ENTITY_INFO, "false");
    entityInfo.put(ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO, Priority.newInstance(0));
    entityInfo.put(ApplicationMetricsConstants.SUBMITTED_TIME_ENTITY_INFO, Integer.MAX_VALUE + 1L);
    entityInfo.put(ApplicationMetricsConstants.APP_MEM_METRICS, 123);
    entityInfo.put(ApplicationMetricsConstants.APP_CPU_METRICS, 345);
    if (!missingPreemptMetrics) {
      entityInfo.put(ApplicationMetricsConstants.APP_MEM_PREEMPT_METRICS, 456);
      entityInfo.put(ApplicationMetricsConstants.APP_CPU_PREEMPT_METRICS, 789);
    }
    if (emptyACLs) {
      entityInfo.put(ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO, "");
    } else {
      entityInfo.put(ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO, "user2");
    }
    Set<String> appTags = new HashSet<String>();
    appTags.add("Test_APP_TAGS_1");
    appTags.add("Test_APP_TAGS_2");
    entityInfo.put(ApplicationMetricsConstants.APP_TAGS_INFO, appTags);
    entity.setOtherInfo(entityInfo);
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    tEvent.setTimestamp(Integer.MAX_VALUE + 2L + appId.getId());
    entity.addEvent(tEvent);
    tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(Integer.MAX_VALUE + 3L + appId.getId());
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ApplicationMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO, "test diagnostics info");
    eventInfo.put(ApplicationMetricsConstants.FINAL_STATUS_EVENT_INFO,
        FinalApplicationStatus.UNDEFINED.toString());
    eventInfo.put(ApplicationMetricsConstants.STATE_EVENT_INFO, state.toString());
    if (!noAttemptId) {
      eventInfo.put(ApplicationMetricsConstants.LATEST_APP_ATTEMPT_EVENT_INFO,
          ApplicationAttemptId.newInstance(appId, 1));
    }
    tEvent.setEventInfo(eventInfo);
    entity.addEvent(tEvent);
    // send a YARN_APPLICATION_STATE_UPDATED event
    // after YARN_APPLICATION_FINISHED
    // The final YarnApplicationState should not be changed
    tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.STATE_UPDATED_EVENT_TYPE);
    tEvent.setTimestamp(Integer.MAX_VALUE + 4L + appId.getId());
    eventInfo = new HashMap<String, Object>();
    eventInfo.put(ApplicationMetricsConstants.STATE_EVENT_INFO, YarnApplicationState.KILLED);
    tEvent.setEventInfo(eventInfo);
    entity.addEvent(tEvent);
    if (enableUpdateEvent) {
      tEvent = new TimelineEvent();
      long updatedTimeIndex = 4L;
      createAppModifiedEvent(appId, tEvent, updatedTimeIndex++, "changed queue", 5);
      entity.addEvent(tEvent);
      // Change priority alone
      tEvent = new TimelineEvent();
      createAppModifiedEvent(appId, tEvent, updatedTimeIndex++, "changed queue", 6);
      // Now change queue
      tEvent = new TimelineEvent();
      createAppModifiedEvent(appId, tEvent, updatedTimeIndex++, "changed queue1", 6);
      entity.addEvent(tEvent);
    }
    return entity;
  }

  private static void createAppModifiedEvent(ApplicationId appId, TimelineEvent tEvent,
      long updatedTimeIndex, String queue, int priority) {
    tEvent.setEventType(ApplicationMetricsConstants.UPDATED_EVENT_TYPE);
    tEvent.setTimestamp(Integer.MAX_VALUE + updatedTimeIndex + appId.getId());
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ApplicationMetricsConstants.QUEUE_ENTITY_INFO, queue);
    eventInfo.put(ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO, priority);
    tEvent.setEventInfo(eventInfo);
  }

  private static TimelineEntity createAppAttemptTimelineEntity(ApplicationAttemptId appAttemptId) {
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityType(AppAttemptMetricsConstants.ENTITY_TYPE);
    entity.setEntityId(appAttemptId.toString());
    entity.setDomainId(TimelineDataManager.DEFAULT_DOMAIN_ID);
    entity.addPrimaryFilter(AppAttemptMetricsConstants.PARENT_PRIMARY_FILTER,
        appAttemptId.getApplicationId().toString());
    entity.addPrimaryFilter(TimelineStore.SystemFilter.ENTITY_OWNER.toString(), "yarn");
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(AppAttemptMetricsConstants.REGISTERED_EVENT_TYPE);
    tEvent.setTimestamp(Integer.MAX_VALUE + 1L);
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(AppAttemptMetricsConstants.TRACKING_URL_INFO, "test tracking url");
    eventInfo.put(AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_INFO,
        "test original tracking url");
    eventInfo.put(AppAttemptMetricsConstants.HOST_INFO, "test host");
    eventInfo.put(AppAttemptMetricsConstants.RPC_PORT_INFO, 100);
    eventInfo.put(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO,
        ContainerId.newContainerId(appAttemptId, 1));
    tEvent.setEventInfo(eventInfo);
    entity.addEvent(tEvent);
    tEvent = new TimelineEvent();
    tEvent.setEventType(AppAttemptMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(Integer.MAX_VALUE + 2L);
    eventInfo = new HashMap<String, Object>();
    eventInfo.put(AppAttemptMetricsConstants.TRACKING_URL_INFO, "test tracking url");
    eventInfo.put(AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_INFO,
        "test original tracking url");
    eventInfo.put(AppAttemptMetricsConstants.DIAGNOSTICS_INFO, "test diagnostics info");
    eventInfo.put(AppAttemptMetricsConstants.FINAL_STATUS_INFO,
        FinalApplicationStatus.UNDEFINED.toString());
    eventInfo.put(AppAttemptMetricsConstants.STATE_INFO,
        YarnApplicationAttemptState.FINISHED.toString());
    tEvent.setEventInfo(eventInfo);
    entity.addEvent(tEvent);
    return entity;
  }

  private static TimelineEntity createContainerEntity(ContainerId containerId) {
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityType(ContainerMetricsConstants.ENTITY_TYPE);
    entity.setEntityId(containerId.toString());
    entity.setDomainId(TimelineDataManager.DEFAULT_DOMAIN_ID);
    entity.addPrimaryFilter(ContainerMetricsConstants.PARENT_PRIMARIY_FILTER,
        containerId.getApplicationAttemptId().toString());
    entity.addPrimaryFilter(TimelineStore.SystemFilter.ENTITY_OWNER.toString(), "yarn");
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_MEMORY_INFO, -1);
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_VCORE_INFO, -1);
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_HOST_INFO, "test host");
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_PORT_INFO, 100);
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_PRIORITY_INFO, -1);
    entityInfo.put(ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_INFO, "http://test:1234");
    entity.setOtherInfo(entityInfo);
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ContainerMetricsConstants.CREATED_EVENT_TYPE);
    tEvent.setTimestamp(Integer.MAX_VALUE + 1L);
    entity.addEvent(tEvent);
    tEvent = new TimelineEvent();
    tEvent.setEventType(ContainerMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(Integer.MAX_VALUE + 2L);
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ContainerMetricsConstants.DIAGNOSTICS_INFO, "test diagnostics info");
    eventInfo.put(ContainerMetricsConstants.EXIT_STATUS_INFO, -1);
    eventInfo.put(ContainerMetricsConstants.STATE_INFO, ContainerState.COMPLETE.toString());
    tEvent.setEventInfo(eventInfo);
    entity.addEvent(tEvent);
    return entity;
  }
}