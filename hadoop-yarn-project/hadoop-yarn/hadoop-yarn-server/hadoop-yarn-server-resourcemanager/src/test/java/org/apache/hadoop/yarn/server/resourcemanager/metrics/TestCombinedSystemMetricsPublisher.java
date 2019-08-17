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

package org.apache.hadoop.yarn.server.resourcemanager.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.timelineservice.RMTimelineCollectorManager;
import org.apache.hadoop.yarn.server.timeline.MemoryTimelineStore;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.timeline.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timeline.recovery.MemoryTimelineStateStore;
import org.apache.hadoop.yarn.server.timeline.recovery.TimelineStateStore;
import org.apache.hadoop.yarn.server.timelineservice.collector.AppLevelTimelineCollector;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineWriterImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter;
import org.apache.hadoop.yarn.util.TimelineServiceHelper;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests that a CombinedSystemMetricsPublisher publishes metrics for timeline
 * services (v1/v2) as specified by the configuration.
 */
public class TestCombinedSystemMetricsPublisher {
  /**
    * The folder where the FileSystemTimelineWriterImpl writes the entities.
    */
  private static File testRootDir = new File("target",
      TestCombinedSystemMetricsPublisher.class.getName() + "-localDir")
      .getAbsoluteFile();

  private static ApplicationHistoryServer timelineServer;
  private static CombinedSystemMetricsPublisher metricsPublisher;
  private static TimelineStore store;
  private static ConcurrentMap<ApplicationId, RMApp> rmAppsMapInContext;
  private static RMTimelineCollectorManager rmTimelineCollectorManager;
  private static DrainDispatcher dispatcher;
  private static YarnConfiguration conf;
  private static TimelineServiceV1Publisher publisherV1;
  private static TimelineServiceV2Publisher publisherV2;
  private static ApplicationAttemptId appAttemptId;
  private static RMApp app;

  private void testSetup(boolean enableV1, boolean enableV2) throws Exception {

    if (testRootDir.exists()) {
      //cleanup before hand
      FileContext.getLocalFSFileContext().delete(
              new Path(testRootDir.getAbsolutePath()), true);
    }

    conf = getConf(enableV1, enableV2);

    RMContext rmContext = mock(RMContext.class);
    rmAppsMapInContext = new ConcurrentHashMap<ApplicationId, RMApp>();
    when(rmContext.getRMApps()).thenReturn(rmAppsMapInContext);
    ResourceManager rm = mock(ResourceManager.class);
    when(rm.getRMContext()).thenReturn(rmContext);

    if (enableV2) {
      dispatcher = new DrainDispatcher();
      rmTimelineCollectorManager = new RMTimelineCollectorManager(rm);
      when(rmContext.getRMTimelineCollectorManager()).thenReturn(
          rmTimelineCollectorManager);

      rmTimelineCollectorManager.init(conf);
      rmTimelineCollectorManager.start();
    } else {
      dispatcher = null;
      rmTimelineCollectorManager = null;
    }

    timelineServer = new ApplicationHistoryServer();
    timelineServer.init(conf);
    timelineServer.start();
    store = timelineServer.getTimelineStore();

    if (enableV2) {
      dispatcher.init(conf);
      dispatcher.start();
    }

    List<SystemMetricsPublisher> publishers =
        new ArrayList<SystemMetricsPublisher>();

    if (YarnConfiguration.timelineServiceV1Enabled(conf)) {
      Assert.assertTrue(enableV1);
      publisherV1 = new TimelineServiceV1Publisher();
      publishers.add(publisherV1);
      publisherV1.init(conf);
      publisherV1.start();
    } else {
      Assert.assertFalse(enableV1);
      publisherV1 = null;
    }

    if (YarnConfiguration.timelineServiceV2Enabled(conf)) {
      Assert.assertTrue(enableV2);
      publisherV2 = new TimelineServiceV2Publisher(
          rmTimelineCollectorManager) {
        @Override
        protected Dispatcher getDispatcher() {
          return dispatcher;
        }
      };
      publishers.add(publisherV2);
      publisherV2.init(conf);
      publisherV2.start();
    } else {
      Assert.assertFalse(enableV2);
      publisherV2 = null;
    }

    if (publishers.isEmpty()) {
      NoOpSystemMetricPublisher noopPublisher =
          new NoOpSystemMetricPublisher();
      publishers.add(noopPublisher);
    }

    metricsPublisher = new CombinedSystemMetricsPublisher(publishers);
  }

  private void testCleanup() throws Exception {
    if (publisherV1 != null) {
      publisherV1.stop();
    }
    if (publisherV2 != null) {
      publisherV2.stop();
    }
    if (timelineServer != null) {
      timelineServer.stop();
    }
    if (testRootDir.exists()) {
      FileContext.getLocalFSFileContext().delete(
          new Path(testRootDir.getAbsolutePath()), true);
    }
    if (rmTimelineCollectorManager != null) {
      rmTimelineCollectorManager.stop();
    }
  }

  private static YarnConfiguration getConf(boolean v1Enabled,
      boolean v2Enabled) {
    YarnConfiguration yarnConf = new YarnConfiguration();

    if (v1Enabled || v2Enabled) {
      yarnConf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    } else {
      yarnConf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, false);
    }

    if (v1Enabled) {
      yarnConf.set(YarnConfiguration.TIMELINE_SERVICE_VERSION, "1.0");
      yarnConf.setClass(YarnConfiguration.TIMELINE_SERVICE_STORE,
          MemoryTimelineStore.class, TimelineStore.class);
      yarnConf.setClass(YarnConfiguration.TIMELINE_SERVICE_STATE_STORE_CLASS,
          MemoryTimelineStateStore.class, TimelineStateStore.class);
    }

    if (v2Enabled) {
      yarnConf.set(YarnConfiguration.TIMELINE_SERVICE_VERSION, "2.0");
      yarnConf.setBoolean(YarnConfiguration.SYSTEM_METRICS_PUBLISHER_ENABLED,
          true);
      yarnConf.setBoolean(
          YarnConfiguration.RM_PUBLISH_CONTAINER_EVENTS_ENABLED, true);
      yarnConf.setClass(YarnConfiguration.TIMELINE_SERVICE_WRITER_CLASS,
          FileSystemTimelineWriterImpl.class, TimelineWriter.class);

      try {
        yarnConf.set(
            FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_DIR_ROOT,
                testRootDir.getCanonicalPath());
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail("Exception while setting the " +
            "TIMELINE_SERVICE_STORAGE_DIR_ROOT ");
      }
    }

    if (v1Enabled && v2Enabled) {
      yarnConf.set(YarnConfiguration.TIMELINE_SERVICE_VERSION, "1.0");
      yarnConf.set(YarnConfiguration.TIMELINE_SERVICE_VERSIONS, "1.0,2.0f");
    }

    yarnConf.setInt(
        YarnConfiguration.RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE, 2);

    return yarnConf;
  }

  // runs test to validate timeline events are published if and only if the
  // service is enabled for v1 and v2 (independently).
  private void runTest(boolean v1Enabled, boolean v2Enabled) throws Exception {
    testSetup(v1Enabled, v2Enabled);
    publishEvents(v1Enabled, v2Enabled);
    validateV1(v1Enabled);
    validateV2(v2Enabled);
    testCleanup();
  }

  @Test(timeout = 10000)
  public void testTimelineServiceEventPublishingV1V2Enabled()
      throws Exception {
    runTest(true, true);
  }

  @Test(timeout = 10000)
  public void testTimelineServiceEventPublishingV1Enabled() throws Exception {
    runTest(true, false);
  }

  @Test(timeout = 10000)
  public void testTimelineServiceEventPublishingV2Enabled() throws Exception {
    runTest(false, true);
  }

  @Test(timeout = 10000)
  public void testTimelineServiceEventPublishingNoService() throws Exception {
    runTest(false, false);
  }

  @Test(timeout = 10000)
  public void testTimelineServiceConfiguration()
      throws Exception {
    Configuration config = new Configuration(false);
    config.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    config.set(YarnConfiguration.TIMELINE_SERVICE_VERSIONS, "2.0,1.5");
    config.set(YarnConfiguration.TIMELINE_SERVICE_VERSION, "2.0");

    Assert.assertTrue(YarnConfiguration.timelineServiceV2Enabled(config));
    Assert.assertTrue(YarnConfiguration.timelineServiceV15Enabled(config));
    Assert.assertTrue(YarnConfiguration.timelineServiceV1Enabled(config));

    config.set(YarnConfiguration.TIMELINE_SERVICE_VERSIONS, "2.0,1");
    config.set(YarnConfiguration.TIMELINE_SERVICE_VERSION, "1.5");
    Assert.assertTrue(YarnConfiguration.timelineServiceV2Enabled(config));
    Assert.assertFalse(YarnConfiguration.timelineServiceV15Enabled(config));
    Assert.assertTrue(YarnConfiguration.timelineServiceV1Enabled(config));

    config.set(YarnConfiguration.TIMELINE_SERVICE_VERSIONS, "2.0");
    config.set(YarnConfiguration.TIMELINE_SERVICE_VERSION, "1.5");
    Assert.assertTrue(YarnConfiguration.timelineServiceV2Enabled(config));
    Assert.assertFalse(YarnConfiguration.timelineServiceV15Enabled(config));
    Assert.assertFalse(YarnConfiguration.timelineServiceV1Enabled(config));
  }

  private void publishEvents(boolean v1Enabled, boolean v2Enabled) {
    long timestamp = (v1Enabled) ? 1 : 2;
    int id = (v2Enabled) ? 3 : 4;
    ApplicationId appId = ApplicationId.newInstance(timestamp, id);

    app = createRMApp(appId);
    rmAppsMapInContext.putIfAbsent(appId, app);

    if (v2Enabled) {
      AppLevelTimelineCollector collector =
          new AppLevelTimelineCollector(appId);
      rmTimelineCollectorManager.putIfAbsent(appId, collector);
    }
    appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    RMAppAttempt appAttempt = createRMAppAttempt(true);

    metricsPublisher.appAttemptRegistered(appAttempt, Integer.MAX_VALUE + 1L);
    metricsPublisher.appAttemptFinished(appAttempt, RMAppAttemptState.FINISHED,
        app, Integer.MAX_VALUE + 2L);
    if (v2Enabled) {
      dispatcher.await();
    }
  }

  private void validateV1(boolean v1Enabled) throws Exception {
    TimelineEntity entity = null;

    if (!v1Enabled) {
      Thread.sleep(1000);
      entity =
          store.getEntity(appAttemptId.toString(),
              AppAttemptMetricsConstants.ENTITY_TYPE,
              EnumSet.allOf(Field.class));
      Assert.assertNull(entity);
      return;
    }

    do {
      entity =
          store.getEntity(appAttemptId.toString(),
              AppAttemptMetricsConstants.ENTITY_TYPE,
              EnumSet.allOf(Field.class));
      Thread.sleep(100);
      // ensure two events are both published before leaving the loop
    } while (entity == null || entity.getEvents().size() < 2);

    boolean hasRegisteredEvent = false;
    boolean hasFinishedEvent = false;
    for (org.apache.hadoop.yarn.api.records.timeline.TimelineEvent event :
        entity.getEvents()) {
      if (event.getEventType().equals(
          AppAttemptMetricsConstants.REGISTERED_EVENT_TYPE)) {
        hasRegisteredEvent = true;
      } else if (event.getEventType().equals(
          AppAttemptMetricsConstants.FINISHED_EVENT_TYPE)) {
        hasFinishedEvent = true;
        Assert.assertEquals(
            FinalApplicationStatus.UNDEFINED.toString(),
            event.getEventInfo().get(
                AppAttemptMetricsConstants.FINAL_STATUS_INFO));
        Assert.assertEquals(
            YarnApplicationAttemptState.FINISHED.toString(),
            event.getEventInfo().get(
                AppAttemptMetricsConstants.STATE_INFO));
      }
      Assert
      .assertEquals(appAttemptId.toString(), entity.getEntityId());
    }
    Assert.assertTrue(hasRegisteredEvent && hasFinishedEvent);
  }

  private void validateV2(boolean v2Enabled) throws Exception {
    String outputDirApp =
        getTimelineEntityDir() + "/"
            + TimelineEntityType.YARN_APPLICATION_ATTEMPT + "/";

    File entityFolder = new File(outputDirApp);
    Assert.assertEquals(v2Enabled, entityFolder.isDirectory());

    if (v2Enabled) {
      String timelineServiceFileName = appAttemptId.toString()
          + FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;
      File entityFile = new File(outputDirApp, timelineServiceFileName);
      Assert.assertTrue(entityFile.exists());
      long idPrefix = TimelineServiceHelper
          .invertLong(appAttemptId.getAttemptId());
      verifyEntity(entityFile, 2,
          AppAttemptMetricsConstants.REGISTERED_EVENT_TYPE, 0, idPrefix);
    }
  }

  private void verifyEntity(File entityFile, long expectedEvents,
      String eventForCreatedTime, long expectedMetrics, long idPrefix)
          throws IOException {

    BufferedReader reader = null;
    String strLine;
    long count = 0;
    long metricsCount = 0;
    try {
      reader = new BufferedReader(new FileReader(entityFile));
      while ((strLine = reader.readLine()) != null) {
        if (strLine.trim().length() > 0) {
          org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity
              entity = FileSystemTimelineReaderImpl
              .getTimelineRecordFromJSON(strLine.trim(),
                  org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity.class);
          metricsCount = entity.getMetrics().size();
          assertEquals(idPrefix, entity.getIdPrefix());
          for (TimelineEvent event : entity.getEvents()) {
            if (event.getId().equals(eventForCreatedTime)) {
              assertTrue(entity.getCreatedTime() > 0);
              break;
            }
          }
          count++;
        }
      }
    } finally {
      reader.close();
    }
    assertEquals("Expected " + expectedEvents + " events to be published",
        expectedEvents, count);
    assertEquals("Expected " + expectedMetrics + " metrics is incorrect",
        expectedMetrics, metricsCount);
  }

  private String getTimelineEntityDir() {
    String outputDirApp =
        testRootDir.getAbsolutePath() + "/"
            + FileSystemTimelineWriterImpl.ENTITIES_DIR + "/"
            + YarnConfiguration.DEFAULT_RM_CLUSTER_ID + "/"
            + app.getUser() + "/"
            + app.getName() + "/"
            + TimelineUtils.DEFAULT_FLOW_VERSION + "/"
            + app.getStartTime() + "/"
            + app.getApplicationId();
    return outputDirApp;
  }

  private static RMAppAttempt createRMAppAttempt(boolean unmanagedAMAttempt) {
    RMAppAttempt appAttempt = mock(RMAppAttempt.class);
    when(appAttempt.getAppAttemptId()).thenReturn(appAttemptId);
    when(appAttempt.getHost()).thenReturn("test host");
    when(appAttempt.getRpcPort()).thenReturn(-100);
    if (!unmanagedAMAttempt) {
      Container container = mock(Container.class);
      when(container.getId())
          .thenReturn(ContainerId.newContainerId(appAttemptId, 1));
      when(appAttempt.getMasterContainer()).thenReturn(container);
    }
    when(appAttempt.getDiagnostics()).thenReturn("test diagnostics info");
    when(appAttempt.getTrackingUrl()).thenReturn("test tracking url");
    when(appAttempt.getOriginalTrackingUrl()).thenReturn(
        "test original tracking url");
    return appAttempt;
  }

  private static RMApp createRMApp(ApplicationId appId) {
    RMApp rmApp = mock(RMAppImpl.class);
    when(rmApp.getApplicationId()).thenReturn(appId);
    when(rmApp.getName()).thenReturn("test app");
    when(rmApp.getApplicationType()).thenReturn("test app type");
    when(rmApp.getUser()).thenReturn("testUser");
    when(rmApp.getQueue()).thenReturn("test queue");
    when(rmApp.getSubmitTime()).thenReturn(Integer.MAX_VALUE + 1L);
    when(rmApp.getStartTime()).thenReturn(Integer.MAX_VALUE + 2L);
    when(rmApp.getFinishTime()).thenReturn(Integer.MAX_VALUE + 3L);
    when(rmApp.getDiagnostics()).thenReturn(
        new StringBuilder("test diagnostics info"));
    RMAppAttempt appAttempt = mock(RMAppAttempt.class);
    when(appAttempt.getAppAttemptId()).thenReturn(
        ApplicationAttemptId.newInstance(appId, 1));
    when(rmApp.getCurrentAppAttempt()).thenReturn(appAttempt);
    when(rmApp.getFinalApplicationStatus()).thenReturn(
        FinalApplicationStatus.UNDEFINED);
    Map<String, Long> resourceMap = new HashMap<>();
    resourceMap
        .put(ResourceInformation.MEMORY_MB.getName(), (long) Integer.MAX_VALUE);
    resourceMap.put(ResourceInformation.VCORES.getName(), Long.MAX_VALUE);
    Map<String, Long> preemptedMap = new HashMap<>();
    preemptedMap
        .put(ResourceInformation.MEMORY_MB.getName(), (long) Integer.MAX_VALUE);
    preemptedMap.put(ResourceInformation.VCORES.getName(), Long.MAX_VALUE);
    when(rmApp.getRMAppMetrics()).thenReturn(
        new RMAppMetrics(Resource.newInstance(0, 0), 0, 0, resourceMap,
            preemptedMap));
    when(rmApp.getApplicationTags()).thenReturn(
        Collections.<String> emptySet());
    ApplicationSubmissionContext appSubmissionContext =
        mock(ApplicationSubmissionContext.class);
    when(appSubmissionContext.getPriority())
        .thenReturn(Priority.newInstance(0));

    ContainerLaunchContext containerLaunchContext =
        mock(ContainerLaunchContext.class);
    when(containerLaunchContext.getCommands())
        .thenReturn(Collections.singletonList("java -Xmx1024m"));
    when(appSubmissionContext.getAMContainerSpec())
        .thenReturn(containerLaunchContext);
    when(rmApp.getApplicationPriority()).thenReturn(Priority.newInstance(10));
    when(rmApp.getApplicationSubmissionContext())
        .thenReturn(appSubmissionContext);
    return rmApp;
  }
}

