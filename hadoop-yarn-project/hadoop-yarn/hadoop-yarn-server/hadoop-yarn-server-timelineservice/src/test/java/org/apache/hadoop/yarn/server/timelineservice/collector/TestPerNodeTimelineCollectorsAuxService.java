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

package org.apache.hadoop.yarn.server.timelineservice.collector;

import java.io.IOException;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocol;
import org.apache.hadoop.yarn.server.api.ContainerInitializationContext;
import org.apache.hadoop.yarn.server.api.ContainerTerminationContext;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextResponse;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineWriterImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestPerNodeTimelineCollectorsAuxService {
  private ApplicationAttemptId appAttemptId;
  private PerNodeTimelineCollectorsAuxService auxService;
  private Configuration conf;
  private ApplicationId appId;

  public TestPerNodeTimelineCollectorsAuxService() {
    appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    conf = new YarnConfiguration();
    // enable timeline service v.2
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
    conf.setClass(YarnConfiguration.TIMELINE_SERVICE_WRITER_CLASS,
        FileSystemTimelineWriterImpl.class, TimelineWriter.class);
    conf.setLong(YarnConfiguration.ATS_APP_COLLECTOR_LINGER_PERIOD_IN_MS,
        1000L);
  }

  @AfterEach
  public void tearDown() throws Shell.ExitCodeException {
    if (auxService != null) {
      auxService.stop();
    }
  }

  @Test
  void testAddApplication() throws Exception {
    auxService = createCollectorAndAddApplication();
    // auxService should have a single app
    assertTrue(auxService.hasApplication(appAttemptId.getApplicationId()));
    auxService.close();
  }

  @Test
  void testAddApplicationNonAMContainer() throws Exception {
    auxService = createCollector();

    ContainerId containerId = getContainerId(2L); // not an AM
    ContainerInitializationContext context =
        mock(ContainerInitializationContext.class);
    when(context.getContainerId()).thenReturn(containerId);
    auxService.initializeContainer(context);
    // auxService should not have that app
    assertFalse(auxService.hasApplication(appAttemptId.getApplicationId()));
  }

  @Test
  void testRemoveApplication() throws Exception {
    auxService = createCollectorAndAddApplication();
    // auxService should have a single app
    assertTrue(auxService.hasApplication(appAttemptId.getApplicationId()));

    ContainerId containerId = getAMContainerId();
    ContainerTerminationContext context =
        mock(ContainerTerminationContext.class);
    when(context.getContainerId()).thenReturn(containerId);
    when(context.getContainerType()).thenReturn(
        ContainerType.APPLICATION_MASTER);
    auxService.stopContainer(context);

    // auxService should not have that app
    assertFalse(auxService.hasApplication(appAttemptId.getApplicationId()));
    auxService.close();
  }

  @Test
  void testRemoveApplicationNonAMContainer() throws Exception {
    auxService = createCollectorAndAddApplication();
    // auxService should have a single app
    assertTrue(auxService.hasApplication(appAttemptId.getApplicationId()));

    ContainerId containerId = getContainerId(2L); // not an AM
    ContainerTerminationContext context =
        mock(ContainerTerminationContext.class);
    when(context.getContainerId()).thenReturn(containerId);
    auxService.stopContainer(context);
    // auxService should still have that app
    assertTrue(auxService.hasApplication(appAttemptId.getApplicationId()));
    auxService.close();
  }

  @Test
  @Timeout(60000)
  void testLaunch() throws Exception {
    ExitUtil.disableSystemExit();
    try {
      auxService =
          PerNodeTimelineCollectorsAuxService.launchServer(new String[0],
              createCollectorManager(), conf);
    } catch (ExitUtil.ExitException e) {
      assertEquals(0, e.status);
      ExitUtil.resetFirstExitException();
      fail();
    }
  }

  private PerNodeTimelineCollectorsAuxService
      createCollectorAndAddApplication() {
    PerNodeTimelineCollectorsAuxService service = createCollector();

    ContainerInitializationContext context =
        createContainerInitalizationContext(1);
    service.initializeContainer(context);
    return service;
  }

  ContainerInitializationContext createContainerInitalizationContext(
      int attempt) {
    appAttemptId = ApplicationAttemptId.newInstance(appId, attempt);
    // create an AM container
    ContainerId containerId = getAMContainerId();
    ContainerInitializationContext context =
        mock(ContainerInitializationContext.class);
    when(context.getContainerId()).thenReturn(containerId);
    when(context.getContainerType())
        .thenReturn(ContainerType.APPLICATION_MASTER);
    return context;
  }

  ContainerTerminationContext createContainerTerminationContext(int attempt) {
    appAttemptId = ApplicationAttemptId.newInstance(appId, attempt);
    // create an AM container
    ContainerId containerId = getAMContainerId();
    ContainerTerminationContext context =
        mock(ContainerTerminationContext.class);
    when(context.getContainerId()).thenReturn(containerId);
    when(context.getContainerType())
        .thenReturn(ContainerType.APPLICATION_MASTER);
    return context;
  }

  private PerNodeTimelineCollectorsAuxService createCollector() {
    NodeTimelineCollectorManager collectorManager = createCollectorManager();
    PerNodeTimelineCollectorsAuxService service =
        spy(new PerNodeTimelineCollectorsAuxService(collectorManager) {
          @Override
          protected Future removeApplicationCollector(ContainerId containerId) {
            Future future = super.removeApplicationCollector(containerId);
            try {
              future.get();
            } catch (Exception e) {
              fail("Expeption thrown while removing collector");
            }
            return future;
          }
        });
    service.init(conf);
    service.start();
    return service;
  }

  private NodeTimelineCollectorManager createCollectorManager() {
    NodeTimelineCollectorManager collectorManager =
        spy(new NodeTimelineCollectorManager());
    doReturn(new Configuration()).when(collectorManager).getConfig();
    CollectorNodemanagerProtocol nmCollectorService =
        mock(CollectorNodemanagerProtocol.class);
    GetTimelineCollectorContextResponse response =
        GetTimelineCollectorContextResponse.newInstance(null, null, null, 0L);
    try {
      when(nmCollectorService.getTimelineCollectorContext(any(
          GetTimelineCollectorContextRequest.class))).thenReturn(response);
    } catch (YarnException | IOException e) {
      fail();
    }
    doReturn(nmCollectorService).when(collectorManager).getNMCollectorService();
    return collectorManager;
  }

  private ContainerId getAMContainerId() {
    return getContainerId(1L);
  }

  private ContainerId getContainerId(long id) {
    return ContainerId.newContainerId(appAttemptId, id);
  }

  @Test
  @Timeout(60000)
  void testRemoveAppWhenSecondAttemptAMCotainerIsLaunchedSameNode()
      throws Exception {
    // add first attempt collector
    auxService = createCollectorAndAddApplication();
    // auxService should have a single app
    assertTrue(auxService.hasApplication(appAttemptId.getApplicationId()));

    // add second attempt collector before first attempt master container stop
    ContainerInitializationContext containerInitalizationContext =
        createContainerInitalizationContext(2);
    auxService.initializeContainer(containerInitalizationContext);

    assertTrue(auxService.hasApplication(appAttemptId.getApplicationId()),
        "Applicatin not found in collectors.");

    // first attempt stop container
    ContainerTerminationContext context = createContainerTerminationContext(1);
    auxService.stopContainer(context);

    // 2nd attempt container removed, still collector should hold application id
    assertTrue(auxService.hasApplication(appAttemptId.getApplicationId()),
        "collector has removed application though 2nd attempt"
            + " is running this node");

    // second attempt stop container
    context = createContainerTerminationContext(2);
    auxService.stopContainer(context);

    // auxService should not have that app
    assertFalse(auxService.hasApplication(appAttemptId.getApplicationId()),
        "Application is not removed from collector");
    auxService.close();
  }

}
