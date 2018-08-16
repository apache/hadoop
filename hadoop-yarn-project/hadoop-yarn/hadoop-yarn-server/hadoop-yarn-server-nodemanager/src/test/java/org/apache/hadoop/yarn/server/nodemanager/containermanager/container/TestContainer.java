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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.refEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.atLeastOnce;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryPolicy;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.ContainerStateTransitionListener;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.DockerContainerDeletionMatcher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationCleanupEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorEventType;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.ContainerScheduler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.ContainerSchedulerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.ContainerSchedulerEventType;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

public class TestContainer {

  final NodeManagerMetrics metrics = NodeManagerMetrics.create();
  final Configuration conf = new YarnConfiguration();
  final String FAKE_LOCALIZATION_ERROR = "Fake localization error";

  /**
   * Verify correct container request events sent to localizer.
   */
  @Test
  public void testLocalizationRequest() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(7, 314159265358979L, 4344, "yak");
      assertEquals(ContainerState.NEW, wc.c.getContainerState());
      wc.initContainer();

      // Verify request for public/private resources to localizer
      ResourcesRequestedMatcher matchesReq =
          new ResourcesRequestedMatcher(wc.localResources, EnumSet.of(
              LocalResourceVisibility.PUBLIC, LocalResourceVisibility.PRIVATE,
              LocalResourceVisibility.APPLICATION));
      verify(wc.localizerBus).handle(argThat(matchesReq));
      assertEquals(ContainerState.LOCALIZING, wc.c.getContainerState());
    }
    finally {
      if (wc != null) {
        wc.finished();
      }
    } 
  }

  /**
   * Verify container launch when all resources already cached.
   */
  @Test
  public void testLocalizationLaunch() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(8, 314159265358979L, 4344, "yak");
      assertEquals(ContainerState.NEW, wc.c.getContainerState());
      wc.initContainer();
      Map<Path, List<String>> localPaths = wc.localizeResources();

      // all resources should be localized
      assertEquals(ContainerState.SCHEDULED, wc.c.getContainerState());
      assertNotNull(wc.c.getLocalizedResources());
      for (Entry<Path, List<String>> loc : wc.c.getLocalizedResources()
          .entrySet()) {
        assertEquals(localPaths.remove(loc.getKey()), loc.getValue());
      }
      assertTrue(localPaths.isEmpty());

      final WrappedContainer wcf = wc;
      // verify container launch
      ArgumentMatcher<ContainersLauncherEvent> matchesContainerLaunch =
        new ArgumentMatcher<ContainersLauncherEvent>() {
          @Override
          public boolean matches(Object o) {
            ContainersLauncherEvent launchEvent = (ContainersLauncherEvent) o;
            return wcf.c == launchEvent.getContainer();
          }
        };
      verify(wc.launcherBus).handle(argThat(matchesContainerLaunch));
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked") // mocked generic
  public void testExternalKill() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(13, 314159265358979L, 4344, "yak");
      wc.initContainer();
      wc.localizeResources();
      int running = metrics.getRunningContainers();
      wc.launchContainer();
      assertEquals(running + 1, metrics.getRunningContainers());
      reset(wc.localizerBus);
      wc.containerKilledOnRequest();
      assertEquals(ContainerState.EXITED_WITH_FAILURE, 
          wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
      int failed = metrics.getFailedContainers();
      wc.containerResourcesCleanup();
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      assertEquals(failed + 1, metrics.getFailedContainers());
      assertEquals(running, metrics.getRunningContainers());
    }
    finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked") // mocked generic
  public void testDockerContainerExternalKill() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(13, 314159265358979L, 4344, "yak");
      wc.setupDockerContainerEnv();
      wc.initContainer();
      wc.localizeResources();
      int running = metrics.getRunningContainers();
      wc.launchContainer();
      assertEquals(running + 1, metrics.getRunningContainers());
      reset(wc.localizerBus);
      wc.containerKilledOnRequest();
      assertEquals(ContainerState.EXITED_WITH_FAILURE,
          wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
      int failed = metrics.getFailedContainers();
      wc.dockerContainerResourcesCleanup();
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      assertEquals(failed + 1, metrics.getFailedContainers());
      assertEquals(running, metrics.getRunningContainers());
    }
    finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked") // mocked generic
  public void testContainerPauseAndResume() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(13, 314159265358979L, 4344, "yak");
      wc.initContainer();
      wc.localizeResources();
      int running = metrics.getRunningContainers();
      wc.launchContainer();
      assertEquals(running + 1, metrics.getRunningContainers());
      reset(wc.localizerBus);
      wc.pauseContainer();
      assertEquals(ContainerState.PAUSED,
          wc.c.getContainerState());
      wc.resumeContainer();
      assertEquals(ContainerState.RUNNING,
          wc.c.getContainerState());
      wc.containerKilledOnRequest();
      assertEquals(ContainerState.EXITED_WITH_FAILURE,
          wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
      int failed = metrics.getFailedContainers();
      wc.containerResourcesCleanup();
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      assertEquals(failed + 1, metrics.getFailedContainers());
      assertEquals(running, metrics.getRunningContainers());
    }
    finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked") // mocked generic
  public void testCleanupOnFailure() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(10, 314159265358979L, 4344, "yak");
      wc.initContainer();
      wc.localizeResources();
      wc.launchContainer();
      reset(wc.localizerBus);
      wc.containerFailed(ExitCode.FORCE_KILLED.getExitCode());
      assertEquals(ContainerState.EXITED_WITH_FAILURE, 
          wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
    }
    finally {
      if (wc != null) {
        wc.finished();
      }
    } 
  }

  @Test
  @SuppressWarnings("unchecked") // mocked generic
  public void testDockerContainerCleanupOnFailure() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(10, 314159265358979L, 4344, "yak");
      wc.setupDockerContainerEnv();
      wc.initContainer();
      wc.localizeResources();
      wc.launchContainer();
      reset(wc.localizerBus);
      wc.containerFailed(ExitCode.FORCE_KILLED.getExitCode());
      assertEquals(ContainerState.EXITED_WITH_FAILURE,
          wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
      wc.dockerContainerResourcesCleanup();
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }
  
  @Test
  @SuppressWarnings("unchecked") // mocked generic
  public void testCleanupOnSuccess() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(11, 314159265358979L, 4344, "yak");
      wc.initContainer();
      wc.localizeResources();
      int running = metrics.getRunningContainers();
      wc.launchContainer();
      assertEquals(running + 1, metrics.getRunningContainers());
      reset(wc.localizerBus);
      wc.containerSuccessful();
      assertEquals(ContainerState.EXITED_WITH_SUCCESS,
          wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
      int completed = metrics.getCompletedContainers();
      wc.containerResourcesCleanup();
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      assertEquals(completed + 1, metrics.getCompletedContainers());
      assertEquals(running, metrics.getRunningContainers());

      ContainerEventType e1 = wc.initStateToEvent.get(ContainerState.NEW);
      ContainerState s2 = wc.eventToFinalState.get(e1);
      ContainerEventType e2 = wc.initStateToEvent.get(s2);
      ContainerState s3 = wc.eventToFinalState.get(e2);
      ContainerEventType e3 = wc.initStateToEvent.get(s3);
      ContainerState s4 = wc.eventToFinalState.get(e3);
      ContainerEventType e4 = wc.initStateToEvent.get(s4);
      ContainerState s5 = wc.eventToFinalState.get(e4);
      ContainerEventType e5 = wc.initStateToEvent.get(s5);
      ContainerState s6 = wc.eventToFinalState.get(e5);

      Assert.assertEquals(ContainerState.LOCALIZING, s2);
      Assert.assertEquals(ContainerState.SCHEDULED, s3);
      Assert.assertEquals(ContainerState.RUNNING, s4);
      Assert.assertEquals(ContainerState.EXITED_WITH_SUCCESS, s5);
      Assert.assertEquals(ContainerState.DONE, s6);

      Assert.assertEquals(ContainerEventType.INIT_CONTAINER, e1);
      Assert.assertEquals(ContainerEventType.RESOURCE_LOCALIZED, e2);
      Assert.assertEquals(ContainerEventType.CONTAINER_LAUNCHED, e3);
      Assert.assertEquals(ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS, e4);
      Assert.assertEquals(ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP, e5);
    }
    finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked") // mocked generic
  public void testDockerContainerCleanupOnSuccess() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(11, 314159265358979L, 4344, "yak");
      wc.setupDockerContainerEnv();
      wc.initContainer();
      wc.localizeResources();
      int running = metrics.getRunningContainers();
      wc.launchContainer();
      assertEquals(running + 1, metrics.getRunningContainers());
      reset(wc.localizerBus);
      wc.containerSuccessful();
      assertEquals(ContainerState.EXITED_WITH_SUCCESS,
          wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
      int completed = metrics.getCompletedContainers();
      wc.dockerContainerResourcesCleanup();
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      assertEquals(completed + 1, metrics.getCompletedContainers());
      assertEquals(running, metrics.getRunningContainers());
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked") // mocked generic
  public void testInitWhileDone() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(6, 314159265358979L, 4344, "yak");
      wc.initContainer();
      wc.localizeResources();
      wc.launchContainer();
      reset(wc.localizerBus);
      wc.containerSuccessful();
      wc.containerResourcesCleanup();
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      verifyOutofBandHeartBeat(wc);
      assertNull(wc.c.getLocalizedResources());
      // Now in DONE, issue INIT
      wc.initContainer();
      // Verify still in DONE
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked") // mocked generic
  public void testDockerContainerInitWhileDone() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(6, 314159265358979L, 4344, "yak");
      wc.setupDockerContainerEnv();
      wc.initContainer();
      wc.localizeResources();
      wc.launchContainer();
      reset(wc.localizerBus);
      wc.containerSuccessful();
      wc.dockerContainerResourcesCleanup();
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      verifyOutofBandHeartBeat(wc);
      assertNull(wc.c.getLocalizedResources());
      // Now in DONE, issue INIT
      wc.initContainer();
      // Verify still in DONE
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  // mocked generic
  public void testLocalizationFailureAtDone() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(6, 314159265358979L, 4344, "yak");
      wc.initContainer();
      wc.localizeResources();
      wc.launchContainer();
      reset(wc.localizerBus);
      wc.containerSuccessful();
      wc.containerResourcesCleanup();
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      verifyOutofBandHeartBeat(wc);
      assertNull(wc.c.getLocalizedResources());
      // Now in DONE, issue RESOURCE_FAILED as done by LocalizeRunner
      wc.resourceFailedContainer();
      // Verify still in DONE
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  // mocked generic
  public void testDockerContainerLocalizationFailureAtDone() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(6, 314159265358979L, 4344, "yak");
      wc.setupDockerContainerEnv();
      wc.initContainer();
      wc.localizeResources();
      wc.launchContainer();
      reset(wc.localizerBus);
      wc.containerSuccessful();
      wc.dockerContainerResourcesCleanup();
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      verifyOutofBandHeartBeat(wc);
      assertNull(wc.c.getLocalizedResources());
      // Now in DONE, issue RESOURCE_FAILED as done by LocalizeRunner
      wc.resourceFailedContainer();
      // Verify still in DONE
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testLocalizationFailureWhileRunning()
      throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(6, 314159265358979L, 4344, "yak");
      wc.initContainer();
      wc.localizeResources();
      wc.launchContainer();
      reset(wc.localizerBus);
      assertEquals(ContainerState.RUNNING, wc.c.getContainerState());
      // Now in RUNNING, handle ContainerResourceFailedEvent, cause NPE before
      wc.handleContainerResourceFailedEvent();
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked") // mocked generic
  public void testCleanupOnKillRequest() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(12, 314159265358979L, 4344, "yak");
      wc.initContainer();
      wc.localizeResources();
      wc.launchContainer();
      reset(wc.localizerBus);
      wc.killContainer();
      assertEquals(ContainerState.KILLING, wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      wc.containerKilledOnRequest();
      
      verifyCleanupCall(wc);
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  public void testKillOnNew() throws Exception {
    WrappedContainer wc = null;

    try {
      wc = new WrappedContainer(13, 314159265358979L, 4344, "yak");
      assertEquals(ContainerState.NEW, wc.c.getContainerState());
      int killed = metrics.getKilledContainers();
      wc.killContainer();
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      verifyOutofBandHeartBeat(wc);
      assertEquals(ContainerExitStatus.KILLED_BY_RESOURCEMANAGER,
          wc.c.cloneAndGetContainerStatus().getExitStatus());
      assertTrue(wc.c.cloneAndGetContainerStatus().getDiagnostics()
          .contains("KillRequest"));
      assertEquals(killed + 1, metrics.getKilledContainers());
      // check container metrics is generated.
      ContainerMetrics containerMetrics =
          ContainerMetrics.forContainer(wc.cId, 1, 5000);
      Assert.assertEquals(ContainerExitStatus.KILLED_BY_RESOURCEMANAGER,
          containerMetrics.exitCode.value());
      Assert.assertTrue(containerMetrics.startTime.value() > 0);
      Assert.assertTrue(containerMetrics.finishTime.value() >=
              containerMetrics.startTime.value());
      Assert.assertEquals(ContainerEventType.KILL_CONTAINER,
          wc.initStateToEvent.get(ContainerState.NEW));
      Assert.assertEquals(ContainerState.DONE,
          wc.eventToFinalState.get(ContainerEventType.KILL_CONTAINER));
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  public void testKillOnLocalizing() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(14, 314159265358979L, 4344, "yak");
      wc.initContainer();
      assertEquals(ContainerState.LOCALIZING, wc.c.getContainerState());
      wc.killContainer();
      assertEquals(ContainerState.KILLING, wc.c.getContainerState());
      assertEquals(ContainerExitStatus.KILLED_BY_RESOURCEMANAGER,
          wc.c.cloneAndGetContainerStatus().getExitStatus());
      assertTrue(wc.c.cloneAndGetContainerStatus().getDiagnostics()
          .contains("KillRequest"));
      int killed = metrics.getKilledContainers();
      wc.containerResourcesCleanup();
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      assertEquals(killed + 1, metrics.getKilledContainers());
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }
  
  @Test
  public void testKillOnLocalizationFailed() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(15, 314159265358979L, 4344, "yak");
      wc.initContainer();
      wc.failLocalizeResources(wc.getLocalResourceCount());
      assertEquals(ContainerState.LOCALIZATION_FAILED, wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      wc.killContainer();
      assertEquals(ContainerState.LOCALIZATION_FAILED, wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
      int failed = metrics.getFailedContainers();
      wc.containerResourcesCleanup();
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      assertEquals(failed + 1, metrics.getFailedContainers());
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  public void testKillOnLocalizedWhenContainerNotLaunchedContainerKilled()
      throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(17, 314159265358979L, 4344, "yak");
      wc.initContainer();
      wc.localizeResources();
      assertEquals(ContainerState.SCHEDULED, wc.c.getContainerState());
      ContainerLaunch launcher = wc.launcher.running.get(wc.c.getContainerId());
      wc.killContainer();
      assertEquals(ContainerState.KILLING, wc.c.getContainerState());

      // check that container cleanup hasn't started at this point.
      LocalizationCleanupMatcher cleanupResources =
          new LocalizationCleanupMatcher(wc.c);
      verify(wc.localizerBus, times(0)).handle(argThat(cleanupResources));

      // check if containerlauncher cleans up the container launch.
      verify(wc.launcherBus)
          .handle(refEq(new ContainersLauncherEvent(wc.c,
              ContainersLauncherEventType.CLEANUP_CONTAINER), "timestamp"));

      launcher.call();
      wc.drainDispatcherEvents();
      assertEquals(ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
          wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
      int killed = metrics.getKilledContainers();
      wc.c.handle(new ContainerEvent(wc.c.getContainerId(),
          ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP));
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      assertEquals(killed + 1, metrics.getKilledContainers());
      assertEquals(0, metrics.getRunningContainers());
      assertEquals(0, wc.launcher.running.size());
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  public void testDockerKillOnLocalizedWhenContainerNotLaunchedContainerKilled()
      throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(17, 314159265358979L, 4344, "yak");
      wc.setupDockerContainerEnv();
      wc.initContainer();
      wc.localizeResources();
      assertEquals(ContainerState.SCHEDULED, wc.c.getContainerState());
      ContainerLaunch launcher = wc.launcher.running.get(wc.c.getContainerId());
      wc.killContainer();
      assertEquals(ContainerState.KILLING, wc.c.getContainerState());
      launcher.call();
      wc.drainDispatcherEvents();
      assertEquals(ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
          wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyDockerContainerCleanupCall(wc);
      int killed = metrics.getKilledContainers();
      wc.c.handle(new ContainerEvent(wc.c.getContainerId(),
          ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP));
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      assertEquals(killed + 1, metrics.getKilledContainers());
      assertEquals(0, metrics.getRunningContainers());
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  public void testKillOnLocalizedWhenContainerNotLaunchedContainerSuccess()
      throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(17, 314159265358979L, 4344, "yak");
      wc.initContainer();
      wc.localizeResources();
      assertEquals(ContainerState.SCHEDULED, wc.c.getContainerState());
      wc.killContainer();
      assertEquals(ContainerState.KILLING, wc.c.getContainerState());
      wc.containerSuccessful();
      wc.drainDispatcherEvents();
      assertEquals(ContainerState.EXITED_WITH_SUCCESS,
          wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
      wc.c.handle(new ContainerEvent(wc.c.getContainerId(),
          ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP));
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      assertEquals(0, metrics.getRunningContainers());
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  public void testKillOnLocalizedWhenContainerNotLaunchedContainerFailure()
      throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(17, 314159265358979L, 4344, "yak");
      wc.initContainer();
      wc.localizeResources();
      assertEquals(ContainerState.SCHEDULED, wc.c.getContainerState());
      wc.killContainer();
      assertEquals(ContainerState.KILLING, wc.c.getContainerState());
      wc.containerFailed(ExitCode.FORCE_KILLED.getExitCode());
      wc.drainDispatcherEvents();
      assertEquals(ContainerState.EXITED_WITH_FAILURE,
          wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
      wc.c.handle(new ContainerEvent(wc.c.getContainerId(),
          ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP));
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      assertEquals(0, metrics.getRunningContainers());
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  public void testDockerKillOnLocalizedContainerNotLaunchedContainerFailure()
      throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(17, 314159265358979L, 4344, "yak");
      wc.setupDockerContainerEnv();
      wc.initContainer();
      wc.localizeResources();
      assertEquals(ContainerState.SCHEDULED, wc.c.getContainerState());
      wc.killContainer();
      assertEquals(ContainerState.KILLING, wc.c.getContainerState());
      wc.containerFailed(ExitCode.FORCE_KILLED.getExitCode());
      wc.drainDispatcherEvents();
      assertEquals(ContainerState.EXITED_WITH_FAILURE,
          wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyDockerContainerCleanupCall(wc);
      wc.c.handle(new ContainerEvent(wc.c.getContainerId(),
          ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP));
      assertEquals(ContainerState.DONE, wc.c.getContainerState());
      assertEquals(0, metrics.getRunningContainers());
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  public void testKillOnLocalizedWhenContainerLaunched() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(17, 314159265358979L, 4344, "yak");
      wc.initContainer();
      wc.localizeResources();
      assertEquals(ContainerState.SCHEDULED, wc.c.getContainerState());
      ContainerLaunch launcher = wc.launcher.running.get(wc.c.getContainerId());
      launcher.call();
      wc.drainDispatcherEvents();
      assertEquals(ContainerState.EXITED_WITH_FAILURE,
          wc.c.getContainerState());
      wc.killContainer();
      assertEquals(ContainerState.EXITED_WITH_FAILURE,
          wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  public void testDockerKillOnLocalizedWhenContainerLaunched()
      throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(17, 314159265358979L, 4344, "yak");
      wc.setupDockerContainerEnv();
      wc.initContainer();
      wc.localizeResources();
      assertEquals(ContainerState.SCHEDULED, wc.c.getContainerState());
      ContainerLaunch launcher = wc.launcher.running.get(wc.c.getContainerId());
      launcher.call();
      wc.drainDispatcherEvents();
      assertEquals(ContainerState.EXITED_WITH_FAILURE,
          wc.c.getContainerState());
      wc.killContainer();
      assertEquals(ContainerState.EXITED_WITH_FAILURE,
          wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyDockerContainerCleanupCall(wc);
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }
  
  @Test
  public void testResourceLocalizedOnLocalizationFailed() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(16, 314159265358979L, 4344, "yak");
      wc.initContainer();
      int failCount = wc.getLocalResourceCount()/2;
      if (failCount == 0) {
        failCount = 1;
      }
      wc.failLocalizeResources(failCount);
      assertEquals(ContainerState.LOCALIZATION_FAILED, wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      wc.localizeResourcesFromInvalidState(failCount);
      assertEquals(ContainerState.LOCALIZATION_FAILED, wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
      Assert.assertTrue(wc.getDiagnostics().contains(FAKE_LOCALIZATION_ERROR));
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }
  
  @Test
  public void testResourceFailedOnLocalizationFailed() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(16, 314159265358979L, 4344, "yak");
      wc.initContainer();
      
      Iterator<String> lRsrcKeys = wc.localResources.keySet().iterator();
      String key1 = lRsrcKeys.next();
      String key2 = lRsrcKeys.next();
      wc.failLocalizeSpecificResource(key1);
      assertEquals(ContainerState.LOCALIZATION_FAILED, wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      wc.failLocalizeSpecificResource(key2);
      assertEquals(ContainerState.LOCALIZATION_FAILED, wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }
  
  @Test
  public void testResourceFailedOnKilling() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(16, 314159265358979L, 4344, "yak");
      wc.initContainer();
      
      Iterator<String> lRsrcKeys = wc.localResources.keySet().iterator();
      String key1 = lRsrcKeys.next();
      wc.killContainer();
      assertEquals(ContainerState.KILLING, wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      wc.failLocalizeSpecificResource(key1);
      assertEquals(ContainerState.KILLING, wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      verifyCleanupCall(wc);
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }
  
  /**
   * Verify serviceData correctly sent.
   */
  @Test
  public void testServiceData() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(9, 314159265358979L, 4344, "yak", false, true);
      assertEquals(ContainerState.NEW, wc.c.getContainerState());
      wc.initContainer();
      
      for (final Map.Entry<String,ByteBuffer> e : wc.serviceData.entrySet()) {
        ArgumentMatcher<AuxServicesEvent> matchesServiceReq =
          new ArgumentMatcher<AuxServicesEvent>() {
            @Override
            public boolean matches(Object o) {
              AuxServicesEvent evt = (AuxServicesEvent) o;
              return e.getKey().equals(evt.getServiceID())
                && 0 == e.getValue().compareTo(evt.getServiceData());
            }
          };
        verify(wc.auxBus).handle(argThat(matchesServiceReq));
      }

      final WrappedContainer wcf = wc;
      // verify launch on empty resource request
      ArgumentMatcher<ContainersLauncherEvent> matchesLaunchReq =
        new ArgumentMatcher<ContainersLauncherEvent>() {
          @Override
          public boolean matches(Object o) {
            ContainersLauncherEvent evt = (ContainersLauncherEvent) o;
            return evt.getType() == ContainersLauncherEventType.LAUNCH_CONTAINER
              && wcf.cId.equals(evt.getContainer().getContainerId());
          }
        };
      verify(wc.launcherBus).handle(argThat(matchesLaunchReq));
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  public void testLaunchAfterKillRequest() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(14, 314159265358979L, 4344, "yak");
      wc.initContainer();
      wc.localizeResources();
      wc.killContainer();
      assertEquals(ContainerState.KILLING, wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      wc.launchContainer();
      assertEquals(ContainerState.KILLING, wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      wc.containerKilledOnRequest();
      verifyCleanupCall(wc);
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  public void testDockerContainerLaunchAfterKillRequest() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(14, 314159265358979L, 4344, "yak");
      wc.setupDockerContainerEnv();
      wc.initContainer();
      wc.localizeResources();
      wc.killContainer();
      assertEquals(ContainerState.KILLING, wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      wc.launchContainer();
      assertEquals(ContainerState.KILLING, wc.c.getContainerState());
      assertNull(wc.c.getLocalizedResources());
      wc.containerKilledOnRequest();
      verifyDockerContainerCleanupCall(wc);
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }
  
  @Test
  public void testContainerRetry() throws Exception{
    ContainerRetryContext containerRetryContext1 = ContainerRetryContext
        .newInstance(ContainerRetryPolicy.NEVER_RETRY, null, 3, 0);
    testContainerRetry(containerRetryContext1, 2, 0);

    ContainerRetryContext containerRetryContext2 = ContainerRetryContext
        .newInstance(ContainerRetryPolicy.RETRY_ON_ALL_ERRORS, null, 3, 0);
    testContainerRetry(containerRetryContext2, 2, 3);

    ContainerRetryContext containerRetryContext3 = ContainerRetryContext
        .newInstance(ContainerRetryPolicy.RETRY_ON_ALL_ERRORS, null, 3, 0);
    // If exit code is 0, it will not retry
    testContainerRetry(containerRetryContext3, 0, 0);

    ContainerRetryContext containerRetryContext4 = ContainerRetryContext
        .newInstance(
            ContainerRetryPolicy.RETRY_ON_SPECIFIC_ERROR_CODES, null, 3, 0);
    testContainerRetry(containerRetryContext4, 2, 0);

    HashSet<Integer> errorCodes = new HashSet<>();
    errorCodes.add(2);
    errorCodes.add(6);
    ContainerRetryContext containerRetryContext5 = ContainerRetryContext
        .newInstance(ContainerRetryPolicy.RETRY_ON_SPECIFIC_ERROR_CODES,
            errorCodes, 3, 0);
    testContainerRetry(containerRetryContext5, 2, 3);

    HashSet<Integer> errorCodes2 = new HashSet<>();
    errorCodes.add(143);
    ContainerRetryContext containerRetryContext6 = ContainerRetryContext
        .newInstance(ContainerRetryPolicy.RETRY_ON_SPECIFIC_ERROR_CODES,
            errorCodes2, 3, 0);
    // If exit code is 143(SIGTERM), it will not retry even it is in errorCodes.
    testContainerRetry(containerRetryContext6, 143, 0);
  }

  private void testContainerRetry(ContainerRetryContext containerRetryContext,
      int exitCode, int expectedRetries) throws Exception{
    WrappedContainer wc = null;
    try {
      int retryTimes = 0;
      wc = new WrappedContainer(24, 314159265358979L, 4344, "yak",
          containerRetryContext);
      wc.initContainer();
      wc.localizeResources();
      wc.launchContainer();
      while (true) {
        wc.containerFailed(exitCode);
        if (wc.c.getContainerState() == ContainerState.RUNNING) {
          retryTimes ++;
        } else {
          break;
        }
      }
      Assert.assertEquals(expectedRetries, retryTimes);
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  public void testContainerRestartInterval() throws IOException {
    conf.setInt(YarnConfiguration.NM_CONTAINER_RETRY_MINIMUM_INTERVAL_MS, 2000);

    ContainerRetryContext containerRetryContext1 = ContainerRetryContext
        .newInstance(ContainerRetryPolicy.NEVER_RETRY, null, 3, 0);
    testContainerRestartInterval(containerRetryContext1, 0);

    ContainerRetryContext containerRetryContext2 = ContainerRetryContext
        .newInstance(ContainerRetryPolicy.RETRY_ON_ALL_ERRORS, null, 3, 0);
    testContainerRestartInterval(containerRetryContext2, 2000);

    ContainerRetryContext containerRetryContext3 = ContainerRetryContext
        .newInstance(ContainerRetryPolicy.RETRY_ON_ALL_ERRORS, null, 3, 4000);
    testContainerRestartInterval(containerRetryContext3, 4000);
  }

  private void testContainerRestartInterval(
      ContainerRetryContext containerRetryContext,
      int expectedRestartInterval) throws IOException {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(25, 314159265358980L, 4345,
          "yak", containerRetryContext);
      Assert.assertEquals(
          ((ContainerImpl)wc.c).getContainerRetryContext().getRetryInterval(),
          expectedRestartInterval);
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  @Test
  public void testContainerRetryFailureValidityInterval() throws Exception {
    ContainerRetryContext containerRetryContext = ContainerRetryContext
        .newInstance(ContainerRetryPolicy.RETRY_ON_ALL_ERRORS, null, 1, 0, 10);
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(25, 314159265358980L, 4200, "test",
          containerRetryContext);
      ControlledClock clock = new ControlledClock();
      wc.getRetryPolicy().setClock(clock);
      wc.initContainer();
      wc.localizeResources();
      wc.launchContainer();
      wc.containerFailed(12);
      assertEquals(ContainerState.RUNNING, wc.c.getContainerState());
      clock.setTime(20);
      wc.containerFailed(12);
      assertEquals(ContainerState.RUNNING, wc.c.getContainerState());
      clock.setTime(40);
      wc.containerFailed(12);
      assertEquals(ContainerState.RUNNING, wc.c.getContainerState());
      clock.setTime(45);
      wc.containerFailed(12);
      assertEquals(ContainerState.EXITED_WITH_FAILURE,
          wc.c.getContainerState());
    } finally {
      if (wc != null) {
        wc.finished();
      }
    }
  }

  private void verifyCleanupCall(WrappedContainer wc) throws Exception {
    ResourcesReleasedMatcher matchesReq =
        new ResourcesReleasedMatcher(wc.localResources, EnumSet.of(
            LocalResourceVisibility.PUBLIC, LocalResourceVisibility.PRIVATE,
            LocalResourceVisibility.APPLICATION), wc.c);
    verify(wc.localizerBus, atLeastOnce()).handle(argThat(matchesReq));
  }

  private void verifyOutofBandHeartBeat(WrappedContainer wc) {
    verify(wc.context.getNodeStatusUpdater()).sendOutofBandHeartBeat();
  }

  private void verifyDockerContainerCleanupCall(WrappedContainer wc)
      throws Exception {
    DeletionService delService = wc.context.getDeletionService();
    verify(delService, times(1)).delete(argThat(
        new DockerContainerDeletionMatcher(delService,
            wc.c.getContainerId().toString())));
  }

  // Argument matcher for matching container localization cleanup event.
  private static class LocalizationCleanupMatcher extends
      ArgumentMatcher<LocalizationEvent> {
    Container c;

    LocalizationCleanupMatcher(Container c){
      this.c = c;
    }

    @Override
    public boolean matches(Object o) {
      if (!(o instanceof ContainerLocalizationCleanupEvent)) {
        return false;
      }
      ContainerLocalizationCleanupEvent evt =
          (ContainerLocalizationCleanupEvent) o;

      return (evt.getContainer() == c);
    }
  }

  private static class ResourcesReleasedMatcher extends
      LocalizationCleanupMatcher {
    final HashSet<LocalResourceRequest> resources =
        new HashSet<LocalResourceRequest>();

    ResourcesReleasedMatcher(Map<String, LocalResource> allResources,
        EnumSet<LocalResourceVisibility> vis, Container c) throws URISyntaxException {
      super(c);
      for (Entry<String, LocalResource> e : allResources.entrySet()) {
        if (vis.contains(e.getValue().getVisibility())) {
          resources.add(new LocalResourceRequest(e.getValue()));
        }
      }
    }

    @Override
    public boolean matches(Object o) {
      // match event type and container.
      if(!super.matches(o)){
        return false;
      }

      // match resources.
      ContainerLocalizationCleanupEvent evt =
          (ContainerLocalizationCleanupEvent) o;
      final HashSet<LocalResourceRequest> expected =
          new HashSet<LocalResourceRequest>(resources);
      for (Collection<LocalResourceRequest> rc : evt.getResources().values()) {
        for (LocalResourceRequest rsrc : rc) {
          if (!expected.remove(rsrc)) {
            return false;
          }
        }
      }
      return expected.isEmpty();
    }
  }

  // Accept iff the resource payload matches.
  private static class ResourcesRequestedMatcher extends
      ArgumentMatcher<LocalizationEvent> {
    final HashSet<LocalResourceRequest> resources =
        new HashSet<LocalResourceRequest>();

    ResourcesRequestedMatcher(Map<String, LocalResource> allResources,
        EnumSet<LocalResourceVisibility> vis) throws URISyntaxException {
      for (Entry<String, LocalResource> e : allResources.entrySet()) {
        if (vis.contains(e.getValue().getVisibility())) {
          resources.add(new LocalResourceRequest(e.getValue()));
        }
      }
    }

    @Override
    public boolean matches(Object o) {
      ContainerLocalizationRequestEvent evt =
          (ContainerLocalizationRequestEvent) o;
      final HashSet<LocalResourceRequest> expected =
          new HashSet<LocalResourceRequest>(resources);
      for (Collection<LocalResourceRequest> rc : evt.getRequestedResources()
          .values()) {
        for (LocalResourceRequest rsrc : rc) {
          if (!expected.remove(rsrc)) {
            return false;
          }
        }
      }
      return expected.isEmpty();
    }
  }

  private static Entry<String, LocalResource> getMockRsrc(Random r,
      LocalResourceVisibility vis) {
    String name = Long.toHexString(r.nextLong());
    URL url = BuilderUtils.newURL("file", null, 0, "/local" + vis + "/" + name);
    LocalResource rsrc =
        BuilderUtils.newLocalResource(url, LocalResourceType.FILE, vis,
            r.nextInt(1024) + 1024L, r.nextInt(1024) + 2048L, false);
    return new SimpleEntry<String, LocalResource>(name, rsrc);
  }

  private static Map<String,LocalResource> createLocalResources(Random r) {
    Map<String,LocalResource> localResources =
      new HashMap<String,LocalResource>();
    for (int i = r.nextInt(5) + 5; i >= 0; --i) {
      Entry<String,LocalResource> rsrc =
        getMockRsrc(r, LocalResourceVisibility.PUBLIC);
      localResources.put(rsrc.getKey(), rsrc.getValue());
    }
    for (int i = r.nextInt(5) + 5; i >= 0; --i) {
      Entry<String,LocalResource> rsrc =
        getMockRsrc(r, LocalResourceVisibility.PRIVATE);
      localResources.put(rsrc.getKey(), rsrc.getValue());
    }
    for (int i = r.nextInt(2) + 2; i >= 0; --i) {
      Entry<String,LocalResource> rsrc =
        getMockRsrc(r, LocalResourceVisibility.APPLICATION);
      localResources.put(rsrc.getKey(), rsrc.getValue());
    }
    return localResources;
  }

  private static Map<String,ByteBuffer> createServiceData(Random r) {
    Map<String,ByteBuffer> serviceData =
      new HashMap<String,ByteBuffer>();
    for (int i = r.nextInt(5) + 5; i >= 0; --i) {
      String service = Long.toHexString(r.nextLong());
      byte[] b = new byte[r.nextInt(1024) + 1024];
      r.nextBytes(b);
      serviceData.put(service, ByteBuffer.wrap(b));
    }
    return serviceData;
  }

  @SuppressWarnings("unchecked")
  private class WrappedContainer {
    final DrainDispatcher dispatcher;
    final EventHandler<LocalizationEvent> localizerBus;
    final EventHandler<ContainersLauncherEvent> launcherBus;
    final EventHandler<ContainersMonitorEvent> monitorBus;
    final EventHandler<AuxServicesEvent> auxBus;
    final EventHandler<ApplicationEvent> appBus;
    final EventHandler<LogHandlerEvent> LogBus;
    final EventHandler<ContainerSchedulerEvent> schedBus;
    final ContainersLauncher launcher;

    final ContainerLaunchContext ctxt;
    final ContainerId cId;
    final Container c;
    final Map<String, LocalResource> localResources;
    final Map<String, ByteBuffer> serviceData;
    final Context context = mock(Context.class);
    private final DeletionService delService;
    private final Map<ContainerState, ContainerEventType> initStateToEvent =
        new HashMap<>();
    private final Map<ContainerEventType, ContainerState> eventToFinalState =
        new HashMap<>();

    WrappedContainer(int appId, long timestamp, int id, String user)
        throws IOException {
      this(appId, timestamp, id, user, null);
    }

    WrappedContainer(int appId, long timestamp, int id, String user,
        ContainerRetryContext containerRetryContext) throws IOException {
      this(appId, timestamp, id, user, true, false, containerRetryContext);
    }

    WrappedContainer(int appId, long timestamp, int id, String user,
        boolean withLocalRes, boolean withServiceData) throws IOException {
      this(appId, timestamp, id, user, withLocalRes, withServiceData, null);
    }

    @SuppressWarnings("rawtypes")
    WrappedContainer(int appId, long timestamp, int id, String user,
        boolean withLocalRes, boolean withServiceData,
        ContainerRetryContext containerRetryContext) throws IOException {
      dispatcher = new DrainDispatcher();
      dispatcher.init(new Configuration());

      localizerBus = mock(EventHandler.class);
      launcherBus = mock(EventHandler.class);
      monitorBus = mock(EventHandler.class);
      auxBus = mock(EventHandler.class);
      appBus = mock(EventHandler.class);
      LogBus = mock(EventHandler.class);
      delService = mock(DeletionService.class);
      schedBus = new ContainerScheduler(context, dispatcher, metrics, 0) {
        @Override
        protected void scheduleContainer(Container container) {
          container.sendLaunchEvent();
        }
      };
      dispatcher.register(LocalizationEventType.class, localizerBus);
      dispatcher.register(ContainersLauncherEventType.class, launcherBus);
      dispatcher.register(ContainersMonitorEventType.class, monitorBus);
      dispatcher.register(ContainerSchedulerEventType.class, schedBus);
      dispatcher.register(AuxServicesEventType.class, auxBus);
      dispatcher.register(ApplicationEventType.class, appBus);
      dispatcher.register(LogHandlerEventType.class, LogBus);

      when(context.getApplications()).thenReturn(
          new ConcurrentHashMap<ApplicationId, Application>());
      NMNullStateStoreService stateStore = new NMNullStateStoreService();
      when(context.getNMStateStore()).thenReturn(stateStore);
      NodeStatusUpdater nodeStatusUpdater = mock(NodeStatusUpdater.class);
      when(context.getNodeStatusUpdater()).thenReturn(nodeStatusUpdater);
      ContainerExecutor executor = mock(ContainerExecutor.class);
      Mockito.doNothing().when(executor).pauseContainer(any(Container.class));
      Mockito.doNothing().when(executor).resumeContainer(any(Container.class));
      launcher =
          new ContainersLauncher(context, dispatcher, executor, null, null);
      // create a mock ExecutorService, which will not really launch
      // ContainerLaunch at all.
      launcher.containerLauncher = mock(ExecutorService.class);
      Future future = mock(Future.class);
      when(launcher.containerLauncher.submit
          (any(Callable.class))).thenReturn(future);
      when(future.isDone()).thenReturn(false);
      when(future.cancel(false)).thenReturn(true);
      launcher.init(new Configuration());
      launcher.start();
      dispatcher.register(ContainersLauncherEventType.class, launcher);

      ctxt = mock(ContainerLaunchContext.class);
      org.apache.hadoop.yarn.api.records.Container mockContainer =
          mock(org.apache.hadoop.yarn.api.records.Container.class);
      cId = BuilderUtils.newContainerId(appId, 1, timestamp, id);
      when(mockContainer.getId()).thenReturn(cId);

      Resource resource = BuilderUtils.newResource(1024, 1);
      when(mockContainer.getResource()).thenReturn(resource);
      String host = "127.0.0.1";
      int port = 1234;
      long currentTime = System.currentTimeMillis();
      ContainerTokenIdentifier identifier = 
          new ContainerTokenIdentifier(cId, "127.0.0.1", user, resource,
            currentTime + 10000L, 123, currentTime, Priority.newInstance(0), 0);
      Token token =
          BuilderUtils.newContainerToken(BuilderUtils.newNodeId(host, port),
            "password".getBytes(), identifier);
      when(mockContainer.getContainerToken()).thenReturn(token);
      if (withLocalRes) {
        Random r = new Random();
        long seed = r.nextLong();
        r.setSeed(seed);
        System.out.println("WrappedContainerLocalResource seed: " + seed);
        localResources = createLocalResources(r);
      } else {
        localResources = Collections.<String, LocalResource> emptyMap();
      }
      when(ctxt.getLocalResources()).thenReturn(localResources);

      if (withServiceData) {
        Random r = new Random();
        long seed = r.nextLong();
        r.setSeed(seed);
        System.out.println("ServiceData seed: " + seed);
        serviceData = createServiceData(r);
      } else {
        serviceData = Collections.<String, ByteBuffer> emptyMap();
      }
      when(ctxt.getServiceData()).thenReturn(serviceData);
      when(ctxt.getContainerRetryContext()).thenReturn(containerRetryContext);
      when(context.getDeletionService()).thenReturn(delService);
      ContainerStateTransitionListener listener =
          new ContainerStateTransitionListener() {
        @Override
        public void init(Context cntxt) {}

        @Override
        public void preTransition(ContainerImpl op, ContainerState beforeState,
            ContainerEvent eventToBeProcessed) {
          initStateToEvent.put(beforeState, eventToBeProcessed.getType());
        }

        @Override
        public void postTransition(ContainerImpl op, ContainerState beforeState,
            ContainerState afterState, ContainerEvent processedEvent) {
          eventToFinalState.put(processedEvent.getType(), afterState);
        }
      };
      NodeManager.DefaultContainerStateListener multi =
          new NodeManager.DefaultContainerStateListener();
      multi.addListener(listener);
      when(context.getContainerStateTransitionListener()).thenReturn(multi);
      c = new ContainerImpl(conf, dispatcher, ctxt, null, metrics, identifier,
          context);
      dispatcher.register(ContainerEventType.class,
          new EventHandler<ContainerEvent>() {
            @Override
            public void handle(ContainerEvent event) {
                c.handle(event);
            }
      });
      dispatcher.start();
    }

    private void drainDispatcherEvents() {
      dispatcher.await();
    }

    public void finished() {
      dispatcher.stop();
    }

    public void initContainer() {
      c.handle(new ContainerEvent(cId, ContainerEventType.INIT_CONTAINER));
      drainDispatcherEvents();
    }

    public void resourceFailedContainer() {
      c.handle(new ContainerEvent(cId, ContainerEventType.RESOURCE_FAILED));
      drainDispatcherEvents();
    }

    public void handleContainerResourceFailedEvent() {
      c.handle(new ContainerResourceFailedEvent(cId, null, null));
      drainDispatcherEvents();
    }

    // Localize resources 
    // Skip some resources so as to consider them failed
    public Map<Path, List<String>> doLocalizeResources(
        boolean checkLocalizingState, int skipRsrcCount)
        throws URISyntaxException {
      Path cache = new Path("file:///cache");
      Map<Path, List<String>> localPaths =
          new HashMap<Path, List<String>>();
      int counter = 0;
      for (Entry<String, LocalResource> rsrc : localResources.entrySet()) {
        if (counter++ < skipRsrcCount) {
          continue;
        }
        if (checkLocalizingState) {
          assertEquals(ContainerState.LOCALIZING, c.getContainerState());
        }
        LocalResourceRequest req = new LocalResourceRequest(rsrc.getValue());
        Path p = new Path(cache, rsrc.getKey());
        localPaths.put(p, Arrays.asList(rsrc.getKey()));
        // rsrc copied to p
        c.handle(new ContainerResourceLocalizedEvent(c.getContainerId(),
                 req, p));
      }
      drainDispatcherEvents();
      return localPaths;
    }
    
    
    public Map<Path, List<String>> localizeResources()
        throws URISyntaxException {
      return doLocalizeResources(true, 0);
    }
    
    public void localizeResourcesFromInvalidState(int skipRsrcCount)
        throws URISyntaxException {
      doLocalizeResources(false, skipRsrcCount);
    }
    
    public void failLocalizeSpecificResource(String rsrcKey)
        throws URISyntaxException {
      LocalResource rsrc = localResources.get(rsrcKey);
      LocalResourceRequest req = new LocalResourceRequest(rsrc);
      Exception e = new Exception(FAKE_LOCALIZATION_ERROR);
      c.handle(new ContainerResourceFailedEvent(c.getContainerId(), req, e
        .getMessage()));
      drainDispatcherEvents();
    }

    // fail to localize some resources
    public void failLocalizeResources(int failRsrcCount)
        throws URISyntaxException {
      int counter = 0;
      for (Entry<String, LocalResource> rsrc : localResources.entrySet()) {
        if (counter >= failRsrcCount) {
          break;
        }
        ++counter;
        LocalResourceRequest req = new LocalResourceRequest(rsrc.getValue());
        Exception e = new Exception(FAKE_LOCALIZATION_ERROR);
        c.handle(new ContainerResourceFailedEvent(c.getContainerId(),
                 req, e.getMessage()));
      }
      drainDispatcherEvents();     
    }

    public void launchContainer() {
      c.handle(new ContainerEvent(cId, ContainerEventType.CONTAINER_LAUNCHED));
      drainDispatcherEvents();
    }

    public void containerSuccessful() {
      c.handle(new ContainerEvent(cId,
          ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS));
      drainDispatcherEvents();
    }
    public void containerResourcesCleanup() {
      c.handle(new ContainerEvent(cId,
          ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP));
      drainDispatcherEvents();
    }

    public void dockerContainerResourcesCleanup() {
      c.handle(new ContainerEvent(cId,
          ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP));
      verify(delService, times(1)).delete(argThat(
          new DockerContainerDeletionMatcher(delService, cId.toString())));
      drainDispatcherEvents();
    }

    public void setupDockerContainerEnv() {
      Map<String, String> env = new HashMap<>();
      env.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE, "docker");
      when(this.ctxt.getEnvironment()).thenReturn(env);
    }

    public void containerFailed(int exitCode) {
      String diagnosticMsg = "Container completed with exit code " + exitCode;
      c.handle(new ContainerExitEvent(cId,
          ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, exitCode,
          diagnosticMsg));
      ContainerStatus containerStatus = c.cloneAndGetContainerStatus();
      assert containerStatus.getDiagnostics().contains(diagnosticMsg);
      assert containerStatus.getExitStatus() == exitCode;
      drainDispatcherEvents();
      // If container needs retry, relaunch it
      if (c.getContainerState() == ContainerState.RELAUNCHING) {
        launchContainer();
      }
    }

    public void killContainer() {
      c.handle(new ContainerKillEvent(cId,
          ContainerExitStatus.KILLED_BY_RESOURCEMANAGER,
          "KillRequest"));
      drainDispatcherEvents();
    }

    public void pauseContainer() {
      c.handle(new ContainerPauseEvent(cId,
          "PauseRequest"));
      drainDispatcherEvents();
    }

    public void resumeContainer() {
      c.handle(new ContainerResumeEvent(cId,
          "ResumeRequest"));
      drainDispatcherEvents();
    }

    public void containerKilledOnRequest() {
      int exitCode = ContainerExitStatus.KILLED_BY_RESOURCEMANAGER;
      String diagnosticMsg = "Container completed with exit code " + exitCode;
      c.handle(new ContainerExitEvent(cId,
          ContainerEventType.CONTAINER_KILLED_ON_REQUEST, exitCode,
          diagnosticMsg));
      ContainerStatus containerStatus = c.cloneAndGetContainerStatus();
      assert containerStatus.getDiagnostics().contains(diagnosticMsg);
      assert containerStatus.getExitStatus() == exitCode; 
      drainDispatcherEvents();
    }
    
    public int getLocalResourceCount() {
      return localResources.size();
    }

    public String getDiagnostics() {
      return c.cloneAndGetContainerStatus().getDiagnostics();
    }

    public SlidingWindowRetryPolicy getRetryPolicy() {
      return ((ContainerImpl)c).getRetryPolicy();
    }
  }

}
