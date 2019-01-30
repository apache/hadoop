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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.application;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerInitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorEventType;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;


public class TestApplication {

  /**
   * All container start events before application running.
   */
  @Test
  public void testApplicationInit1() {
    WrappedApplication wa = null;
    try {
      wa = new WrappedApplication(1, 314159265358979L, "yak", 3);
      wa.initApplication();
      wa.initContainer(1);
      assertEquals(ApplicationState.INITING, wa.app.getApplicationState());
      assertEquals(1, wa.app.getContainers().size());
      wa.initContainer(0);
      wa.initContainer(2);
      assertEquals(ApplicationState.INITING, wa.app.getApplicationState());
      assertEquals(3, wa.app.getContainers().size());
      wa.applicationInited();
      assertEquals(ApplicationState.RUNNING, wa.app.getApplicationState());

      for (int i = 0; i < wa.containers.size(); i++) {
        verify(wa.containerBus).handle(
            argThat(new ContainerInitMatcher(wa.containers.get(i)
                .getContainerId())));
      }
    } finally {
      if (wa != null)
        wa.finished();
    }
  }

  /**
   * Container start events after Application Running
   */
  @Test
  public void testApplicationInit2() {
    WrappedApplication wa = null;
    try {
      wa = new WrappedApplication(2, 314159265358979L, "yak", 3);
      wa.initApplication();
      wa.initContainer(0);
      assertEquals(ApplicationState.INITING, wa.app.getApplicationState());
      assertEquals(1, wa.app.getContainers().size());

      wa.applicationInited();
      assertEquals(ApplicationState.RUNNING, wa.app.getApplicationState());
      verify(wa.containerBus).handle(
          argThat(new ContainerInitMatcher(wa.containers.get(0)
              .getContainerId())));

      wa.initContainer(1);
      wa.initContainer(2);
      assertEquals(ApplicationState.RUNNING, wa.app.getApplicationState());
      assertEquals(3, wa.app.getContainers().size());

      for (int i = 1; i < wa.containers.size(); i++) {
        verify(wa.containerBus).handle(
            argThat(new ContainerInitMatcher(wa.containers.get(i)
                .getContainerId())));
      }
    } finally {
      if (wa != null)
        wa.finished();
    }
  }

  /**
   * App state RUNNING after all containers complete, before RM sends
   * APP_FINISHED
   */
  @Test
  public void testAppRunningAfterContainersComplete() {
    WrappedApplication wa = null;
    try {
      wa = new WrappedApplication(3, 314159265358979L, "yak", 3);
      wa.initApplication();
      wa.initContainer(-1);
      assertEquals(ApplicationState.INITING, wa.app.getApplicationState());
      wa.applicationInited();
      assertEquals(ApplicationState.RUNNING, wa.app.getApplicationState());

      wa.containerFinished(0);
      assertEquals(ApplicationState.RUNNING, wa.app.getApplicationState());
      assertEquals(2, wa.app.getContainers().size());

      wa.containerFinished(1);
      wa.containerFinished(2);
      assertEquals(ApplicationState.RUNNING, wa.app.getApplicationState());
      assertEquals(0, wa.app.getContainers().size());
    } finally {
      if (wa != null)
        wa.finished();
    }
  }

  /**
   * Finished containers properly tracked when only container finishes in APP_INITING
   */
  @Test
  public void testContainersCompleteDuringAppInit1() {
    WrappedApplication wa = null;
    try {
      wa = new WrappedApplication(3, 314159265358979L, "yak", 1);
      wa.initApplication();
      wa.initContainer(-1);
      assertEquals(ApplicationState.INITING, wa.app.getApplicationState());

      wa.containerFinished(0);
      assertEquals(ApplicationState.INITING, wa.app.getApplicationState());

      wa.applicationInited();
      assertEquals(ApplicationState.RUNNING, wa.app.getApplicationState());
      assertEquals(0, wa.app.getContainers().size());
    } finally {
      if (wa != null)
        wa.finished();
    }
  }

  /**
   * Finished containers properly tracked when 1 of several containers finishes in APP_INITING
   */
  @Test
  public void testContainersCompleteDuringAppInit2() {
    WrappedApplication wa = null;
    try {
      wa = new WrappedApplication(3, 314159265358979L, "yak", 3);
      wa.initApplication();
      wa.initContainer(-1);
      assertEquals(ApplicationState.INITING, wa.app.getApplicationState());

      wa.containerFinished(0);

      assertEquals(ApplicationState.INITING, wa.app.getApplicationState());

      wa.applicationInited();
      assertEquals(ApplicationState.RUNNING, wa.app.getApplicationState());
      assertEquals(2, wa.app.getContainers().size());

      wa.containerFinished(1);
      wa.containerFinished(2);
      assertEquals(ApplicationState.RUNNING, wa.app.getApplicationState());
      assertEquals(0, wa.app.getContainers().size());
    } finally {
      if (wa != null)
        wa.finished();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAppFinishedOnRunningContainers() {
    WrappedApplication wa = null;
    try {
      wa = new WrappedApplication(4, 314159265358979L, "yak", 3);
      wa.initApplication();
      wa.initContainer(-1);
      assertEquals(ApplicationState.INITING, wa.app.getApplicationState());
      wa.applicationInited();
      assertEquals(ApplicationState.RUNNING, wa.app.getApplicationState());

      wa.containerFinished(0);
      assertEquals(ApplicationState.RUNNING, wa.app.getApplicationState());
      assertEquals(2, wa.app.getContainers().size());

      wa.appFinished();
      assertEquals(ApplicationState.FINISHING_CONTAINERS_WAIT,
          wa.app.getApplicationState());
      assertEquals(2, wa.app.getContainers().size());

      for (int i = 1; i < wa.containers.size(); i++) {
        verify(wa.containerBus).handle(
            argThat(new ContainerKillMatcher(wa.containers.get(i)
                .getContainerId())));
      }

      wa.containerFinished(1);
      assertEquals(ApplicationState.FINISHING_CONTAINERS_WAIT,
          wa.app.getApplicationState());
      assertEquals(1, wa.app.getContainers().size());

      reset(wa.localizerBus);
      wa.containerFinished(2);
      // All containers finished. Cleanup should be called.
      assertEquals(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
          wa.app.getApplicationState());
      assertEquals(0, wa.app.getContainers().size());

      verify(wa.localizerBus).handle(
          refEq(new ApplicationLocalizationEvent(
              LocalizationEventType.DESTROY_APPLICATION_RESOURCES,
              wa.app), "timestamp"));

      verify(wa.auxBus).handle(
          refEq(new AuxServicesEvent(
              AuxServicesEventType.APPLICATION_STOP, wa.appId)));

      wa.appResourcesCleanedup();
      for (Container container : wa.containers) {
        ContainerTokenIdentifier identifier =
            wa.getContainerTokenIdentifier(container.getContainerId());
        waitForContainerTokenToExpire(identifier);
        Assert.assertTrue(wa.context.getContainerTokenSecretManager()
          .isValidStartContainerRequest(identifier));
      }
      assertEquals(ApplicationState.FINISHED, wa.app.getApplicationState());

    } finally {
      if (wa != null)
        wa.finished();
    }
  }

  protected ContainerTokenIdentifier waitForContainerTokenToExpire(
      ContainerTokenIdentifier identifier) {
    int attempts = 5;
    while (System.currentTimeMillis() < identifier.getExpiryTimeStamp()
        && attempts-- > 0) {
      try {
        Thread.sleep(1000);
      } catch (Exception e) {}
    }
    return identifier;
  }

  @Test
  public void testApplicationOnAppLogHandlingInitedEvtShouldStoreLogInitedTime()
      throws IOException {
    WrappedApplication wa = new WrappedApplication(5,  314159265358979L,
        "yak", 0);
    wa.initApplication();

    ArgumentCaptor<ContainerManagerApplicationProto> applicationProto =
        ArgumentCaptor.forClass(ContainerManagerApplicationProto.class);

    final long timestamp = wa.applicationLogInited();

    verify(wa.stateStoreService).storeApplication(any(ApplicationId.class),
        applicationProto.capture());

    assertEquals(applicationProto.getValue().getAppLogAggregationInitedTime()
        , timestamp);
  }


  @Test
  @SuppressWarnings("unchecked")
  public void testAppFinishedOnCompletedContainers() {
    WrappedApplication wa = null;
    try {
      wa = new WrappedApplication(5, 314159265358979L, "yak", 3);
      wa.initApplication();
      wa.initContainer(-1);
      assertEquals(ApplicationState.INITING, wa.app.getApplicationState());
      wa.applicationInited();
      assertEquals(ApplicationState.RUNNING, wa.app.getApplicationState());

      reset(wa.localizerBus);
      wa.containerFinished(0);
      wa.containerFinished(1);
      wa.containerFinished(2);
      assertEquals(ApplicationState.RUNNING, wa.app.getApplicationState());
      assertEquals(0, wa.app.getContainers().size());

      wa.appFinished();
      assertEquals(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
          wa.app.getApplicationState());

      verify(wa.localizerBus).handle(
          refEq(new ApplicationLocalizationEvent(
              LocalizationEventType.DESTROY_APPLICATION_RESOURCES, wa.app),
              "timestamp"));

      wa.appResourcesCleanedup();
      for ( Container container : wa.containers) {
        ContainerTokenIdentifier identifier =
            wa.getContainerTokenIdentifier(container.getContainerId());
        waitForContainerTokenToExpire(identifier);
        Assert.assertTrue(wa.context.getContainerTokenSecretManager()
          .isValidStartContainerRequest(identifier));
      }
      assertEquals(ApplicationState.FINISHED, wa.app.getApplicationState());
    } finally {
      if (wa != null)
        wa.finished();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testStartContainerAfterAppRunning() {
    WrappedApplication wa = null;
    try {
      wa = new WrappedApplication(5, 314159265358979L, "yak", 4);
      wa.initApplication();
      wa.initContainer(0);
      assertEquals(ApplicationState.INITING, wa.app.getApplicationState());
      wa.applicationInited();
      assertEquals(ApplicationState.RUNNING, wa.app.getApplicationState());

      assertEquals(ApplicationState.RUNNING, wa.app.getApplicationState());
      assertEquals(1, wa.app.getContainers().size());

      wa.appFinished();
      verify(wa.containerBus).handle(
          argThat(new ContainerKillMatcher(wa.containers.get(0)
              .getContainerId())));
      assertEquals(ApplicationState.FINISHING_CONTAINERS_WAIT,
          wa.app.getApplicationState());

      wa.initContainer(1);
      verify(wa.containerBus).handle(
          argThat(new ContainerKillMatcher(wa.containers.get(1)
              .getContainerId())));
      assertEquals(ApplicationState.FINISHING_CONTAINERS_WAIT,
          wa.app.getApplicationState());
      wa.containerFinished(1);
      assertEquals(ApplicationState.FINISHING_CONTAINERS_WAIT,
          wa.app.getApplicationState());

      wa.containerFinished(0);
      assertEquals(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
          wa.app.getApplicationState());
      verify(wa.localizerBus).handle(
          refEq(new ApplicationLocalizationEvent(
              LocalizationEventType.DESTROY_APPLICATION_RESOURCES,
              wa.app), "timestamp"));

      wa.initContainer(2);
      verify(wa.containerBus).handle(
          argThat(new ContainerKillMatcher(wa.containers.get(2)
              .getContainerId())));
      assertEquals(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
          wa.app.getApplicationState());
      wa.containerFinished(2);
      assertEquals(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
          wa.app.getApplicationState());

      wa.appResourcesCleanedup();
      assertEquals(ApplicationState.FINISHED, wa.app.getApplicationState());

      wa.initContainer(3);
      verify(wa.containerBus).handle(
          argThat(new ContainerKillMatcher(wa.containers.get(3)
              .getContainerId())));
      assertEquals(ApplicationState.FINISHED, wa.app.getApplicationState());
      wa.containerFinished(3);
      assertEquals(ApplicationState.FINISHED, wa.app.getApplicationState());
    } finally {
      if (wa != null)
        wa.finished();
    }
  }

//TODO Re-work after Application transitions are changed.
//  @Test
  @SuppressWarnings("unchecked")
  public void testAppFinishedOnIniting() {
    // AM may send a startContainer() - AM APP_FINIHSED processed after
    // APP_FINISHED on another NM
    WrappedApplication wa = null;
    try {
      wa = new WrappedApplication(1, 314159265358979L, "yak", 3);
      wa.initApplication();
      wa.initContainer(0);
      assertEquals(ApplicationState.INITING, wa.app.getApplicationState());
      assertEquals(1, wa.app.getContainers().size());

      reset(wa.localizerBus);
      wa.appFinished();

      verify(wa.containerBus).handle(
          argThat(new ContainerKillMatcher(wa.containers.get(0)
              .getContainerId())));
      assertEquals(ApplicationState.FINISHING_CONTAINERS_WAIT,
          wa.app.getApplicationState());

      wa.containerFinished(0);
      assertEquals(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
          wa.app.getApplicationState());
      verify(wa.localizerBus).handle(
          refEq(new ApplicationLocalizationEvent(
              LocalizationEventType.DESTROY_APPLICATION_RESOURCES, wa.app)));

      wa.initContainer(1);
      assertEquals(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
          wa.app.getApplicationState());
      assertEquals(0, wa.app.getContainers().size());

      wa.appResourcesCleanedup();
      assertEquals(ApplicationState.FINISHED, wa.app.getApplicationState());
    } finally {
      if (wa != null)
        wa.finished();
    }
  }

  @Test
  public void testNMTokenSecretManagerCleanup() {
    WrappedApplication wa = null;
    try {
      wa = new WrappedApplication(1, 314159265358979L, "yak", 1);
      wa.initApplication();
      wa.initContainer(0);
      assertEquals(ApplicationState.INITING, wa.app.getApplicationState());
      assertEquals(1, wa.app.getContainers().size());
      wa.appFinished();
      wa.containerFinished(0);
      wa.appResourcesCleanedup();
      assertEquals(ApplicationState.FINISHED, wa.app.getApplicationState());
      verify(wa.nmTokenSecretMgr).appFinished(eq(wa.appId));
    } finally {
      if (wa != null) {
        wa.finished();
      }
    }
  }

  private class ContainerKillMatcher implements
      ArgumentMatcher<ContainerEvent> {
    private ContainerId cId;

    public ContainerKillMatcher(ContainerId cId) {
      this.cId = cId;
    }

    @Override
    public boolean matches(ContainerEvent argument) {
      if (argument instanceof ContainerKillEvent) {
        ContainerKillEvent event = (ContainerKillEvent) argument;
        return event.getContainerID().equals(cId);
      }
      return false;
    }
  }

  private class ContainerInitMatcher implements
      ArgumentMatcher<ContainerEvent> {
    private ContainerId cId;

    public ContainerInitMatcher(ContainerId cId) {
      this.cId = cId;
    }

    @Override
    public boolean matches(ContainerEvent argument) {
      if (argument instanceof ContainerInitEvent) {
        ContainerInitEvent event = (ContainerInitEvent) argument;
        return event.getContainerID().equals(cId);
      }
      return false;
    }
  }

  @SuppressWarnings("unchecked")
  private class WrappedApplication {
    final DrainDispatcher dispatcher;
    final EventHandler<LocalizationEvent> localizerBus;
    final EventHandler<ContainersLauncherEvent> launcherBus;
    final EventHandler<ContainersMonitorEvent> monitorBus;
    final EventHandler<AuxServicesEvent> auxBus;
    final EventHandler<ContainerEvent> containerBus;
    final EventHandler<LogHandlerEvent> logAggregationBus;
    final String user;
    final List<Container> containers;
    final Context context;
    final Map<ContainerId, ContainerTokenIdentifier> containerTokenIdentifierMap;
    final NMTokenSecretManagerInNM nmTokenSecretMgr;
    final NMStateStoreService stateStoreService;
    final ApplicationId appId;
    final Application app;

    WrappedApplication(int id, long timestamp, String user, int numContainers) {
      Configuration conf = new Configuration();
      
      dispatcher = new DrainDispatcher();
      containerTokenIdentifierMap =
          new HashMap<ContainerId, ContainerTokenIdentifier>();
      dispatcher.init(conf);

      localizerBus = mock(EventHandler.class);
      launcherBus = mock(EventHandler.class);
      monitorBus = mock(EventHandler.class);
      auxBus = mock(EventHandler.class);
      containerBus = mock(EventHandler.class);
      logAggregationBus = mock(EventHandler.class);

      dispatcher.register(LocalizationEventType.class, localizerBus);
      dispatcher.register(ContainersLauncherEventType.class, launcherBus);
      dispatcher.register(ContainersMonitorEventType.class, monitorBus);
      dispatcher.register(AuxServicesEventType.class, auxBus);
      dispatcher.register(ContainerEventType.class, containerBus);
      dispatcher.register(LogHandlerEventType.class, logAggregationBus);

      nmTokenSecretMgr = mock(NMTokenSecretManagerInNM.class);
      stateStoreService = mock(NMStateStoreService.class);
      context = mock(Context.class);
      
      when(context.getContainerTokenSecretManager()).thenReturn(
        new NMContainerTokenSecretManager(conf));
      when(context.getApplicationACLsManager()).thenReturn(
        new ApplicationACLsManager(conf));
      when(context.getNMTokenSecretManager()).thenReturn(nmTokenSecretMgr);
      when(context.getNMStateStore()).thenReturn(stateStoreService);
      when(context.getConf()).thenReturn(conf);

      // Setting master key
      MasterKey masterKey = new MasterKeyPBImpl();
      masterKey.setKeyId(123);
      masterKey.setBytes(ByteBuffer.wrap(new byte[] { (new Integer(123)
        .byteValue()) }));
      context.getContainerTokenSecretManager().setMasterKey(masterKey);
      
      this.user = user;
      this.appId = BuilderUtils.newApplicationId(timestamp, id);

      app = new ApplicationImpl(
          dispatcher, this.user, appId, null, context);
      containers = new ArrayList<Container>();
      for (int i = 0; i < numContainers; i++) {
        Container container = createMockedContainer(this.appId, i);
        containers.add(container);
        long currentTime = System.currentTimeMillis();
        ContainerTokenIdentifier identifier =
            new ContainerTokenIdentifier(container.getContainerId(), "", "",
              null, currentTime + 2000, masterKey.getKeyId(), currentTime,
              Priority.newInstance(0), 0);
        containerTokenIdentifierMap
          .put(identifier.getContainerID(), identifier);
        context.getContainerTokenSecretManager().startContainerSuccessful(
          identifier);
        Assert.assertFalse(context.getContainerTokenSecretManager()
          .isValidStartContainerRequest(identifier));
      }

      dispatcher.start();
    }

    private void drainDispatcherEvents() {
      dispatcher.await();
    }

    public void finished() {
      dispatcher.stop();
    }

    public void initApplication() {
      app.handle(new ApplicationInitEvent(appId,
          new HashMap<ApplicationAccessType, String>()));
    }

    public void initContainer(int containerNum) {
      if (containerNum == -1) {
        for (int i = 0; i < containers.size(); i++) {
          app.handle(new ApplicationContainerInitEvent(containers.get(i)));
        }
      } else {
        app.handle(new ApplicationContainerInitEvent(containers.get(containerNum)));
      }
      drainDispatcherEvents();
    }

    public void containerFinished(int containerNum) {
      app.handle(new ApplicationContainerFinishedEvent(containers.get(
          containerNum).cloneAndGetContainerStatus(), 0));
      drainDispatcherEvents();
    }

    public void applicationInited() {
      app.handle(new ApplicationInitedEvent(appId));
      drainDispatcherEvents();
    }

    public long applicationLogInited() {
      ApplicationEvent appEvt = new ApplicationEvent(app.getAppId(),
          ApplicationEventType.APPLICATION_LOG_HANDLING_INITED);
      app.handle(appEvt);
      return appEvt.getTimestamp();
    }

    public void appFinished() {
      app.handle(new ApplicationFinishEvent(appId,
          "Finish Application"));
      drainDispatcherEvents();
    }

    public void appResourcesCleanedup() {
      app.handle(new ApplicationEvent(appId,
          ApplicationEventType.APPLICATION_RESOURCES_CLEANEDUP));
      drainDispatcherEvents();
    }
    
    public ContainerTokenIdentifier getContainerTokenIdentifier(
        ContainerId containerId) {
      return this.containerTokenIdentifierMap.get(containerId);
    }
  }

  private Container createMockedContainer(ApplicationId appId, int containerId) {
    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(appId, 1);
    ContainerId cId = BuilderUtils.newContainerId(appAttemptId, containerId);
    Container c = mock(Container.class);
    when(c.getContainerId()).thenReturn(cId);
    ContainerLaunchContext launchContext = mock(ContainerLaunchContext.class);
    when(c.getLaunchContext()).thenReturn(launchContext);
    when(launchContext.getApplicationACLs()).thenReturn(
        new HashMap<ApplicationAccessType, String>());
    when(c.cloneAndGetContainerStatus()).thenReturn(
        BuilderUtils.newContainerStatus(cId,
            ContainerState.NEW, "", 0, Resource.newInstance(1024, 1)));
    return c;
  }
}
