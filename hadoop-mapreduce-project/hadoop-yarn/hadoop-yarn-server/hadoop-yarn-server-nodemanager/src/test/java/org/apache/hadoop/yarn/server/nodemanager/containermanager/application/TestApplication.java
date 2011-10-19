package org.apache.hadoop.yarn.server.nodemanager.containermanager.application;

import static org.mockito.Mockito.*;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.event.LogAggregatorEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.event.LogAggregatorEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorEventType;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;
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
                .getContainerID())));
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
              .getContainerID())));

      wa.initContainer(1);
      wa.initContainer(2);
      assertEquals(ApplicationState.RUNNING, wa.app.getApplicationState());
      assertEquals(3, wa.app.getContainers().size());

      for (int i = 1; i < wa.containers.size(); i++) {
        verify(wa.containerBus).handle(
            argThat(new ContainerInitMatcher(wa.containers.get(i)
                .getContainerID())));
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
                .getContainerID())));
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
              LocalizationEventType.DESTROY_APPLICATION_RESOURCES, wa.app)));

      verify(wa.auxBus).handle(
          refEq(new AuxServicesEvent(
              AuxServicesEventType.APPLICATION_STOP, wa.appId)));

      wa.appResourcesCleanedup();
      assertEquals(ApplicationState.FINISHED, wa.app.getApplicationState());

    } finally {
      if (wa != null)
        wa.finished();
    }
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
              LocalizationEventType.DESTROY_APPLICATION_RESOURCES, wa.app)));

      wa.appResourcesCleanedup();
      assertEquals(ApplicationState.FINISHED, wa.app.getApplicationState());
    } finally {
      if (wa != null)
        wa.finished();
    }
  }

//TODO Re-work after Application transitions are changed.
//  @Test
  @SuppressWarnings("unchecked")
  public void testStartContainerAfterAppFinished() {
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
              LocalizationEventType.DESTROY_APPLICATION_RESOURCES, wa.app)));

      wa.appResourcesCleanedup();
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
              .getContainerID())));
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

  private class ContainerKillMatcher extends ArgumentMatcher<ContainerEvent> {
    private ContainerId cId;

    public ContainerKillMatcher(ContainerId cId) {
      this.cId = cId;
    }

    @Override
    public boolean matches(Object argument) {
      if (argument instanceof ContainerKillEvent) {
        ContainerKillEvent event = (ContainerKillEvent) argument;
        return event.getContainerID().equals(cId);
      }
      return false;
    }
  }

  private class ContainerInitMatcher extends ArgumentMatcher<ContainerEvent> {
    private ContainerId cId;

    public ContainerInitMatcher(ContainerId cId) {
      this.cId = cId;
    }

    @Override
    public boolean matches(Object argument) {
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
    final EventHandler<LogAggregatorEvent> logAggregationBus;
    final String user;
    final List<Container> containers;

    final ApplicationId appId;
    final Application app;

    WrappedApplication(int id, long timestamp, String user, int numContainers) {
      dispatcher = new DrainDispatcher();
      dispatcher.init(null);

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
      dispatcher.register(LogAggregatorEventType.class, logAggregationBus);

      this.user = user;
      this.appId = BuilderUtils.newApplicationId(timestamp, id);

      app = new ApplicationImpl(dispatcher, this.user, appId, null);
      containers = new ArrayList<Container>();
      for (int i = 0; i < numContainers; i++) {
        containers.add(createMockedContainer(this.appId, i));
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
      app.handle(new ApplicationInitEvent(appId));
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
          containerNum).getContainerID()));
      drainDispatcherEvents();
    }

    public void applicationInited() {
      app.handle(new ApplicationInitedEvent(appId));
      drainDispatcherEvents();
    }

    public void appFinished() {
      app.handle(new ApplicationEvent(appId,
          ApplicationEventType.FINISH_APPLICATION));
      drainDispatcherEvents();
    }

    public void appResourcesCleanedup() {
      app.handle(new ApplicationEvent(appId,
          ApplicationEventType.APPLICATION_RESOURCES_CLEANEDUP));
      drainDispatcherEvents();
    }
  }

  private Container createMockedContainer(ApplicationId appId, int containerId) {
    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(appId, 1);
    ContainerId cId = BuilderUtils.newContainerId(appAttemptId, containerId);
    Container c = mock(Container.class);
    when(c.getContainerID()).thenReturn(cId);
    return c;
  }
}
