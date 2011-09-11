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
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationCleanupEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorEventType;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

public class TestContainer {

  final NodeManagerMetrics metrics = NodeManagerMetrics.create();

  
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
      Map<Path, String> localPaths = wc.localizeResources();

      // all resources should be localized
      assertEquals(ContainerState.LOCALIZED, wc.c.getContainerState());
      for (Entry<Path,String> loc : wc.c.getLocalizedResources().entrySet()) {
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
  public void testCleanupOnFailure() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(10, 314159265358979L, 4344, "yak");
      wc.initContainer();
      wc.localizeResources();
      wc.launchContainer();
      reset(wc.localizerBus);
      wc.containerFailed(ExitCode.KILLED.getExitCode());
      assertEquals(ContainerState.EXITED_WITH_FAILURE, 
          wc.c.getContainerState());
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
  public void testCleanupOnSuccess() throws Exception {
    WrappedContainer wc = null;
    try {
      wc = new WrappedContainer(11, 314159265358979L, 4344, "yak");
      wc.initContainer();
      wc.localizeResources();
      wc.launchContainer();
      reset(wc.localizerBus);
      wc.containerSuccessful();
      assertEquals(ContainerState.EXITED_WITH_SUCCESS,
          wc.c.getContainerState());
      
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
      wc.containerKilledOnRequest();
      
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
              && wcf.cId == evt.getContainer().getContainerID();
          }
        };
      verify(wc.launcherBus).handle(argThat(matchesLaunchReq));
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
            LocalResourceVisibility.APPLICATION));
    verify(wc.localizerBus).handle(argThat(matchesReq));
  }

  private static class ResourcesReleasedMatcher extends
      ArgumentMatcher<LocalizationEvent> {
    final HashSet<LocalResourceRequest> resources =
        new HashSet<LocalResourceRequest>();

    ResourcesReleasedMatcher(Map<String, LocalResource> allResources,
        EnumSet<LocalResourceVisibility> vis) throws URISyntaxException {
      for (Entry<String, LocalResource> e : allResources.entrySet()) {
        if (vis.contains(e.getValue().getVisibility())) {
          resources.add(new LocalResourceRequest(e.getValue()));
        }
      }
    }

    @Override
    public boolean matches(Object o) {
      if (!(o instanceof ContainerLocalizationCleanupEvent)) {
        return false;
      }
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
            r.nextInt(1024) + 1024L, r.nextInt(1024) + 2048L);
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

  private Container newContainer(Dispatcher disp, ContainerLaunchContext ctx) {
    return new ContainerImpl(disp, ctx, null, metrics);
  }
  
  @SuppressWarnings("unchecked")
  private class WrappedContainer {
    final DrainDispatcher dispatcher;
    final EventHandler<LocalizationEvent> localizerBus;
    final EventHandler<ContainersLauncherEvent> launcherBus;
    final EventHandler<ContainersMonitorEvent> monitorBus;
    final EventHandler<AuxServicesEvent> auxBus;

    final ContainerLaunchContext ctxt;
    final ContainerId cId;
    final Container c;
    final Map<String, LocalResource> localResources;
    final Map<String, ByteBuffer> serviceData;
    final String user;

    WrappedContainer(int appId, long timestamp, int id, String user) {
      this(appId, timestamp, id, user, true, false);
    }

    WrappedContainer(int appId, long timestamp, int id, String user,
        boolean withLocalRes, boolean withServiceData) {
      dispatcher = new DrainDispatcher();
      dispatcher.init(null);

      localizerBus = mock(EventHandler.class);
      launcherBus = mock(EventHandler.class);
      monitorBus = mock(EventHandler.class);
      auxBus = mock(EventHandler.class);
      dispatcher.register(LocalizationEventType.class, localizerBus);
      dispatcher.register(ContainersLauncherEventType.class, launcherBus);
      dispatcher.register(ContainersMonitorEventType.class, monitorBus);
      dispatcher.register(AuxServicesEventType.class, auxBus);
      this.user = user;

      ctxt = mock(ContainerLaunchContext.class);
      cId = BuilderUtils.newContainerId(appId, 1, timestamp, id);
      when(ctxt.getUser()).thenReturn(this.user);
      when(ctxt.getContainerId()).thenReturn(cId);

      Resource resource = BuilderUtils.newResource(1024);
      when(ctxt.getResource()).thenReturn(resource);

      if (withLocalRes) {
        Random r = new Random();
        long seed = r.nextLong();
        r.setSeed(seed);
        System.out.println("WrappedContainerLocalResource seed: " + seed);
        localResources = createLocalResources(r);
      } else {
        localResources = Collections.<String, LocalResource> emptyMap();
      }
      when(ctxt.getAllLocalResources()).thenReturn(localResources);

      if (withServiceData) {
        Random r = new Random();
        long seed = r.nextLong();
        r.setSeed(seed);
        System.out.println("ServiceData seed: " + seed);
        serviceData = createServiceData(r);
      } else {
        serviceData = Collections.<String, ByteBuffer> emptyMap();
      }
      when(ctxt.getAllServiceData()).thenReturn(serviceData);

      c = newContainer(dispatcher, ctxt);
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

    public Map<Path, String> localizeResources() throws URISyntaxException {
      Path cache = new Path("file:///cache");
      Map<Path, String> localPaths = new HashMap<Path, String>();
      for (Entry<String, LocalResource> rsrc : localResources.entrySet()) {
        assertEquals(ContainerState.LOCALIZING, c.getContainerState());
        LocalResourceRequest req = new LocalResourceRequest(rsrc.getValue());
        Path p = new Path(cache, rsrc.getKey());
        localPaths.put(p, rsrc.getKey());
        // rsrc copied to p
        c.handle(new ContainerResourceLocalizedEvent(c.getContainerID(), 
                 req, p));
      }
      drainDispatcherEvents();
      return localPaths;
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

    public void containerFailed(int exitCode) {
      c.handle(new ContainerExitEvent(cId,
          ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, exitCode));
      drainDispatcherEvents();
    }

    public void killContainer() {
      c.handle(new ContainerKillEvent(cId, "KillRequest"));
      drainDispatcherEvents();
    }

    public void containerKilledOnRequest() {
      c.handle(new ContainerExitEvent(cId,
          ContainerEventType.CONTAINER_KILLED_ON_REQUEST, ExitCode.KILLED
              .getExitCode()));
      drainDispatcherEvents();
    }
  }
}
