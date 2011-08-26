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

import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import java.net.URISyntaxException;

import java.nio.ByteBuffer;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;

import org.junit.Test;
import static org.junit.Assert.*;

import org.mockito.ArgumentMatcher;
import static org.mockito.Mockito.*;

public class TestContainer {

  final NodeManagerMetrics metrics = NodeManagerMetrics.create();

  /**
   * Verify correct container request events sent to localizer.
   */
  @Test
  @SuppressWarnings("unchecked") // mocked generic
  public void testLocalizationRequest() throws Exception {
    DrainDispatcher dispatcher = new DrainDispatcher();
    dispatcher.init(null);
    try {
      dispatcher.start();
      EventHandler<LocalizationEvent> localizerBus = mock(EventHandler.class);
      dispatcher.register(LocalizationEventType.class, localizerBus);
      // null serviceData; no registered AuxServicesEventType handler

      ContainerLaunchContext ctxt = mock(ContainerLaunchContext.class);
      ContainerId cId = getMockContainerId(7, 314159265358979L, 4344);
      when(ctxt.getUser()).thenReturn("yak");
      when(ctxt.getContainerId()).thenReturn(cId);

      Random r = new Random();
      long seed = r.nextLong();
      r.setSeed(seed);
      System.out.println("testLocalizationRequest seed: " + seed);
      final Map<String,LocalResource> localResources = createLocalResources(r);
      when(ctxt.getAllLocalResources()).thenReturn(localResources);

      final Container c = newContainer(dispatcher, ctxt);
      assertEquals(ContainerState.NEW, c.getContainerState());

      // Verify request for public/private resources to localizer
      c.handle(new ContainerEvent(cId, ContainerEventType.INIT_CONTAINER));
      dispatcher.await();
      ContainerReqMatcher matchesPublicReq =
        new ContainerReqMatcher(localResources,
            EnumSet.of(LocalResourceVisibility.PUBLIC));
      ContainerReqMatcher matchesPrivateReq =
        new ContainerReqMatcher(localResources,
            EnumSet.of(LocalResourceVisibility.PRIVATE));
      ContainerReqMatcher matchesAppReq =
        new ContainerReqMatcher(localResources,
            EnumSet.of(LocalResourceVisibility.APPLICATION));
      verify(localizerBus).handle(argThat(matchesPublicReq));
      verify(localizerBus).handle(argThat(matchesPrivateReq));
      verify(localizerBus).handle(argThat(matchesAppReq));
      assertEquals(ContainerState.LOCALIZING, c.getContainerState());
    } finally {
      dispatcher.stop();
    }
  }

  /**
   * Verify container launch when all resources already cached.
   */
  @Test
  @SuppressWarnings("unchecked") // mocked generic
  public void testLocalizationLaunch() throws Exception {
    DrainDispatcher dispatcher = new DrainDispatcher();
    dispatcher.init(null);
    try {
      dispatcher.start();
      EventHandler<LocalizationEvent> localizerBus = mock(EventHandler.class);
      dispatcher.register(LocalizationEventType.class, localizerBus);
      EventHandler<ContainersLauncherEvent> launcherBus =
        mock(EventHandler.class);
      dispatcher.register(ContainersLauncherEventType.class, launcherBus);
      // null serviceData; no registered AuxServicesEventType handler

      ContainerLaunchContext ctxt = mock(ContainerLaunchContext.class);
      ContainerId cId = getMockContainerId(8, 314159265358979L, 4344);
      when(ctxt.getUser()).thenReturn("yak");
      when(ctxt.getContainerId()).thenReturn(cId);

      Random r = new Random();
      long seed = r.nextLong();
      r.setSeed(seed);
      System.out.println("testLocalizationLaunch seed: " + seed);
      final Map<String,LocalResource> localResources = createLocalResources(r);
      when(ctxt.getAllLocalResources()).thenReturn(localResources);
      final Container c = newContainer(dispatcher, ctxt);
      assertEquals(ContainerState.NEW, c.getContainerState());

      c.handle(new ContainerEvent(cId, ContainerEventType.INIT_CONTAINER));
      dispatcher.await();

      // Container prepared for localization events
      Path cache = new Path("file:///cache");
      Map<Path,String> localPaths = new HashMap<Path,String>();
      for (Entry<String,LocalResource> rsrc : localResources.entrySet()) {
        assertEquals(ContainerState.LOCALIZING, c.getContainerState());
        LocalResourceRequest req = new LocalResourceRequest(rsrc.getValue());
        Path p = new Path(cache, rsrc.getKey());
        localPaths.put(p, rsrc.getKey());
        // rsrc copied to p
        c.handle(new ContainerResourceLocalizedEvent(c.getContainerID(), req, p));
      }
      dispatcher.await();

      // all resources should be localized
      assertEquals(ContainerState.LOCALIZED, c.getContainerState());
      for (Entry<Path,String> loc : c.getLocalizedResources().entrySet()) {
        assertEquals(localPaths.remove(loc.getKey()), loc.getValue());
      }
      assertTrue(localPaths.isEmpty());

      // verify container launch
      ArgumentMatcher<ContainersLauncherEvent> matchesContainerLaunch =
        new ArgumentMatcher<ContainersLauncherEvent>() {
          @Override
          public boolean matches(Object o) {
            ContainersLauncherEvent launchEvent = (ContainersLauncherEvent) o;
            return c == launchEvent.getContainer();
          }
        };
      verify(launcherBus).handle(argThat(matchesContainerLaunch));
    } finally {
      dispatcher.stop();
    }
  }

  /**
   * Verify serviceData correctly sent.
   */
  @Test
  @SuppressWarnings("unchecked") // mocked generic
  public void testServiceData() throws Exception {
    DrainDispatcher dispatcher = new DrainDispatcher();
    dispatcher.init(null);
    dispatcher.start();
    try {
      EventHandler<LocalizationEvent> localizerBus = mock(EventHandler.class);
      dispatcher.register(LocalizationEventType.class, localizerBus);
      EventHandler<AuxServicesEvent> auxBus = mock(EventHandler.class);
      dispatcher.register(AuxServicesEventType.class, auxBus);
      EventHandler<ContainersLauncherEvent> launchBus = mock(EventHandler.class);
      dispatcher.register(ContainersLauncherEventType.class, launchBus);

      ContainerLaunchContext ctxt = mock(ContainerLaunchContext.class);
      final ContainerId cId = getMockContainerId(9, 314159265358979L, 4344);
      when(ctxt.getUser()).thenReturn("yak");
      when(ctxt.getContainerId()).thenReturn(cId);
      when(ctxt.getAllLocalResources()).thenReturn(
          Collections.<String,LocalResource>emptyMap());

      Random r = new Random();
      long seed = r.nextLong();
      r.setSeed(seed);
      System.out.println("testServiceData seed: " + seed);
      final Map<String,ByteBuffer> serviceData = createServiceData(r);
      when(ctxt.getAllServiceData()).thenReturn(serviceData);

      final Container c = newContainer(dispatcher, ctxt);
      assertEquals(ContainerState.NEW, c.getContainerState());

      // Verify propagation of service data to AuxServices
      c.handle(new ContainerEvent(cId, ContainerEventType.INIT_CONTAINER));
      dispatcher.await();
      for (final Map.Entry<String,ByteBuffer> e : serviceData.entrySet()) {
        ArgumentMatcher<AuxServicesEvent> matchesServiceReq =
          new ArgumentMatcher<AuxServicesEvent>() {
            @Override
            public boolean matches(Object o) {
              AuxServicesEvent evt = (AuxServicesEvent) o;
              return e.getKey().equals(evt.getServiceID())
                && 0 == e.getValue().compareTo(evt.getServiceData());
            }
          };
        verify(auxBus).handle(argThat(matchesServiceReq));
      }

      // verify launch on empty resource request
      ArgumentMatcher<ContainersLauncherEvent> matchesLaunchReq =
        new ArgumentMatcher<ContainersLauncherEvent>() {
          @Override
          public boolean matches(Object o) {
            ContainersLauncherEvent evt = (ContainersLauncherEvent) o;
            return evt.getType() == ContainersLauncherEventType.LAUNCH_CONTAINER
              && cId == evt.getContainer().getContainerID();
          }
        };
      verify(launchBus).handle(argThat(matchesLaunchReq));
    } finally {
      dispatcher.stop();
    }
  }

  // Accept iff the resource request payload matches.
  static class ContainerReqMatcher extends ArgumentMatcher<LocalizationEvent> {
    final HashSet<LocalResourceRequest> resources =
      new HashSet<LocalResourceRequest>();
    ContainerReqMatcher(Map<String,LocalResource> allResources,
        EnumSet<LocalResourceVisibility> vis) throws URISyntaxException {
      for (Entry<String,LocalResource> e : allResources.entrySet()) {
        if (vis.contains(e.getValue().getVisibility())) {
          resources.add(new LocalResourceRequest(e.getValue()));
        }
      }
    }
    @Override
    public boolean matches(Object o) {
      ContainerLocalizationRequestEvent evt = (ContainerLocalizationRequestEvent) o;
      final HashSet<LocalResourceRequest> expected =
        new HashSet<LocalResourceRequest>(resources);
      for (LocalResourceRequest rsrc : evt.getRequestedResources()) {
        if (!expected.remove(rsrc)) {
          return false;
        }
      }
      return expected.isEmpty();
    }
  }

  static Entry<String,LocalResource> getMockRsrc(Random r,
      LocalResourceVisibility vis) {
    LocalResource rsrc = mock(LocalResource.class);

    String name = Long.toHexString(r.nextLong());
    URL uri = mock(org.apache.hadoop.yarn.api.records.URL.class);
    when(uri.getScheme()).thenReturn("file");
    when(uri.getHost()).thenReturn(null);
    when(uri.getFile()).thenReturn("/local/" + vis + "/" + name);

    when(rsrc.getResource()).thenReturn(uri);
    when(rsrc.getSize()).thenReturn(r.nextInt(1024) + 1024L);
    when(rsrc.getTimestamp()).thenReturn(r.nextInt(1024) + 2048L);
    when(rsrc.getType()).thenReturn(LocalResourceType.FILE);
    when(rsrc.getVisibility()).thenReturn(vis);

    return new SimpleEntry<String,LocalResource>(name, rsrc);
  }

  static Map<String,LocalResource> createLocalResources(Random r) {
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

  static ContainerId getMockContainerId(int appId, long timestamp, int id) {
    ApplicationId aId = mock(ApplicationId.class);
    when(aId.getId()).thenReturn(appId);
    when(aId.getClusterTimestamp()).thenReturn(timestamp);
    ContainerId cId = mock(ContainerId.class);
    when(cId.getId()).thenReturn(id);
    when(cId.getAppId()).thenReturn(aId);
    return cId;
  }

  static Map<String,ByteBuffer> createServiceData(Random r) {
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

  Container newContainer(Dispatcher disp, ContainerLaunchContext ctx) {
    return new ContainerImpl(disp, ctx, null, metrics);
  }
}
