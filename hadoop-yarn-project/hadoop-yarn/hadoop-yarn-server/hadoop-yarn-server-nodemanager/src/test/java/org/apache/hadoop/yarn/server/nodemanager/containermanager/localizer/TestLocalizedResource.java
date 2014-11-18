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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerResourceRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceLocalizedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceReleaseEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceRequestEvent;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

public class TestLocalizedResource {

  static ContainerId getMockContainer(long id) {
    ApplicationId appId = mock(ApplicationId.class);
    when(appId.getClusterTimestamp()).thenReturn(314159265L);
    when(appId.getId()).thenReturn(3);
    ApplicationAttemptId appAttemptId = mock(ApplicationAttemptId.class);
    when(appAttemptId.getApplicationId()).thenReturn(appId);
    when(appAttemptId.getAttemptId()).thenReturn(0);
    ContainerId container = mock(ContainerId.class);
    when(container.getContainerId()).thenReturn(id);
    when(container.getApplicationAttemptId()).thenReturn(appAttemptId);
    return container;
  }

  @Test
  @SuppressWarnings("unchecked") // mocked generic
  public void testNotification() throws Exception {
    DrainDispatcher dispatcher = new DrainDispatcher();
    dispatcher.init(new Configuration());
    try {
      dispatcher.start();
      EventHandler<ContainerEvent> containerBus = mock(EventHandler.class);
      EventHandler<LocalizerEvent> localizerBus = mock(EventHandler.class);
      dispatcher.register(ContainerEventType.class, containerBus);
      dispatcher.register(LocalizerEventType.class, localizerBus);

      // mock resource
      LocalResource apiRsrc = createMockResource();

      final ContainerId container0 = getMockContainer(0L);
      final Credentials creds0 = new Credentials();
      final LocalResourceVisibility vis0 = LocalResourceVisibility.PRIVATE;
      final LocalizerContext ctxt0 =
        new LocalizerContext("yak", container0, creds0);
      LocalResourceRequest rsrcA = new LocalResourceRequest(apiRsrc);
      LocalizedResource local = new LocalizedResource(rsrcA, dispatcher);
      local.handle(new ResourceRequestEvent(rsrcA, vis0, ctxt0));
      dispatcher.await();

      // Register C0, verify request event
      LocalizerEventMatcher matchesL0Req =
        new LocalizerEventMatcher(container0, creds0, vis0,
            LocalizerEventType.REQUEST_RESOURCE_LOCALIZATION);
      verify(localizerBus).handle(argThat(matchesL0Req));
      assertEquals(ResourceState.DOWNLOADING, local.getState());

      // Register C1, verify request event
      final Credentials creds1 = new Credentials();
      final ContainerId container1 = getMockContainer(1L);
      final LocalizerContext ctxt1 =
        new LocalizerContext("yak", container1, creds1);
      final LocalResourceVisibility vis1 = LocalResourceVisibility.PUBLIC;
      local.handle(new ResourceRequestEvent(rsrcA, vis1, ctxt1));
      dispatcher.await();
      LocalizerEventMatcher matchesL1Req =
        new LocalizerEventMatcher(container1, creds1, vis1,
            LocalizerEventType.REQUEST_RESOURCE_LOCALIZATION);
      verify(localizerBus).handle(argThat(matchesL1Req));

      // Release C0 container localization, verify no notification
      local.handle(new ResourceReleaseEvent(rsrcA, container0));
      dispatcher.await();
      verify(containerBus, never()).handle(isA(ContainerEvent.class));
      assertEquals(ResourceState.DOWNLOADING, local.getState());

      // Release C1 container localization, verify no notification
      local.handle(new ResourceReleaseEvent(rsrcA, container1));
      dispatcher.await();
      verify(containerBus, never()).handle(isA(ContainerEvent.class));
      assertEquals(ResourceState.DOWNLOADING, local.getState());

      // Register C2, C3
      final ContainerId container2 = getMockContainer(2L);
      final LocalResourceVisibility vis2 = LocalResourceVisibility.PRIVATE;
      final Credentials creds2 = new Credentials();
      final LocalizerContext ctxt2 =
        new LocalizerContext("yak", container2, creds2);

      final ContainerId container3 = getMockContainer(3L);
      final LocalResourceVisibility vis3 = LocalResourceVisibility.PRIVATE;
      final Credentials creds3 = new Credentials();
      final LocalizerContext ctxt3 =
        new LocalizerContext("yak", container3, creds3);

      local.handle(new ResourceRequestEvent(rsrcA, vis2, ctxt2));
      local.handle(new ResourceRequestEvent(rsrcA, vis3, ctxt3));
      dispatcher.await();
      LocalizerEventMatcher matchesL2Req =
        new LocalizerEventMatcher(container2, creds2, vis2,
            LocalizerEventType.REQUEST_RESOURCE_LOCALIZATION);
      verify(localizerBus).handle(argThat(matchesL2Req));
      LocalizerEventMatcher matchesL3Req =
        new LocalizerEventMatcher(container3, creds3, vis3,
            LocalizerEventType.REQUEST_RESOURCE_LOCALIZATION);
      verify(localizerBus).handle(argThat(matchesL3Req));

      // Successful localization. verify notification C2, C3
      Path locA = new Path("file:///cache/rsrcA");
      local.handle(new ResourceLocalizedEvent(rsrcA, locA, 10));
      dispatcher.await();
      ContainerEventMatcher matchesC2Localized =
        new ContainerEventMatcher(container2,
            ContainerEventType.RESOURCE_LOCALIZED);
      ContainerEventMatcher matchesC3Localized =
        new ContainerEventMatcher(container3,
            ContainerEventType.RESOURCE_LOCALIZED);
      verify(containerBus).handle(argThat(matchesC2Localized));
      verify(containerBus).handle(argThat(matchesC3Localized));
      assertEquals(ResourceState.LOCALIZED, local.getState());

      // Register C4, verify notification
      final ContainerId container4 = getMockContainer(4L);
      final Credentials creds4 = new Credentials();
      final LocalizerContext ctxt4 =
        new LocalizerContext("yak", container4, creds4);
      final LocalResourceVisibility vis4 = LocalResourceVisibility.PRIVATE;
      local.handle(new ResourceRequestEvent(rsrcA, vis4, ctxt4));
      dispatcher.await();
      ContainerEventMatcher matchesC4Localized =
        new ContainerEventMatcher(container4,
            ContainerEventType.RESOURCE_LOCALIZED);
      verify(containerBus).handle(argThat(matchesC4Localized));
      assertEquals(ResourceState.LOCALIZED, local.getState());
    } finally {
      dispatcher.stop();
    }
  }

  static LocalResource createMockResource() {
    // mock rsrc location
    org.apache.hadoop.yarn.api.records.URL uriA =
      mock(org.apache.hadoop.yarn.api.records.URL.class);
    when(uriA.getScheme()).thenReturn("file");
    when(uriA.getHost()).thenReturn(null);
    when(uriA.getFile()).thenReturn("/localA/rsrc");

    LocalResource apiRsrc = mock(LocalResource.class);
    when(apiRsrc.getResource()).thenReturn(uriA);
    when(apiRsrc.getTimestamp()).thenReturn(4344L);
    when(apiRsrc.getType()).thenReturn(LocalResourceType.FILE);
    return apiRsrc;
  }


  static class LocalizerEventMatcher extends ArgumentMatcher<LocalizerEvent> {
    Credentials creds;
    LocalResourceVisibility vis;
    private final ContainerId idRef;
    private final LocalizerEventType type;

    public LocalizerEventMatcher(ContainerId idRef, Credentials creds,
        LocalResourceVisibility vis, LocalizerEventType type) {
      this.vis = vis;
      this.type = type;
      this.creds = creds;
      this.idRef = idRef;
    }
    @Override
    public boolean matches(Object o) {
      if (!(o instanceof LocalizerResourceRequestEvent)) return false;
      LocalizerResourceRequestEvent evt = (LocalizerResourceRequestEvent) o;
      return idRef == evt.getContext().getContainerId()
          && type == evt.getType()
          && vis == evt.getVisibility()
          && creds == evt.getContext().getCredentials();
    }
  }

  static class ContainerEventMatcher extends ArgumentMatcher<ContainerEvent> {
    private final ContainerId idRef;
    private final ContainerEventType type;
    public ContainerEventMatcher(ContainerId idRef, ContainerEventType type) {
      this.idRef = idRef;
      this.type = type;
    }
    @Override
    public boolean matches(Object o) {
      if (!(o instanceof ContainerEvent)) return false;
      ContainerEvent evt = (ContainerEvent) o;
      return idRef == evt.getContainerID() && type == evt.getType();
    }
  }

}
