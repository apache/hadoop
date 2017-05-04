/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.server.appmaster.timelineservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity.Identifier;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.client.api.impl.TimelineV2ClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.ApplicationState;
import org.apache.slider.api.resource.Artifact;
import org.apache.slider.api.resource.Component;
import org.apache.slider.api.resource.Container;
import org.apache.slider.api.resource.ContainerState;
import org.apache.slider.api.resource.PlacementPolicy;
import org.apache.slider.api.resource.Resource;
import org.apache.slider.server.appmaster.actions.ActionStopSlider;
import org.apache.slider.server.appmaster.state.AppState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test class for ServiceTimelinePublisher.
 */
public class TestServiceTimelinePublisher {
  private TimelineV2Client timelineClient;
  private Configuration config;
  private ServiceTimelinePublisher serviceTimelinePublisher;
  private static String SERVICE_NAME = "HBASE";
  private static String SERVICEID = "application_1490093646524_0005";
  private static String ARTIFACTID = "ARTIFACTID";
  private static String COMPONENT_NAME = "DEFAULT";
  private static String CONTAINER_ID =
      "container_e02_1490093646524_0005_01_000001";
  private static String CONTAINER_IP =
      "localhost";
  private static String CONTAINER_HOSTNAME =
      "cnl124-localhost.site";
  private static String CONTAINER_BAREHOST =
      "localhost.com";

  @Before
  public void setUp() throws Exception {
    config = new Configuration();
    config.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    config.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
    timelineClient =
        new DummyTimelineClient(ApplicationId.fromString(SERVICEID));
    serviceTimelinePublisher = new ServiceTimelinePublisher(timelineClient);
    timelineClient.init(config);
    serviceTimelinePublisher.init(config);
    timelineClient.start();
    serviceTimelinePublisher.start();
  }

  @After
  public void tearDown() throws Exception {
    if (serviceTimelinePublisher != null) {
      serviceTimelinePublisher.stop();
    }
    if (timelineClient != null) {
      timelineClient.stop();
    }
  }

  @Test
  public void testServiceAttemptEntity() {
    AppState appState = createMockAppState();
    int exitCode = 0;
    String message = "Stopped by user";
    ActionStopSlider stopAction = mock(ActionStopSlider.class);
    when(stopAction.getExitCode()).thenReturn(exitCode);
    when(stopAction.getFinalApplicationStatus())
        .thenReturn(FinalApplicationStatus.SUCCEEDED);
    when(stopAction.getMessage()).thenReturn(message);

    serviceTimelinePublisher.serviceAttemptRegistered(appState);

    Collection<TimelineEntity> lastPublishedEntities =
        ((DummyTimelineClient) timelineClient).getLastPublishedEntities();
    // 2 entities because during registration component also registered.
    assertEquals(2, lastPublishedEntities.size());
    for (TimelineEntity timelineEntity : lastPublishedEntities) {
      if (timelineEntity.getType() == SliderTimelineEntityType.COMPONENT
          .toString()) {
        verifyComponentTimelineEntity(timelineEntity);
      } else {
        verifyServiceAttemptTimelineEntity(timelineEntity, 0, null, true);
      }
    }

    serviceTimelinePublisher.serviceAttemptUnregistered(appState, stopAction);
    lastPublishedEntities =
        ((DummyTimelineClient) timelineClient).getLastPublishedEntities();
    for (TimelineEntity timelineEntity : lastPublishedEntities) {
      if (timelineEntity.getType() == SliderTimelineEntityType.SERVICE_ATTEMPT
          .toString()) {
        verifyServiceAttemptTimelineEntity(timelineEntity, exitCode, message,
            false);
      }
    }
  }

  @Test
  public void testComponentInstanceEntity() {
    Container container = new Container();
    container.id(CONTAINER_ID).ip(CONTAINER_IP).bareHost(CONTAINER_BAREHOST)
        .hostname(CONTAINER_HOSTNAME).state(ContainerState.INIT)
        .launchTime(new Date());
    serviceTimelinePublisher.componentInstanceStarted(container,
        COMPONENT_NAME);

    Collection<TimelineEntity> lastPublishedEntities =
        ((DummyTimelineClient) timelineClient).getLastPublishedEntities();
    assertEquals(1, lastPublishedEntities.size());
    TimelineEntity entity = lastPublishedEntities.iterator().next();

    assertEquals(1, entity.getEvents().size());
    assertEquals(CONTAINER_ID, entity.getId());
    assertEquals(CONTAINER_BAREHOST,
        entity.getInfo().get(SliderTimelineMetricsConstants.BARE_HOST));
    assertEquals(COMPONENT_NAME,
        entity.getInfo().get(SliderTimelineMetricsConstants.COMPONENT_NAME));
    assertEquals(ContainerState.INIT.toString(),
        entity.getInfo().get(SliderTimelineMetricsConstants.STATE));

    // updated container state
    container.setState(ContainerState.READY);
    serviceTimelinePublisher.componentInstanceUpdated(container,
        COMPONENT_NAME);
    lastPublishedEntities =
        ((DummyTimelineClient) timelineClient).getLastPublishedEntities();
    assertEquals(1, lastPublishedEntities.size());
    entity = lastPublishedEntities.iterator().next();
    assertEquals(2, entity.getEvents().size());
    assertEquals(ContainerState.READY.toString(),
        entity.getInfo().get(SliderTimelineMetricsConstants.STATE));

  }

  private void verifyServiceAttemptTimelineEntity(TimelineEntity timelineEntity,
      int exitCode, String message, boolean isRegistedEntity) {
    assertEquals(SERVICEID, timelineEntity.getId());
    assertEquals(SERVICE_NAME,
        timelineEntity.getInfo().get(SliderTimelineMetricsConstants.NAME));
    if (isRegistedEntity) {
      assertEquals(ApplicationState.STARTED.toString(),
          timelineEntity.getInfo().get(SliderTimelineMetricsConstants.STATE));
      assertEquals(SliderTimelineEvent.SERVICE_ATTEMPT_REGISTERED.toString(),
          timelineEntity.getEvents().iterator().next().getId());
    } else {
      assertEquals("SUCCEEDED",
          timelineEntity.getInfo().get(SliderTimelineMetricsConstants.STATE));
      assertEquals(exitCode, timelineEntity.getInfo()
          .get(SliderTimelineMetricsConstants.EXIT_STATUS_CODE));
      assertEquals(message, timelineEntity.getInfo()
          .get(SliderTimelineMetricsConstants.EXIT_REASON));

      assertEquals(2, timelineEntity.getEvents().size());
      assertEquals(SliderTimelineEvent.SERVICE_ATTEMPT_UNREGISTERED.toString(),
          timelineEntity.getEvents().iterator().next().getId());
    }
  }

  private void verifyComponentTimelineEntity(TimelineEntity entity) {
    Map<String, Object> info = entity.getInfo();
    assertEquals("DEFAULT", entity.getId());
    assertEquals(ARTIFACTID,
        info.get(SliderTimelineMetricsConstants.ARTIFACT_ID));
    assertEquals("DOCKER",
        info.get(SliderTimelineMetricsConstants.ARTIFACT_TYPE));
    assertEquals("medium",
        info.get(SliderTimelineMetricsConstants.RESOURCE_PROFILE));
    assertEquals(1, info.get(SliderTimelineMetricsConstants.RESOURCE_CPU));
    assertEquals("1024",
        info.get(SliderTimelineMetricsConstants.RESOURCE_MEMORY));
    assertEquals("sleep 1",
        info.get(SliderTimelineMetricsConstants.LAUNCH_COMMAND));
    assertEquals("false",
        info.get(SliderTimelineMetricsConstants.UNIQUE_COMPONENT_SUPPORT));
    assertEquals("false",
        info.get(SliderTimelineMetricsConstants.RUN_PRIVILEGED_CONTAINER));
    assertEquals("label",
        info.get(SliderTimelineMetricsConstants.PLACEMENT_POLICY));
  }

  private static AppState createMockAppState() {
    AppState appState = mock(AppState.class);
    Application application = mock(Application.class);

    when(application.getId()).thenReturn(SERVICEID);
    when(application.getLaunchTime()).thenReturn(new Date());
    when(application.getState()).thenReturn(ApplicationState.STARTED);
    when(application.getName()).thenReturn(SERVICE_NAME);
    when(application.getConfiguration())
        .thenReturn(new org.apache.slider.api.resource.Configuration());

    Component component = mock(Component.class);
    Artifact artifact = new Artifact();
    artifact.setId(ARTIFACTID);
    Resource resource = new Resource();
    resource.setCpus(1);
    resource.setMemory(1024 + "");
    resource.setProfile("medium");
    when(component.getArtifact()).thenReturn(artifact);
    when(component.getName()).thenReturn(COMPONENT_NAME);
    when(component.getResource()).thenReturn(resource);
    when(component.getLaunchCommand()).thenReturn("sleep 1");
    PlacementPolicy placementPolicy = new PlacementPolicy();
    placementPolicy.setLabel("label");
    when(component.getPlacementPolicy()).thenReturn(placementPolicy);
    when(component.getConfiguration())
        .thenReturn(new org.apache.slider.api.resource.Configuration());
    List<Component> components = new ArrayList<Component>();
    components.add(component);

    when(application.getComponents()).thenReturn(components);
    when(appState.getClusterStatus()).thenReturn(application);
    return appState;
  }

  public static void main(String[] args) {
    Application application = createMockAppState().getClusterStatus();
    System.out.println(application.getConfiguration());
  }

  protected static class DummyTimelineClient extends TimelineV2ClientImpl {
    private Map<Identifier, TimelineEntity> lastPublishedEntities =
        new HashMap<>();

    public DummyTimelineClient(ApplicationId appId) {
      super(appId);
    }

    @Override
    public void putEntitiesAsync(TimelineEntity... entities)
        throws IOException, YarnException {
      putEntities(entities);
    }

    @Override
    public void putEntities(TimelineEntity... entities)
        throws IOException, YarnException {
      for (TimelineEntity timelineEntity : entities) {
        TimelineEntity entity =
            lastPublishedEntities.get(timelineEntity.getIdentifier());
        if (entity == null) {
          lastPublishedEntities.put(timelineEntity.getIdentifier(),
              timelineEntity);
        } else {
          entity.addMetrics(timelineEntity.getMetrics());
          entity.addEvents(timelineEntity.getEvents());
          entity.addInfo(timelineEntity.getInfo());
          entity.addConfigs(timelineEntity.getConfigs());
          entity.addRelatesToEntities(timelineEntity.getRelatesToEntities());
          entity
              .addIsRelatedToEntities(timelineEntity.getIsRelatedToEntities());
        }
      }
    }

    public Collection<TimelineEntity> getLastPublishedEntities() {
      return lastPublishedEntities.values();
    }

    public void reset() {
      lastPublishedEntities = null;
    }
  }
}
