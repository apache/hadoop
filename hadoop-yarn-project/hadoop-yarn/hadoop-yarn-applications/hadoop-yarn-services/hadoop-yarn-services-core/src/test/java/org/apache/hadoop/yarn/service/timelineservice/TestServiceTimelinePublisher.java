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

package org.apache.hadoop.yarn.service.timelineservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity.Identifier;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.client.api.impl.TimelineV2ClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.service.api.records.PlacementConstraint;
import org.apache.hadoop.yarn.service.api.records.PlacementPolicy;
import org.apache.hadoop.yarn.service.api.records.PlacementType;
import org.apache.hadoop.yarn.service.api.records.Resource;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
    serviceTimelinePublisher.init(config);
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
    Service service = createMockApplication();
    serviceTimelinePublisher
        .serviceAttemptRegistered(service, new YarnConfiguration());

    Collection<TimelineEntity> lastPublishedEntities =
        ((DummyTimelineClient) timelineClient).getLastPublishedEntities();
    // 2 entities because during registration component also registered.
    assertEquals(2, lastPublishedEntities.size());
    for (TimelineEntity timelineEntity : lastPublishedEntities) {
      if (timelineEntity.getType() == ServiceTimelineEntityType.COMPONENT
          .toString()) {
        verifyComponentTimelineEntity(timelineEntity);
      } else {
        verifyServiceAttemptTimelineEntity(timelineEntity, null, true);
      }
    }

    ServiceContext context = new ServiceContext();
    context.attemptId = ApplicationAttemptId
        .newInstance(ApplicationId.fromString(service.getId()), 1);
    String exitDiags = "service killed";
    serviceTimelinePublisher.serviceAttemptUnregistered(context,
        FinalApplicationStatus.ENDED, exitDiags);
    lastPublishedEntities =
        ((DummyTimelineClient) timelineClient).getLastPublishedEntities();
    for (TimelineEntity timelineEntity : lastPublishedEntities) {
      if (timelineEntity.getType() == ServiceTimelineEntityType.SERVICE_ATTEMPT
          .toString()) {
        verifyServiceAttemptTimelineEntity(timelineEntity, exitDiags,
            false);
      }
    }
  }

  @Test
  public void testComponentInstanceEntity() {
    Container container = new Container();
    container.id(CONTAINER_ID).ip(CONTAINER_IP).bareHost(CONTAINER_BAREHOST)
        .hostname(CONTAINER_HOSTNAME).state(ContainerState.RUNNING_BUT_UNREADY)
        .launchTime(new Date());
    ComponentInstanceId id = new ComponentInstanceId(0, COMPONENT_NAME);
    ComponentInstance instance = mock(ComponentInstance.class);
    when(instance.getCompName()).thenReturn(COMPONENT_NAME);
    when(instance.getCompInstanceName()).thenReturn("comp_instance_name");
    serviceTimelinePublisher.componentInstanceStarted(container,
        instance);

    Collection<TimelineEntity> lastPublishedEntities =
        ((DummyTimelineClient) timelineClient).getLastPublishedEntities();
    assertEquals(1, lastPublishedEntities.size());
    TimelineEntity entity = lastPublishedEntities.iterator().next();

    assertEquals(1, entity.getEvents().size());
    assertEquals(CONTAINER_ID, entity.getId());
    assertEquals(CONTAINER_BAREHOST,
        entity.getInfo().get(ServiceTimelineMetricsConstants.BARE_HOST));
    assertEquals(COMPONENT_NAME,
        entity.getInfo().get(ServiceTimelineMetricsConstants.COMPONENT_NAME));
    assertEquals(ContainerState.RUNNING_BUT_UNREADY.toString(),
        entity.getInfo().get(ServiceTimelineMetricsConstants.STATE));

    // updated container state
    container.setState(ContainerState.READY);
    serviceTimelinePublisher.componentInstanceIPHostUpdated(container);
    lastPublishedEntities =
        ((DummyTimelineClient) timelineClient).getLastPublishedEntities();
    assertEquals(1, lastPublishedEntities.size());
    entity = lastPublishedEntities.iterator().next();
    assertEquals(2, entity.getEvents().size());
    assertEquals(ContainerState.READY.toString(),
        entity.getInfo().get(ServiceTimelineMetricsConstants.STATE));

  }

  private void verifyServiceAttemptTimelineEntity(TimelineEntity timelineEntity,
      String message, boolean isRegistedEntity) {
    assertEquals(SERVICEID, timelineEntity.getId());
    assertEquals(SERVICE_NAME,
        timelineEntity.getInfo().get(ServiceTimelineMetricsConstants.NAME));
    if (isRegistedEntity) {
      assertEquals(ServiceState.STARTED.toString(),
          timelineEntity.getInfo().get(ServiceTimelineMetricsConstants.STATE));
      assertEquals(ServiceTimelineEvent.SERVICE_ATTEMPT_REGISTERED.toString(),
          timelineEntity.getEvents().iterator().next().getId());
    } else {
      assertEquals("ENDED",
          timelineEntity.getInfo().get(ServiceTimelineMetricsConstants.STATE).toString());
      assertEquals(message, timelineEntity.getInfo()
          .get(ServiceTimelineMetricsConstants.DIAGNOSTICS_INFO));
      assertEquals(2, timelineEntity.getEvents().size());
      assertEquals(ServiceTimelineEvent.SERVICE_ATTEMPT_UNREGISTERED.toString(),
          timelineEntity.getEvents().iterator().next().getId());
    }
  }

  private void verifyComponentTimelineEntity(TimelineEntity entity) {
    Map<String, Object> info = entity.getInfo();
    assertEquals("DEFAULT", entity.getId());
    assertEquals(ARTIFACTID,
        info.get(ServiceTimelineMetricsConstants.ARTIFACT_ID));
    assertEquals("DOCKER",
        info.get(ServiceTimelineMetricsConstants.ARTIFACT_TYPE));
    assertEquals("medium",
        info.get(ServiceTimelineMetricsConstants.RESOURCE_PROFILE));
    assertEquals(1, info.get(ServiceTimelineMetricsConstants.RESOURCE_CPU));
    assertEquals("1024",
        info.get(ServiceTimelineMetricsConstants.RESOURCE_MEMORY));
    assertEquals("sleep 1",
        info.get(ServiceTimelineMetricsConstants.LAUNCH_COMMAND));
    assertEquals("false",
        info.get(ServiceTimelineMetricsConstants.RUN_PRIVILEGED_CONTAINER));
  }

  private static Service createMockApplication() {
    Service service = mock(Service.class);

    when(service.getId()).thenReturn(SERVICEID);
    when(service.getLaunchTime()).thenReturn(new Date());
    when(service.getState()).thenReturn(ServiceState.STARTED);
    when(service.getName()).thenReturn(SERVICE_NAME);
    when(service.getConfiguration()).thenReturn(
        new org.apache.hadoop.yarn.service.api.records.Configuration());

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
    PlacementConstraint placementConstraint = new PlacementConstraint();
    placementConstraint.setType(PlacementType.ANTI_AFFINITY);
    placementPolicy
        .setConstraints(Collections.singletonList(placementConstraint));
    when(component.getPlacementPolicy()).thenReturn(placementPolicy);
    when(component.getConfiguration()).thenReturn(
        new org.apache.hadoop.yarn.service.api.records.Configuration());
    List<Component> components = new ArrayList<Component>();
    components.add(component);

    when(service.getComponents()).thenReturn(components);
    return service;
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
