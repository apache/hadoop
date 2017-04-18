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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Component;
import org.apache.slider.api.resource.ConfigFile;
import org.apache.slider.api.resource.Configuration;
import org.apache.slider.api.resource.Container;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.server.appmaster.actions.ActionStopSlider;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single service that publishes all the Timeline Entities.
 */
public class ServiceTimelinePublisher extends CompositeService {

  // Number of bytes of config which can be published in one shot to ATSv2.
  public static final int ATS_CONFIG_PUBLISH_SIZE_BYTES = 10 * 1024;

  private TimelineV2Client timelineClient;

  private volatile boolean stopped = false;

  private static final Logger log =
      LoggerFactory.getLogger(ServiceTimelinePublisher.class);

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
  }

  public boolean isStopped() {
    return stopped;
  }

  public ServiceTimelinePublisher(TimelineV2Client client) {
    super(ServiceTimelinePublisher.class.getName());
    timelineClient = client;
  }

  public void serviceAttemptRegistered(AppState appState) {
    Application application = appState.getClusterStatus();
    long currentTimeMillis = application.getLaunchTime() == null
        ? System.currentTimeMillis() : application.getLaunchTime().getTime();

    TimelineEntity entity = createServiceAttemptEntity(application.getId());
    entity.setCreatedTime(currentTimeMillis);

    // create info keys
    Map<String, Object> entityInfos = new HashMap<String, Object>();
    entityInfos.put(SliderTimelineMetricsConstants.NAME, application.getName());
    entityInfos.put(SliderTimelineMetricsConstants.STATE,
        application.getState().toString());
    entityInfos.put(SliderTimelineMetricsConstants.LAUNCH_TIME,
        currentTimeMillis);
    entity.addInfo(entityInfos);

    // add an event
    TimelineEvent startEvent = new TimelineEvent();
    startEvent.setId(SliderTimelineEvent.SERVICE_ATTEMPT_REGISTERED.toString());
    startEvent.setTimestamp(currentTimeMillis);
    entity.addEvent(startEvent);

    // publish before configurations published
    putEntity(entity);

    // publish application specific configurations
    publishConfigurations(application.getConfiguration(), application.getId(),
        SliderTimelineEntityType.SERVICE_ATTEMPT.toString(), true);

    // publish component as separate entity.
    publishComponents(application.getComponents());
  }

  public void serviceAttemptUnregistered(AppState appState,
      ActionStopSlider stopAction) {
    long currentTimeMillis = System.currentTimeMillis();

    TimelineEntity entity =
        createServiceAttemptEntity(appState.getClusterStatus().getId());

    // add info
    Map<String, Object> entityInfos = new HashMap<String, Object>();
    entityInfos.put(SliderTimelineMetricsConstants.EXIT_STATUS_CODE,
        stopAction.getExitCode());
    entityInfos.put(SliderTimelineMetricsConstants.STATE,
        stopAction.getFinalApplicationStatus().toString());
    if (stopAction.getMessage() != null) {
      entityInfos.put(SliderTimelineMetricsConstants.EXIT_REASON,
          stopAction.getMessage());
    }
    if (stopAction.getEx() != null) {
      entityInfos.put(SliderTimelineMetricsConstants.DIAGNOSTICS_INFO,
          stopAction.getEx().toString());
    }
    entity.addInfo(entityInfos);

    // add an event
    TimelineEvent startEvent = new TimelineEvent();
    startEvent
        .setId(SliderTimelineEvent.SERVICE_ATTEMPT_UNREGISTERED.toString());
    startEvent.setTimestamp(currentTimeMillis);
    entity.addEvent(startEvent);

    putEntity(entity);
  }

  public void componentInstanceStarted(Container container,
      String componentName) {

    TimelineEntity entity = createComponentInstanceEntity(container.getId());
    entity.setCreatedTime(container.getLaunchTime().getTime());

    // create info keys
    Map<String, Object> entityInfos = new HashMap<String, Object>();
    entityInfos.put(SliderTimelineMetricsConstants.BARE_HOST,
        container.getBareHost());
    entityInfos.put(SliderTimelineMetricsConstants.STATE,
        container.getState().toString());
    entityInfos.put(SliderTimelineMetricsConstants.LAUNCH_TIME,
        container.getLaunchTime().getTime());
    entityInfos.put(SliderTimelineMetricsConstants.COMPONENT_NAME,
        componentName);
    entity.addInfo(entityInfos);

    // add an event
    TimelineEvent startEvent = new TimelineEvent();
    startEvent
        .setId(SliderTimelineEvent.COMPONENT_INSTANCE_REGISTERED.toString());
    startEvent.setTimestamp(container.getLaunchTime().getTime());
    entity.addEvent(startEvent);

    putEntity(entity);
  }

  public void componentInstanceFinished(RoleInstance instance) {
    TimelineEntity entity = createComponentInstanceEntity(instance.id);

    // create info keys
    Map<String, Object> entityInfos = new HashMap<String, Object>();
    entityInfos.put(SliderTimelineMetricsConstants.EXIT_STATUS_CODE,
        instance.exitCode);
    entityInfos.put(SliderTimelineMetricsConstants.DIAGNOSTICS_INFO,
        instance.diagnostics);
    // TODO need to change the state based on enum value.
    entityInfos.put(SliderTimelineMetricsConstants.STATE, "FINISHED");
    entity.addInfo(entityInfos);

    // add an event
    TimelineEvent startEvent = new TimelineEvent();
    startEvent
        .setId(SliderTimelineEvent.COMPONENT_INSTANCE_UNREGISTERED.toString());
    startEvent.setTimestamp(System.currentTimeMillis());
    entity.addEvent(startEvent);

    putEntity(entity);
  }

  public void componentInstanceUpdated(Container container,
      String componentName) {
    TimelineEntity entity = createComponentInstanceEntity(container.getId());

    // create info keys
    Map<String, Object> entityInfos = new HashMap<String, Object>();
    entityInfos.put(SliderTimelineMetricsConstants.IP, container.getIp());
    entityInfos.put(SliderTimelineMetricsConstants.HOSTNAME,
        container.getHostname());
    entityInfos.put(SliderTimelineMetricsConstants.STATE,
        container.getState().toString());
    entity.addInfo(entityInfos);

    TimelineEvent updateEvent = new TimelineEvent();
    updateEvent
        .setId(SliderTimelineEvent.COMPONENT_INSTANCE_UPDATED.toString());
    updateEvent.setTimestamp(System.currentTimeMillis());
    entity.addEvent(updateEvent);

    putEntity(entity);
  }

  private void publishComponents(List<Component> components) {
    long currentTimeMillis = System.currentTimeMillis();
    for (Component component : components) {
      TimelineEntity entity = createComponentEntity(component.getName());
      entity.setCreatedTime(currentTimeMillis);

      // create info keys
      Map<String, Object> entityInfos = new HashMap<String, Object>();
      entityInfos.put(SliderTimelineMetricsConstants.ARTIFACT_ID,
          component.getArtifact().getId());
      entityInfos.put(SliderTimelineMetricsConstants.ARTIFACT_TYPE,
          component.getArtifact().getType().toString());
      if (component.getResource().getProfile() != null) {
        entityInfos.put(SliderTimelineMetricsConstants.RESOURCE_PROFILE,
            component.getResource().getProfile());
      }
      entityInfos.put(SliderTimelineMetricsConstants.RESOURCE_CPU,
          component.getResource().getCpus());
      entityInfos.put(SliderTimelineMetricsConstants.RESOURCE_MEMORY,
          component.getResource().getMemory());

      if (component.getLaunchCommand() != null) {
        entityInfos.put(SliderTimelineMetricsConstants.LAUNCH_COMMAND,
            component.getLaunchCommand());
      }
      entityInfos.put(SliderTimelineMetricsConstants.UNIQUE_COMPONENT_SUPPORT,
          component.getUniqueComponentSupport().toString());
      entityInfos.put(SliderTimelineMetricsConstants.RUN_PRIVILEGED_CONTAINER,
          component.getRunPrivilegedContainer().toString());
      if (component.getPlacementPolicy() != null) {
        entityInfos.put(SliderTimelineMetricsConstants.PLACEMENT_POLICY,
            component.getPlacementPolicy().getLabel());
      }
      entity.addInfo(entityInfos);

      putEntity(entity);

      // publish component specific configurations
      publishConfigurations(component.getConfiguration(), component.getName(),
          SliderTimelineEntityType.COMPONENT.toString(), false);
    }
  }

  private void publishConfigurations(Configuration configuration,
      String entityId, String entityType, boolean isServiceAttemptEntity) {
    if (isServiceAttemptEntity) {
      // publish slider-client.xml properties at service level
      publishConfigurations(SliderUtils.loadSliderClientXML().iterator(),
          entityId, entityType);
    }
    publishConfigurations(configuration.getProperties().entrySet().iterator(),
        entityId, entityType);

    publishConfigurations(configuration.getEnv().entrySet().iterator(),
        entityId, entityType);

    for (ConfigFile configFile : configuration.getFiles()) {
      publishConfigurations(configFile.getProps().entrySet().iterator(),
          entityId, entityType);
    }
  }

  private void publishConfigurations(Iterator<Entry<String, String>> iterator,
      String entityId, String entityType) {
    int configSize = 0;
    TimelineEntity entity = createTimelineEntity(entityId, entityType);
    while (iterator.hasNext()) {
      Entry<String, String> entry = iterator.next();
      int size = entry.getKey().length() + entry.getValue().length();
      configSize += size;
      // Configs are split into multiple entities if they exceed 100kb in size.
      if (configSize > ATS_CONFIG_PUBLISH_SIZE_BYTES) {
        if (entity.getConfigs().size() > 0) {
          putEntity(entity);
          entity = createTimelineEntity(entityId, entityType);
        }
        configSize = size;
      }
      entity.addConfig(entry.getKey(), entry.getValue());
    }
    if (configSize > 0) {
      putEntity(entity);
    }
  }

  /**
   * Called from SliderMetricsSink at regular interval of time.
   * @param metrics of service or components
   * @param entityId Id of entity
   * @param entityType Type of entity
   * @param timestamp
   */
  public void publishMetrics(Iterable<AbstractMetric> metrics, String entityId,
      String entityType, long timestamp) {
    TimelineEntity entity = createTimelineEntity(entityId, entityType);
    Set<TimelineMetric> entityMetrics = new HashSet<TimelineMetric>();
    for (AbstractMetric metric : metrics) {
      TimelineMetric timelineMetric = new TimelineMetric();
      timelineMetric.setId(metric.name());
      timelineMetric.addValue(timestamp, metric.value());
      entityMetrics.add(timelineMetric);
    }
    entity.setMetrics(entityMetrics);
    putEntity(entity);
  }

  private TimelineEntity createServiceAttemptEntity(String serviceId) {
    TimelineEntity entity = createTimelineEntity(serviceId,
        SliderTimelineEntityType.SERVICE_ATTEMPT.toString());
    return entity;
  }

  private TimelineEntity createComponentInstanceEntity(String instanceId) {
    TimelineEntity entity = createTimelineEntity(instanceId,
        SliderTimelineEntityType.COMPONENT_INSTANCE.toString());
    return entity;
  }

  private TimelineEntity createComponentEntity(String componentId) {
    TimelineEntity entity = createTimelineEntity(componentId,
        SliderTimelineEntityType.COMPONENT.toString());
    return entity;
  }

  private TimelineEntity createTimelineEntity(String entityId,
      String entityType) {
    TimelineEntity entity = new TimelineEntity();
    entity.setId(entityId);
    entity.setType(entityType);
    return entity;
  }

  private void putEntity(TimelineEntity entity) {
    try {
      if (log.isDebugEnabled()) {
        log.debug("Publishing the entity " + entity + ", JSON-style content: "
            + TimelineUtils.dumpTimelineRecordtoJSON(entity));
      }
      if (timelineClient != null) {
        timelineClient.putEntitiesAsync(entity);
      } else {
        log.error("Seems like client has been removed before the entity "
            + "could be published for " + entity);
      }
    } catch (Exception e) {
      log.error("Error when publishing entity " + entity, e);
    }
  }
}
