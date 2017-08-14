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

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Component;
import org.apache.slider.api.resource.ConfigFile;
import org.apache.slider.api.resource.Configuration;
import org.apache.slider.api.resource.Container;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.server.appmaster.actions.ActionStopSlider;
import org.apache.hadoop.yarn.service.compinstance.ComponentInstance;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
  protected void serviceInit(org.apache.hadoop.conf.Configuration configuration)
      throws Exception {
    addService(timelineClient);
  }


  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    super.serviceStop();
  }

  public boolean isStopped() {
    return stopped;
  }

  public ServiceTimelinePublisher(TimelineV2Client client) {
    super(ServiceTimelinePublisher.class.getName());
    timelineClient = client;
  }

  public void serviceAttemptRegistered(Application application) {
    long currentTimeMillis = application.getLaunchTime() == null
        ? System.currentTimeMillis() : application.getLaunchTime().getTime();

    TimelineEntity entity = createServiceAttemptEntity(application.getId());
    entity.setCreatedTime(currentTimeMillis);

    // create info keys
    Map<String, Object> entityInfos = new HashMap<String, Object>();
    entityInfos.put(ServiceTimelineMetricsConstants.NAME, application.getName());
    entityInfos.put(ServiceTimelineMetricsConstants.STATE,
        application.getState().toString());
    entityInfos.put(ServiceTimelineMetricsConstants.LAUNCH_TIME,
        currentTimeMillis);
    entity.addInfo(ServiceTimelineMetricsConstants.QUICK_LINKS,
        application.getQuicklinks());
    entity.addInfo(entityInfos);

    // add an event
    TimelineEvent startEvent = new TimelineEvent();
    startEvent.setId(ServiceTimelineEvent.SERVICE_ATTEMPT_REGISTERED.toString());
    startEvent.setTimestamp(currentTimeMillis);
    entity.addEvent(startEvent);

    // publish before configurations published
    putEntity(entity);

    // publish application specific configurations
    publishConfigurations(application.getConfiguration(), application.getId(),
        ServiceTimelineEntityType.SERVICE_ATTEMPT.toString(), true);

    // publish component as separate entity.
    publishComponents(application.getComponents());
  }

  public void serviceAttemptUpdated(Application application) {
    TimelineEntity entity = createServiceAttemptEntity(application.getId());
    entity.addInfo(ServiceTimelineMetricsConstants.QUICK_LINKS,
        application.getQuicklinks());
    putEntity(entity);
  }

  public void serviceAttemptUnregistered(ServiceContext context) {
    TimelineEntity entity = createServiceAttemptEntity(
        context.attemptId.getApplicationId().toString());
    Map<String, Object> entityInfos = new HashMap<String, Object>();
    entityInfos.put(ServiceTimelineMetricsConstants.STATE,
        FinalApplicationStatus.FAILED);
    entity.addInfo(entityInfos);

    // add an event
    TimelineEvent finishEvent = new TimelineEvent();
    finishEvent
        .setId(ServiceTimelineEvent.SERVICE_ATTEMPT_UNREGISTERED.toString());
    finishEvent.setTimestamp(System.currentTimeMillis());
    entity.addEvent(finishEvent);

    putEntity(entity);
  }

  public void serviceAttemptUnregistered(AppState appState,
      ActionStopSlider stopAction) {
    long currentTimeMillis = System.currentTimeMillis();

    TimelineEntity entity =
        createServiceAttemptEntity(appState.getClusterStatus().getId());

    // add info
    Map<String, Object> entityInfos = new HashMap<String, Object>();
    entityInfos.put(ServiceTimelineMetricsConstants.EXIT_STATUS_CODE,
        stopAction.getExitCode());
    entityInfos.put(ServiceTimelineMetricsConstants.STATE,
        stopAction.getFinalApplicationStatus().toString());
    if (stopAction.getMessage() != null) {
      entityInfos.put(ServiceTimelineMetricsConstants.EXIT_REASON,
          stopAction.getMessage());
    }
    if (stopAction.getEx() != null) {
      entityInfos.put(ServiceTimelineMetricsConstants.DIAGNOSTICS_INFO,
          stopAction.getEx().toString());
    }
    entity.addInfo(entityInfos);

    // add an event
    TimelineEvent startEvent = new TimelineEvent();
    startEvent
        .setId(ServiceTimelineEvent.SERVICE_ATTEMPT_UNREGISTERED.toString());
    startEvent.setTimestamp(currentTimeMillis);
    entity.addEvent(startEvent);

    putEntity(entity);
  }

  public void componentInstanceStarted(Container container,
      ComponentInstance instance) {

    TimelineEntity entity = createComponentInstanceEntity(container.getId());
    entity.setCreatedTime(container.getLaunchTime().getTime());

    // create info keys
    Map<String, Object> entityInfos = new HashMap<String, Object>();
    entityInfos.put(ServiceTimelineMetricsConstants.BARE_HOST,
        container.getBareHost());
    entityInfos.put(ServiceTimelineMetricsConstants.STATE,
        container.getState().toString());
    entityInfos.put(ServiceTimelineMetricsConstants.LAUNCH_TIME,
        container.getLaunchTime().getTime());
    entityInfos.put(ServiceTimelineMetricsConstants.COMPONENT_NAME,
        instance.getCompName());
    entityInfos.put(ServiceTimelineMetricsConstants.COMPONENT_INSTANCE_NAME,
        instance.getCompInstanceName());
    entity.addInfo(entityInfos);

    // add an event
    TimelineEvent startEvent = new TimelineEvent();
    startEvent
        .setId(ServiceTimelineEvent.COMPONENT_INSTANCE_REGISTERED.toString());
    startEvent.setTimestamp(container.getLaunchTime().getTime());
    entity.addEvent(startEvent);

    putEntity(entity);
  }

  public void componentInstanceFinished(RoleInstance instance) {
    TimelineEntity entity = createComponentInstanceEntity(instance.id);

    // create info keys
    Map<String, Object> entityInfos = new HashMap<String, Object>();
    entityInfos.put(ServiceTimelineMetricsConstants.EXIT_STATUS_CODE,
        instance.exitCode);
    entityInfos.put(ServiceTimelineMetricsConstants.DIAGNOSTICS_INFO,
        instance.diagnostics);
    // TODO need to change the state based on enum value.
    entityInfos.put(ServiceTimelineMetricsConstants.STATE, "FINISHED");
    entity.addInfo(entityInfos);

    // add an event
    TimelineEvent startEvent = new TimelineEvent();
    startEvent
        .setId(ServiceTimelineEvent.COMPONENT_INSTANCE_UNREGISTERED.toString());
    startEvent.setTimestamp(System.currentTimeMillis());
    entity.addEvent(startEvent);

    putEntity(entity);
  }

  public void componentInstanceFinished(ComponentInstance instance,
      int exitCode, ContainerState state, String diagnostics) {
    TimelineEntity entity = createComponentInstanceEntity(
        instance.getContainer().getId().toString());

    // create info keys
    Map<String, Object> entityInfos = new HashMap<String, Object>();
    entityInfos.put(ServiceTimelineMetricsConstants.EXIT_STATUS_CODE,
        exitCode);
    entityInfos.put(ServiceTimelineMetricsConstants.DIAGNOSTICS_INFO, diagnostics);
    entityInfos.put(ServiceTimelineMetricsConstants.STATE, state);
    entity.addInfo(entityInfos);

    // add an event
    TimelineEvent startEvent = new TimelineEvent();
    startEvent
        .setId(ServiceTimelineEvent.COMPONENT_INSTANCE_UNREGISTERED.toString());
    startEvent.setTimestamp(System.currentTimeMillis());
    entity.addEvent(startEvent);

    putEntity(entity);
  }

  public void componentInstanceUpdated(Container container) {
    TimelineEntity entity = createComponentInstanceEntity(container.getId());

    // create info keys
    Map<String, Object> entityInfos = new HashMap<String, Object>();
    entityInfos.put(ServiceTimelineMetricsConstants.IP, container.getIp());
    entityInfos.put(ServiceTimelineMetricsConstants.HOSTNAME,
        container.getHostname());
    entityInfos.put(ServiceTimelineMetricsConstants.STATE,
        container.getState().toString());
    entity.addInfo(entityInfos);

    TimelineEvent updateEvent = new TimelineEvent();
    updateEvent
        .setId(ServiceTimelineEvent.COMPONENT_INSTANCE_UPDATED.toString());
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
      entityInfos.put(ServiceTimelineMetricsConstants.ARTIFACT_ID,
          component.getArtifact().getId());
      entityInfos.put(ServiceTimelineMetricsConstants.ARTIFACT_TYPE,
          component.getArtifact().getType().toString());
      if (component.getResource().getProfile() != null) {
        entityInfos.put(ServiceTimelineMetricsConstants.RESOURCE_PROFILE,
            component.getResource().getProfile());
      }
      entityInfos.put(ServiceTimelineMetricsConstants.RESOURCE_CPU,
          component.getResource().getCpus());
      entityInfos.put(ServiceTimelineMetricsConstants.RESOURCE_MEMORY,
          component.getResource().getMemory());

      if (component.getLaunchCommand() != null) {
        entityInfos.put(ServiceTimelineMetricsConstants.LAUNCH_COMMAND,
            component.getLaunchCommand());
      }
      entityInfos.put(ServiceTimelineMetricsConstants.UNIQUE_COMPONENT_SUPPORT,
          component.getUniqueComponentSupport().toString());
      entityInfos.put(ServiceTimelineMetricsConstants.RUN_PRIVILEGED_CONTAINER,
          component.getRunPrivilegedContainer().toString());
      if (component.getPlacementPolicy() != null) {
        entityInfos.put(ServiceTimelineMetricsConstants.PLACEMENT_POLICY,
            component.getPlacementPolicy().getLabel());
      }
      entity.addInfo(entityInfos);

      putEntity(entity);

      // publish component specific configurations
      publishConfigurations(component.getConfiguration(), component.getName(),
          ServiceTimelineEntityType.COMPONENT.toString(), false);
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
   * Called from ServiceMetricsSink at regular interval of time.
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
        ServiceTimelineEntityType.SERVICE_ATTEMPT.toString());
    return entity;
  }

  private TimelineEntity createComponentInstanceEntity(String instanceId) {
    TimelineEntity entity = createTimelineEntity(instanceId,
        ServiceTimelineEntityType.COMPONENT_INSTANCE.toString());
    return entity;
  }

  private TimelineEntity createComponentEntity(String componentId) {
    TimelineEntity entity = createTimelineEntity(componentId,
        ServiceTimelineEntityType.COMPONENT.toString());
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
