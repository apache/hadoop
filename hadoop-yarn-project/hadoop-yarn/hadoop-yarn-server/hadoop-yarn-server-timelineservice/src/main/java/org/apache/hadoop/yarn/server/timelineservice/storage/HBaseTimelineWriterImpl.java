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
package org.apache.hadoop.yarn.server.timelineservice.storage;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.EventColumnName;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.StringKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TypedBufferedMutator;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationCompactionDimension;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationOperation;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunTable;

/**
 * This implements a hbase based backend for storing the timeline entity
 * information.
 * It writes to multiple tables at the backend
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HBaseTimelineWriterImpl extends AbstractService implements
    TimelineWriter {

  private static final Log LOG = LogFactory
      .getLog(HBaseTimelineWriterImpl.class);

  private Connection conn;
  private TypedBufferedMutator<EntityTable> entityTable;
  private TypedBufferedMutator<AppToFlowTable> appToFlowTable;
  private TypedBufferedMutator<ApplicationTable> applicationTable;
  private TypedBufferedMutator<FlowActivityTable> flowActivityTable;
  private TypedBufferedMutator<FlowRunTable> flowRunTable;

  /**
   * Used to convert strings key components to and from storage format.
   */
  private final KeyConverter<String> stringKeyConverter =
      new StringKeyConverter();

  /**
   * Used to convert Long key components to and from storage format.
   */
  private final KeyConverter<Long> longKeyConverter = new LongKeyConverter();

  public HBaseTimelineWriterImpl() {
    super(HBaseTimelineWriterImpl.class.getName());
  }

  public HBaseTimelineWriterImpl(Configuration conf) throws IOException {
    super(conf.get("yarn.application.id",
        HBaseTimelineWriterImpl.class.getName()));
  }

  /**
   * initializes the hbase connection to write to the entity table.
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    Configuration hbaseConf = HBaseConfiguration.create(conf);
    conn = ConnectionFactory.createConnection(hbaseConf);
    entityTable = new EntityTable().getTableMutator(hbaseConf, conn);
    appToFlowTable = new AppToFlowTable().getTableMutator(hbaseConf, conn);
    applicationTable = new ApplicationTable().getTableMutator(hbaseConf, conn);
    flowRunTable = new FlowRunTable().getTableMutator(hbaseConf, conn);
    flowActivityTable =
        new FlowActivityTable().getTableMutator(hbaseConf, conn);
  }

  /**
   * Stores the entire information in TimelineEntities to the timeline store.
   */
  @Override
  public TimelineWriteResponse write(String clusterId, String userId,
      String flowName, String flowVersion, long flowRunId, String appId,
      TimelineEntities data) throws IOException {

    TimelineWriteResponse putStatus = new TimelineWriteResponse();
    // defensive coding to avoid NPE during row key construction
    if ((flowName == null) || (appId == null) || (clusterId == null)
        || (userId == null)) {
      LOG.warn("Found null for one of: flowName=" + flowName + " appId=" + appId
          + " userId=" + userId + " clusterId=" + clusterId
          + " . Not proceeding with writing to hbase");
      return putStatus;
    }

    for (TimelineEntity te : data.getEntities()) {

      // a set can have at most 1 null
      if (te == null) {
        continue;
      }

      // if the entity is the application, the destination is the application
      // table
      boolean isApplication = isApplicationEntity(te);
      byte[] rowKey;
      if (isApplication) {
        ApplicationRowKey applicationRowKey =
            new ApplicationRowKey(clusterId, userId, flowName, flowRunId,
                appId);
        rowKey = applicationRowKey.getRowKey();
      } else {
        EntityRowKey entityRowKey =
            new EntityRowKey(clusterId, userId, flowName, flowRunId, appId,
                te.getType(), te.getId());
        rowKey = entityRowKey.getRowKey();
      }

      storeInfo(rowKey, te, flowVersion, isApplication);
      storeEvents(rowKey, te.getEvents(), isApplication);
      storeConfig(rowKey, te.getConfigs(), isApplication);
      storeMetrics(rowKey, te.getMetrics(), isApplication);
      storeRelations(rowKey, te, isApplication);

      if (isApplication) {
        TimelineEvent event =
            getApplicationEvent(te,
                ApplicationMetricsConstants.CREATED_EVENT_TYPE);
        FlowRunRowKey flowRunRowKey =
            new FlowRunRowKey(clusterId, userId, flowName, flowRunId);
        if (event != null) {
          AppToFlowRowKey appToFlowRowKey =
              new AppToFlowRowKey(clusterId, appId);
          onApplicationCreated(flowRunRowKey, appToFlowRowKey, appId, userId,
              flowVersion, te, event.getTimestamp());
        }
        // if it's an application entity, store metrics
        storeFlowMetricsAppRunning(flowRunRowKey, appId, te);
        // if application has finished, store it's finish time and write final
        // values of all metrics
        event = getApplicationEvent(te,
            ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
        if (event != null) {
          onApplicationFinished(flowRunRowKey, flowVersion, appId, te,
              event.getTimestamp());
        }
      }
    }
    return putStatus;
  }

  private void onApplicationCreated(FlowRunRowKey flowRunRowKey,
      AppToFlowRowKey appToFlowRowKey, String appId, String userId,
      String flowVersion, TimelineEntity te, long appCreatedTimeStamp)
      throws IOException {

    String flowName = flowRunRowKey.getFlowName();
    Long flowRunId = flowRunRowKey.getFlowRunId();

    // store in App to flow table
    byte[] rowKey = appToFlowRowKey.getRowKey();
    AppToFlowColumn.FLOW_ID.store(rowKey, appToFlowTable, null, flowName);
    AppToFlowColumn.FLOW_RUN_ID.store(rowKey, appToFlowTable, null, flowRunId);
    AppToFlowColumn.USER_ID.store(rowKey, appToFlowTable, null, userId);

    // store in flow run table
    storeAppCreatedInFlowRunTable(flowRunRowKey, appId, te);

    // store in flow activity table
    byte[] flowActivityRowKeyBytes =
        new FlowActivityRowKey(flowRunRowKey.getClusterId(),
            appCreatedTimeStamp, flowRunRowKey.getUserId(), flowName)
            .getRowKey();
    byte[] qualifier = longKeyConverter.encode(flowRunRowKey.getFlowRunId());
    FlowActivityColumnPrefix.RUN_ID.store(flowActivityRowKeyBytes,
        flowActivityTable, qualifier, null, flowVersion,
        AggregationCompactionDimension.APPLICATION_ID.getAttribute(appId));
  }

  /*
   * updates the {@link FlowRunTable} with Application Created information
   */
  private void storeAppCreatedInFlowRunTable(FlowRunRowKey flowRunRowKey,
      String appId, TimelineEntity te) throws IOException {
    byte[] rowKey = flowRunRowKey.getRowKey();
    FlowRunColumn.MIN_START_TIME.store(rowKey, flowRunTable, null,
        te.getCreatedTime(),
        AggregationCompactionDimension.APPLICATION_ID.getAttribute(appId));
  }


  /*
   * updates the {@link FlowRunTable} and {@link FlowActivityTable} when an
   * application has finished
   */
  private void onApplicationFinished(FlowRunRowKey flowRunRowKey,
      String flowVersion, String appId, TimelineEntity te,
      long appFinishedTimeStamp) throws IOException {
    // store in flow run table
    storeAppFinishedInFlowRunTable(flowRunRowKey, appId, te,
        appFinishedTimeStamp);

    // indicate in the flow activity table that the app has finished
    byte[] rowKey =
        new FlowActivityRowKey(flowRunRowKey.getClusterId(),
            appFinishedTimeStamp, flowRunRowKey.getUserId(),
            flowRunRowKey.getFlowName()).getRowKey();
    byte[] qualifier = longKeyConverter.encode(flowRunRowKey.getFlowRunId());
    FlowActivityColumnPrefix.RUN_ID.store(rowKey, flowActivityTable, qualifier,
        null, flowVersion,
        AggregationCompactionDimension.APPLICATION_ID.getAttribute(appId));
  }

  /*
   * Update the {@link FlowRunTable} with Application Finished information
   */
  private void storeAppFinishedInFlowRunTable(FlowRunRowKey flowRunRowKey,
      String appId, TimelineEntity te, long appFinishedTimeStamp)
      throws IOException {
    byte[] rowKey = flowRunRowKey.getRowKey();
    Attribute attributeAppId =
        AggregationCompactionDimension.APPLICATION_ID.getAttribute(appId);
    FlowRunColumn.MAX_END_TIME.store(rowKey, flowRunTable, null,
        appFinishedTimeStamp, attributeAppId);

    // store the final value of metrics since application has finished
    Set<TimelineMetric> metrics = te.getMetrics();
    if (metrics != null) {
      storeFlowMetrics(rowKey, metrics, attributeAppId,
          AggregationOperation.SUM_FINAL.getAttribute());
    }
  }

  /*
   * Updates the {@link FlowRunTable} with Application Metrics
   */
  private void storeFlowMetricsAppRunning(FlowRunRowKey flowRunRowKey,
      String appId, TimelineEntity te) throws IOException {
    Set<TimelineMetric> metrics = te.getMetrics();
    if (metrics != null) {
      byte[] rowKey = flowRunRowKey.getRowKey();
      storeFlowMetrics(rowKey, metrics,
          AggregationCompactionDimension.APPLICATION_ID.getAttribute(appId),
          AggregationOperation.SUM.getAttribute());
    }
  }

  private void storeFlowMetrics(byte[] rowKey, Set<TimelineMetric> metrics,
      Attribute... attributes) throws IOException {
    for (TimelineMetric metric : metrics) {
      byte[] metricColumnQualifier = stringKeyConverter.encode(metric.getId());
      Map<Long, Number> timeseries = metric.getValues();
      for (Map.Entry<Long, Number> timeseriesEntry : timeseries.entrySet()) {
        Long timestamp = timeseriesEntry.getKey();
        FlowRunColumnPrefix.METRIC.store(rowKey, flowRunTable,
            metricColumnQualifier, timestamp, timeseriesEntry.getValue(),
            attributes);
      }
    }
  }

  private void storeRelations(byte[] rowKey, TimelineEntity te,
      boolean isApplication) throws IOException {
    if (isApplication) {
      storeRelations(rowKey, te.getIsRelatedToEntities(),
          ApplicationColumnPrefix.IS_RELATED_TO, applicationTable);
      storeRelations(rowKey, te.getRelatesToEntities(),
          ApplicationColumnPrefix.RELATES_TO, applicationTable);
    } else {
      storeRelations(rowKey, te.getIsRelatedToEntities(),
          EntityColumnPrefix.IS_RELATED_TO, entityTable);
      storeRelations(rowKey, te.getRelatesToEntities(),
          EntityColumnPrefix.RELATES_TO, entityTable);
    }
  }

  /**
   * Stores the Relations from the {@linkplain TimelineEntity} object.
   */
  private <T> void storeRelations(byte[] rowKey,
      Map<String, Set<String>> connectedEntities,
      ColumnPrefix<T> columnPrefix, TypedBufferedMutator<T> table)
          throws IOException {
    for (Map.Entry<String, Set<String>> connectedEntity : connectedEntities
        .entrySet()) {
      // id3?id4?id5
      String compoundValue =
          Separator.VALUES.joinEncoded(connectedEntity.getValue());
      columnPrefix.store(rowKey, table,
          stringKeyConverter.encode(connectedEntity.getKey()), null,
          compoundValue);
    }
  }

  /**
   * Stores information from the {@linkplain TimelineEntity} object.
   */
  private void storeInfo(byte[] rowKey, TimelineEntity te, String flowVersion,
      boolean isApplication) throws IOException {

    if (isApplication) {
      ApplicationColumn.ID.store(rowKey, applicationTable, null, te.getId());
      ApplicationColumn.CREATED_TIME.store(rowKey, applicationTable, null,
          te.getCreatedTime());
      ApplicationColumn.FLOW_VERSION.store(rowKey, applicationTable, null,
          flowVersion);
      Map<String, Object> info = te.getInfo();
      if (info != null) {
        for (Map.Entry<String, Object> entry : info.entrySet()) {
          ApplicationColumnPrefix.INFO.store(rowKey, applicationTable,
              stringKeyConverter.encode(entry.getKey()), null,
              entry.getValue());
        }
      }
    } else {
      EntityColumn.ID.store(rowKey, entityTable, null, te.getId());
      EntityColumn.TYPE.store(rowKey, entityTable, null, te.getType());
      EntityColumn.CREATED_TIME.store(rowKey, entityTable, null,
          te.getCreatedTime());
      EntityColumn.FLOW_VERSION.store(rowKey, entityTable, null, flowVersion);
      Map<String, Object> info = te.getInfo();
      if (info != null) {
        for (Map.Entry<String, Object> entry : info.entrySet()) {
          EntityColumnPrefix.INFO.store(rowKey, entityTable,
              stringKeyConverter.encode(entry.getKey()), null,
              entry.getValue());
        }
      }
    }
  }

  /**
   * stores the config information from {@linkplain TimelineEntity}.
   */
  private void storeConfig(byte[] rowKey, Map<String, String> config,
      boolean isApplication) throws IOException {
    if (config == null) {
      return;
    }
    for (Map.Entry<String, String> entry : config.entrySet()) {
      byte[] configKey = stringKeyConverter.encode(entry.getKey());
      if (isApplication) {
        ApplicationColumnPrefix.CONFIG.store(rowKey, applicationTable,
            configKey, null, entry.getValue());
      } else {
        EntityColumnPrefix.CONFIG.store(rowKey, entityTable, configKey,
            null, entry.getValue());
      }
    }
  }

  /**
   * stores the {@linkplain TimelineMetric} information from the
   * {@linkplain TimelineEvent} object.
   */
  private void storeMetrics(byte[] rowKey, Set<TimelineMetric> metrics,
      boolean isApplication) throws IOException {
    if (metrics != null) {
      for (TimelineMetric metric : metrics) {
        byte[] metricColumnQualifier =
            stringKeyConverter.encode(metric.getId());
        Map<Long, Number> timeseries = metric.getValues();
        for (Map.Entry<Long, Number> timeseriesEntry : timeseries.entrySet()) {
          Long timestamp = timeseriesEntry.getKey();
          if (isApplication) {
            ApplicationColumnPrefix.METRIC.store(rowKey, applicationTable,
                metricColumnQualifier, timestamp, timeseriesEntry.getValue());
          } else {
            EntityColumnPrefix.METRIC.store(rowKey, entityTable,
                metricColumnQualifier, timestamp, timeseriesEntry.getValue());
          }
        }
      }
    }
  }

  /**
   * Stores the events from the {@linkplain TimelineEvent} object.
   */
  private void storeEvents(byte[] rowKey, Set<TimelineEvent> events,
      boolean isApplication) throws IOException {
    if (events != null) {
      for (TimelineEvent event : events) {
        if (event != null) {
          String eventId = event.getId();
          if (eventId != null) {
            long eventTimestamp = event.getTimestamp();
            // if the timestamp is not set, use the current timestamp
            if (eventTimestamp == TimelineEvent.INVALID_TIMESTAMP) {
              LOG.warn("timestamp is not set for event " + eventId +
                  "! Using the current timestamp");
              eventTimestamp = System.currentTimeMillis();
            }
            Map<String, Object> eventInfo = event.getInfo();
            if ((eventInfo == null) || (eventInfo.size() == 0)) {
              byte[] columnQualifierBytes =
                  new EventColumnName(eventId, eventTimestamp, null)
                      .getColumnQualifier();
              if (isApplication) {
                ApplicationColumnPrefix.EVENT.store(rowKey, applicationTable,
                    columnQualifierBytes, null, Separator.EMPTY_BYTES);
              } else {
                EntityColumnPrefix.EVENT.store(rowKey, entityTable,
                    columnQualifierBytes, null, Separator.EMPTY_BYTES);
              }
            } else {
              for (Map.Entry<String, Object> info : eventInfo.entrySet()) {
                // eventId=infoKey
                byte[] columnQualifierBytes =
                    new EventColumnName(eventId, eventTimestamp, info.getKey())
                        .getColumnQualifier();
                if (isApplication) {
                  ApplicationColumnPrefix.EVENT.store(rowKey, applicationTable,
                      columnQualifierBytes, null, info.getValue());
                } else {
                  EntityColumnPrefix.EVENT.store(rowKey, entityTable,
                      columnQualifierBytes, null, info.getValue());
                }
              } // for info: eventInfo
            }
          }
        }
      } // event : events
    }
  }

  /**
   * Checks if the input TimelineEntity object is an ApplicationEntity.
   *
   * @param te TimelineEntity object.
   * @return true if input is an ApplicationEntity, false otherwise
   */
  static boolean isApplicationEntity(TimelineEntity te) {
    return te.getType().equals(TimelineEntityType.YARN_APPLICATION.toString());
  }

  /**
   * @param te TimelineEntity object.
   * @param eventId event with this id needs to be fetched
   * @return TimelineEvent if TimelineEntity contains the desired event.
   */
  private static TimelineEvent getApplicationEvent(TimelineEntity te,
      String eventId) {
    if (isApplicationEntity(te)) {
      for (TimelineEvent event : te.getEvents()) {
        if (event.getId().equals(eventId)) {
          return event;
        }
      }
    }
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage
   * .TimelineWriter#aggregate
   * (org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity,
   * org.apache
   * .hadoop.yarn.server.timelineservice.storage.TimelineAggregationTrack)
   */
  @Override
  public TimelineWriteResponse aggregate(TimelineEntity data,
      TimelineAggregationTrack track) throws IOException {
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter#flush
   * ()
   */
  @Override
  public void flush() throws IOException {
    // flush all buffered mutators
    entityTable.flush();
    appToFlowTable.flush();
    applicationTable.flush();
    flowRunTable.flush();
    flowActivityTable.flush();
  }

  /**
   * close the hbase connections The close APIs perform flushing and release any
   * resources held.
   */
  @Override
  protected void serviceStop() throws Exception {
    if (entityTable != null) {
      LOG.info("closing the entity table");
      // The close API performs flushing and releases any resources held
      entityTable.close();
    }
    if (appToFlowTable != null) {
      LOG.info("closing the app_flow table");
      // The close API performs flushing and releases any resources held
      appToFlowTable.close();
    }
    if (applicationTable != null) {
      LOG.info("closing the application table");
      applicationTable.close();
    }
    if (flowRunTable != null) {
      LOG.info("closing the flow run table");
      // The close API performs flushing and releases any resources held
      flowRunTable.close();
    }
    if (flowActivityTable != null) {
      LOG.info("closing the flowActivityTable table");
      // The close API performs flushing and releases any resources held
      flowActivityTable.close();
    }
    if (conn != null) {
      LOG.info("closing the hbase Connection");
      conn.close();
    }
    super.serviceStop();
  }

}
