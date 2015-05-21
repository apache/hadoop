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
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineEntitySchemaConstants;

/**
 * This implements a hbase based backend for storing application timeline entity
 * information.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HBaseTimelineWriterImpl extends AbstractService implements
    TimelineWriter {

  private Connection conn;
  private BufferedMutator entityTable;

  private static final Log LOG = LogFactory
      .getLog(HBaseTimelineWriterImpl.class);

  public HBaseTimelineWriterImpl() {
    super(HBaseTimelineWriterImpl.class.getName());
  }

  public HBaseTimelineWriterImpl(Configuration conf) throws IOException {
    super(conf.get("yarn.application.id",
        HBaseTimelineWriterImpl.class.getName()));
  }

  /**
   * initializes the hbase connection to write to the entity table
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    Configuration hbaseConf = HBaseConfiguration.create(conf);
    conn = ConnectionFactory.createConnection(hbaseConf);
    TableName entityTableName = TableName.valueOf(hbaseConf.get(
        TimelineEntitySchemaConstants.ENTITY_TABLE_NAME,
        TimelineEntitySchemaConstants.DEFAULT_ENTITY_TABLE_NAME));
    entityTable = conn.getBufferedMutator(entityTableName);
  }

  /**
   * Stores the entire information in TimelineEntities to the timeline store.
   */
  @Override
  public TimelineWriteResponse write(String clusterId, String userId,
      String flowName, String flowVersion, long flowRunId, String appId,
      TimelineEntities data) throws IOException {

    byte[] rowKeyPrefix = TimelineWriterUtils.getRowKeyPrefix(clusterId,
        userId, flowName, flowRunId, appId);

    TimelineWriteResponse putStatus = new TimelineWriteResponse();
    for (TimelineEntity te : data.getEntities()) {

      // a set can have at most 1 null
      if (te == null) {
        continue;
      }
      // get row key
      byte[] row = TimelineWriterUtils.join(
          TimelineEntitySchemaConstants.ROW_KEY_SEPARATOR_BYTES, rowKeyPrefix,
          Bytes.toBytes(te.getType()), Bytes.toBytes(te.getId()));

      storeInfo(row, te, flowVersion);
      storeEvents(row, te.getEvents());
      storeConfig(row, te.getConfigs());
      storeMetrics(row, te.getMetrics());
      storeRelations(row, te.getIsRelatedToEntities(),
          EntityColumnDetails.PREFIX_IS_RELATED_TO);
      storeRelations(row, te.getRelatesToEntities(),
          EntityColumnDetails.PREFIX_RELATES_TO);
    }

    return putStatus;
  }

  /**
   * Stores the Relations from the {@linkplain TimelineEntity} object
   */
  private void storeRelations(byte[] rowKey,
      Map<String, Set<String>> connectedEntities,
      EntityColumnDetails columnNamePrefix) throws IOException {
    for (Map.Entry<String, Set<String>> entry : connectedEntities.entrySet()) {
      columnNamePrefix.store(rowKey, entityTable, entry.getKey(),
          entry.getValue());
    }
  }

  /**
   * Stores information from the {@linkplain TimelineEntity} object
   */
  private void storeInfo(byte[] rowKey, TimelineEntity te, String flowVersion)
      throws IOException {

    EntityColumnDetails.ID.store(rowKey, entityTable, te.getId());
    EntityColumnDetails.TYPE.store(rowKey, entityTable, te.getType());
    EntityColumnDetails.CREATED_TIME.store(rowKey, entityTable,
        te.getCreatedTime());
    EntityColumnDetails.MODIFIED_TIME.store(rowKey, entityTable,
        te.getModifiedTime());
    EntityColumnDetails.FLOW_VERSION.store(rowKey, entityTable, flowVersion);
  }

  /**
   * stores the config information from {@linkplain TimelineEntity}
   */
  private void storeConfig(byte[] rowKey, Map<String, String> config)
      throws IOException {
    if (config == null) {
      return;
    }
    for (Map.Entry<String, String> entry : config.entrySet()) {
      EntityColumnFamily.CONFIG.store(rowKey, entityTable,
          entry.getKey(), entry.getValue());
    }
  }

  /**
   * stores the {@linkplain TimelineMetric} information from the
   * {@linkplain TimelineEvent} object
   */
  private void storeMetrics(byte[] rowKey, Set<TimelineMetric> metrics)
      throws IOException {
    if (metrics != null) {
      for (TimelineMetric metric : metrics) {
        String key = metric.getId();
        Map<Long, Number> timeseries = metric.getValues();
        for (Map.Entry<Long, Number> entry : timeseries.entrySet()) {
          EntityColumnFamily.METRICS.store(rowKey, entityTable, key,
              entry.getKey(), entry.getValue());
        }
      }
    }
  }

  /**
   * Stores the events from the {@linkplain TimelineEvent} object
   */
  private void storeEvents(byte[] rowKey, Set<TimelineEvent> events)
      throws IOException {
    if (events != null) {
      for (TimelineEvent event : events) {
        if (event != null) {
          String id = event.getId();
          if (id != null) {
            byte[] idBytes = Bytes.toBytes(id);
            Map<String, Object> eventInfo = event.getInfo();
            if (eventInfo != null) {
              for (Map.Entry<String, Object> info : eventInfo.entrySet()) {
                EntityColumnDetails.PREFIX_EVENTS.store(rowKey,
                    entityTable, idBytes, info.getKey(), info.getValue());
              }
            }
          }
        }
      }
    }
  }

  @Override
  public TimelineWriteResponse aggregate(TimelineEntity data,
      TimelineAggregationTrack track) throws IOException {
    return null;
  }

  /**
   * close the hbase connections
   * The close APIs perform flushing and release any
   * resources held
   */
  @Override
  protected void serviceStop() throws Exception {
    if (entityTable != null) {
      LOG.info("closing entity table");
      // The close API performs flushing and releases any resources held
      entityTable.close();
    }
    if (conn != null) {
      LOG.info("closing the hbase Connection");
      conn.close();
    }
    super.serviceStop();
  }

}