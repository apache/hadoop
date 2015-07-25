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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timeline.GenericObjectMapper;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@Private
@Unstable
public class PhoenixTimelineWriterImpl extends AbstractService
    implements TimelineWriter {

  public static final String TIMELINE_SERVICE_PHOENIX_STORAGE_CONN_STR
      = YarnConfiguration.TIMELINE_SERVICE_PREFIX
          + "writer.phoenix.connectionString";

  public static final String TIMELINE_SERVICE_PHEONIX_STORAGE_CONN_STR_DEFAULT
      = "jdbc:phoenix:localhost:2181:/hbase";

  private static final Log LOG
      = LogFactory.getLog(PhoenixTimelineWriterImpl.class);
  private static final String PHOENIX_COL_FAMILY_PLACE_HOLDER
      = "timeline_cf_placeholder";
  // These lists are not taking effects in table creations.
  private static final String[] PHOENIX_STORAGE_PK_LIST
      = {"cluster", "user", "flow_name", "flow_version", "flow_run", "app_id",
         "type", "entity_id"};
  private static final String[] TIMELINE_EVENT_EXTRA_PK_LIST =
      {"timestamp", "event_id"};
  private static final String[] TIMELINE_METRIC_EXTRA_PK_LIST =
      {"metric_id"};
  /** Default Phoenix JDBC driver name */
  private static final String DRIVER_CLASS_NAME
      = "org.apache.phoenix.jdbc.PhoenixDriver";

  /** Default Phoenix timeline entity table name */
  @VisibleForTesting
  static final String ENTITY_TABLE_NAME = "timeline_entity";
  /** Default Phoenix event table name */
  @VisibleForTesting
  static final String EVENT_TABLE_NAME = "timeline_event";
  /** Default Phoenix metric table name */
  @VisibleForTesting
  static final String METRIC_TABLE_NAME = "metric_singledata";

  /** Default Phoenix timeline config column family */
  private static final String CONFIG_COLUMN_FAMILY = "c.";
  /** Default Phoenix timeline info column family */
  private static final String INFO_COLUMN_FAMILY = "i.";
  /** Default Phoenix event info column family */
  private static final String EVENT_INFO_COLUMN_FAMILY = "ei.";
  /** Default Phoenix isRelatedTo column family */
  private static final String IS_RELATED_TO_FAMILY = "ir.";
  /** Default Phoenix relatesTo column family */
  private static final String RELATES_TO_FAMILY = "rt.";
  /** Default separator for Phoenix storage */
  private static final String PHOENIX_STORAGE_SEPARATOR = ";";

  /** Connection string to the deployed Phoenix cluster */
  @VisibleForTesting
  String connString = null;
  @VisibleForTesting
  Properties connProperties = new Properties();

  PhoenixTimelineWriterImpl() {
    super((PhoenixTimelineWriterImpl.class.getName()));
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // so check it here and only read in the config if it's not overridden.
    connString =
        conf.get(TIMELINE_SERVICE_PHOENIX_STORAGE_CONN_STR,
        TIMELINE_SERVICE_PHEONIX_STORAGE_CONN_STR_DEFAULT);
    createTables();
    super.init(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
  }

  @Override
  public TimelineWriteResponse write(String clusterId, String userId,
      String flowName, String flowVersion, long flowRunId, String appId,
      TimelineEntities entities) throws IOException {
    TimelineWriteResponse response = new TimelineWriteResponse();
    TimelineCollectorContext currContext = new TimelineCollectorContext(
        clusterId, userId, flowName, flowVersion, flowRunId, appId);
    String sql = "UPSERT INTO " + ENTITY_TABLE_NAME
        + " (" + StringUtils.join(PHOENIX_STORAGE_PK_LIST, ",")
        + ", creation_time, modified_time, configs) "
        + "VALUES (" + StringUtils.repeat("?,", PHOENIX_STORAGE_PK_LIST.length)
        + "?, ?, ?)";
    if (LOG.isDebugEnabled()) {
      LOG.debug("TimelineEntity write SQL: " + sql);
    }

    try (Connection conn = getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      for (TimelineEntity entity : entities.getEntities()) {
        int idx = setStringsForPrimaryKey(ps, currContext, entity, 1);
        ps.setLong(idx++, entity.getCreatedTime());
        ps.setLong(idx++, entity.getModifiedTime());
        String configKeys = StringUtils.join(
            entity.getConfigs().keySet(), PHOENIX_STORAGE_SEPARATOR);
        ps.setString(idx++, configKeys);
        ps.execute();

        storeEntityVariableLengthFields(entity, currContext, conn);
        storeEvents(entity, currContext, conn);
        storeMetrics(entity, currContext, conn);

        conn.commit();
      }
    } catch (SQLException se) {
      LOG.error("Failed to add entity to Phoenix " + se.getMessage());
      throw new IOException(se);
    } catch (Exception e) {
      LOG.error("Exception on getting connection: " + e.getMessage());
      throw new IOException(e);
    }
    return response;
  }

  /**
   * Aggregates the entity information to the timeline store based on which
   * track this entity is to be rolled up to The tracks along which aggregations
   * are to be done are given by {@link TimelineAggregationTrack}
   *
   * Any errors occurring for individual write request objects will be reported
   * in the response.
   *
   * @param data
   *          a {@link TimelineEntity} object
   *          a {@link TimelineAggregationTrack} enum value
   * @return a {@link TimelineWriteResponse} object.
   * @throws IOException
   */
  @Override
  public TimelineWriteResponse aggregate(TimelineEntity data,
      TimelineAggregationTrack track) throws IOException {
    return null;

  }

  @Override
  public void flush() throws IOException {
    // currently no-op
  }

  // Utility functions
  @Private
  @VisibleForTesting
  Connection getConnection() throws IOException {
    Connection conn;
    try {
      Class.forName(DRIVER_CLASS_NAME);
      conn = DriverManager.getConnection(connString, connProperties);
      conn.setAutoCommit(false);
    } catch (SQLException se) {
      LOG.error("Failed to connect to phoenix server! "
          + se.getLocalizedMessage());
      throw new IOException(se);
    } catch (ClassNotFoundException e) {
      LOG.error("Class not found! " + e.getLocalizedMessage());
      throw new IOException(e);
    }
    return conn;
  }

  private void createTables() throws Exception {
    // Create tables if necessary
    try (Connection conn = getConnection();
        Statement stmt = conn.createStatement()) {
      // Table schema defined as in YARN-3134.
      String sql = "CREATE TABLE IF NOT EXISTS " + ENTITY_TABLE_NAME
          + "(user VARCHAR NOT NULL, cluster VARCHAR NOT NULL, "
          + "flow_name VARCHAR NOT NULL, flow_version VARCHAR NOT NULL, "
          + "flow_run UNSIGNED_LONG NOT NULL, "
          + "app_id VARCHAR NOT NULL, type VARCHAR NOT NULL, "
          + "entity_id VARCHAR NOT NULL, "
          + "creation_time UNSIGNED_LONG, modified_time UNSIGNED_LONG, "
          + "configs VARCHAR, "
          + CONFIG_COLUMN_FAMILY + PHOENIX_COL_FAMILY_PLACE_HOLDER + " VARCHAR, "
          + INFO_COLUMN_FAMILY + PHOENIX_COL_FAMILY_PLACE_HOLDER + " VARBINARY, "
          + IS_RELATED_TO_FAMILY + PHOENIX_COL_FAMILY_PLACE_HOLDER + " VARCHAR, "
          + RELATES_TO_FAMILY + PHOENIX_COL_FAMILY_PLACE_HOLDER + " VARCHAR "
          + "CONSTRAINT pk PRIMARY KEY("
          + "user, cluster, flow_name, flow_version, flow_run DESC, app_id, "
          + "type, entity_id))";
      stmt.executeUpdate(sql);
      sql = "CREATE TABLE IF NOT EXISTS " + EVENT_TABLE_NAME
          + "(user VARCHAR NOT NULL, cluster VARCHAR NOT NULL, "
          + "flow_name VARCHAR NOT NULL, flow_version VARCHAR NOT NULL, "
          + "flow_run UNSIGNED_LONG NOT NULL, "
          + "app_id VARCHAR NOT NULL, type VARCHAR NOT NULL, "
          + "entity_id VARCHAR NOT NULL, "
          + "timestamp UNSIGNED_LONG NOT NULL, event_id VARCHAR NOT NULL, "
          + EVENT_INFO_COLUMN_FAMILY + PHOENIX_COL_FAMILY_PLACE_HOLDER + " VARBINARY "
          + "CONSTRAINT pk PRIMARY KEY("
          + "user, cluster, flow_name, flow_version, flow_run DESC, app_id, "
          + "type, entity_id, timestamp DESC, event_id))";
      stmt.executeUpdate(sql);
      sql = "CREATE TABLE IF NOT EXISTS " + METRIC_TABLE_NAME
          + "(user VARCHAR NOT NULL, cluster VARCHAR NOT NULL, "
          + "flow_name VARCHAR NOT NULL, flow_version VARCHAR NOT NULL, "
          + "flow_run UNSIGNED_LONG NOT NULL, "
          + "app_id VARCHAR NOT NULL, type VARCHAR NOT NULL, "
          + "entity_id VARCHAR NOT NULL, "
          + "metric_id VARCHAR NOT NULL, "
          + "singledata VARBINARY, "
          + "time UNSIGNED_LONG "
          + "CONSTRAINT pk PRIMARY KEY("
          + "user, cluster, flow_name, flow_version, flow_run DESC, app_id, "
          + "type, entity_id, metric_id))";
      stmt.executeUpdate(sql);
      conn.commit();
    } catch (SQLException se) {
      LOG.error("Failed in init data " + se.getLocalizedMessage());
      throw se;
    }
    return;
  }

  private static class DynamicColumns<K> {
    static final String COLUMN_FAMILY_TYPE_BYTES = " VARBINARY";
    static final String COLUMN_FAMILY_TYPE_STRING = " VARCHAR";
    String columnFamilyPrefix;
    String type;
    Set<K> columns;

    public DynamicColumns(String columnFamilyPrefix, String type,
        Set<K> keyValues) {
      this.columnFamilyPrefix = columnFamilyPrefix;
      this.columns = keyValues;
      this.type = type;
    }
  }

  private static <K> StringBuilder appendColumnsSQL(
      StringBuilder colNames, DynamicColumns<K> cfInfo) {
    // Prepare the sql template by iterating through all keys
    for (K key : cfInfo.columns) {
      colNames.append(",").append(cfInfo.columnFamilyPrefix)
          .append(key.toString()).append(cfInfo.type);
    }
    return colNames;
  }

  private static <K, V> int setValuesForColumnFamily(
      PreparedStatement ps, Map<K, V> keyValues, int startPos,
      boolean converToBytes) throws SQLException {
    int idx = startPos;
    for (Map.Entry<K, V> entry : keyValues.entrySet()) {
      V value = entry.getValue();
      if (value instanceof Collection) {
        ps.setString(idx++, StringUtils.join(
            (Collection) value, PHOENIX_STORAGE_SEPARATOR));
      } else {
        if (converToBytes) {
          try {
            ps.setBytes(idx++, GenericObjectMapper.write(entry.getValue()));
          } catch (IOException ie) {
            LOG.error("Exception in converting values into bytes "
                + ie.getMessage());
            throw new SQLException(ie);
          }
        } else {
          ps.setString(idx++, value.toString());
        }
      }
    }
    return idx;
  }

  private static <K, V> int setBytesForColumnFamily(
      PreparedStatement ps, Map<K, V> keyValues, int startPos)
      throws SQLException {
    return setValuesForColumnFamily(ps, keyValues, startPos, true);
  }

  private static <K, V> int setStringsForColumnFamily(
      PreparedStatement ps, Map<K, V> keyValues, int startPos)
      throws SQLException {
    return setValuesForColumnFamily(ps, keyValues, startPos, false);
  }

  private static int setStringsForPrimaryKey(PreparedStatement ps,
      TimelineCollectorContext context, TimelineEntity entity, int startPos)
      throws SQLException {
    int idx = startPos;
    ps.setString(idx++, context.getClusterId());
    ps.setString(idx++, context.getUserId());
    ps.setString(idx++,
        context.getFlowName());
    ps.setString(idx++, context.getFlowVersion());
    ps.setLong(idx++, context.getFlowRunId());
    ps.setString(idx++, context.getAppId());
    ps.setString(idx++, entity.getType());
    ps.setString(idx++, entity.getId());
    return idx;
  }

  private static void storeEntityVariableLengthFields(TimelineEntity entity,
      TimelineCollectorContext context, Connection conn) throws SQLException {
    int numPlaceholders = 0;
    StringBuilder columnDefs = new StringBuilder(
        StringUtils.join(PHOENIX_STORAGE_PK_LIST, ","));
    if (entity.getConfigs() != null) {
      Set<String> keySet = entity.getConfigs().keySet();
      appendColumnsSQL(columnDefs, new DynamicColumns<>(
          CONFIG_COLUMN_FAMILY, DynamicColumns.COLUMN_FAMILY_TYPE_STRING,
          keySet));
      numPlaceholders += keySet.size();
    }
    if (entity.getInfo() != null) {
      Set<String> keySet = entity.getInfo().keySet();
      appendColumnsSQL(columnDefs, new DynamicColumns<>(
          INFO_COLUMN_FAMILY, DynamicColumns.COLUMN_FAMILY_TYPE_BYTES,
          keySet));
      numPlaceholders += keySet.size();
    }
    if (entity.getIsRelatedToEntities() != null) {
      Set<String> keySet = entity.getIsRelatedToEntities().keySet();
      appendColumnsSQL(columnDefs, new DynamicColumns<>(
          IS_RELATED_TO_FAMILY, DynamicColumns.COLUMN_FAMILY_TYPE_STRING,
          keySet));
      numPlaceholders += keySet.size();
    }
    if (entity.getRelatesToEntities() != null) {
      Set<String> keySet = entity.getRelatesToEntities().keySet();
      appendColumnsSQL(columnDefs, new DynamicColumns<>(
          RELATES_TO_FAMILY, DynamicColumns.COLUMN_FAMILY_TYPE_STRING,
          keySet));
      numPlaceholders += keySet.size();
    }
    if (numPlaceholders == 0) {
      return;
    }
    StringBuilder placeholders = new StringBuilder();
    placeholders.append(
        StringUtils.repeat("?,", PHOENIX_STORAGE_PK_LIST.length));
    // numPlaceholders >= 1 now
    placeholders.append("?")
        .append(StringUtils.repeat(",?", numPlaceholders - 1));
    String sqlVariableLengthFields = new StringBuilder("UPSERT INTO ")
        .append(ENTITY_TABLE_NAME).append(" (").append(columnDefs)
        .append(") VALUES(").append(placeholders).append(")").toString();
    if (LOG.isDebugEnabled()) {
      LOG.debug("SQL statement for variable length fields: "
          + sqlVariableLengthFields);
    }
    // Use try with resource statement for the prepared statement
    try (PreparedStatement psVariableLengthFields =
        conn.prepareStatement(sqlVariableLengthFields)) {
      int idx = setStringsForPrimaryKey(
          psVariableLengthFields, context, entity, 1);
      if (entity.getConfigs() != null) {
        idx = setStringsForColumnFamily(
            psVariableLengthFields, entity.getConfigs(), idx);
      }
      if (entity.getInfo() != null) {
        idx = setBytesForColumnFamily(
            psVariableLengthFields, entity.getInfo(), idx);
      }
      if (entity.getIsRelatedToEntities() != null) {
        idx = setStringsForColumnFamily(
            psVariableLengthFields, entity.getIsRelatedToEntities(), idx);
      }
      if (entity.getRelatesToEntities() != null) {
        idx = setStringsForColumnFamily(
            psVariableLengthFields, entity.getRelatesToEntities(), idx);
      }
      psVariableLengthFields.execute();
    }
  }

  private static void storeMetrics(TimelineEntity entity,
      TimelineCollectorContext context, Connection conn) throws SQLException {
    if (entity.getMetrics() == null) {
      return;
    }
    Set<TimelineMetric> metrics = entity.getMetrics();
    for (TimelineMetric metric : metrics) {
      StringBuilder sqlColumns = new StringBuilder(
          StringUtils.join(PHOENIX_STORAGE_PK_LIST, ","));
      sqlColumns.append(",")
          .append(StringUtils.join(TIMELINE_METRIC_EXTRA_PK_LIST, ","));
      sqlColumns.append(",").append("singledata, time");
      StringBuilder placeholders = new StringBuilder();
      placeholders.append(
          StringUtils.repeat("?,", PHOENIX_STORAGE_PK_LIST.length))
          .append(StringUtils.repeat("?,", TIMELINE_METRIC_EXTRA_PK_LIST.length));
      placeholders.append("?, ?");
      String sqlMetric = new StringBuilder("UPSERT INTO ")
          .append(METRIC_TABLE_NAME).append(" (").append(sqlColumns)
          .append(") VALUES(").append(placeholders).append(")").toString();
      if (LOG.isDebugEnabled()) {
        LOG.debug("SQL statement for metric: " + sqlMetric);
      }
      try (PreparedStatement psMetrics = conn.prepareStatement(sqlMetric)) {
        if (metric.getType().equals(TimelineMetric.Type.TIME_SERIES)) {
          LOG.warn("The incoming timeline metric contains time series data, "
              + "which is currently not supported by Phoenix storage. "
              + "Time series will be truncated. ");
        }
        int idx = setStringsForPrimaryKey(psMetrics, context, entity, 1);
        psMetrics.setString(idx++, metric.getId());
        Iterator<Map.Entry<Long, Number>> currNumIter =
            metric.getValues().entrySet().iterator();
        if (currNumIter.hasNext()) {
          // TODO: support time series storage
          Map.Entry<Long, Number> currEntry = currNumIter.next();
          psMetrics.setBytes(idx++,
              GenericObjectMapper.write(currEntry.getValue()));
          psMetrics.setLong(idx++, currEntry.getKey());
        } else {
          psMetrics.setBytes(idx++, GenericObjectMapper.write(null));
          LOG.warn("The incoming metric contains an empty value set. ");
        }
        psMetrics.execute();
      } catch (IOException ie) {
        LOG.error("Exception on converting single data to bytes: "
            + ie.getMessage());
        throw new SQLException(ie);
      }
    }
  }

  private static void storeEvents(TimelineEntity entity,
      TimelineCollectorContext context, Connection conn) throws SQLException {
    if (entity.getEvents() == null) {
      return;
    }
    Set<TimelineEvent> events = entity.getEvents();
    for (TimelineEvent event : events) {
      // We need this number to check if the incoming event's info field is empty
      int numPlaceholders = 0;
      StringBuilder sqlColumns = new StringBuilder(
          StringUtils.join(PHOENIX_STORAGE_PK_LIST, ","));
      sqlColumns.append(",")
          .append(StringUtils.join(TIMELINE_EVENT_EXTRA_PK_LIST, ","));
      appendColumnsSQL(sqlColumns, new DynamicColumns<>(
          EVENT_INFO_COLUMN_FAMILY, DynamicColumns.COLUMN_FAMILY_TYPE_BYTES,
          event.getInfo().keySet()));
      numPlaceholders += event.getInfo().keySet().size();
      if (numPlaceholders == 0) {
        continue;
      }
      StringBuilder placeholders = new StringBuilder();
      placeholders.append(
          StringUtils.repeat("?,", PHOENIX_STORAGE_PK_LIST.length))
          .append(StringUtils.repeat("?,", TIMELINE_EVENT_EXTRA_PK_LIST.length));
      // numPlaceholders >= 1 now
      placeholders.append("?")
            .append(StringUtils.repeat(",?", numPlaceholders - 1));
      String sqlEvents = new StringBuilder("UPSERT INTO ")
          .append(EVENT_TABLE_NAME).append(" (").append(sqlColumns)
          .append(") VALUES(").append(placeholders).append(")").toString();
      if (LOG.isDebugEnabled()) {
        LOG.debug("SQL statement for events: " + sqlEvents);
      }
      try (PreparedStatement psEvent = conn.prepareStatement(sqlEvents)) {
        int idx = setStringsForPrimaryKey(psEvent, context, entity, 1);
        psEvent.setLong(idx++, event.getTimestamp());
        psEvent.setString(idx++, event.getId());
        setBytesForColumnFamily(psEvent, event.getInfo(), idx);
        psEvent.execute();
      }
    }
  }

  // WARNING: This method will permanently drop a table!
  @Private
  @VisibleForTesting
  void dropTable(String tableName) throws Exception {
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement()) {
      String sql = "DROP TABLE " + tableName;
      stmt.executeUpdate(sql);
    } catch (SQLException se) {
      LOG.error("Failed in dropping entity table " + se.getLocalizedMessage());
      throw se;
    }
  }
}
