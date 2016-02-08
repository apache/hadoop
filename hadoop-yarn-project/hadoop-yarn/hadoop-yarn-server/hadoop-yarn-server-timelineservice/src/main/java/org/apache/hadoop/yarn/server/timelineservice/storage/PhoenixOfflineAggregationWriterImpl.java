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
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timeline.GenericObjectMapper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.OfflineAggregationInfo;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.phoenix.util.PropertiesUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Offline aggregation Phoenix storage. This storage currently consists of two
 * aggregation tables, one for flow level aggregation and one for user level
 * aggregation.
 *
 * Example table record:
 *
 * <pre>
 * |---------------------------|
 * |  Primary   | Column Family|
 * |  key       | metrics      |
 * |---------------------------|
 * | row_key    | metricId1:   |
 * |            | metricValue1 |
 * |            | @timestamp1  |
 * |            |              |
 * |            | metriciD1:   |
 * |            | metricValue2 |
 * |            | @timestamp2  |
 * |            |              |
 * |            | metricId2:   |
 * |            | metricValue1 |
 * |            | @timestamp2  |
 * |            |              |
 * |            |              |
 * |            |              |
 * |            |              |
 * |            |              |
 * |            |              |
 * |            |              |
 * |            |              |
 * |            |              |
 * |            |              |
 * |            |              |
 * |            |              |
 * |            |              |
 * |            |              |
 * |---------------------------|
 * </pre>
 *
 * For the flow aggregation table, the primary key contains user, cluster, and
 * flow id. For user aggregation table,the primary key is user.
 *
 * Metrics column family stores all aggregated metrics for each record.
 */
@Private
@Unstable
public class PhoenixOfflineAggregationWriterImpl
    extends OfflineAggregationWriter {

  private static final Log LOG
      = LogFactory.getLog(PhoenixOfflineAggregationWriterImpl.class);
  private static final String PHOENIX_COL_FAMILY_PLACE_HOLDER
      = "timeline_cf_placeholder";

  /** Default Phoenix JDBC driver name. */
  private static final String DRIVER_CLASS_NAME
      = "org.apache.phoenix.jdbc.PhoenixDriver";

  /** Default Phoenix timeline config column family. */
  private static final String METRIC_COLUMN_FAMILY = "m.";
  /** Default Phoenix timeline info column family. */
  private static final String INFO_COLUMN_FAMILY = "i.";
  /** Default separator for Phoenix storage. */
  private static final String AGGREGATION_STORAGE_SEPARATOR = ";";

  /** Connection string to the deployed Phoenix cluster. */
  private String connString = null;
  private Properties connProperties = new Properties();

  public PhoenixOfflineAggregationWriterImpl(Properties prop) {
    super(PhoenixOfflineAggregationWriterImpl.class.getName());
    connProperties = PropertiesUtil.deepCopy(prop);
  }

  public PhoenixOfflineAggregationWriterImpl() {
    super(PhoenixOfflineAggregationWriterImpl.class.getName());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    Class.forName(DRIVER_CLASS_NAME);
    // so check it here and only read in the config if it's not overridden.
    connString =
        conf.get(YarnConfiguration.PHOENIX_OFFLINE_STORAGE_CONN_STR,
            YarnConfiguration.PHOENIX_OFFLINE_STORAGE_CONN_STR_DEFAULT);
    super.init(conf);
  }

  @Override
  public TimelineWriteResponse writeAggregatedEntity(
      TimelineCollectorContext context, TimelineEntities entities,
      OfflineAggregationInfo info) throws IOException {
    TimelineWriteResponse response = new TimelineWriteResponse();
    String sql = "UPSERT INTO " + info.getTableName()
        + " (" + StringUtils.join(info.getPrimaryKeyList(), ",")
        + ", created_time, metric_names) "
        + "VALUES ("
        + StringUtils.repeat("?,", info.getPrimaryKeyList().length)
        + "?, ?)";
    if (LOG.isDebugEnabled()) {
      LOG.debug("TimelineEntity write SQL: " + sql);
    }

    try (Connection conn = getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      for (TimelineEntity entity : entities.getEntities()) {
        HashMap<String, TimelineMetric> formattedMetrics = new HashMap<>();
        if (entity.getMetrics() != null) {
          for (TimelineMetric m : entity.getMetrics()) {
            formattedMetrics.put(m.getId(), m);
          }
        }
        int idx = info.setStringsForPrimaryKey(ps, context, null, 1);
        ps.setLong(idx++, entity.getCreatedTime());
        ps.setString(idx++,
            StringUtils.join(formattedMetrics.keySet().toArray(),
            AGGREGATION_STORAGE_SEPARATOR));
        ps.execute();

        storeEntityVariableLengthFields(entity, formattedMetrics, context, conn,
            info);

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
   * Create Phoenix tables for offline aggregation storage if the tables do not
   * exist.
   *
   * @throws IOException if any problem happens while creating Phoenix tables.
   */
  public void createPhoenixTables() throws IOException {
    // Create tables if necessary
    try (Connection conn = getConnection();
        Statement stmt = conn.createStatement()) {
      // Table schema defined as in YARN-3817.
      String sql = "CREATE TABLE IF NOT EXISTS "
          + OfflineAggregationInfo.FLOW_AGGREGATION_TABLE_NAME
          + "(user VARCHAR NOT NULL, cluster VARCHAR NOT NULL, "
          + "flow_name VARCHAR NOT NULL, "
          + "created_time UNSIGNED_LONG, "
          + METRIC_COLUMN_FAMILY + PHOENIX_COL_FAMILY_PLACE_HOLDER
          + " VARBINARY, "
          + "metric_names VARCHAR, info_keys VARCHAR "
          + "CONSTRAINT pk PRIMARY KEY("
          + "user, cluster, flow_name))";
      stmt.executeUpdate(sql);
      sql = "CREATE TABLE IF NOT EXISTS "
          + OfflineAggregationInfo.USER_AGGREGATION_TABLE_NAME
          + "(user VARCHAR NOT NULL, cluster VARCHAR NOT NULL, "
          + "created_time UNSIGNED_LONG, "
          + METRIC_COLUMN_FAMILY + PHOENIX_COL_FAMILY_PLACE_HOLDER
          + " VARBINARY, "
          + "metric_names VARCHAR, info_keys VARCHAR "
          + "CONSTRAINT pk PRIMARY KEY(user, cluster))";
      stmt.executeUpdate(sql);
      conn.commit();
    } catch (SQLException se) {
      LOG.error("Failed in init data " + se.getLocalizedMessage());
      throw new IOException(se);
    }
    return;
  }

  // Utility functions
  @Private
  @VisibleForTesting
  Connection getConnection() throws IOException {
    Connection conn;
    try {
      conn = DriverManager.getConnection(connString, connProperties);
      conn.setAutoCommit(false);
    } catch (SQLException se) {
      LOG.error("Failed to connect to phoenix server! "
          + se.getLocalizedMessage());
      throw new IOException(se);
    }
    return conn;
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

  private static class DynamicColumns<K> {
    static final String COLUMN_FAMILY_TYPE_BYTES = " VARBINARY";
    static final String COLUMN_FAMILY_TYPE_STRING = " VARCHAR";
    private String columnFamilyPrefix;
    private String type;
    private Set<K> columns;

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
            (Collection) value, AGGREGATION_STORAGE_SEPARATOR));
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

  private static void storeEntityVariableLengthFields(TimelineEntity entity,
      Map<String, TimelineMetric> formattedMetrics,
      TimelineCollectorContext context, Connection conn,
      OfflineAggregationInfo aggregationInfo) throws SQLException {
    int numPlaceholders = 0;
    StringBuilder columnDefs = new StringBuilder(
        StringUtils.join(aggregationInfo.getPrimaryKeyList(), ","));
    if (formattedMetrics != null && formattedMetrics.size() > 0) {
      appendColumnsSQL(columnDefs, new DynamicColumns<>(
          METRIC_COLUMN_FAMILY, DynamicColumns.COLUMN_FAMILY_TYPE_BYTES,
          formattedMetrics.keySet()));
      numPlaceholders += formattedMetrics.keySet().size();
    }
    if (numPlaceholders == 0) {
      return;
    }
    StringBuilder placeholders = new StringBuilder();
    placeholders.append(
        StringUtils.repeat("?,", aggregationInfo.getPrimaryKeyList().length));
    // numPlaceholders >= 1 now
    placeholders.append("?")
        .append(StringUtils.repeat(",?", numPlaceholders - 1));
    String sqlVariableLengthFields = new StringBuilder("UPSERT INTO ")
        .append(aggregationInfo.getTableName()).append(" (").append(columnDefs)
        .append(") VALUES(").append(placeholders).append(")").toString();
    if (LOG.isDebugEnabled()) {
      LOG.debug("SQL statement for variable length fields: "
          + sqlVariableLengthFields);
    }
    // Use try with resource statement for the prepared statement
    try (PreparedStatement psVariableLengthFields =
        conn.prepareStatement(sqlVariableLengthFields)) {
      int idx = aggregationInfo.setStringsForPrimaryKey(
          psVariableLengthFields, context, null, 1);
      if (formattedMetrics != null && formattedMetrics.size() > 0) {
        idx = setBytesForColumnFamily(
            psVariableLengthFields, formattedMetrics, idx);
      }
      psVariableLengthFields.execute();
    }
  }
}
