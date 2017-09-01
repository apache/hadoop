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
package org.apache.hadoop.yarn.server.timelineservice.storage.subapplication;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineHBaseSchemaConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The sub application table has column families:
 * info, config and metrics.
 * Info stores information about a timeline entity object
 * config stores configuration data of a timeline entity object
 * metrics stores the metrics of a timeline entity object
 *
 * Example sub application table record:
 *
 * <pre>
 * |-------------------------------------------------------------------------|
 * |  Row          | Column Family             | Column Family| Column Family|
 * |  key          | info                      | metrics      | config       |
 * |-------------------------------------------------------------------------|
 * | subAppUserId! | id:entityId               | metricId1:   | configKey1:  |
 * | clusterId!    | type:entityType           | metricValue1 | configValue1 |
 * | entityType!   |                           | @timestamp1  |              |
 * | idPrefix!|    |                           |              | configKey2:  |
 * | entityId!     | created_time:             | metricId1:   | configValue2 |
 * | userId        | 1392993084018             | metricValue2 |              |
 * |               |                           | @timestamp2  |              |
 * |               | i!infoKey:                |              |              |
 * |               | infoValue                 | metricId1:   |              |
 * |               |                           | metricValue1 |              |
 * |               |                           | @timestamp2  |              |
 * |               | e!eventId=timestamp=      |              |              |
 * |               | infoKey:                  |              |              |
 * |               | eventInfoValue            |              |              |
 * |               |                           |              |              |
 * |               | r!relatesToKey:           |              |              |
 * |               | id3=id4=id5               |              |              |
 * |               |                           |              |              |
 * |               | s!isRelatedToKey          |              |              |
 * |               | id7=id9=id6               |              |              |
 * |               |                           |              |              |
 * |               | flowVersion:              |              |              |
 * |               | versionValue              |              |              |
 * |-------------------------------------------------------------------------|
 * </pre>
 */
public class SubApplicationTable extends BaseTable<SubApplicationTable> {
  /** sub app prefix. */
  private static final String PREFIX =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + "subapplication";

  /** config param name that specifies the subapplication table name. */
  public static final String TABLE_NAME_CONF_NAME = PREFIX + ".table.name";

  /**
   * config param name that specifies the TTL for metrics column family in
   * subapplication table.
   */
  private static final String METRICS_TTL_CONF_NAME = PREFIX
      + ".table.metrics.ttl";

  /**
   * config param name that specifies max-versions for
   * metrics column family in subapplication table.
   */
  private static final String METRICS_MAX_VERSIONS =
      PREFIX + ".table.metrics.max-versions";

  /** default value for subapplication table name. */
  public static final String DEFAULT_TABLE_NAME =
      "timelineservice.subapplication";

  /** default TTL is 30 days for metrics timeseries. */
  private static final int DEFAULT_METRICS_TTL = 2592000;

  /** default max number of versions. */
  private static final int DEFAULT_METRICS_MAX_VERSIONS = 10000;

  private static final Logger LOG = LoggerFactory.getLogger(
      SubApplicationTable.class);

  public SubApplicationTable() {
    super(TABLE_NAME_CONF_NAME, DEFAULT_TABLE_NAME);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.BaseTable#createTable
   * (org.apache.hadoop.hbase.client.Admin,
   * org.apache.hadoop.conf.Configuration)
   */
  public void createTable(Admin admin, Configuration hbaseConf)
      throws IOException {

    TableName table = getTableName(hbaseConf);
    if (admin.tableExists(table)) {
      // do not disable / delete existing table
      // similar to the approach taken by map-reduce jobs when
      // output directory exists
      throw new IOException("Table " + table.getNameAsString()
          + " already exists.");
    }

    HTableDescriptor subAppTableDescp = new HTableDescriptor(table);
    HColumnDescriptor infoCF =
        new HColumnDescriptor(SubApplicationColumnFamily.INFO.getBytes());
    infoCF.setBloomFilterType(BloomType.ROWCOL);
    subAppTableDescp.addFamily(infoCF);

    HColumnDescriptor configCF =
        new HColumnDescriptor(SubApplicationColumnFamily.CONFIGS.getBytes());
    configCF.setBloomFilterType(BloomType.ROWCOL);
    configCF.setBlockCacheEnabled(true);
    subAppTableDescp.addFamily(configCF);

    HColumnDescriptor metricsCF =
        new HColumnDescriptor(SubApplicationColumnFamily.METRICS.getBytes());
    subAppTableDescp.addFamily(metricsCF);
    metricsCF.setBlockCacheEnabled(true);
    // always keep 1 version (the latest)
    metricsCF.setMinVersions(1);
    metricsCF.setMaxVersions(
        hbaseConf.getInt(METRICS_MAX_VERSIONS, DEFAULT_METRICS_MAX_VERSIONS));
    metricsCF.setTimeToLive(hbaseConf.getInt(METRICS_TTL_CONF_NAME,
        DEFAULT_METRICS_TTL));
    subAppTableDescp.setRegionSplitPolicyClassName(
        "org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy");
    subAppTableDescp.setValue("KeyPrefixRegionSplitPolicy.prefix_length",
        TimelineHBaseSchemaConstants.USERNAME_SPLIT_KEY_PREFIX_LENGTH);
    admin.createTable(subAppTableDescp,
        TimelineHBaseSchemaConstants.getUsernameSplits());
    LOG.info("Status of table creation for " + table.getNameAsString() + "="
        + admin.tableExists(table));
  }

  /**
   * @param metricsTTL time to live parameter for the metricss in this table.
   * @param hbaseConf configururation in which to set the metrics TTL config
   *          variable.
   */
  public void setMetricsTTL(int metricsTTL, Configuration hbaseConf) {
    hbaseConf.setInt(METRICS_TTL_CONF_NAME, metricsTTL);
  }

}
