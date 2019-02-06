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
package org.apache.hadoop.yarn.server.timelineservice.storage.application;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineHBaseSchemaConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create, read and write to the Application Table.
 */
public class ApplicationTableRW extends BaseTableRW<ApplicationTable> {
  /** application prefix. */
  private static final String PREFIX =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + "application";

  /** config param name that specifies the application table name. */
  public static final String TABLE_NAME_CONF_NAME = PREFIX + ".table.name";

  /**
   * config param name that specifies the TTL for metrics column family in
   * application table.
   */
  private static final String METRICS_TTL_CONF_NAME = PREFIX
      + ".table.metrics.ttl";

  /**
   * config param name that specifies max-versions for metrics column family in
   * entity table.
   */
  private static final String METRICS_MAX_VERSIONS =
      PREFIX + ".table.metrics.max-versions";

  /** default value for application table name. */
  private static final String DEFAULT_TABLE_NAME =
      "timelineservice.application";

  /** default TTL is 30 days for metrics timeseries. */
  private static final int DEFAULT_METRICS_TTL = 2592000;

  /** default max number of versions. */
  private static final int DEFAULT_METRICS_MAX_VERSIONS = 10000;

  private static final Logger LOG =
      LoggerFactory.getLogger(ApplicationTableRW.class);

  public ApplicationTableRW() {
    super(TABLE_NAME_CONF_NAME, DEFAULT_TABLE_NAME);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.BaseTableRW#
   * createTable(org.apache.hadoop.hbase.client.Admin,
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

    HTableDescriptor applicationTableDescp = new HTableDescriptor(table);
    HColumnDescriptor infoCF =
        new HColumnDescriptor(ApplicationColumnFamily.INFO.getBytes());
    infoCF.setBloomFilterType(BloomType.ROWCOL);
    applicationTableDescp.addFamily(infoCF);

    HColumnDescriptor configCF =
        new HColumnDescriptor(ApplicationColumnFamily.CONFIGS.getBytes());
    configCF.setBloomFilterType(BloomType.ROWCOL);
    configCF.setBlockCacheEnabled(true);
    applicationTableDescp.addFamily(configCF);

    HColumnDescriptor metricsCF =
        new HColumnDescriptor(ApplicationColumnFamily.METRICS.getBytes());
    applicationTableDescp.addFamily(metricsCF);
    metricsCF.setBlockCacheEnabled(true);
    // always keep 1 version (the latest)
    metricsCF.setMinVersions(1);
    metricsCF.setMaxVersions(
        hbaseConf.getInt(METRICS_MAX_VERSIONS, DEFAULT_METRICS_MAX_VERSIONS));
    metricsCF.setTimeToLive(hbaseConf.getInt(METRICS_TTL_CONF_NAME,
        DEFAULT_METRICS_TTL));
    applicationTableDescp.setRegionSplitPolicyClassName(
        "org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy");
    applicationTableDescp.setValue("KeyPrefixRegionSplitPolicy.prefix_length",
        TimelineHBaseSchemaConstants.USERNAME_SPLIT_KEY_PREFIX_LENGTH);
    admin.createTable(applicationTableDescp,
        TimelineHBaseSchemaConstants.getUsernameSplits());
    LOG.info("Status of table creation for " + table.getNameAsString() + "="
        + admin.tableExists(table));
  }

  /**
   * @param metricsTTL time to live parameter for the metrics in this table.
   * @param hbaseConf configuration in which to set the metrics TTL config
   *          variable.
   */
  public void setMetricsTTL(int metricsTTL, Configuration hbaseConf) {
    hbaseConf.setInt(METRICS_TTL_CONF_NAME, metricsTTL);
  }

}
