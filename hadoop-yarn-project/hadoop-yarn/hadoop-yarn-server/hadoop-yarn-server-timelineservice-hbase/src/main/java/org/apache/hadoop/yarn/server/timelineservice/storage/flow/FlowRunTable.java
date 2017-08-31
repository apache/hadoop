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
package org.apache.hadoop.yarn.server.timelineservice.storage.flow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;

/**
 * The flow run table has column family info
 * Stores per flow run information
 * aggregated across applications.
 *
 * Metrics are also stored in the info column family.
 *
 * Example flow run table record:
 *
 * <pre>
 * flow_run table
 * |-------------------------------------------|
 * |  Row key   | Column Family                |
 * |            | info                         |
 * |-------------------------------------------|
 * | clusterId! | flow_version:version7        |
 * | userName!  |                              |
 * | flowName!  | running_apps:1               |
 * | flowRunId  |                              |
 * |            | min_start_time:1392995080000 |
 * |            | #0:""                        |
 * |            |                              |
 * |            | min_start_time:1392995081012 |
 * |            | #0:appId2                    |
 * |            |                              |
 * |            | min_start_time:1392993083210 |
 * |            | #0:appId3                    |
 * |            |                              |
 * |            |                              |
 * |            | max_end_time:1392993084018   |
 * |            | #0:""                        |
 * |            |                              |
 * |            |                              |
 * |            | m!mapInputRecords:127        |
 * |            | #0:""                        |
 * |            |                              |
 * |            | m!mapInputRecords:31         |
 * |            | #2:appId2                    |
 * |            |                              |
 * |            | m!mapInputRecords:37         |
 * |            | #1:appId3                    |
 * |            |                              |
 * |            |                              |
 * |            | m!mapOutputRecords:181       |
 * |            | #0:""                        |
 * |            |                              |
 * |            | m!mapOutputRecords:37        |
 * |            | #1:appId3                    |
 * |            |                              |
 * |            |                              |
 * |-------------------------------------------|
 * </pre>
 */
public class FlowRunTable extends BaseTable<FlowRunTable> {
  /** entity prefix. */
  private static final String PREFIX =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + ".flowrun";

  /** config param name that specifies the flowrun table name. */
  public static final String TABLE_NAME_CONF_NAME = PREFIX + ".table.name";

  /** default value for flowrun table name. */
  public static final String DEFAULT_TABLE_NAME = "timelineservice.flowrun";

  private static final Logger LOG =
      LoggerFactory.getLogger(FlowRunTable.class);

  /** default max number of versions. */
  public static final int DEFAULT_METRICS_MAX_VERSIONS = Integer.MAX_VALUE;

  public FlowRunTable() {
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

    HTableDescriptor flowRunTableDescp = new HTableDescriptor(table);
    HColumnDescriptor infoCF =
        new HColumnDescriptor(FlowRunColumnFamily.INFO.getBytes());
    infoCF.setBloomFilterType(BloomType.ROWCOL);
    flowRunTableDescp.addFamily(infoCF);
    infoCF.setMinVersions(1);
    infoCF.setMaxVersions(DEFAULT_METRICS_MAX_VERSIONS);

    // TODO: figure the split policy
    String coprocessorJarPathStr = hbaseConf.get(
        YarnConfiguration.FLOW_RUN_COPROCESSOR_JAR_HDFS_LOCATION,
        YarnConfiguration.DEFAULT_HDFS_LOCATION_FLOW_RUN_COPROCESSOR_JAR);

    Path coprocessorJarPath = new Path(coprocessorJarPathStr);
    LOG.info("CoprocessorJarPath=" + coprocessorJarPath.toString());
    flowRunTableDescp.addCoprocessor(
        FlowRunCoprocessor.class.getCanonicalName(), coprocessorJarPath,
        Coprocessor.PRIORITY_USER, null);
    admin.createTable(flowRunTableDescp);
    LOG.info("Status of table creation for " + table.getNameAsString() + "="
        + admin.tableExists(table));
  }
}
