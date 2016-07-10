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
package org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineHBaseSchemaConstants;

import java.io.IOException;

/**
 * The app_flow table as column families mapping. Mapping stores
 * appId to flowName and flowRunId mapping information
 *
 * Example app_flow table record:
 *
 * <pre>
 * |--------------------------------------|
 * |  Row       | Column Family           |
 * |  key       | info                    |
 * |--------------------------------------|
 * | clusterId! | flowName:               |
 * | AppId      | foo@daily_hive_report   |
 * |            |                         |
 * |            | flowRunId:              |
 * |            | 1452828720457           |
 * |            |                         |
 * |            | user_id:                |
 * |            | admin                   |
 * |            |                         |
 * |            |                         |
 * |            |                         |
 * |--------------------------------------|
 * </pre>
 */
public class AppToFlowTable extends BaseTable<AppToFlowTable> {
  /** app_flow prefix. */
  private static final String PREFIX =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + "app-flow";

  /** config param name that specifies the app_flow table name. */
  public static final String TABLE_NAME_CONF_NAME = PREFIX + ".table.name";

  /** default value for app_flow table name. */
  private static final String DEFAULT_TABLE_NAME = "timelineservice.app_flow";

  private static final Log LOG = LogFactory.getLog(AppToFlowTable.class);

  public AppToFlowTable() {
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

    HTableDescriptor appToFlowTableDescp = new HTableDescriptor(table);
    HColumnDescriptor mappCF =
        new HColumnDescriptor(AppToFlowColumnFamily.MAPPING.getBytes());
    mappCF.setBloomFilterType(BloomType.ROWCOL);
    appToFlowTableDescp.addFamily(mappCF);

    appToFlowTableDescp
        .setRegionSplitPolicyClassName(
            "org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy");
    appToFlowTableDescp.setValue("KeyPrefixRegionSplitPolicy.prefix_length",
        TimelineHBaseSchemaConstants.USERNAME_SPLIT_KEY_PREFIX_LENGTH);
    admin.createTable(appToFlowTableDescp,
        TimelineHBaseSchemaConstants.getUsernameSplits());
    LOG.info("Status of table creation for " + table.getNameAsString() + "="
        + admin.tableExists(table));
  }
}
