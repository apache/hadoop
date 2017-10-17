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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunTable;

/**
 * Unit tests for checking different schema prefixes.
 */
public class TestHBaseTimelineStorageSchema {
  private static HBaseTestingUtility util;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util = new HBaseTestingUtility();
    Configuration conf = util.getConfiguration();
    conf.setInt("hfile.format.version", 3);
    util.startMiniCluster();
  }

  @Test
  public void createWithDefaultPrefix() throws IOException {
    Configuration hbaseConf = util.getConfiguration();
    DataGeneratorForTest.createSchema(hbaseConf);
    Connection conn = null;
    conn = ConnectionFactory.createConnection(hbaseConf);
    Admin admin = conn.getAdmin();

    TableName entityTableName = BaseTable.getTableName(hbaseConf,
        EntityTable.TABLE_NAME_CONF_NAME, EntityTable.DEFAULT_TABLE_NAME);
    assertTrue(admin.tableExists(entityTableName));
    assertTrue(entityTableName.getNameAsString().startsWith(
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_HBASE_SCHEMA_PREFIX));
    Table entityTable = conn.getTable(BaseTable.getTableName(hbaseConf,
        EntityTable.TABLE_NAME_CONF_NAME, EntityTable.DEFAULT_TABLE_NAME));
    assertNotNull(entityTable);

    TableName flowRunTableName = BaseTable.getTableName(hbaseConf,
        FlowRunTable.TABLE_NAME_CONF_NAME, FlowRunTable.DEFAULT_TABLE_NAME);
    assertTrue(admin.tableExists(flowRunTableName));
    assertTrue(flowRunTableName.getNameAsString().startsWith(
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_HBASE_SCHEMA_PREFIX));
    Table flowRunTable = conn.getTable(BaseTable.getTableName(hbaseConf,
        FlowRunTable.TABLE_NAME_CONF_NAME, FlowRunTable.DEFAULT_TABLE_NAME));
    assertNotNull(flowRunTable);
  }

  @Test
  public void createWithSetPrefix() throws IOException {
    Configuration hbaseConf = util.getConfiguration();
    String prefix = "unit-test.";
    hbaseConf.set(YarnConfiguration.TIMELINE_SERVICE_HBASE_SCHEMA_PREFIX_NAME,
        prefix);
    DataGeneratorForTest.createSchema(hbaseConf);
    Connection conn = null;
    conn = ConnectionFactory.createConnection(hbaseConf);
    Admin admin = conn.getAdmin();

    TableName entityTableName = BaseTable.getTableName(hbaseConf,
        EntityTable.TABLE_NAME_CONF_NAME, EntityTable.DEFAULT_TABLE_NAME);
    assertTrue(admin.tableExists(entityTableName));
    assertTrue(entityTableName.getNameAsString().startsWith(prefix));
    Table entityTable = conn.getTable(BaseTable.getTableName(hbaseConf,
        EntityTable.TABLE_NAME_CONF_NAME, EntityTable.DEFAULT_TABLE_NAME));
    assertNotNull(entityTable);

    TableName flowRunTableName = BaseTable.getTableName(hbaseConf,
        FlowRunTable.TABLE_NAME_CONF_NAME, FlowRunTable.DEFAULT_TABLE_NAME);
    assertTrue(admin.tableExists(flowRunTableName));
    assertTrue(flowRunTableName.getNameAsString().startsWith(prefix));
    Table flowRunTable = conn.getTable(BaseTable.getTableName(hbaseConf,
        FlowRunTable.TABLE_NAME_CONF_NAME, FlowRunTable.DEFAULT_TABLE_NAME));
    assertNotNull(flowRunTable);

    // create another set with a diff prefix
    hbaseConf
        .unset(YarnConfiguration.TIMELINE_SERVICE_HBASE_SCHEMA_PREFIX_NAME);
    prefix = "yet-another-unit-test.";
    hbaseConf.set(YarnConfiguration.TIMELINE_SERVICE_HBASE_SCHEMA_PREFIX_NAME,
        prefix);
    DataGeneratorForTest.createSchema(hbaseConf);
    entityTableName = BaseTable.getTableName(hbaseConf,
        EntityTable.TABLE_NAME_CONF_NAME, EntityTable.DEFAULT_TABLE_NAME);
    assertTrue(admin.tableExists(entityTableName));
    assertTrue(entityTableName.getNameAsString().startsWith(prefix));
    entityTable = conn.getTable(BaseTable.getTableName(hbaseConf,
        EntityTable.TABLE_NAME_CONF_NAME, EntityTable.DEFAULT_TABLE_NAME));
    assertNotNull(entityTable);

    flowRunTableName = BaseTable.getTableName(hbaseConf,
        FlowRunTable.TABLE_NAME_CONF_NAME, FlowRunTable.DEFAULT_TABLE_NAME);
    assertTrue(admin.tableExists(flowRunTableName));
    assertTrue(flowRunTableName.getNameAsString().startsWith(prefix));
    flowRunTable = conn.getTable(BaseTable.getTableName(hbaseConf,
        FlowRunTable.TABLE_NAME_CONF_NAME, FlowRunTable.DEFAULT_TABLE_NAME));
    assertNotNull(flowRunTable);
    hbaseConf
    .unset(YarnConfiguration.TIMELINE_SERVICE_HBASE_SCHEMA_PREFIX_NAME);
  }
}
