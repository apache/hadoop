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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * This creates the schema for a hbase based backend for storing application
 * timeline information.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TimelineSchemaCreator {

  final static String NAME = TimelineSchemaCreator.class.getSimpleName();
  private static final Log LOG = LogFactory.getLog(TimelineSchemaCreator.class);
  final static byte[][] splits = { Bytes.toBytes("a"), Bytes.toBytes("ad"),
      Bytes.toBytes("an"), Bytes.toBytes("b"), Bytes.toBytes("ca"),
      Bytes.toBytes("cl"), Bytes.toBytes("d"), Bytes.toBytes("e"),
      Bytes.toBytes("f"), Bytes.toBytes("g"), Bytes.toBytes("h"),
      Bytes.toBytes("i"), Bytes.toBytes("j"), Bytes.toBytes("k"),
      Bytes.toBytes("l"), Bytes.toBytes("m"), Bytes.toBytes("n"),
      Bytes.toBytes("o"), Bytes.toBytes("q"), Bytes.toBytes("r"),
      Bytes.toBytes("s"), Bytes.toBytes("se"), Bytes.toBytes("t"),
      Bytes.toBytes("u"), Bytes.toBytes("v"), Bytes.toBytes("w"),
      Bytes.toBytes("x"), Bytes.toBytes("y"), Bytes.toBytes("z") };

  public static final String SPLIT_KEY_PREFIX_LENGTH = "4";

  public static void main(String[] args) throws Exception {

    Configuration hbaseConf = HBaseConfiguration.create();
    // Grab input args and allow for -Dxyz style arguments
    String[] otherArgs = new GenericOptionsParser(hbaseConf, args)
        .getRemainingArgs();

    // Grab the arguments we're looking for.
    CommandLine commandLine = parseArgs(otherArgs);

    // Grab the entityTableName argument
    String entityTableName = commandLine.getOptionValue("e");
    if (StringUtils.isNotBlank(entityTableName)) {
      hbaseConf.set(TimelineEntitySchemaConstants.ENTITY_TABLE_NAME,
          entityTableName);
    }
    String entityTable_TTL_Metrics = commandLine.getOptionValue("m");
    if (StringUtils.isNotBlank(entityTable_TTL_Metrics)) {
      hbaseConf.set(TimelineEntitySchemaConstants.ENTITY_TABLE_METRICS_TTL,
          entityTable_TTL_Metrics);
    }
    createAllTables(hbaseConf);
  }

  /**
   * Parse command-line arguments.
   *
   * @param args
   *          command line arguments passed to program.
   * @return parsed command line.
   * @throws ParseException
   */
  private static CommandLine parseArgs(String[] args) throws ParseException {
    Options options = new Options();

    // Input
    Option o = new Option("e", "entityTableName", true, "entity table name");
    o.setArgName("entityTableName");
    o.setRequired(false);
    options.addOption(o);

    o = new Option("m", "metricsTTL", true, "TTL for metrics column family");
    o.setArgName("metricsTTL");
    o.setRequired(false);
    options.addOption(o);

    CommandLineParser parser = new PosixParser();
    CommandLine commandLine = null;
    try {
      commandLine = parser.parse(options, args);
    } catch (Exception e) {
      LOG.error("ERROR: " + e.getMessage() + "\n");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(NAME + " ", options, true);
      System.exit(-1);
    }

    return commandLine;
  }

  private static void createAllTables(Configuration hbaseConf)
      throws IOException {

    Connection conn = null;
    try {
      conn = ConnectionFactory.createConnection(hbaseConf);
      Admin admin = conn.getAdmin();
      if (admin == null) {
        throw new IOException("Cannot create table since admin is null");
      }
      createTimelineEntityTable(admin, hbaseConf);
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }

  /**
   * Creates a table with column families info, config and metrics
   * info stores information about a timeline entity object
   * config stores configuration data of a timeline entity object
   * metrics stores the metrics of a timeline entity object
   *
   * Example entity table record:
   * <pre>
   *|------------------------------------------------------------|
   *|  Row       | Column Family  | Column Family | Column Family|
   *|  key       | info           | metrics       | config       |
   *|------------------------------------------------------------|
   *| userName!  | id:entityId    | metricName1:  | configKey1:  |
   *| clusterId! |                | metricValue1  | configValue1 |
   *| flowId!    | type:entityType| @timestamp1   |              |
   *| flowRunId! |                |               | configKey2:  |
   *| AppId!     | created_time:  | metricName1:  | configValue2 |
   *| entityType!| 1392993084018  | metricValue2  |              |
   *| entityId   |                | @timestamp2   |              |
   *|            | modified_time: |               |              |
   *|            | 1392995081012  | metricName2:  |              |
   *|            |                | metricValue1  |              |
   *|            | r!relatesToKey:| @timestamp2   |              |
   *|            | id3!id4!id5    |               |              |
   *|            |                |               |              |
   *|            | s!isRelatedToKey|              |              |
   *|            | id7!id9!id5    |               |              |
   *|            |                |               |              |
   *|            | e!eventKey:    |               |              |
   *|            | eventValue     |               |              |
   *|            |                |               |              |
   *|            | flowVersion:   |               |              |
   *|            | versionValue   |               |              |
   *|------------------------------------------------------------|
   *</pre>
   * @param admin
   * @param hbaseConf
   * @throws IOException
   */
  public static void createTimelineEntityTable(Admin admin,
      Configuration hbaseConf) throws IOException {

    TableName table = TableName.valueOf(hbaseConf.get(
        TimelineEntitySchemaConstants.ENTITY_TABLE_NAME,
        TimelineEntitySchemaConstants.DEFAULT_ENTITY_TABLE_NAME));
    if (admin.tableExists(table)) {
      // do not disable / delete existing table
      // similar to the approach taken by map-reduce jobs when
      // output directory exists
      throw new IOException("Table " + table.getNameAsString()
          + " already exists.");
    }

    HTableDescriptor entityTableDescp = new HTableDescriptor(table);
    HColumnDescriptor cf1 = new HColumnDescriptor(
        EntityColumnFamily.INFO.getInBytes());
    cf1.setBloomFilterType(BloomType.ROWCOL);
    entityTableDescp.addFamily(cf1);

    HColumnDescriptor cf2 = new HColumnDescriptor(
        EntityColumnFamily.CONFIG.getInBytes());
    cf2.setBloomFilterType(BloomType.ROWCOL);
    cf2.setBlockCacheEnabled(true);
    entityTableDescp.addFamily(cf2);

    HColumnDescriptor cf3 = new HColumnDescriptor(
        EntityColumnFamily.METRICS.getInBytes());
    entityTableDescp.addFamily(cf3);
    cf3.setBlockCacheEnabled(true);
    // always keep 1 version (the latest)
    cf3.setMinVersions(1);
    cf3.setMaxVersions(TimelineEntitySchemaConstants.ENTITY_TABLE_METRICS_MAX_VERSIONS_DEFAULT);
    cf3.setTimeToLive(hbaseConf.getInt(
        TimelineEntitySchemaConstants.ENTITY_TABLE_METRICS_TTL,
        TimelineEntitySchemaConstants.ENTITY_TABLE_METRICS_TTL_DEFAULT));
    entityTableDescp
        .setRegionSplitPolicyClassName("org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy");
    entityTableDescp.setValue("KeyPrefixRegionSplitPolicy.prefix_length",
        SPLIT_KEY_PREFIX_LENGTH);
    admin.createTable(entityTableDescp, splits);
    LOG.info("Status of table creation for " + table.getNameAsString() + "="
        + admin.tableExists(table));

  }
}
