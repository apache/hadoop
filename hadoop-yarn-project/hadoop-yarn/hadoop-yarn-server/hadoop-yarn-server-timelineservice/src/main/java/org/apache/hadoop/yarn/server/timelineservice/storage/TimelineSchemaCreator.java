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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTable;

/**
 * This creates the schema for a hbase based backend for storing application
 * timeline information.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TimelineSchemaCreator {

  final static String NAME = TimelineSchemaCreator.class.getSimpleName();
  private static final Log LOG = LogFactory.getLog(TimelineSchemaCreator.class);

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
      hbaseConf.set(EntityTable.TABLE_NAME_CONF_NAME, entityTableName);
    }
    String entityTableTTLMetrics = commandLine.getOptionValue("m");
    if (StringUtils.isNotBlank(entityTableTTLMetrics)) {
      int metricsTTL = Integer.parseInt(entityTableTTLMetrics);
      new EntityTable().setMetricsTTL(metricsTTL, hbaseConf);
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
      new EntityTable().createTable(admin, hbaseConf);
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }


}
