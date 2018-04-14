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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.domain.DomainTableRW;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This creates the schema for a hbase based backend for storing application
 * timeline information.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class TimelineSchemaCreator {
  private TimelineSchemaCreator() {
  }

  final static String NAME = TimelineSchemaCreator.class.getSimpleName();
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineSchemaCreator.class);
  private static final String SKIP_EXISTING_TABLE_OPTION_SHORT = "s";
  private static final String APP_METRICS_TTL_OPTION_SHORT = "ma";
  private static final String SUB_APP_METRICS_TTL_OPTION_SHORT = "msa";
  private static final String APP_TABLE_NAME_SHORT = "a";
  private static final String SUB_APP_TABLE_NAME_SHORT = "sa";
  private static final String APP_TO_FLOW_TABLE_NAME_SHORT = "a2f";
  private static final String ENTITY_METRICS_TTL_OPTION_SHORT = "me";
  private static final String ENTITY_TABLE_NAME_SHORT = "e";
  private static final String HELP_SHORT = "h";
  private static final String CREATE_TABLES_SHORT = "c";

  public static void main(String[] args) throws Exception {

    LOG.info("Starting the schema creation");
    Configuration hbaseConf =
        HBaseTimelineStorageUtils.getTimelineServiceHBaseConf(
            new YarnConfiguration());
    // Grab input args and allow for -Dxyz style arguments
    String[] otherArgs = new GenericOptionsParser(hbaseConf, args)
        .getRemainingArgs();

    // Grab the arguments we're looking for.
    CommandLine commandLine = parseArgs(otherArgs);

    if (commandLine.hasOption(HELP_SHORT)) {
      // -help option has the highest precedence
      printUsage();
    } else if (commandLine.hasOption(CREATE_TABLES_SHORT)) {
      // Grab the entityTableName argument
      String entityTableName = commandLine.getOptionValue(
          ENTITY_TABLE_NAME_SHORT);
      if (StringUtils.isNotBlank(entityTableName)) {
        hbaseConf.set(EntityTableRW.TABLE_NAME_CONF_NAME, entityTableName);
      }
      // Grab the entity metrics TTL
      String entityTableMetricsTTL = commandLine.getOptionValue(
          ENTITY_METRICS_TTL_OPTION_SHORT);
      if (StringUtils.isNotBlank(entityTableMetricsTTL)) {
        int entityMetricsTTL = Integer.parseInt(entityTableMetricsTTL);
        new EntityTableRW().setMetricsTTL(entityMetricsTTL, hbaseConf);
      }
      // Grab the appToflowTableName argument
      String appToflowTableName = commandLine.getOptionValue(
          APP_TO_FLOW_TABLE_NAME_SHORT);
      if (StringUtils.isNotBlank(appToflowTableName)) {
        hbaseConf.set(
            AppToFlowTableRW.TABLE_NAME_CONF_NAME, appToflowTableName);
      }
      // Grab the applicationTableName argument
      String applicationTableName = commandLine.getOptionValue(
          APP_TABLE_NAME_SHORT);
      if (StringUtils.isNotBlank(applicationTableName)) {
        hbaseConf.set(ApplicationTableRW.TABLE_NAME_CONF_NAME,
            applicationTableName);
      }
      // Grab the application metrics TTL
      String applicationTableMetricsTTL = commandLine.getOptionValue(
          APP_METRICS_TTL_OPTION_SHORT);
      if (StringUtils.isNotBlank(applicationTableMetricsTTL)) {
        int appMetricsTTL = Integer.parseInt(applicationTableMetricsTTL);
        new ApplicationTableRW().setMetricsTTL(appMetricsTTL, hbaseConf);
      }

      // Grab the subApplicationTableName argument
      String subApplicationTableName = commandLine.getOptionValue(
          SUB_APP_TABLE_NAME_SHORT);
      if (StringUtils.isNotBlank(subApplicationTableName)) {
        hbaseConf.set(SubApplicationTableRW.TABLE_NAME_CONF_NAME,
            subApplicationTableName);
      }
      // Grab the subApplication metrics TTL
      String subApplicationTableMetricsTTL = commandLine
          .getOptionValue(SUB_APP_METRICS_TTL_OPTION_SHORT);
      if (StringUtils.isNotBlank(subApplicationTableMetricsTTL)) {
        int subAppMetricsTTL = Integer.parseInt(subApplicationTableMetricsTTL);
        new SubApplicationTableRW().setMetricsTTL(subAppMetricsTTL, hbaseConf);
      }

      // create all table schemas in hbase
      final boolean skipExisting = commandLine.hasOption(
          SKIP_EXISTING_TABLE_OPTION_SHORT);
      createAllSchemas(hbaseConf, skipExisting);
    } else {
      // print usage information if -create is not specified
      printUsage();
    }
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
    Option o = new Option(HELP_SHORT, "help", false, "print help information");
    o.setRequired(false);
    options.addOption(o);

    o = new Option(CREATE_TABLES_SHORT, "create", false,
        "a mandatory option to create hbase tables");
    o.setRequired(false);
    options.addOption(o);

    o = new Option(ENTITY_TABLE_NAME_SHORT, "entityTableName", true,
        "entity table name");
    o.setArgName("entityTableName");
    o.setRequired(false);
    options.addOption(o);

    o = new Option(ENTITY_METRICS_TTL_OPTION_SHORT, "entityMetricsTTL", true,
        "TTL for metrics column family");
    o.setArgName("entityMetricsTTL");
    o.setRequired(false);
    options.addOption(o);

    o = new Option(APP_TO_FLOW_TABLE_NAME_SHORT, "appToflowTableName", true,
        "app to flow table name");
    o.setArgName("appToflowTableName");
    o.setRequired(false);
    options.addOption(o);

    o = new Option(APP_TABLE_NAME_SHORT, "applicationTableName", true,
        "application table name");
    o.setArgName("applicationTableName");
    o.setRequired(false);
    options.addOption(o);

    o = new Option(APP_METRICS_TTL_OPTION_SHORT, "applicationMetricsTTL", true,
        "TTL for metrics column family");
    o.setArgName("applicationMetricsTTL");
    o.setRequired(false);
    options.addOption(o);

    o = new Option(SUB_APP_TABLE_NAME_SHORT, "subApplicationTableName", true,
        "subApplication table name");
    o.setArgName("subApplicationTableName");
    o.setRequired(false);
    options.addOption(o);

    o = new Option(SUB_APP_METRICS_TTL_OPTION_SHORT, "subApplicationMetricsTTL",
        true, "TTL for metrics column family");
    o.setArgName("subApplicationMetricsTTL");
    o.setRequired(false);
    options.addOption(o);

    // Options without an argument
    // No need to set arg name since we do not need an argument here
    o = new Option(SKIP_EXISTING_TABLE_OPTION_SHORT, "skipExistingTable",
        false, "skip existing Hbase tables and continue to create new tables");
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

  private static void printUsage() {
    StringBuilder usage = new StringBuilder("Command Usage: \n");
    usage.append("TimelineSchemaCreator [-help] Display help info" +
        " for all commands. Or\n");
    usage.append("TimelineSchemaCreator -create [OPTIONAL_OPTIONS]" +
        " Create hbase tables.\n\n");
    usage.append("The Optional options for creating tables include: \n");
    usage.append("[-entityTableName <Entity Table Name>] " +
        "The name of the Entity table\n");
    usage.append("[-entityMetricsTTL <Entity Table Metrics TTL>]" +
        " TTL for metrics in the Entity table\n");
    usage.append("[-appToflowTableName <AppToflow Table Name>]" +
        " The name of the AppToFlow table\n");
    usage.append("[-applicationTableName <Application Table Name>]" +
        " The name of the Application table\n");
    usage.append("[-applicationMetricsTTL <Application Table Metrics TTL>]" +
        " TTL for metrics in the Application table\n");
    usage.append("[-subApplicationTableName <SubApplication Table Name>]" +
        " The name of the SubApplication table\n");
    usage.append("[-subApplicationMetricsTTL " +
        " <SubApplication Table Metrics TTL>]" +
        " TTL for metrics in the SubApplication table\n");
    usage.append("[-skipExistingTable] Whether to skip existing" +
        " hbase tables\n");
    System.out.println(usage.toString());
  }

  /**
   * Create all table schemas and log success or exception if failed.
   * @param hbaseConf the hbase configuration to create tables with
   * @param skipExisting whether to skip existing hbase tables
   */
  private static void createAllSchemas(Configuration hbaseConf,
      boolean skipExisting) {
    List<Exception> exceptions = new ArrayList<>();
    try {
      if (skipExisting) {
        LOG.info("Will skip existing tables and continue on htable creation "
            + "exceptions!");
      }
      createAllTables(hbaseConf, skipExisting);
      LOG.info("Successfully created HBase schema. ");
    } catch (IOException e) {
      LOG.error("Error in creating hbase tables: ", e);
      exceptions.add(e);
    }

    if (exceptions.size() > 0) {
      LOG.warn("Schema creation finished with the following exceptions");
      for (Exception e : exceptions) {
        LOG.warn(e.getMessage());
      }
      System.exit(-1);
    } else {
      LOG.info("Schema creation finished successfully");
    }
  }

  @VisibleForTesting
  public static void createAllTables(Configuration hbaseConf,
      boolean skipExisting) throws IOException {

    Connection conn = null;
    try {
      conn = ConnectionFactory.createConnection(hbaseConf);
      Admin admin = conn.getAdmin();
      if (admin == null) {
        throw new IOException("Cannot create table since admin is null");
      }
      try {
        new EntityTableRW().createTable(admin, hbaseConf);
      } catch (IOException e) {
        if (skipExisting) {
          LOG.warn("Skip and continue on: " + e.getMessage());
        } else {
          throw e;
        }
      }
      try {
        new AppToFlowTableRW().createTable(admin, hbaseConf);
      } catch (IOException e) {
        if (skipExisting) {
          LOG.warn("Skip and continue on: " + e.getMessage());
        } else {
          throw e;
        }
      }
      try {
        new ApplicationTableRW().createTable(admin, hbaseConf);
      } catch (IOException e) {
        if (skipExisting) {
          LOG.warn("Skip and continue on: " + e.getMessage());
        } else {
          throw e;
        }
      }
      try {
        new FlowRunTableRW().createTable(admin, hbaseConf);
      } catch (IOException e) {
        if (skipExisting) {
          LOG.warn("Skip and continue on: " + e.getMessage());
        } else {
          throw e;
        }
      }
      try {
        new FlowActivityTableRW().createTable(admin, hbaseConf);
      } catch (IOException e) {
        if (skipExisting) {
          LOG.warn("Skip and continue on: " + e.getMessage());
        } else {
          throw e;
        }
      }
      try {
        new SubApplicationTableRW().createTable(admin, hbaseConf);
      } catch (IOException e) {
        if (skipExisting) {
          LOG.warn("Skip and continue on: " + e.getMessage());
        } else {
          throw e;
        }
      }
      try {
        new DomainTableRW().createTable(admin, hbaseConf);
      } catch (IOException e) {
        if (skipExisting) {
          LOG.warn("Skip and continue on: " + e.getMessage());
        } else {
          throw e;
        }
      }
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }


}
