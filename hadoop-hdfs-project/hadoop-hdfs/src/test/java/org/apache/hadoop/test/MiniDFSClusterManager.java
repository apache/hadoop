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
package org.apache.hadoop.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.eclipse.jetty.util.ajax.JSON;

/**
 * This class drives the creation of a mini-cluster on the local machine. By
 * default, a MiniDFSCluster is spawned on the first available ports that are
 * found.
 * 
 * A series of command line flags controls the startup cluster options.
 * 
 * This class can dump a Hadoop configuration and some basic metadata (in JSON)
 * into a textfile.
 * 
 * To shutdown the cluster, kill the process.
 * 
 * To run this from the command line, do the following (replacing the jar
 * version as appropriate):
 * 
 * $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/hdfs/hadoop-hdfs-0.24.0-SNAPSHOT-tests.jar org.apache.hadoop.test.MiniDFSClusterManager -options...
 */
public class MiniDFSClusterManager {
  private static final Log LOG =
    LogFactory.getLog(MiniDFSClusterManager.class);

  private MiniDFSCluster dfs;
  private String writeDetails;
  private int numDataNodes;
  private int nameNodePort;
  private int nameNodeHttpPort;
  private StartupOption dfsOpts;
  private String writeConfig;
  private Configuration conf;
  private boolean format;
  
  private static final long SLEEP_INTERVAL_MS = 1000 * 60;

  /**
   * Creates configuration options object.
   */
  @SuppressWarnings("static-access")
  private Options makeOptions() {
    Options options = new Options();
    options
        .addOption("datanodes", true, "How many datanodes to start (default 1)")
        .addOption("format", false, "Format the DFS (default false)")
        .addOption("cmdport", true,
            "Which port to listen on for commands (default 0--we choose)")
        .addOption("nnport", true, "NameNode port (default 0--we choose)")
        .addOption("httpport", true, "NameNode http port (default 0--we choose)")
        .addOption("namenode", true, "URL of the namenode (default "
            + "is either the DFS cluster or a temporary dir)")     
        .addOption(OptionBuilder
            .hasArgs()
            .withArgName("property=value")
            .withDescription("Options to pass into configuration object")
            .create("D"))
        .addOption(OptionBuilder
            .hasArg()
            .withArgName("path")
            .withDescription("Save configuration to this XML file.")
            .create("writeConfig"))
         .addOption(OptionBuilder
            .hasArg()
            .withArgName("path")
            .withDescription("Write basic information to this JSON file.")
            .create("writeDetails"))
        .addOption(OptionBuilder.withDescription("Prints option help.")
            .create("help"));
    return options;
  }

  /**
   * Main entry-point.
   */
  public void run(String[] args) throws IOException {
    if (!parseArguments(args)) {
      return;
    }
    start();
    sleepForever();
  }

  private void sleepForever() {
    while (true) {
      try {
        Thread.sleep(SLEEP_INTERVAL_MS);
        if (!dfs.isClusterUp()) {
          LOG.info("Cluster is no longer up, exiting");
          return;
        }
      } catch (InterruptedException _) {
        // nothing
      }
    }
  }

  /**
   * Starts DFS as specified in member-variable options. Also writes out
   * configuration and details, if requested.
   */
  public void start() throws IOException, FileNotFoundException {
    dfs = new MiniDFSCluster.Builder(conf).nameNodePort(nameNodePort)
                                          .nameNodeHttpPort(nameNodeHttpPort)
                                          .numDataNodes(numDataNodes)
                                          .startupOption(dfsOpts)
                                          .format(format)
                                          .build();
    dfs.waitActive();
    
    LOG.info("Started MiniDFSCluster -- namenode on port "
        + dfs.getNameNodePort());

    if (writeConfig != null) {
      FileOutputStream fos = new FileOutputStream(new File(writeConfig));
      conf.writeXml(fos);
      fos.close();
    }

    if (writeDetails != null) {
      Map<String, Object> map = new TreeMap<String, Object>();
      if (dfs != null) {
        map.put("namenode_port", dfs.getNameNodePort());
      }

      FileWriter fw = new FileWriter(new File(writeDetails));
      fw.write(new JSON().toJSON(map));
      fw.close();
    }
  }

  /**
   * Parses arguments and fills out the member variables.
   * @param args Command-line arguments.
   * @return true on successful parse; false to indicate that the
   * program should exit.
   */
  private boolean parseArguments(String[] args) {
    Options options = makeOptions();
    CommandLine cli;
    try {
      CommandLineParser parser = new GnuParser();
      cli = parser.parse(options, args);
    } catch(ParseException e) {
      LOG.warn("options parsing failed:  "+e.getMessage());
      new HelpFormatter().printHelp("...", options);
      return false;
    }

    if (cli.hasOption("help")) {
      new HelpFormatter().printHelp("...", options);
      return false;
    }
    
    if (cli.getArgs().length > 0) {
      for (String arg : cli.getArgs()) {
        LOG.error("Unrecognized option: " + arg);
        new HelpFormatter().printHelp("...", options);
        return false;
      }
    }

    // HDFS
    numDataNodes = intArgument(cli, "datanodes", 1);
    nameNodePort = intArgument(cli, "nnport", 0);
    nameNodeHttpPort = intArgument(cli, "httpport", 0);
    if (cli.hasOption("format")) {
      dfsOpts = StartupOption.FORMAT;
      format = true;
    } else {
      dfsOpts = StartupOption.REGULAR;
      format = false;
    }

    // Runner
    writeDetails = cli.getOptionValue("writeDetails");
    writeConfig = cli.getOptionValue("writeConfig");

    // General
    conf = new HdfsConfiguration();
    updateConfiguration(conf, cli.getOptionValues("D"));

    return true;
  }

  /**
   * Updates configuration based on what's given on the command line.
   *
   * @param conf2 The configuration object
   * @param keyvalues An array of interleaved key value pairs.
   */
  private void updateConfiguration(Configuration conf2, String[] keyvalues) {
    int num_confs_updated = 0;
    if (keyvalues != null) {
      for (String prop : keyvalues) {
        String[] keyval = prop.split("=", 2);
        if (keyval.length == 2) {
          conf2.set(keyval[0], keyval[1]);
          num_confs_updated++;
        } else {
          LOG.warn("Ignoring -D option " + prop);
        }
      }
    }
    LOG.info("Updated " + num_confs_updated +
        " configuration settings from command line.");
  }

  /**
   * Extracts an integer argument with specified default value.
   */
  private int intArgument(CommandLine cli, String argName, int defaultValue) {
    String o = cli.getOptionValue(argName);
    try {
      if (o != null) {
        return Integer.parseInt(o);
      } 
    } catch (NumberFormatException ex) {
      LOG.error("Couldn't parse value (" + o + ") for option " 
          + argName + ". Using default: " + defaultValue);
    }
    
    return defaultValue;    
  }

  /**
   * Starts a MiniDFSClusterManager with parameters drawn from the command line.
   */
  public static void main(String[] args) throws IOException {
    new MiniDFSClusterManager().run(args);
  }
}
