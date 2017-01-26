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
package org.apache.hadoop.mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.mortbay.util.ajax.JSON;

/**
 * This class drives the creation of a mini-cluster on the local machine. By
 * default, a MiniDFSCluster and MiniMRCluster are spawned on the first
 * available ports that are found.
 *
 * A series of command line flags controls the startup cluster options.
 *
 * This class can dump a Hadoop configuration and some basic metadata (in JSON)
 * into a text file.
 *
 * To shutdown the cluster, kill the process.
 */
public class MiniHadoopClusterManager {
  private static final Log LOG = LogFactory
      .getLog(MiniHadoopClusterManager.class);

  private MiniMRClientCluster mr;
  private MiniDFSCluster dfs;
  private String writeDetails;
  private int numNodeManagers;
  private int numDataNodes;
  private int nnPort;
  private int rmPort;
  private int jhsPort;
  private StartupOption dfsOpts;
  private boolean noDFS;
  private boolean noMR;
  private String fs;
  private String writeConfig;
  private JobConf conf;

  /**
   * Creates configuration options object.
   */
  @SuppressWarnings("static-access")
  private Options makeOptions() {
    Options options = new Options();
    options
        .addOption("nodfs", false, "Don't start a mini DFS cluster")
        .addOption("nomr", false, "Don't start a mini MR cluster")
        .addOption("nodemanagers", true,
            "How many nodemanagers to start (default 1)")
        .addOption("datanodes", true, "How many datanodes to start (default 1)")
        .addOption("format", false, "Format the DFS (default false)")
        .addOption("nnport", true, "NameNode port (default 0--we choose)")
        .addOption(
            "namenode",
            true,
            "URL of the namenode (default "
                + "is either the DFS cluster or a temporary dir)")
        .addOption("rmport", true,
            "ResourceManager port (default 0--we choose)")
        .addOption("jhsport", true,
            "JobHistoryServer port (default 0--we choose)")
        .addOption(
            OptionBuilder.hasArgs().withArgName("property=value")
                .withDescription("Options to pass into configuration object")
                .create("D"))
        .addOption(
            OptionBuilder.hasArg().withArgName("path").withDescription(
                "Save configuration to this XML file.").create("writeConfig"))
        .addOption(
            OptionBuilder.hasArg().withArgName("path").withDescription(
                "Write basic information to this JSON file.").create(
                "writeDetails"))
        .addOption(
            OptionBuilder.withDescription("Prints option help.").create("help"));
    return options;
  }

  /**
   * Main entry-point.
   *
   * @throws URISyntaxException
   */
  public void run(String[] args) throws IOException, URISyntaxException {
    if (!parseArguments(args)) {
      return;
    }
    start();
    sleepForever();
  }

  private void sleepForever() {
    while (true) {
      try {
        Thread.sleep(1000 * 60);
      } catch (InterruptedException _) {
        // nothing
      }
    }
  }

  /**
   * Starts DFS and MR clusters, as specified in member-variable options. Also
   * writes out configuration and details, if requested.
   *
   * @throws IOException
   * @throws FileNotFoundException
   * @throws URISyntaxException
   */
  public void start() throws IOException, FileNotFoundException,
      URISyntaxException {
    if (!noDFS) {
      dfs = new MiniDFSCluster.Builder(conf).nameNodePort(nnPort)
          .numDataNodes(numDataNodes).startupOption(dfsOpts).build();
      LOG.info("Started MiniDFSCluster -- namenode on port "
          + dfs.getNameNodePort());
    }
    if (!noMR) {
      if (fs == null && dfs != null) {
        fs = dfs.getFileSystem().getUri().toString();
      } else if (fs == null) {
        fs = "file:///tmp/minimr-" + System.nanoTime();
      }
      FileSystem.setDefaultUri(conf, new URI(fs));
      // Instruct the minicluster to use fixed ports, so user will know which
      // ports to use when communicating with the cluster.
      conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
      conf.setBoolean(JHAdminConfig.MR_HISTORY_MINICLUSTER_FIXED_PORTS, true);
      conf.set(YarnConfiguration.RM_ADDRESS, MiniYARNCluster.getHostname()
          + ":" + this.rmPort);
      conf.set(JHAdminConfig.MR_HISTORY_ADDRESS, MiniYARNCluster.getHostname()
          + ":" + this.jhsPort);
      mr = MiniMRClientClusterFactory.create(this.getClass(), numNodeManagers,
          conf);
      LOG.info("Started MiniMRCluster");
    }

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
      if (mr != null) {
        map.put("resourcemanager_port", mr.getConfig().get(
            YarnConfiguration.RM_ADDRESS).split(":")[1]);
      }
      FileWriter fw = new FileWriter(new File(writeDetails));
      fw.write(new JSON().toJSON(map));
      fw.close();
    }
  }

  /**
   * Shuts down in-process clusters.
   *
   * @throws IOException
   */
  public void stop() throws IOException {
    if (mr != null) {
      mr.stop();
    }
    if (dfs != null) {
      dfs.shutdown();
    }
  }

  /**
   * Parses arguments and fills out the member variables.
   *
   * @param args
   *          Command-line arguments.
   * @return true on successful parse; false to indicate that the program should
   *         exit.
   */
  private boolean parseArguments(String[] args) {
    Options options = makeOptions();
    CommandLine cli;
    try {
      CommandLineParser parser = new GnuParser();
      cli = parser.parse(options, args);
    } catch (ParseException e) {
      LOG.warn("options parsing failed:  " + e.getMessage());
      new HelpFormatter().printHelp("...", options);
      return false;
    }

    if (cli.hasOption("help")) {
      new HelpFormatter().printHelp("...", options);
      return false;
    }
    if (cli.getArgs().length > 0) {
      for (String arg : cli.getArgs()) {
        System.err.println("Unrecognized option: " + arg);
        new HelpFormatter().printHelp("...", options);
        return false;
      }
    }

    // MR
    noMR = cli.hasOption("nomr");
    numNodeManagers = intArgument(cli, "nodemanagers", 1);
    rmPort = intArgument(cli, "rmport", 0);
    jhsPort = intArgument(cli, "jhsport", 0);
    fs = cli.getOptionValue("namenode");

    // HDFS
    noDFS = cli.hasOption("nodfs");
    numDataNodes = intArgument(cli, "datanodes", 1);
    nnPort = intArgument(cli, "nnport", 0);
    dfsOpts = cli.hasOption("format") ? StartupOption.FORMAT
        : StartupOption.REGULAR;

    // Runner
    writeDetails = cli.getOptionValue("writeDetails");
    writeConfig = cli.getOptionValue("writeConfig");

    // General
    conf = new JobConf();
    updateConfiguration(conf, cli.getOptionValues("D"));

    return true;
  }

  /**
   * Updates configuration based on what's given on the command line.
   *
   * @param conf
   *          The configuration object
   * @param keyvalues
   *          An array of interleaved key value pairs.
   */
  private void updateConfiguration(JobConf conf, String[] keyvalues) {
    int num_confs_updated = 0;
    if (keyvalues != null) {
      for (String prop : keyvalues) {
        String[] keyval = prop.split("=", 2);
        if (keyval.length == 2) {
          conf.set(keyval[0], keyval[1]);
          num_confs_updated++;
        } else {
          LOG.warn("Ignoring -D option " + prop);
        }
      }
    }
    LOG.info("Updated " + num_confs_updated
        + " configuration settings from command line.");
  }

  /**
   * Extracts an integer argument with specified default value.
   */
  private int intArgument(CommandLine cli, String argName, int default_) {
    String o = cli.getOptionValue(argName);
    if (o == null) {
      return default_;
    } else {
      return Integer.parseInt(o);
    }
  }

  /**
   * Starts a MiniHadoopCluster.
   *
   * @throws URISyntaxException
   */
  public static void main(String[] args) throws IOException, URISyntaxException {
    new MiniHadoopClusterManager().run(args);
  }
}
