/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.tools;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.diskbalancer.command.Command;
import org.apache.hadoop.hdfs.server.diskbalancer.command.PlanCommand;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * DiskBalancer is a tool that can be used to ensure that data is spread evenly
 * across volumes of same storage type.
 * <p>
 * For example, if you have 3 disks, with 100 GB , 600 GB and 200 GB on each
 * disk, this tool will ensure that each disk will have 300 GB.
 * <p>
 * This tool can be run while data nodes are fully functional.
 * <p>
 * At very high level diskbalancer computes a set of moves that will make disk
 * utilization equal and then those moves are executed by the datanode.
 */
public class DiskBalancer extends Configured implements Tool {
  /**
   * Construct a DiskBalancer.
   *
   * @param conf
   */
  public DiskBalancer(Configuration conf) {
    super(conf);
  }

  /**
   * NameNodeURI can point to either a real namenode, or a json file that
   * contains the diskBalancer data in json form, that jsonNodeConnector knows
   * how to deserialize.
   * <p>
   * Expected formats are :
   * <p>
   * hdfs://namenode.uri or file:///data/myCluster.json
   */
  public static final String NAMENODEURI = "uri";

  /**
   * Computes a plan for a given set of nodes.
   */
  public static final String PLAN = "plan";

  /**
   * Output file name, for commands like report, plan etc. This is an optional
   * argument, by default diskbalancer will write all its output to
   * /system/reports/diskbalancer of the current cluster it is operating
   * against.
   */
  public static final String OUTFILE = "out";

  /**
   * Help for the program.
   */
  public static final String HELP = "help";

  /**
   * Percentage of data unevenness that we are willing to live with. For example
   * - a value like 10 indicates that we are okay with 10 % +/- from
   * idealStorage Target.
   */
  public static final String THRESHOLD = "thresholdPercentage";

  /**
   * Specifies the maximum disk bandwidth to use per second.
   */
  public static final String BANDWIDTH = "bandwidth";

  /**
   * Specifies the maximum errors to tolerate.
   */
  public static final String MAXERROR = "maxerror";

  /**
   * Node name or IP against which Disk Balancer is being run.
   */
  public static final String NODE = "node";

  /**
   * Runs the command in verbose mode.
   */
  public static final String VERBOSE = "v";

  /**
   * Template for the Before File. It is node.before.json.
   */
  public static final String BEFORE_TEMPLATE = "%s.before.json";

  /**
   * Template for the plan file. it is node.plan.json.
   */
  public static final String PLAN_TEMPLATE = "%s.plan.json";

  private static final Logger LOG =
      LoggerFactory.getLogger(DiskBalancer.class);

  /**
   * Main for the  DiskBalancer Command handling.
   *
   * @param argv - System Args Strings[]
   * @throws Exception
   */
  public static void main(String[] argv) throws Exception {
    DiskBalancer shell = new DiskBalancer(new HdfsConfiguration());
    int res = 0;
    try {
      res = ToolRunner.run(shell, argv);
    } catch (Exception ex) {
      LOG.error(ex.toString());
      System.exit(1);
    }
    System.exit(res);
  }

  /**
   * Execute the command with the given arguments.
   *
   * @param args command specific arguments.
   * @return exit code.
   * @throws Exception
   */
  @Override
  public int run(String[] args) throws Exception {
    Options opts = getOpts();
    CommandLine cmd = parseArgs(args, opts);
    return dispatch(cmd, opts);
  }

  /**
   * returns the Command Line Options.
   *
   * @return Options
   */
  private Options getOpts() {
    Options opts = new Options();
    addCommands(opts);
    return opts;
  }

  /**
   * Adds commands that we handle to opts.
   *
   * @param opt - Optins
   */
  private void addCommands(Options opt) {

    Option nameNodeUri =
        new Option(NAMENODEURI, true, "NameNode URI. e.g http://namenode" +
            ".mycluster.com or file:///myCluster" +
            ".json");
    opt.addOption(nameNodeUri);

    Option outFile =
        new Option(OUTFILE, true, "File to write output to, if not specified " +
            "defaults will be used." +
            "e.g -out outfile.txt");
    opt.addOption(outFile);

    Option plan = new Option(PLAN, false, "write plan to the default file");
    opt.addOption(plan);

    Option bandwidth = new Option(BANDWIDTH, true, "Maximum disk bandwidth to" +
        " be consumed by diskBalancer. " +
        "Expressed as MBs per second.");
    opt.addOption(bandwidth);

    Option threshold = new Option(THRESHOLD, true, "Percentage skew that we " +
        "tolerate before diskbalancer starts working or stops when reaching " +
        "that range.");
    opt.addOption(threshold);

    Option maxErrors = new Option(MAXERROR, true, "Describes how many errors " +
        "can be tolerated while copying between a pair of disks.");
    opt.addOption(maxErrors);

    Option node = new Option(NODE, true, "Node Name or IP");
    opt.addOption(node);

    Option help =
        new Option(HELP, true, "Help about a command or this message");
    opt.addOption(help);

  }

  /**
   * This function parses all command line arguments and returns the appropriate
   * values.
   *
   * @param argv - Argv from main
   * @return CommandLine
   */
  private CommandLine parseArgs(String[] argv, Options opts)
      throws org.apache.commons.cli.ParseException {
    BasicParser parser = new BasicParser();
    return parser.parse(opts, argv);
  }

  /**
   * Dispatches calls to the right command Handler classes.
   *
   * @param cmd - CommandLine
   * @throws IOException
   * @throws URISyntaxException
   */
  private int dispatch(CommandLine cmd, Options opts)
      throws IOException, URISyntaxException {
    Command currentCommand = null;

    try {
      if (cmd.hasOption(DiskBalancer.PLAN)) {
        currentCommand = new PlanCommand(getConf());
      } else {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(80, "hdfs diskbalancer -uri [args]",
            "disk balancer commands", opts,
            "Please correct your command and try again.");
        return 1;
      }

      currentCommand.execute(cmd);

    } catch (Exception ex) {
      System.err.printf(ex.getMessage());
      return 1;
    }
    return 0;
  }

}
