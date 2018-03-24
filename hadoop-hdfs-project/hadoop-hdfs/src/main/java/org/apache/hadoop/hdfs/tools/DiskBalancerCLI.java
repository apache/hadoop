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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.diskbalancer.command.CancelCommand;
import org.apache.hadoop.hdfs.server.diskbalancer.command.Command;
import org.apache.hadoop.hdfs.server.diskbalancer.command.ExecuteCommand;
import org.apache.hadoop.hdfs.server.diskbalancer.command.HelpCommand;
import org.apache.hadoop.hdfs.server.diskbalancer.command.PlanCommand;
import org.apache.hadoop.hdfs.server.diskbalancer.command.QueryCommand;
import org.apache.hadoop.hdfs.server.diskbalancer.command.ReportCommand;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;

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
public class DiskBalancerCLI extends Configured implements Tool {
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
   * Executes a given plan file on the target datanode.
   */
  public static final String EXECUTE = "execute";

  /**
   * Skips date check(now by default the plan is valid for 24 hours), and force
   * execute the plan.
   */
  public static final String SKIPDATECHECK = "skipDateCheck";

  /**
   * The report command prints out a disk fragmentation report about the data
   * cluster. By default it prints the DEFAULT_TOP machines names with high
   * nodeDataDensity {DiskBalancerDataNode#getNodeDataDensity} values. This
   * means that these are the nodes that deviates from the ideal data
   * distribution.
   */
  public static final String REPORT = "report";
  /**
   * specify top number of nodes to be processed.
   */
  public static final String TOP = "top";
  /**
   * specify default top number of nodes to be processed.
   */
  public static final int DEFAULT_TOP = 100;
  /**
   * Name or address of the node to execute against.
   */
  public static final String NODE = "node";
  /**
   * Runs the command in verbose mode.
   */
  public static final String VERBOSE = "v";
  public static final int PLAN_VERSION = 1;
  /**
   * Reports the status of disk balancer operation.
   */
  public static final String QUERY = "query";
  /**
   * Cancels a running plan.
   */
  public static final String CANCEL = "cancel";
  /**
   * Template for the Before File. It is node.before.json.
   */
  public static final String BEFORE_TEMPLATE = "%s.before.json";
  /**
   * Template for the plan file. it is node.plan.json.
   */
  public static final String PLAN_TEMPLATE = "%s.plan.json";
  private static final Logger LOG =
      LoggerFactory.getLogger(DiskBalancerCLI.class);

  private static final Options PLAN_OPTIONS = new Options();
  private static final Options EXECUTE_OPTIONS = new Options();
  private static final Options QUERY_OPTIONS = new Options();
  private static final Options HELP_OPTIONS = new Options();
  private static final Options CANCEL_OPTIONS = new Options();
  private static final Options REPORT_OPTIONS = new Options();

  private final PrintStream printStream;

  private Command currentCommand = null;

  /**
   * Construct a DiskBalancer.
   *
   * @param conf
   */
  public DiskBalancerCLI(Configuration conf) {
    this(conf, System.out);
  }

  public DiskBalancerCLI(Configuration conf, final PrintStream printStream) {
    super(conf);
    this.printStream = printStream;
  }

  /**
   * Main for the  DiskBalancer Command handling.
   *
   * @param argv - System Args Strings[]
   * @throws Exception
   */
  public static void main(String[] argv) throws Exception {
    DiskBalancerCLI shell = new DiskBalancerCLI(new HdfsConfiguration());
    int res = 0;
    try {
      res = ToolRunner.run(shell, argv);
    } catch (Exception ex) {
      LOG.error(ex.toString());
      res = 1;
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
    addPlanCommands(opts);
    addHelpCommands(opts);
    addExecuteCommands(opts);
    addQueryCommands(opts);
    addCancelCommands(opts);
    addReportCommands(opts);
    return opts;
  }

  /**
   * Returns Plan options.
   *
   * @return Options.
   */
  public static Options getPlanOptions() {
    return PLAN_OPTIONS;
  }

  /**
   * Returns help options.
   *
   * @return - help options.
   */
  public static Options getHelpOptions() {
    return HELP_OPTIONS;
  }

  /**
   * Retuns execute options.
   *
   * @return - execute options.
   */
  public static Options getExecuteOptions() {
    return EXECUTE_OPTIONS;
  }

  /**
   * Returns Query Options.
   *
   * @return query Options
   */
  public static Options getQueryOptions() {
    return QUERY_OPTIONS;
  }

  /**
   * Returns Cancel Options.
   *
   * @return Options
   */
  public static Options getCancelOptions() {
    return CANCEL_OPTIONS;
  }

  /**
   * Returns Report Options.
   *
   * @return Options
   */
  public static Options getReportOptions() {
    return REPORT_OPTIONS;
  }

  /**
   * Adds commands for plan command.
   *
   * @return Options.
   */
  private void addPlanCommands(Options opt) {

    Option plan = OptionBuilder.withLongOpt(PLAN)
        .withDescription("Hostname, IP address or UUID of datanode " +
            "for which a plan is created.")
        .hasArg()
        .create();
    getPlanOptions().addOption(plan);
    opt.addOption(plan);


    Option outFile = OptionBuilder.withLongOpt(OUTFILE).hasArg()
        .withDescription(
            "Local path of file to write output to, if not specified "
                + "defaults will be used.")
        .create();
    getPlanOptions().addOption(outFile);
    opt.addOption(outFile);

    Option bandwidth = OptionBuilder.withLongOpt(BANDWIDTH).hasArg()
        .withDescription(
            "Maximum disk bandwidth (MB/s) in integer to be consumed by "
                + "diskBalancer. e.g. 10 MB/s.")
        .create();
    getPlanOptions().addOption(bandwidth);
    opt.addOption(bandwidth);

    Option threshold = OptionBuilder.withLongOpt(THRESHOLD)
        .hasArg()
        .withDescription("Percentage of data skew that is tolerated before"
            + " disk balancer starts working. For example, if"
            + " total data on a 2 disk node is 100 GB then disk"
            + " balancer calculates the expected value on each disk,"
            + " which is 50 GB. If the tolerance is 10% then data"
            + " on a single disk needs to be more than 60 GB"
            + " (50 GB + 10% tolerance value) for Disk balancer to"
            + " balance the disks.")
        .create();
    getPlanOptions().addOption(threshold);
    opt.addOption(threshold);


    Option maxError = OptionBuilder.withLongOpt(MAXERROR)
        .hasArg()
        .withDescription("Describes how many errors " +
            "can be tolerated while copying between a pair of disks.")
        .create();
    getPlanOptions().addOption(maxError);
    opt.addOption(maxError);

    Option verbose = OptionBuilder.withLongOpt(VERBOSE)
        .withDescription("Print out the summary of the plan on console")
        .create();
    getPlanOptions().addOption(verbose);
    opt.addOption(verbose);
  }

  /**
   * Adds Help to the options.
   */
  private void addHelpCommands(Options opt) {
    Option help = OptionBuilder.withLongOpt(HELP)
        .hasOptionalArg()
        .withDescription("valid commands are plan | execute | query | cancel" +
            " | report")
        .create();
    getHelpOptions().addOption(help);
    opt.addOption(help);
  }

  /**
   * Adds execute command options.
   *
   * @param opt Options
   */
  private void addExecuteCommands(Options opt) {
    Option execute = OptionBuilder.withLongOpt(EXECUTE)
        .hasArg()
        .withDescription("Takes a plan file and " +
            "submits it for execution by the datanode.")
        .create();
    getExecuteOptions().addOption(execute);


    Option skipDateCheck = OptionBuilder.withLongOpt(SKIPDATECHECK)
        .withDescription("skips the date check and force execute the plan")
        .create();
    getExecuteOptions().addOption(skipDateCheck);

    opt.addOption(execute);
    opt.addOption(skipDateCheck);
  }

  /**
   * Adds query command options.
   *
   * @param opt Options
   */
  private void addQueryCommands(Options opt) {
    Option query = OptionBuilder.withLongOpt(QUERY)
        .hasArg()
        .withDescription("Queries the disk balancer " +
            "status of a given datanode.")
        .create();
    getQueryOptions().addOption(query);
    opt.addOption(query);

    // Please note: Adding this only to Query options since -v is already
    // added to global table.
    Option verbose = OptionBuilder.withLongOpt(VERBOSE)
        .withDescription("Prints details of the plan that is being executed " +
            "on the node.")
        .create();
    getQueryOptions().addOption(verbose);
  }

  /**
   * Adds cancel command options.
   *
   * @param opt Options
   */
  private void addCancelCommands(Options opt) {
    Option cancel = OptionBuilder.withLongOpt(CANCEL)
        .hasArg()
        .withDescription("Cancels a running plan using a plan file.")
        .create();
    getCancelOptions().addOption(cancel);
    opt.addOption(cancel);

    Option node = OptionBuilder.withLongOpt(NODE)
        .hasArg()
        .withDescription("Cancels a running plan using a plan ID and hostName")
        .create();

    getCancelOptions().addOption(node);
    opt.addOption(node);
  }

  /**
   * Adds report command options.
   *
   * @param opt Options
   */
  private void addReportCommands(Options opt) {
    Option report = OptionBuilder.withLongOpt(REPORT)
        .withDescription("List nodes that will benefit from running " +
            "DiskBalancer.")
        .create();
    getReportOptions().addOption(report);
    opt.addOption(report);

    Option top = OptionBuilder.withLongOpt(TOP)
        .hasArg()
        .withDescription("specify the number of nodes to be listed which has" +
            " data imbalance.")
        .create();
    getReportOptions().addOption(top);
    opt.addOption(top);

    Option node =  OptionBuilder.withLongOpt(NODE)
        .hasArg()
        .withDescription("Datanode address, " +
            "it can be DataNodeID, IP or hostname.")
        .create();
    getReportOptions().addOption(node);
    opt.addOption(node);
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
   * Gets current command associated with this instance of DiskBalancer.
   */
  public Command getCurrentCommand() {
    return currentCommand;
  }

  /**
   * Dispatches calls to the right command Handler classes.
   *
   * @param cmd  - CommandLine
   * @param opts options of command line
   * @param out  the output stream used for printing
   */
  private int dispatch(CommandLine cmd, Options opts)
      throws Exception {
    Command dbCmd = null;
    try {
      if (cmd.hasOption(DiskBalancerCLI.PLAN)) {
        dbCmd = new PlanCommand(getConf(), printStream);
      }

      if (cmd.hasOption(DiskBalancerCLI.EXECUTE)) {
        dbCmd = new ExecuteCommand(getConf());
      }

      if (cmd.hasOption(DiskBalancerCLI.QUERY)) {
        dbCmd = new QueryCommand(getConf());
      }

      if (cmd.hasOption(DiskBalancerCLI.CANCEL)) {
        dbCmd = new CancelCommand(getConf());
      }

      if (cmd.hasOption(DiskBalancerCLI.REPORT)) {
        dbCmd = new ReportCommand(getConf(), this.printStream);
      }

      if (cmd.hasOption(DiskBalancerCLI.HELP)) {
        dbCmd = new HelpCommand(getConf());
      }

      // Invoke main help here.
      if (dbCmd == null) {
        dbCmd = new HelpCommand(getConf());
        dbCmd.execute(null);
        return 1;
      }

      dbCmd.execute(cmd);
      return 0;
    } finally {
      if (dbCmd != null) {
        dbCmd.close();
      }
    }
  }
}
