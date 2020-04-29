/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdfs.server.diskbalancer.command;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.TextStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel
    .DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.NodePlan;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.Step;
import org.apache.hadoop.hdfs.tools.DiskBalancerCLI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.io.PrintStream;

/**
 * Class that implements Plan Command.
 * <p>
 * Plan command reads the Cluster Info and creates a plan for specified data
 * node or a set of Data nodes.
 * <p>
 * It writes the output to a default location unless changed by the user.
 */
public class PlanCommand extends Command {
  private double thresholdPercentage;
  private int bandwidth;
  private int maxError;

  /**
   * Constructs a plan command.
   */
  public PlanCommand(Configuration conf) {
    this(conf, System.out);
  }

  /**
   * Constructs a plan command.
   */
  public PlanCommand(Configuration conf, final PrintStream ps) {
    super(conf, ps);
    this.thresholdPercentage = 1;
    this.bandwidth = 0;
    this.maxError = 0;
    addValidCommandParameters(DiskBalancerCLI.OUTFILE, "Output directory in " +
        "HDFS. The generated plan will be written to a file in this " +
        "directory.");
    addValidCommandParameters(DiskBalancerCLI.BANDWIDTH,
        "Maximum Bandwidth to be used while copying.");
    addValidCommandParameters(DiskBalancerCLI.THRESHOLD,
        "Percentage skew that we tolerate before diskbalancer starts working.");
    addValidCommandParameters(DiskBalancerCLI.MAXERROR,
        "Max errors to tolerate between 2 disks");
    addValidCommandParameters(DiskBalancerCLI.VERBOSE, "Run plan command in " +
        "verbose mode.");
    addValidCommandParameters(DiskBalancerCLI.PLAN, "Plan Command");
  }

  /**
   * Runs the plan command. This command can be run with various options like
   * <p>
   * -plan -node IP -plan -node hostName -plan -node DatanodeUUID
   *
   * @param cmd - CommandLine
   * @throws Exception
   */
  @Override
  public void execute(CommandLine cmd) throws Exception {
    TextStringBuilder result = new TextStringBuilder();
    String outputLine = "";
    LOG.debug("Processing Plan Command.");
    Preconditions.checkState(cmd.hasOption(DiskBalancerCLI.PLAN));
    verifyCommandOptions(DiskBalancerCLI.PLAN, cmd);

    if (cmd.getOptionValue(DiskBalancerCLI.PLAN) == null) {
      throw new IllegalArgumentException("A node name is required to create a" +
          " plan.");
    }

    if (cmd.hasOption(DiskBalancerCLI.BANDWIDTH)) {
      this.bandwidth = Integer.parseInt(cmd.getOptionValue(DiskBalancerCLI
          .BANDWIDTH));
    }

    if (cmd.hasOption(DiskBalancerCLI.MAXERROR)) {
      this.maxError = Integer.parseInt(cmd.getOptionValue(DiskBalancerCLI
          .MAXERROR));
    }

    readClusterInfo(cmd);
    String output = null;
    if (cmd.hasOption(DiskBalancerCLI.OUTFILE)) {
      output = cmd.getOptionValue(DiskBalancerCLI.OUTFILE);
    }
    setOutputPath(output);

    // -plan nodename is the command line argument.
    DiskBalancerDataNode node =
        getNode(cmd.getOptionValue(DiskBalancerCLI.PLAN));
    if (node == null) {
      throw new IllegalArgumentException("Unable to find the specified node. " +
          cmd.getOptionValue(DiskBalancerCLI.PLAN));
    }

    try (FSDataOutputStream beforeStream = create(String.format(
        DiskBalancerCLI.BEFORE_TEMPLATE,
        cmd.getOptionValue(DiskBalancerCLI.PLAN)))) {
      beforeStream.write(getCluster().toJson()
          .getBytes(StandardCharsets.UTF_8));
    }

    this.thresholdPercentage = getThresholdPercentage(cmd);

    LOG.debug("threshold Percentage is {}", this.thresholdPercentage);
    setNodesToProcess(node);
    populatePathNames(node);

    NodePlan plan = null;
    List<NodePlan> plans = getCluster().computePlan(this.thresholdPercentage);
    setPlanParams(plans);

    if (plans.size() > 0) {
      plan = plans.get(0);
    }

    try {
      if (plan != null && plan.getVolumeSetPlans().size() > 0) {
        outputLine = String.format("Writing plan to:");
        recordOutput(result, outputLine);

        final String planFileName = String.format(
            DiskBalancerCLI.PLAN_TEMPLATE,
            cmd.getOptionValue(DiskBalancerCLI.PLAN));
        final String planFileFullName =
            new Path(getOutputPath(), planFileName).toString();
        recordOutput(result, planFileFullName);

        try (FSDataOutputStream planStream = create(planFileName)) {
          planStream.write(plan.toJson().getBytes(StandardCharsets.UTF_8));
        }
      } else {
        outputLine = String.format(
            "No plan generated. DiskBalancing not needed for node: %s"
                + " threshold used: %s",
            cmd.getOptionValue(DiskBalancerCLI.PLAN), this.thresholdPercentage);
        recordOutput(result, outputLine);
      }

      if (cmd.hasOption(DiskBalancerCLI.VERBOSE) && plans.size() > 0) {
        printToScreen(plans);
      }
    } catch (Exception e) {
      final String errMsg =
          "Errors while recording the output of plan command.";
      LOG.error(errMsg, e);
      result.appendln(errMsg).appendln(Throwables.getStackTraceAsString(e));
    }

    getPrintStream().print(result.toString());
  }


  /**
   * Gets extended help for this command.
   */
  @Override
  public void printHelp() {
    String header = "Creates a plan that describes how much data should be " +
        "moved between disks.\n\n";

    String footer = "\nPlan command creates a set of steps that represent a " +
        "planned data move. A plan file can be executed on a data node, which" +
        " will balance the data.";

    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("hdfs diskbalancer -plan <hostname> [options]",
        header, DiskBalancerCLI.getPlanOptions(), footer);
  }

  /**
   * Get Threshold for planning purpose.
   *
   * @param cmd - Command Line Argument.
   * @return double
   */
  private double getThresholdPercentage(CommandLine cmd) {
    Double value = 0.0;
    if (cmd.hasOption(DiskBalancerCLI.THRESHOLD)) {
      value = Double.parseDouble(cmd.getOptionValue(DiskBalancerCLI.THRESHOLD));
    }

    if ((value <= 0.0) || (value > 100.0)) {
      value = getConf().getDouble(
          DFSConfigKeys.DFS_DISK_BALANCER_PLAN_THRESHOLD,
          DFSConfigKeys.DFS_DISK_BALANCER_PLAN_THRESHOLD_DEFAULT);
    }
    return value;
  }

  /**
   * Prints a quick summary of the plan to screen.
   *
   * @param plans - List of NodePlans.
   */
  static private void printToScreen(List<NodePlan> plans) {
    System.out.println("\nPlan :\n");
    System.out.println(StringUtils.repeat("=", 80));

    System.out.println(
        StringUtils.center("Source Disk", 30) +
            StringUtils.center("Dest.Disk", 30) +
            StringUtils.center("Size", 10) +
            StringUtils.center("Type", 10));

    for (NodePlan plan : plans) {
      for (Step step : plan.getVolumeSetPlans()) {
        System.out.println(String.format("%s %s %s %s",
            StringUtils.center(step.getSourceVolume().getPath(), 30),
            StringUtils.center(step.getDestinationVolume().getPath(), 30),
            StringUtils.center(step.getSizeString(step.getBytesToMove()), 10),
            StringUtils.center(step.getDestinationVolume().getStorageType(),
                10)));
      }
    }

    System.out.println(StringUtils.repeat("=", 80));
  }

  /**
   * Sets user specified plan parameters.
   *
   * @param plans - list of plans.
   */
  private void setPlanParams(List<NodePlan> plans) {
    for (NodePlan plan : plans) {
      for (Step step : plan.getVolumeSetPlans()) {
        if (this.bandwidth > 0) {
          LOG.debug("Setting bandwidth to {}", this.bandwidth);
          step.setBandwidth(this.bandwidth);
        }
        if (this.maxError > 0) {
          LOG.debug("Setting max error to {}", this.maxError);
          step.setMaxDiskErrors(this.maxError);
        }
      }
    }
  }
}
