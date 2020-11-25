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
package org.apache.hadoop.tools.dynamometer;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;

/**
 * Options supplied to the Client which are then passed through to the
 * ApplicationMaster.
 */
final class AMOptions {

  public static final String NAMENODE_MEMORY_MB_ARG = "namenode_memory_mb";
  public static final String NAMENODE_MEMORY_MB_DEFAULT = "2048";
  public static final String NAMENODE_VCORES_ARG = "namenode_vcores";
  public static final String NAMENODE_VCORES_DEFAULT = "1";
  public static final String NAMENODE_NODELABEL_ARG = "namenode_nodelabel";
  public static final String NAMENODE_ARGS_ARG = "namenode_args";
  public static final String DATANODE_MEMORY_MB_ARG = "datanode_memory_mb";
  public static final String DATANODE_MEMORY_MB_DEFAULT = "2048";
  public static final String DATANODE_VCORES_ARG = "datanode_vcores";
  public static final String DATANODE_VCORES_DEFAULT = "1";
  public static final String DATANODE_NODELABEL_ARG = "datanode_nodelabel";
  public static final String DATANODE_ARGS_ARG = "datanode_args";
  public static final String NAMENODE_METRICS_PERIOD_ARG =
      "namenode_metrics_period";
  public static final String NAMENODE_METRICS_PERIOD_DEFAULT = "60";
  public static final String SHELL_ENV_ARG = "shell_env";
  public static final String DATANODES_PER_CLUSTER_ARG =
      "datanodes_per_cluster";
  public static final String DATANODES_PER_CLUSTER_DEFAULT = "1";
  public static final String DATANODE_LAUNCH_DELAY_ARG =
      "datanode_launch_delay";
  public static final String DATANODE_LAUNCH_DELAY_DEFAULT = "0s";
  public static final String NAMENODE_NAME_DIR_ARG = "namenode_name_dir";
  public static final String NAMENODE_EDITS_DIR_ARG = "namenode_edits_dir";

  private final int datanodeMemoryMB;
  private final int datanodeVirtualCores;
  private final String datanodeArgs;
  private final String datanodeNodeLabelExpression;
  private final int datanodesPerCluster;
  private final String datanodeLaunchDelay;
  private final int namenodeMemoryMB;
  private final int namenodeVirtualCores;
  private final String namenodeArgs;
  private final String namenodeNodeLabelExpression;
  private final int namenodeMetricsPeriod;
  private final String namenodeNameDir;
  private final String namenodeEditsDir;
  // Original shellEnv as passed in through arguments
  private final Map<String, String> originalShellEnv;
  // Extended shellEnv including custom environment variables
  private final Map<String, String> shellEnv;

  @SuppressWarnings("checkstyle:parameternumber")
  private AMOptions(int datanodeMemoryMB, int datanodeVirtualCores,
      String datanodeArgs, String datanodeNodeLabelExpression,
      int datanodesPerCluster, String datanodeLaunchDelay, int namenodeMemoryMB,
      int namenodeVirtualCores, String namenodeArgs,
      String namenodeNodeLabelExpression, int namenodeMetricsPeriod,
      String namenodeNameDir, String namenodeEditsDir,
      Map<String, String> shellEnv) {
    this.datanodeMemoryMB = datanodeMemoryMB;
    this.datanodeVirtualCores = datanodeVirtualCores;
    this.datanodeArgs = datanodeArgs;
    this.datanodeNodeLabelExpression = datanodeNodeLabelExpression;
    this.datanodesPerCluster = datanodesPerCluster;
    this.datanodeLaunchDelay = datanodeLaunchDelay;
    this.namenodeMemoryMB = namenodeMemoryMB;
    this.namenodeVirtualCores = namenodeVirtualCores;
    this.namenodeArgs = namenodeArgs;
    this.namenodeNodeLabelExpression = namenodeNodeLabelExpression;
    this.namenodeMetricsPeriod = namenodeMetricsPeriod;
    this.namenodeNameDir = namenodeNameDir;
    this.namenodeEditsDir = namenodeEditsDir;
    this.originalShellEnv = shellEnv;
    this.shellEnv = new HashMap<>(this.originalShellEnv);
    this.shellEnv.put(DynoConstants.NN_ADDITIONAL_ARGS_ENV, this.namenodeArgs);
    this.shellEnv.put(DynoConstants.DN_ADDITIONAL_ARGS_ENV, this.datanodeArgs);
    this.shellEnv.put(DynoConstants.NN_FILE_METRIC_PERIOD_ENV,
        String.valueOf(this.namenodeMetricsPeriod));
    this.shellEnv.put(DynoConstants.NN_NAME_DIR_ENV, this.namenodeNameDir);
    this.shellEnv.put(DynoConstants.NN_EDITS_DIR_ENV, this.namenodeEditsDir);
  }

  /**
   * Verifies that arguments are valid; throws IllegalArgumentException if not.
   */
  void verify(long maxMemory, int maxVcores) throws IllegalArgumentException {
    Preconditions.checkArgument(
        datanodeMemoryMB > 0 && datanodeMemoryMB <= maxMemory,
        "datanodeMemoryMB (%s) must be between 0 and %s", datanodeMemoryMB,
        maxMemory);
    Preconditions.checkArgument(
        datanodeVirtualCores > 0 && datanodeVirtualCores <= maxVcores,
        "datanodeVirtualCores (%s) must be between 0 and %s",
        datanodeVirtualCores, maxVcores);
    Preconditions.checkArgument(
        namenodeMemoryMB > 0 && namenodeMemoryMB <= maxMemory,
        "namenodeMemoryMB (%s) must be between 0 and %s", namenodeMemoryMB,
        maxMemory);
    Preconditions.checkArgument(
        namenodeVirtualCores > 0 && namenodeVirtualCores <= maxVcores,
        "namenodeVirtualCores (%s) must be between 0 and %s",
        namenodeVirtualCores, maxVcores);
    Preconditions.checkArgument(datanodesPerCluster > 0,
        "datanodesPerCluster (%s) must be > 0", datanodesPerCluster);
  }

  /**
   * Same as {@link #verify(long, int)} but does not set a max.
   */
  void verify() throws IllegalArgumentException {
    verify(Integer.MAX_VALUE, Integer.MAX_VALUE);
  }

  void addToVargs(List<String> vargs) {
    vargs.add("--" + DATANODE_MEMORY_MB_ARG + " " + datanodeMemoryMB);
    vargs.add("--" + DATANODE_VCORES_ARG + " " + datanodeVirtualCores);
    addStringValToVargs(vargs, DATANODE_ARGS_ARG, datanodeArgs);
    addStringValToVargs(vargs, DATANODE_NODELABEL_ARG,
        datanodeNodeLabelExpression);
    vargs.add("--" + DATANODES_PER_CLUSTER_ARG + " " + datanodesPerCluster);
    vargs.add("--" + DATANODE_LAUNCH_DELAY_ARG + " " + datanodeLaunchDelay);
    vargs.add("--" + NAMENODE_MEMORY_MB_ARG + " " + namenodeMemoryMB);
    vargs.add("--" + NAMENODE_VCORES_ARG + " " + namenodeVirtualCores);
    addStringValToVargs(vargs, NAMENODE_ARGS_ARG, namenodeArgs);
    addStringValToVargs(vargs, NAMENODE_NODELABEL_ARG,
        namenodeNodeLabelExpression);
    vargs.add("--" + NAMENODE_METRICS_PERIOD_ARG + " " + namenodeMetricsPeriod);
    addStringValToVargs(vargs, NAMENODE_NAME_DIR_ARG, namenodeNameDir);
    addStringValToVargs(vargs, NAMENODE_EDITS_DIR_ARG, namenodeEditsDir);
    for (Map.Entry<String, String> entry : originalShellEnv.entrySet()) {
      vargs.add(
          "--" + SHELL_ENV_ARG + " " + entry.getKey() + "=" + entry.getValue());
    }
  }

  private void addStringValToVargs(List<String> vargs, String optionName,
      String val) {
    if (!val.isEmpty()) {
      vargs.add("--" + optionName + " \\\"" + val + "\\\"");
    }
  }

  int getDataNodeMemoryMB() {
    return datanodeMemoryMB;
  }

  int getDataNodeVirtualCores() {
    return datanodeVirtualCores;
  }

  String getDataNodeNodeLabelExpression() {
    return datanodeNodeLabelExpression;
  }

  int getDataNodesPerCluster() {
    return datanodesPerCluster;
  }

  long getDataNodeLaunchDelaySec() {
    // Leverage the human-readable time parsing capabilities of Configuration
    String tmpConfKey = "___temp_config_property___";
    Configuration tmpConf = new Configuration();
    tmpConf.set(tmpConfKey, datanodeLaunchDelay);
    return tmpConf.getTimeDuration(tmpConfKey, 0, TimeUnit.SECONDS);
  }

  int getNameNodeMemoryMB() {
    return namenodeMemoryMB;
  }

  int getNameNodeVirtualCores() {
    return namenodeVirtualCores;
  }

  String getNameNodeNodeLabelExpression() {
    return namenodeNodeLabelExpression;
  }

  Map<String, String> getShellEnv() {
    return shellEnv;
  }

  /**
   * Set all of the command line options relevant to this class into the passed
   * {@link Options}.
   *
   * @param opts
   *          Where to set the command line options.
   */
  static void setOptions(Options opts) {
    opts.addOption(SHELL_ENV_ARG, true,
        "Environment for shell script. Specified as env_key=env_val pairs");
    opts.addOption(NAMENODE_MEMORY_MB_ARG, true,
        "Amount of memory in MB to be requested to run the NN (default "
            + NAMENODE_MEMORY_MB_DEFAULT + "). "
            + "Ignored unless the NameNode is run within YARN.");
    opts.addOption(NAMENODE_VCORES_ARG, true,
        "Amount of virtual cores to be requested to run the NN (default "
            + NAMENODE_VCORES_DEFAULT + "). "
            + "Ignored unless the NameNode is run within YARN.");
    opts.addOption(NAMENODE_ARGS_ARG, true,
        "Additional arguments to add when starting the NameNode. "
            + "Ignored unless the NameNode is run within YARN.");
    opts.addOption(NAMENODE_NODELABEL_ARG, true,
        "The node label to specify for the container to use to "
            + "run the NameNode.");
    opts.addOption(NAMENODE_METRICS_PERIOD_ARG, true,
        "The period in seconds for the NameNode's metrics to be emitted to "
            + "file; if <=0, disables this functionality. Otherwise, a "
            + "metrics file will be stored in the container logs for the "
            + "NameNode (default " + NAMENODE_METRICS_PERIOD_DEFAULT + ").");
    opts.addOption(NAMENODE_NAME_DIR_ARG, true,
        "The directory to use for the NameNode's name data directory. "
            + "If not specified, a location  within the container's working "
            + "directory will be used.");
    opts.addOption(NAMENODE_EDITS_DIR_ARG, true,
        "The directory to use for the NameNode's edits directory. "
            + "If not specified, a location  within the container's working "
            + "directory will be used.");
    opts.addOption(DATANODE_MEMORY_MB_ARG, true,
        "Amount of memory in MB to be requested to run the DNs (default "
            + DATANODE_MEMORY_MB_DEFAULT + ")");
    opts.addOption(DATANODE_VCORES_ARG, true,
        "Amount of virtual cores to be requested to run the DNs (default "
            + DATANODE_VCORES_DEFAULT + ")");
    opts.addOption(DATANODE_ARGS_ARG, true,
        "Additional arguments to add when starting the DataNodes.");
    opts.addOption(DATANODE_NODELABEL_ARG, true, "The node label to specify "
        + "for the container to use to run the DataNode.");
    opts.addOption(DATANODES_PER_CLUSTER_ARG, true,
        "How many simulated DataNodes to run within each YARN container "
            + "(default " + DATANODES_PER_CLUSTER_DEFAULT + ")");
    opts.addOption(DATANODE_LAUNCH_DELAY_ARG, true,
        "The period over which to launch the DataNodes; this will "
            + "be used as the maximum delay and each DataNode container will "
            + "be launched with some random delay less than  this value. "
            + "Accepts human-readable time durations (e.g. 10s, 1m) (default "
            + DATANODE_LAUNCH_DELAY_DEFAULT + ")");

    opts.addOption("help", false, "Print usage");
  }

  /**
   * Initialize an {@code AMOptions} from a command line parser.
   *
   * @param cliParser
   *          Where to initialize from.
   * @return A new {@code AMOptions} filled out with options from the parser.
   */
  static AMOptions initFromParser(CommandLine cliParser) {
    Map<String, String> originalShellEnv = new HashMap<>();
    if (cliParser.hasOption(SHELL_ENV_ARG)) {
      for (String env : cliParser.getOptionValues(SHELL_ENV_ARG)) {
        String trimmed = env.trim();
        int index = trimmed.indexOf('=');
        if (index == -1) {
          originalShellEnv.put(trimmed, "");
          continue;
        }
        String key = trimmed.substring(0, index);
        String val = "";
        if (index < (trimmed.length() - 1)) {
          val = trimmed.substring(index + 1);
        }
        originalShellEnv.put(key, val);
      }
    }
    return new AMOptions(
        Integer.parseInt(cliParser.getOptionValue(DATANODE_MEMORY_MB_ARG,
            DATANODE_MEMORY_MB_DEFAULT)),
        Integer.parseInt(cliParser.getOptionValue(DATANODE_VCORES_ARG,
            DATANODE_VCORES_DEFAULT)),
        cliParser.getOptionValue(DATANODE_ARGS_ARG, ""),
        cliParser.getOptionValue(DATANODE_NODELABEL_ARG, ""),
        Integer.parseInt(cliParser.getOptionValue(DATANODES_PER_CLUSTER_ARG,
            DATANODES_PER_CLUSTER_DEFAULT)),
        cliParser.getOptionValue(DATANODE_LAUNCH_DELAY_ARG,
            DATANODE_LAUNCH_DELAY_DEFAULT),
        Integer.parseInt(cliParser.getOptionValue(NAMENODE_MEMORY_MB_ARG,
            NAMENODE_MEMORY_MB_DEFAULT)),
        Integer.parseInt(cliParser.getOptionValue(NAMENODE_VCORES_ARG,
            NAMENODE_VCORES_DEFAULT)),
        cliParser.getOptionValue(NAMENODE_ARGS_ARG, ""),
        cliParser.getOptionValue(NAMENODE_NODELABEL_ARG, ""),
        Integer.parseInt(cliParser.getOptionValue(NAMENODE_METRICS_PERIOD_ARG,
            NAMENODE_METRICS_PERIOD_DEFAULT)),
        cliParser.getOptionValue(NAMENODE_NAME_DIR_ARG, ""),
        cliParser.getOptionValue(NAMENODE_EDITS_DIR_ARG, ""), originalShellEnv);
  }

}
