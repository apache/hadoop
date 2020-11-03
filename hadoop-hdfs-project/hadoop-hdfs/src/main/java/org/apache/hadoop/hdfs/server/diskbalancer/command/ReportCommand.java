/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.diskbalancer.command;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.TextStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerException;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolumeSet;
import org.apache.hadoop.hdfs.tools.DiskBalancerCLI;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

/**
 * Executes the report command.
 *
 * This command will report volume information for a specific DataNode or top X
 * DataNode(s) benefiting from running DiskBalancer.
 *
 * This is done by reading the cluster info, sorting the DiskbalancerNodes by
 * their NodeDataDensity and printing out the info.
 */
public class ReportCommand extends Command {

  public ReportCommand(Configuration conf) {
    this(conf, System.out);
  }

  public ReportCommand(Configuration conf, final PrintStream ps) {
    super(conf, ps);

    addValidCommandParameters(DiskBalancerCLI.REPORT,
        "Report volume information of nodes.");

    String desc = String.format(
        "Top number of nodes to be processed. Default: %d", getDefaultTop());
    addValidCommandParameters(DiskBalancerCLI.TOP, desc);

    desc = String.format("Print out volume information for DataNode(s).");
    addValidCommandParameters(DiskBalancerCLI.NODE, desc);
  }

  @Override
  public void execute(CommandLine cmd) throws Exception {
    TextStringBuilder result = new TextStringBuilder();
    String outputLine = "Processing report command";
    recordOutput(result, outputLine);

    Preconditions.checkState(cmd.hasOption(DiskBalancerCLI.REPORT));
    verifyCommandOptions(DiskBalancerCLI.REPORT, cmd);
    readClusterInfo(cmd);

    final String nodeFormat =
        "%d/%d %s[%s:%d] - <%s>: %d volumes with node data density %.2f.";
    final String nodeFormatWithoutSequence =
        "%s[%s:%d] - <%s>: %d volumes with node data density %.2f.";
    final String volumeFormat =
        "[%s: volume-%s] - %.2f used: %d/%d, %.2f free: %d/%d, "
        + "isFailed: %s, isReadOnly: %s, isSkip: %s, isTransient: %s.";

    if (cmd.hasOption(DiskBalancerCLI.NODE)) {
      /*
       * Reporting volume information for specific DataNode(s)
       */
      handleNodeReport(cmd, result, nodeFormatWithoutSequence, volumeFormat);

    } else { // handle TOP
      /*
       * Reporting volume information for top X DataNode(s)
       */
      handleTopReport(cmd, result, nodeFormat);
    }

    getPrintStream().println(result.toString());
  }

  private void handleTopReport(final CommandLine cmd, final TextStringBuilder result,
      final String nodeFormat) throws IllegalArgumentException {
    Collections.sort(getCluster().getNodes(), Collections.reverseOrder());

    /* extract value that identifies top X DataNode(s) */
    setTopNodes(parseTopNodes(cmd, result));

    /*
     * Reporting volume information of top X DataNode(s) in summary
     */
    final String outputLine = String.format(
        "Reporting top %d DataNode(s) benefiting from running DiskBalancer.",
        getTopNodes());
    recordOutput(result, outputLine);

    ListIterator<DiskBalancerDataNode> li = getCluster().getNodes()
        .listIterator();

    for (int i = 0; i < getTopNodes() && li.hasNext(); i++) {
      DiskBalancerDataNode dbdn = li.next();
      result.appendln(String.format(nodeFormat,
          i+1,
          getTopNodes(),
          dbdn.getDataNodeName(),
          dbdn.getDataNodeIP(),
          dbdn.getDataNodePort(),
          dbdn.getDataNodeUUID(),
          dbdn.getVolumeCount(),
          dbdn.getNodeDataDensity()));
    }
  }

  private void handleNodeReport(final CommandLine cmd, TextStringBuilder result,
      final String nodeFormat, final String volumeFormat) throws Exception {
    String outputLine = "";
    /*
     * get value that identifies DataNode(s) from command line, it could be
     * UUID, IP address or host name.
     */
    final String nodeVal = cmd.getOptionValue(DiskBalancerCLI.NODE);

    if (StringUtils.isBlank(nodeVal)) {
      outputLine = "The value for '-node' is neither specified or empty.";
      recordOutput(result, outputLine);
    } else {
      /*
       * Reporting volume information for specific DataNode(s)
       */
      outputLine = String.format(
          "Reporting volume information for DataNode(s). "
          + "These DataNode(s) are parsed from '%s'.", nodeVal);

      recordOutput(result, outputLine);

      List<DiskBalancerDataNode> dbdns = Lists.newArrayList();
      try {
        dbdns = getNodes(nodeVal);
      } catch (DiskBalancerException e) {
        // If there are some invalid nodes that contained in nodeVal,
        // the exception will be threw.
        recordOutput(result, e.getMessage());
        return;
      }

      if (!dbdns.isEmpty()) {
        for (DiskBalancerDataNode node : dbdns) {
          recordNodeReport(result, node, nodeFormat, volumeFormat);
          result.append(System.lineSeparator());
        }
      }
    }
  }

  /**
   * Put node report lines to string buffer.
   */
  private void recordNodeReport(TextStringBuilder result, DiskBalancerDataNode dbdn,
      final String nodeFormat, final String volumeFormat) throws Exception {
    final String trueStr = "True";
    final String falseStr = "False";

    // get storage path of datanode
    populatePathNames(dbdn);
    result.appendln(String.format(nodeFormat,
        dbdn.getDataNodeName(),
        dbdn.getDataNodeIP(),
        dbdn.getDataNodePort(),
        dbdn.getDataNodeUUID(),
        dbdn.getVolumeCount(),
        dbdn.getNodeDataDensity()));

    List<String> volumeList = Lists.newArrayList();
    for (DiskBalancerVolumeSet vset : dbdn.getVolumeSets().values()) {
      for (DiskBalancerVolume vol : vset.getVolumes()) {
        volumeList.add(String.format(volumeFormat,
            vol.getStorageType(),
            vol.getPath(),
            vol.getUsedRatio(),
            vol.getUsed(),
            vol.getCapacity(),
            vol.getFreeRatio(),
            vol.getFreeSpace(),
            vol.getCapacity(),
            vol.isFailed() ? trueStr : falseStr,
            vol.isReadOnly() ? trueStr: falseStr,
            vol.isSkip() ? trueStr : falseStr,
            vol.isTransient() ? trueStr : falseStr));
      }
    }

    Collections.sort(volumeList);
    result.appendln(
        StringUtils.join(volumeList.toArray(), System.lineSeparator()));
  }

  /**
   * Prints the help message.
   */
  @Override
  public void printHelp() {
    String header = "Report command reports the volume information of given" +
        " datanode(s), or prints out the list of nodes that will benefit " +
        "from running disk balancer. Top defaults to " + getDefaultTop();
    String footer = ". E.g.:\n"
        + "hdfs diskbalancer -report\n"
        + "hdfs diskbalancer -report -top 5\n"
        + "hdfs diskbalancer -report "
        + "-node <file://> | [<DataNodeID|IP|Hostname>,...]";

    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("hdfs diskbalancer -fs http://namenode.uri " +
        "-report [options]",
        header, DiskBalancerCLI.getReportOptions(), footer);
  }
}
