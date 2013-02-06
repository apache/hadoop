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
package org.apache.hadoop.yarn.client.cli;

import java.io.PrintWriter;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class NodeCLI extends YarnCLI {
  private static final String NODES_PATTERN = "%16s\t%17s\t%26s\t%18s\n";
  public static void main(String[] args) throws Exception {
    NodeCLI cli = new NodeCLI();
    cli.setSysOutPrintStream(System.out);
    cli.setSysErrPrintStream(System.err);
    int res = ToolRunner.run(cli, args);
    cli.stop();
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {

    Options opts = new Options();
    opts.addOption(STATUS_CMD, true, "Prints the status report of the node.");
    opts.addOption(LIST_CMD, false, "Lists all the nodes.");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    int exitCode = -1;
    if (cliParser.hasOption("status")) {
      if (args.length != 2) {
        printUsage(opts);
        return exitCode;
      }
      printNodeStatus(cliParser.getOptionValue("status"));
    } else if (cliParser.hasOption("list")) {
      listClusterNodes();
    } else {
      syserr.println("Invalid Command Usage : ");
      printUsage(opts);
    }
    return 0;
  }

  /**
   * It prints the usage of the command
   * 
   * @param opts
   */
  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("node", opts);
  }

  /**
   * Lists all the nodes present in the cluster
   * 
   * @throws YarnRemoteException
   */
  private void listClusterNodes() throws YarnRemoteException {
    PrintWriter writer = new PrintWriter(sysout);
    List<NodeReport> nodesReport = client.getNodeReports();
    writer.println("Total Nodes:" + nodesReport.size());
    writer.printf(NODES_PATTERN, "Node-Id", "Node-Http-Address",
        "Health-Status(isNodeHealthy)", "Running-Containers");
    for (NodeReport nodeReport : nodesReport) {
      writer.printf(NODES_PATTERN, nodeReport.getNodeId(),
          nodeReport.getHttpAddress(), nodeReport
          .getNodeHealthStatus().getIsNodeHealthy(), nodeReport
          .getNumContainers());
    }
    writer.flush();
  }

  /**
   * Prints the node report for node id.
   * 
   * @param nodeIdStr
   * @throws YarnRemoteException
   */
  private void printNodeStatus(String nodeIdStr) throws YarnRemoteException {
    NodeId nodeId = ConverterUtils.toNodeId(nodeIdStr);
    List<NodeReport> nodesReport = client.getNodeReports();
    StringBuffer nodeReportStr = new StringBuffer();
    NodeReport nodeReport = null;
    for (NodeReport report : nodesReport) {
      if (!report.getNodeId().equals(nodeId)) {
        continue;
      }
      nodeReport = report;
      nodeReportStr.append("Node Report : ");
      nodeReportStr.append("\n\tNode-Id : ");
      nodeReportStr.append(nodeReport.getNodeId());
      nodeReportStr.append("\n\tRack : ");
      nodeReportStr.append(nodeReport.getRackName());
      nodeReportStr.append("\n\tNode-Http-Address : ");
      nodeReportStr.append(nodeReport.getHttpAddress());
      nodeReportStr.append("\n\tHealth-Status(isNodeHealthy) : ");
      nodeReportStr.append(nodeReport.getNodeHealthStatus()
          .getIsNodeHealthy());
      nodeReportStr.append("\n\tLast-Last-Health-Update : ");
      nodeReportStr.append(nodeReport.getNodeHealthStatus()
          .getLastHealthReportTime());
      nodeReportStr.append("\n\tHealth-Report : ");
      nodeReportStr
          .append(nodeReport.getNodeHealthStatus().getHealthReport());
      nodeReportStr.append("\n\tContainers : ");
      nodeReportStr.append(nodeReport.getNumContainers());
      nodeReportStr.append("\n\tMemory-Used : ");
      nodeReportStr.append((nodeReport.getUsed() == null) ? "0M"
          : (nodeReport.getUsed().getMemory() + "M"));
      nodeReportStr.append("\n\tMemory-Capacity : ");
      nodeReportStr.append(nodeReport.getCapability().getMemory());
    }

    if (nodeReport == null) {
      nodeReportStr.append("Could not find the node report for node id : "
          + nodeIdStr);
    }

    sysout.println(nodeReportStr.toString());
  }
}
