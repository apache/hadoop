/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdfs.server.diskbalancer.command;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.TextStringBuilder;
import org.apache.hadoop.util.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerException;
import org.apache.hadoop.hdfs.tools.DiskBalancerCLI;
import org.apache.hadoop.net.NetUtils;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/**
 * Gets the current status of disk balancer command.
 */
public class QueryCommand extends Command {

  /**
   * Constructs QueryCommand.
   *
   * @param conf - Configuration.
   */
  public QueryCommand(Configuration conf) {
    this(conf, System.out);
  }

  public QueryCommand(Configuration conf, final PrintStream ps) {
    super(conf, ps);
    addValidCommandParameters(DiskBalancerCLI.QUERY,
        "Queries the status of disk plan running on given datanode(s).");
    addValidCommandParameters(DiskBalancerCLI.VERBOSE,
        "Prints verbose results.");
  }

  /**
   * Executes the Client Calls.
   *
   * @param cmd - CommandLine
   */
  @Override
  public void execute(CommandLine cmd) throws Exception {
    LOG.info("Executing \"query plan\" command.");
    TextStringBuilder result = new TextStringBuilder();
    Preconditions.checkState(cmd.hasOption(DiskBalancerCLI.QUERY));
    verifyCommandOptions(DiskBalancerCLI.QUERY, cmd);
    String nodeVal = cmd.getOptionValue(DiskBalancerCLI.QUERY);
    if (StringUtils.isBlank(nodeVal)) {
      String warnMsg = "The number of input nodes is 0. "
          + "Please input the valid nodes.";
      throw new DiskBalancerException(warnMsg,
          DiskBalancerException.Result.INVALID_NODE);
    }
    nodeVal = nodeVal.trim();
    Set<String> resultSet = new TreeSet<>();
    String[] nodes = nodeVal.split(",");
    Collections.addAll(resultSet, nodes);
    String outputLine = String.format(
        "Get current status of the diskbalancer for DataNode(s). "
            + "These DataNode(s) are parsed from '%s'.", nodeVal);
    recordOutput(result, outputLine);
    for (String nodeName : resultSet) {
      // if the string is not name:port format use the default port.
      String nodeAddress = nodeName;
      if (!nodeName.matches("[^\\:]+:[0-9]{2,5}")) {
        int defaultIPC = NetUtils.createSocketAddr(
            getConf().getTrimmed(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY,
                DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_DEFAULT)).getPort();
        nodeAddress = nodeName + ":" + defaultIPC;
        LOG.debug("Using default data node port :  {}", nodeAddress);
      }

      ClientDatanodeProtocol dataNode = getDataNodeProxy(nodeAddress);
      try {
        DiskBalancerWorkStatus workStatus = dataNode.queryDiskBalancerPlan();
        outputLine = String.format("DataNode: %s%nPlan File: %s%nPlan ID: %s%nResult: %s%n",
            nodeAddress,
            workStatus.getPlanFile(),
            workStatus.getPlanID(),
            workStatus.getResult().toString());
        result.append(outputLine);
        if (cmd.hasOption(DiskBalancerCLI.VERBOSE)) {
          outputLine = String.format("%s", workStatus.currentStateString());
          result.append(outputLine);
        }
        result.append(System.lineSeparator());
      } catch (DiskBalancerException ex) {
        LOG.error("Query plan failed by {}", nodeAddress, ex);
        throw ex;
      }
    }
    getPrintStream().println(result);
  }

  /**
   * Gets extended help for this command.
   */
  @Override
  public void printHelp() {
    String header = "Query Plan queries given datanode(s) about the " +
        "current state of disk balancer execution.\n\n";

    String footer = "\nQuery command retrievs the plan ID and the current " +
        "running state. ";
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("hdfs diskbalancer -query <hostname,hostname,...> " +
            " [options]",
        header, DiskBalancerCLI.getQueryOptions(), footer);
  }
}
