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

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerException;
import org.apache.hadoop.hdfs.tools.DiskBalancerCLI;
import org.apache.hadoop.net.NetUtils;

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
    super(conf);
    addValidCommandParameters(DiskBalancerCLI.QUERY,
        "Queries the status of disk plan running on a given datanode.");
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
    Preconditions.checkState(cmd.hasOption(DiskBalancerCLI.QUERY));
    verifyCommandOptions(DiskBalancerCLI.QUERY, cmd);
    String nodeName = cmd.getOptionValue(DiskBalancerCLI.QUERY);
    Preconditions.checkNotNull(nodeName);
    nodeName = nodeName.trim();
    String nodeAddress = nodeName;

    // if the string is not name:port format use the default port.
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
      System.out.printf("Plan File: %s%nPlan ID: %s%nResult: %s%n",
              workStatus.getPlanFile(),
              workStatus.getPlanID(),
              workStatus.getResult().toString());

      if (cmd.hasOption(DiskBalancerCLI.VERBOSE)) {
        System.out.printf("%s", workStatus.currentStateString());
      }
    } catch (DiskBalancerException ex) {
      LOG.error("Query plan failed. ex: {}", ex);
      throw ex;
    }
  }

  /**
   * Gets extended help for this command.
   */
  @Override
  public void printHelp() {
    String header = "Query Plan queries a given data node about the " +
        "current state of disk balancer execution.\n\n";

    String footer = "\nQuery command retrievs the plan ID and the current " +
        "running state. ";

    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("hdfs diskbalancer -query <hostname>  [options]",
        header, DiskBalancerCLI.getQueryOptions(), footer);
  }
}
