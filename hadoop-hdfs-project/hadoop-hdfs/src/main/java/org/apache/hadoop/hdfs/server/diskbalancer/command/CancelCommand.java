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
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerException;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.NodePlan;
import org.apache.hadoop.hdfs.tools.DiskBalancerCLI;

import java.io.IOException;

/**
 * Cancels a running plan.
 */
public class CancelCommand extends Command {
  /**
   * Contructs a cancel Command.
   *
   * @param conf - Conf
   */
  public CancelCommand(Configuration conf) {
    super(conf);
    addValidCommandParameters(DiskBalancerCLI.CANCEL,
        "Cancels a running plan.");
    addValidCommandParameters(DiskBalancerCLI.NODE,
        "Node to run the command against in node:port format.");
  }

  /**
   * Executes the Client Calls.
   *
   * @param cmd - CommandLine
   */
  @Override
  public void execute(CommandLine cmd) throws Exception {
    LOG.info("Executing \"Cancel plan\" command.");
    Preconditions.checkState(cmd.hasOption(DiskBalancerCLI.CANCEL));
    verifyCommandOptions(DiskBalancerCLI.CANCEL, cmd);

    // We can cancel a plan using datanode address and plan ID
    // that you can read from a datanode using queryStatus
    if(cmd.hasOption(DiskBalancerCLI.NODE)) {
      String nodeAddress = cmd.getOptionValue(DiskBalancerCLI.NODE);
      String planHash = cmd.getOptionValue(DiskBalancerCLI.CANCEL);
      cancelPlanUsingHash(nodeAddress, planHash);
    } else {
      // Or you can cancel a plan using the plan file. If the user
      // points us to the plan file, we can compute the hash as well as read
      // the address of the datanode from the plan file.
      String planFile = cmd.getOptionValue(DiskBalancerCLI.CANCEL);
      Preconditions.checkArgument(planFile != null && !planFile.isEmpty(),
          "Invalid plan file specified.");
      String planData = null;
      try (FSDataInputStream plan = open(planFile)) {
        planData = IOUtils.toString(plan);
      }
      cancelPlan(planData);
    }
  }

  /**
   * Cancels a running plan.
   *
   * @param planData - Plan data.
   * @throws IOException
   */
  private void cancelPlan(String planData) throws IOException {
    Preconditions.checkNotNull(planData);
    NodePlan plan = NodePlan.parseJson(planData);
    String dataNodeAddress = plan.getNodeName() + ":" + plan.getPort();
    Preconditions.checkNotNull(dataNodeAddress);
    ClientDatanodeProtocol dataNode = getDataNodeProxy(dataNodeAddress);
    String planHash = DigestUtils.shaHex(planData);
    try {
      dataNode.cancelDiskBalancePlan(planHash);
    } catch (DiskBalancerException ex) {
      LOG.error("Cancelling plan on  {} failed. Result: {}, Message: {}",
          plan.getNodeName(), ex.getResult().toString(), ex.getMessage());
      throw ex;
    }
  }

  /**
   * Cancels a running plan.
   * @param nodeAddress - Address of the data node.
   * @param hash - Sha512 hash of the plan, which can be read from datanode
   *             using query status command.
   * @throws IOException
   */
  private void cancelPlanUsingHash(String nodeAddress, String hash) throws
      IOException {
    Preconditions.checkNotNull(nodeAddress);
    Preconditions.checkNotNull(hash);
    ClientDatanodeProtocol dataNode = getDataNodeProxy(nodeAddress);
    try {
      dataNode.cancelDiskBalancePlan(hash);
    } catch (DiskBalancerException ex) {
      LOG.error("Cancelling plan on  {} failed. Result: {}, Message: {}",
          nodeAddress, ex.getResult().toString(), ex.getMessage());
      throw ex;
    }
  }


  /**
   * Gets extended help for this command.
   */
  @Override
  public void printHelp() {
    String header = "Cancel command cancels a running disk balancer operation" +
        ".\n\n";

    String footer = "\nCancel command can be run via pointing to a plan file," +
        " or by reading the plan ID using the query command and then using " +
        "planID and hostname. Examples of how to run this command are \n" +
        "hdfs diskbalancer -cancel <planfile> \n" +
        "hdfs diskbalancer -cancel <planID> -node <hostname>";

    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("hdfs diskbalancer -cancel <planFile> | -cancel " +
        "<planID> -node <hostname>",
        header, DiskBalancerCLI.getCancelOptions(), footer);
  }
}
