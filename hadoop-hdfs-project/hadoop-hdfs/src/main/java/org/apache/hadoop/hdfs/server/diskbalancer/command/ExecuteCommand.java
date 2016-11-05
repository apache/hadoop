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
 * executes a given plan.
 */
public class ExecuteCommand extends Command {

  /**
   * Constructs ExecuteCommand.
   *
   * @param conf - Configuration.
   */
  public ExecuteCommand(Configuration conf) {
    super(conf);
    addValidCommandParameters(DiskBalancerCLI.EXECUTE,
        "Executes a given plan.");
  }

  /**
   * Executes the Client Calls.
   *
   * @param cmd - CommandLine
   */
  @Override
  public void execute(CommandLine cmd) throws Exception {
    LOG.info("Executing \"execute plan\" command");
    Preconditions.checkState(cmd.hasOption(DiskBalancerCLI.EXECUTE));
    verifyCommandOptions(DiskBalancerCLI.EXECUTE, cmd);

    String planFile = cmd.getOptionValue(DiskBalancerCLI.EXECUTE);
    Preconditions.checkArgument(planFile != null && !planFile.isEmpty(),
        "Invalid plan file specified.");

    String planData = null;
    try (FSDataInputStream plan = open(planFile)) {
      planData = IOUtils.toString(plan);
    }
    submitPlan(planFile, planData);
  }

  /**
   * Submits plan to a given data node.
   *
   * @param planFile - Plan file name
   * @param planData - Plan data in json format
   * @throws IOException
   */
  private void submitPlan(final String planFile, final String planData)
          throws IOException {
    Preconditions.checkNotNull(planData);
    NodePlan plan = NodePlan.parseJson(planData);
    String dataNodeAddress = plan.getNodeName() + ":" + plan.getPort();
    Preconditions.checkNotNull(dataNodeAddress);
    ClientDatanodeProtocol dataNode = getDataNodeProxy(dataNodeAddress);
    String planHash = DigestUtils.shaHex(planData);
    try {
      // TODO : Support skipping date check.
      dataNode.submitDiskBalancerPlan(planHash, DiskBalancerCLI.PLAN_VERSION,
                                      planFile, planData, false);
    } catch (DiskBalancerException ex) {
      LOG.error("Submitting plan on  {} failed. Result: {}, Message: {}",
          plan.getNodeName(), ex.getResult().toString(), ex.getMessage());
      throw ex;
    }
  }

  /**
   * Gets extended help for this command.
   */
  @Override
  public void printHelp() {
    String header = "Execute command runs a submits a plan for execution on " +
        "the given data node.\n\n";

    String footer = "\nExecute command submits the job to data node and " +
        "returns immediately. The state of job can be monitored via query " +
        "command. ";

    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("hdfs diskbalancer -execute <planfile>",
        header, DiskBalancerCLI.getExecuteOptions(), footer);
  }
}
