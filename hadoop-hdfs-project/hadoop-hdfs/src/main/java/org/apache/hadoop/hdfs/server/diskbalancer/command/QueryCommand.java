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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerException;
import org.apache.hadoop.hdfs.tools.DiskBalancer;

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
    addValidCommandParameters(DiskBalancer.QUERY, "Queries the status of disk" +
        " plan running on a given datanode.");
    addValidCommandParameters(DiskBalancer.VERBOSE, "Prints verbose results.");
  }
  /**
   * Executes the Client Calls.
   *
   * @param cmd - CommandLine
   */
  @Override
  public void execute(CommandLine cmd) throws Exception {
    LOG.info("Executing \"query plan\" command.");
    Preconditions.checkState(cmd.hasOption(DiskBalancer.QUERY));
    verifyCommandOptions(DiskBalancer.QUERY, cmd);
    String nodeName = cmd.getOptionValue(DiskBalancer.QUERY);
    Preconditions.checkNotNull(nodeName);
    ClientDatanodeProtocol dataNode = getDataNodeProxy(nodeName);
    try {
      DiskBalancerWorkStatus workStatus = dataNode.queryDiskBalancerPlan();
      System.out.printf("Plan ID: %s Result: %s%n", workStatus.getPlanID(),
          workStatus.getResult().toString());

      if(cmd.hasOption(DiskBalancer.VERBOSE)) {
        System.out.printf("%s", workStatus.currentStateString());
      }
    } catch (DiskBalancerException ex) {
      LOG.error("Query plan failed. ex: {}", ex);
      throw ex;
    }
  }

  /**
   * Gets extended help for this command.
   *
   * @return Help Message
   */
  @Override
  protected String getHelp() {
    return "Gets the status of disk balancing on a given node";
  }
}
