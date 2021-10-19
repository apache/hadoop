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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.tools.DiskBalancerCLI;

/**
 * Help Command prints out detailed help about each command.
 */
public class HelpCommand extends Command {

  /**
   * Constructs a help command.
   *
   * @param conf - config
   */
  public HelpCommand(Configuration conf) {
    super(conf);
    addValidCommandParameters(DiskBalancerCLI.HELP, "Help Command");
  }

  /**
   * Executes the Client Calls.
   *
   * @param cmd - CommandLine
   */
  @Override
  public void execute(CommandLine cmd) throws Exception {
    LOG.debug("Processing help Command.");
    if (cmd == null) {
      this.printHelp();
      return;
    }

    Preconditions.checkState(cmd.hasOption(DiskBalancerCLI.HELP));
    verifyCommandOptions(DiskBalancerCLI.HELP, cmd);
    String helpCommand = cmd.getOptionValue(DiskBalancerCLI.HELP);
    if (helpCommand == null || helpCommand.isEmpty()) {
      this.printHelp();
      return;
    }

    helpCommand = helpCommand.trim();
    helpCommand = helpCommand.toLowerCase();
    Command command = null;
    switch (helpCommand) {
    case DiskBalancerCLI.PLAN:
      command = new PlanCommand(getConf());
      break;
    case DiskBalancerCLI.EXECUTE:
      command = new ExecuteCommand(getConf());
      break;
    case DiskBalancerCLI.QUERY:
      command = new QueryCommand(getConf());
      break;
    case DiskBalancerCLI.CANCEL:
      command = new CancelCommand(getConf());
      break;
    case DiskBalancerCLI.REPORT:
      command = new ReportCommand(getConf());
      break;
    default:
      command = this;
      break;
    }
    command.printHelp();

  }

  /**
   * Gets extended help for this command.
   */
  @Override
  public void printHelp() {
    String header = "\nDiskBalancer distributes data evenly between " +
        "different disks on a datanode. " +
        "DiskBalancer operates by generating a plan, that tells datanode " +
        "how to move data between disks. Users can execute a plan by " +
        "submitting it to the datanode. \nTo get specific help on a " +
        "particular command please run \n\n hdfs diskbalancer -help <command>.";

    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("hdfs diskbalancer [command] [options]",
        header, DiskBalancerCLI.getHelpOptions(), "");
  }


}
