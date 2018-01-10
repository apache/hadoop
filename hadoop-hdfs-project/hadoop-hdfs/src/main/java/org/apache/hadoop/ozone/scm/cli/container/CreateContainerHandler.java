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
package org.apache.hadoop.ozone.scm.cli.container;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.ozone.scm.cli.OzoneCommandHandler;
import org.apache.hadoop.scm.client.ScmClient;

import java.io.IOException;

import static org.apache.hadoop.ozone.scm.cli.SCMCLI.CMD_WIDTH;
import static org.apache.hadoop.ozone.scm.cli.SCMCLI.HELP_OP;

/**
 * This is the handler that process container creation command.
 */
public class CreateContainerHandler extends OzoneCommandHandler {

  public static final String CONTAINER_CREATE = "create";
  public static final String OPT_CONTAINER_NAME = "c";
  public static final String containerOwner = "OZONE";
  // TODO Support an optional -p <pipelineID> option to create
  // container on given datanodes.

  public CreateContainerHandler(ScmClient scmClient) {
    super(scmClient);
  }

  @Override
  public void execute(CommandLine cmd) throws IOException {
    if (!cmd.hasOption(CONTAINER_CREATE)) {
      throw new IOException("Expecting container create");
    }
    if (!cmd.hasOption(OPT_CONTAINER_NAME)) {
      displayHelp();
      if (!cmd.hasOption(HELP_OP)) {
        throw new IOException("Expecting container name");
      } else {
        return;
      }
    }
    String containerName = cmd.getOptionValue(OPT_CONTAINER_NAME);

    logOut("Creating container : %s.", containerName);
    getScmClient().createContainer(containerName, containerOwner);
    logOut("Container created.");
  }

  @Override
  public void displayHelp() {
    Options options = new Options();
    addOptions(options);
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp(CMD_WIDTH, "hdfs scm -container -create <option>",
        "where <option> is", options, "");
  }

  public static void addOptions(Options options) {
    Option containerNameOpt = new Option(OPT_CONTAINER_NAME,
        true, "Specify container name");
    options.addOption(containerNameOpt);
  }
}
