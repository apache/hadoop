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
package org.apache.hadoop.hdds.scm.cli.container;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hdds.scm.cli.OzoneCommandHandler;
import org.apache.hadoop.hdds.scm.client.ScmClient;

import java.io.IOException;

import static org.apache.hadoop.hdds.scm.cli.SCMCLI.CMD_WIDTH;
import static org.apache.hadoop.hdds.scm.cli.SCMCLI.HELP_OP;

/**
 * This is the handler that process container creation command.
 */
public class CreateContainerHandler extends OzoneCommandHandler {

  public static final String CONTAINER_CREATE = "create";
  public static final String CONTAINER_OWNER = "OZONE";
  // TODO Support an optional -p <pipelineID> option to create
  // container on given datanodes.

  public CreateContainerHandler(ScmClient scmClient) {
    super(scmClient);
  }

  @Override
  public void execute(CommandLine cmd) throws IOException {
    if (cmd.hasOption(HELP_OP)) {
      displayHelp();
    }

    if (!cmd.hasOption(CONTAINER_CREATE)) {
      throw new IOException("Expecting container create");
    }

    logOut("Creating container...");
    getScmClient().createContainer(CONTAINER_OWNER);
    logOut("Container created.");
  }

  @Override
  public void displayHelp() {
    Options options = new Options();
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp(CMD_WIDTH, "hdfs scm -container -create",
        null, options, null);
  }
}
