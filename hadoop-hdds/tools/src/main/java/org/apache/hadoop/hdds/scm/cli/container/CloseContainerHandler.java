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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hdds.scm.cli.OzoneCommandHandler;
import org.apache.hadoop.hdds.scm.cli.SCMCLI;
import org.apache.hadoop.hdds.scm.client.ScmClient;

import java.io.IOException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;

/**
 * The handler of close container command.
 */
public class CloseContainerHandler extends OzoneCommandHandler {

  public static final String CONTAINER_CLOSE = "close";
  public static final String OPT_CONTAINER_ID = "c";

  @Override
  public void execute(CommandLine cmd) throws IOException {
    if (!cmd.hasOption(CONTAINER_CLOSE)) {
      throw new IOException("Expecting container close");
    }
    if (!cmd.hasOption(OPT_CONTAINER_ID)) {
      displayHelp();
      if (!cmd.hasOption(SCMCLI.HELP_OP)) {
        throw new IOException("Expecting container id");
      } else {
        return;
      }
    }
    String containerID = cmd.getOptionValue(OPT_CONTAINER_ID);

    ContainerWithPipeline container = getScmClient().
        getContainerWithPipeline(Long.parseLong(containerID));
    if (container == null) {
      throw new IOException("Cannot close an non-exist container "
          + containerID);
    }
    logOut("Closing container : %s.", containerID);
    getScmClient()
        .closeContainer(container.getContainerInfo().getContainerID());
    logOut("Container closed.");
  }

  @Override
  public void displayHelp() {
    Options options = new Options();
    addOptions(options);
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter
        .printHelp(SCMCLI.CMD_WIDTH, "hdfs scm -container -close <option>",
            "where <option> is", options, "");
  }

  public static void addOptions(Options options) {
    Option containerNameOpt = new Option(OPT_CONTAINER_ID,
        true, "Specify container ID");
    options.addOption(containerNameOpt);
  }

  CloseContainerHandler(ScmClient client) {
    super(client);
  }
}
