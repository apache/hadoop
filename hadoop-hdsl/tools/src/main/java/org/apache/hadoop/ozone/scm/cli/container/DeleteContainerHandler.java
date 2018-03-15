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

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.ozone.scm.cli.OzoneCommandHandler;
import org.apache.hadoop.scm.client.ScmClient;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

import java.io.IOException;

import static org.apache.hadoop.ozone.scm.cli.SCMCLI.CMD_WIDTH;
import static org.apache.hadoop.ozone.scm.cli.SCMCLI.HELP_OP;

/**
 * This is the handler that process delete container command.
 */
public class DeleteContainerHandler extends OzoneCommandHandler {

  protected static final String CONTAINER_DELETE = "delete";
  protected static final String OPT_FORCE = "f";
  protected static final String OPT_CONTAINER_NAME = "c";

  public DeleteContainerHandler(ScmClient scmClient) {
    super(scmClient);
  }

  @Override
  public void execute(CommandLine cmd) throws IOException {
    Preconditions.checkArgument(cmd.hasOption(CONTAINER_DELETE),
        "Expecting command delete");
    if (!cmd.hasOption(OPT_CONTAINER_NAME)) {
      displayHelp();
      if (!cmd.hasOption(HELP_OP)) {
        throw new IOException("Expecting container name");
      } else {
        return;
      }
    }

    String containerName = cmd.getOptionValue(OPT_CONTAINER_NAME);

    Pipeline pipeline = getScmClient().getContainer(containerName);
    if (pipeline == null) {
      throw new IOException("Cannot delete an non-exist container "
          + containerName);
    }

    logOut("Deleting container : %s.", containerName);
    getScmClient().deleteContainer(pipeline, cmd.hasOption(OPT_FORCE));
    logOut("Container %s deleted.", containerName);
  }

  @Override
  public void displayHelp() {
    Options options = new Options();
    addOptions(options);
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp(CMD_WIDTH, "hdfs scm -container -delete <option>",
        "where <option> is", options, "");
  }

  public static void addOptions(Options options) {
    Option forceOpt = new Option(OPT_FORCE,
        false,
        "forcibly delete a container");
    options.addOption(forceOpt);
    Option containerNameOpt = new Option(OPT_CONTAINER_NAME,
        true, "Specify container name");
    options.addOption(containerNameOpt);
  }
}
