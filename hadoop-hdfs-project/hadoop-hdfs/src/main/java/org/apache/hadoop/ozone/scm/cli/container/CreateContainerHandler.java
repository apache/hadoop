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
  public static final String PIPELINE_ID = "p";

  public CreateContainerHandler(ScmClient scmClient) {
    super(scmClient);
  }

  @Override
  public void execute(CommandLine cmd) throws IOException {
    if (!cmd.hasOption(CONTAINER_CREATE)) {
      throw new IOException("Expecting container create");
    }
    // TODO requires pipeline id (instead of optional as in the design) for now
    if (!cmd.hasOption(PIPELINE_ID)) {
      displayHelp();
      if (!cmd.hasOption(HELP_OP)) {
        throw new IOException("Expecting container ID");
      } else {
        return;
      }
    }
    String pipelineID = cmd.getOptionValue(PIPELINE_ID);
    LOG.info("Create container :" + pipelineID + " " + getScmClient());
    getScmClient().createContainer(pipelineID);
    LOG.debug("Container creation returned");
  }

  @Override
  public void displayHelp() {
    // TODO : may need to change this if we decide to make -p optional later
    Options options = new Options();
    addOptions(options);
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp(CMD_WIDTH, "hdfs scm -container -create <option>",
        "where <option> is", options, "");
  }

  public static void addOptions(Options options) {
    Option pipelineID = new Option(PIPELINE_ID, true, "Specify pipeline ID");
    options.addOption(pipelineID);
  }
}
