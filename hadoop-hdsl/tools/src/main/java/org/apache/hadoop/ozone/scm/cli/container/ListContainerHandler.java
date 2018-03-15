/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.hadoop.ozone.web.utils.JsonUtils;
import org.apache.hadoop.scm.client.ScmClient;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.scm.cli.SCMCLI.CMD_WIDTH;
import static org.apache.hadoop.ozone.scm.cli.SCMCLI.HELP_OP;

/**
 * This is the handler that process container list command.
 */
public class ListContainerHandler extends OzoneCommandHandler {

  public static final String CONTAINER_LIST = "list";
  public static final String OPT_START_CONTAINER = "start";
  public static final String OPT_PREFIX_CONTAINER = "prefix";
  public static final String OPT_COUNT = "count";

  /**
   * Constructs a handler object.
   *
   * @param scmClient scm client
   */
  public ListContainerHandler(ScmClient scmClient) {
    super(scmClient);
  }

  @Override
  public void execute(CommandLine cmd) throws IOException {
    if (!cmd.hasOption(CONTAINER_LIST)) {
      throw new IOException("Expecting container list");
    }
    if (cmd.hasOption(HELP_OP)) {
      displayHelp();
      return;
    }

    if (!cmd.hasOption(OPT_COUNT)) {
      displayHelp();
      if (!cmd.hasOption(HELP_OP)) {
        throw new IOException("Expecting container count");
      } else {
        return;
      }
    }

    String startName = cmd.getOptionValue(OPT_START_CONTAINER);
    String prefixName = cmd.getOptionValue(OPT_PREFIX_CONTAINER);
    int count = 0;

    if (cmd.hasOption(OPT_COUNT)) {
      count = Integer.parseInt(cmd.getOptionValue(OPT_COUNT));
      if (count < 0) {
        displayHelp();
        throw new IOException("-count should not be negative");
      }
    }

    List<ContainerInfo> containerList =
        getScmClient().listContainer(startName, prefixName, count);

    // Output data list
    for (ContainerInfo container : containerList) {
      outputContainerPipeline(container.getPipeline());
    }
  }

  private void outputContainerPipeline(Pipeline pipeline) throws IOException {
    // Print container report info.
    logOut("%s", JsonUtils.toJsonStringWithDefaultPrettyPrinter(
        pipeline.toJsonString()));
  }

  @Override
  public void displayHelp() {
    Options options = new Options();
    addOptions(options);
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp(CMD_WIDTH, "hdfs scm -container -list <option>",
        "where <option> can be the following", options, "");
  }

  public static void addOptions(Options options) {
    Option startContainerOpt = new Option(OPT_START_CONTAINER,
        true, "Specify start container name");
    Option endContainerOpt = new Option(OPT_PREFIX_CONTAINER,
        true, "Specify prefix container name");
    Option countOpt = new Option(OPT_COUNT, true,
        "Specify count number, required");
    options.addOption(countOpt);
    options.addOption(startContainerOpt);
    options.addOption(endContainerOpt);
  }
}
