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
package org.apache.hadoop.hdds.scm.cli.container;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hdds.scm.cli.OzoneCommandHandler;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerLifeCycleState;

import java.io.IOException;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;

import static org.apache.hadoop.hdds.scm.cli.SCMCLI.CMD_WIDTH;
import static org.apache.hadoop.hdds.scm.cli.SCMCLI.HELP_OP;

/**
 * This is the handler that process container info command.
 */
public class InfoContainerHandler extends OzoneCommandHandler {

  public static final String CONTAINER_INFO = "info";
  protected static final String OPT_CONTAINER_ID = "c";

  /**
   * Constructs a handler object.
   *
   * @param scmClient scm client.
   */
  public InfoContainerHandler(ScmClient scmClient) {
    super(scmClient);
  }

  @Override
  public void execute(CommandLine cmd) throws IOException {
    if (!cmd.hasOption(CONTAINER_INFO)) {
      throw new IOException("Expecting container info");
    }
    if (!cmd.hasOption(OPT_CONTAINER_ID)) {
      displayHelp();
      if (!cmd.hasOption(HELP_OP)) {
        throw new IOException("Expecting container name");
      } else {
        return;
      }
    }
    String containerID = cmd.getOptionValue(OPT_CONTAINER_ID);
    ContainerWithPipeline container = getScmClient().
        getContainerWithPipeline(Long.parseLong(containerID));
    Preconditions.checkNotNull(container, "Container cannot be null");

    ContainerData containerData = getScmClient().readContainer(container
        .getContainerInfo().getContainerID(), container.getPipeline());

    // Print container report info.
    logOut("Container id: %s", containerID);
    String openStatus =
        containerData.getState() == ContainerLifeCycleState.OPEN ? "OPEN" :
            "CLOSED";
    logOut("Container State: %s", openStatus);
    logOut("Container Path: %s", containerData.getContainerPath());

    // Output meta data.
    String metadataStr = containerData.getMetadataList().stream().map(
        p -> p.getKey() + ":" + p.getValue()).collect(Collectors.joining(", "));
    logOut("Container Metadata: {%s}", metadataStr);

    // Print pipeline of an existing container.
    logOut("LeaderID: %s", container.getPipeline()
        .getLeader().getHostName());
    String machinesStr = container.getPipeline()
        .getMachines().stream().map(
        DatanodeDetails::getHostName).collect(Collectors.joining(","));
    logOut("Datanodes: [%s]", machinesStr);
  }

  @Override
  public void displayHelp() {
    Options options = new Options();
    addOptions(options);
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp(CMD_WIDTH, "hdfs scm -container -info <option>",
        "where <option> is", options, "");
  }

  public static void addOptions(Options options) {
    Option containerIdOpt = new Option(OPT_CONTAINER_ID,
        true, "Specify container id");
    options.addOption(containerIdOpt);
  }
}
