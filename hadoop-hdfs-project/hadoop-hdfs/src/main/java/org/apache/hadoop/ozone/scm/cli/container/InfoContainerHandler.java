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

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerData;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.scm.cli.OzoneCommandHandler;
import org.apache.hadoop.scm.client.ScmClient;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

import java.io.IOException;
import java.util.stream.Collectors;

/**
 * This is the handler that process container info command.
 */
public class InfoContainerHandler extends OzoneCommandHandler {

  public static final String CONTAINER_INFO = "info";

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
    String containerName = cmd.getOptionValue(CONTAINER_INFO);
    Pipeline pipeline = getScmClient().getContainer(containerName);
    Preconditions.checkNotNull(pipeline, "Pipeline cannot be null");

    ContainerData containerData =
        getScmClient().readContainer(pipeline);

    // Print container report info.
    logOut("Container Name: %s",
        containerData.getName());
    String openStatus = containerData.getOpen() ? "OPEN" : "CLOSED";
    logOut("Container State: %s", openStatus);
    if (!containerData.getHash().isEmpty()) {
      logOut("Container Hash: %s", containerData.getHash());
    }
    logOut("Container DB Path: %s", containerData.getDbPath());
    logOut("Container Path: %s", containerData.getContainerPath());

    // Output meta data.
    String metadataStr = containerData.getMetadataList().stream().map(
        p -> p.getKey() + ":" + p.getValue()).collect(Collectors.joining(", "));
    logOut("Container Metadata: {%s}", metadataStr);

    // Print pipeline of an existing container.
    logOut("LeaderID: %s", pipeline.getLeader().getHostName());
    String machinesStr = pipeline.getMachines().stream().map(
        DatanodeID::getHostName).collect(Collectors.joining(","));
    logOut("Datanodes: [%s]", machinesStr);
  }

  @Override
  public void displayHelp() {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("hdfs scm -container -info <container name>",
        new Options());
  }
}
