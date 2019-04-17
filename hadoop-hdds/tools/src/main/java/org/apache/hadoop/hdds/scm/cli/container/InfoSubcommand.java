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

import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerDataProto;
import org.apache.hadoop.hdds.scm.cli.SCMCLI;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .ContainerWithPipeline;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

/**
 * This is the handler that process container info command.
 */
@Command(
    name = "info",
    description = "Show information about a specific container",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class InfoSubcommand implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(InfoSubcommand.class);

  @ParentCommand
  private SCMCLI parent;

  @Parameters(description = "Decimal id of the container.")
  private long containerID;

  @Override
  public Void call() throws Exception {
    try (ScmClient scmClient = parent.createScmClient()) {
      ContainerWithPipeline container = scmClient.
          getContainerWithPipeline(containerID);
      Preconditions.checkNotNull(container, "Container cannot be null");

      ContainerDataProto containerData = scmClient.readContainer(container
          .getContainerInfo().getContainerID(), container.getPipeline());

      // Print container report info.
      LOG.info("Container id: {}", containerID);
      String openStatus =
          containerData.getState() == ContainerDataProto.State.OPEN ? "OPEN" :
              "CLOSED";
      LOG.info("Container State: {}", openStatus);
      LOG.info("Container Path: {}", containerData.getContainerPath());

      // Output meta data.
      String metadataStr = containerData.getMetadataList().stream().map(
          p -> p.getKey() + ":" + p.getValue())
          .collect(Collectors.joining(", "));
      LOG.info("Container Metadata: {}", metadataStr);

      // Print pipeline of an existing container.
      String machinesStr = container.getPipeline().getNodes().stream().map(
              DatanodeDetails::getHostName).collect(Collectors.joining(","));
      LOG.info("Datanodes: [{}]", machinesStr);
      return null;
    }
  }
}
