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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.SCMCLI;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.ozone.web.utils.JsonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Visibility;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * This is the handler that process container list command.
 */
@Command(
    name = "list",
    description = "List containers",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ListSubcommand implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ListSubcommand.class);

  @ParentCommand
  private SCMCLI parent;

  @Option(names = {"-s", "--start"},
      description = "Container id to start the iteration", required = true)
  private long startId = 1;

  @Option(names = {"-c", "--count"},
      description = "Maximum number of containers to list",
      defaultValue = "20", showDefaultValue = Visibility.ALWAYS)
  private int count = 20;

  private void outputContainerInfo(ContainerInfo containerInfo)
      throws IOException {
    // Print container report info.
    LOG.info("{}", JsonUtils.toJsonStringWithDefaultPrettyPrinter(
        containerInfo.toJsonString()));
  }

  @Override
  public Void call() throws Exception {
    try (ScmClient scmClient = parent.createScmClient()) {

      List<ContainerInfo> containerList =
          scmClient.listContainer(startId, count);

      // Output data list
      for (ContainerInfo container : containerList) {
        outputContainerInfo(container);
      }
      return null;
    }
  }
}
