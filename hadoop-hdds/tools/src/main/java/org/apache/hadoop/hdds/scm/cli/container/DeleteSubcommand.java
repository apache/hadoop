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

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.SCMCLI;
import org.apache.hadoop.hdds.scm.client.ScmClient;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

/**
 * This is the handler that process delete container command.
 */
@Command(
    name = "delete",
    description = "Delete container",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DeleteSubcommand implements Callable<Void> {

  @Parameters(description = "Id of the container to close")
  private long containerId;

  @Option(names = {"-f",
      "--force"}, description = "forcibly delete the container")
  private boolean force;

  @ParentCommand
  private SCMCLI parent;

  @Override
  public Void call() throws Exception {
    try (ScmClient scmClient = parent.createScmClient()) {
      parent.checkContainerExists(scmClient, containerId);
      scmClient.deleteContainer(containerId, force);
      return null;
    }
  }
}
