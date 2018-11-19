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

package org.apache.hadoop.hdds.scm.cli.pipeline;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.SCMCLI;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * Handler of listPipelines command.
 */
@CommandLine.Command(
    name = "listPipelines",
    description = "List all active pipelines",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ListPipelinesSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private SCMCLI parent;

  @Override
  public Void call() throws Exception {
    try (ScmClient scmClient = parent.createScmClient()) {
      scmClient.listPipelines().forEach(System.out::println);
      return null;
    }
  }
}