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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.SCMCLI;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * Handler of closePipeline command.
 */
@CommandLine.Command(
    name = "closePipeline",
    description = "Close pipeline",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ClosePipelineSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private SCMCLI parent;

  @CommandLine.Parameters(description = "ID of the pipeline to close")
  private String pipelineId;

  @Override
  public Void call() throws Exception {
    try (ScmClient scmClient = parent.createScmClient()) {
      scmClient.closePipeline(
          HddsProtos.PipelineID.newBuilder().setId(pipelineId).build());
      return null;
    }
  }
}