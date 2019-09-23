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
package org.apache.hadoop.hdds.scm.cli.node;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Place decommissioned or maintenance nodes back into service.
 */
@Command(
    name = "recommission",
    description = "Return a datanode to service",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DatanodeAdminRecommissionSubCommand implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeAdminRecommissionSubCommand.class);

  @CommandLine.Parameters(description = "List of fully qualified host names")
  private List<String> hosts = new ArrayList<String>();

  @ParentCommand
  private DatanodeAdminCommands parent;

  @Override
  public Void call() throws Exception {
    try (ScmClient scmClient = parent.getParent().createScmClient()) {
      scmClient.recommissionNodes(hosts);
      return null;
    }
  }
}