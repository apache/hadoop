/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;

import java.util.Map;

/**
 * Encapsulates the docker rm command and its command
 * line arguments.
 */
public class DockerRmCommand extends DockerCommand {
  private static final String RM_COMMAND = "rm";
  private static final String CGROUP_HIERARCHY = "hierarchy";
  private String cGroupArg;

  public DockerRmCommand(String containerName, String hierarchy) {
    super(RM_COMMAND);
    super.addCommandArguments("name", containerName);
    if ((hierarchy != null) && !hierarchy.isEmpty()) {
      super.addCommandArguments(CGROUP_HIERARCHY, hierarchy);
      this.cGroupArg = hierarchy;
    }
  }

  @Override
  public PrivilegedOperation preparePrivilegedOperation(
      DockerCommand dockerCommand, String containerName, Map<String,
      String> env, Context nmContext) {
    PrivilegedOperation dockerOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.REMOVE_DOCKER_CONTAINER);
    if (this.cGroupArg != null) {
      dockerOp.appendArgs(cGroupArg);
    }
    dockerOp.appendArgs(containerName);
    return dockerOp;
  }
}
