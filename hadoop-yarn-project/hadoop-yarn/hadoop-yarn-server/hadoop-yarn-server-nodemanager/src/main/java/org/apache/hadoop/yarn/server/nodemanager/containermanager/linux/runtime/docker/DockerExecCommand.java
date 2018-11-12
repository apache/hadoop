/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates the docker exec command and its command
 * line arguments.
 */
public class DockerExecCommand extends DockerCommand {
  private static final String EXEC_COMMAND = "exec";
  private final Map<String, String> userEnv;

  public DockerExecCommand(String containerId) {
    super(EXEC_COMMAND);
    super.addCommandArguments("name", containerId);
    this.userEnv = new LinkedHashMap<String, String>();
  }

  public DockerExecCommand setInteractive() {
    super.addCommandArguments("interactive", "true");
    return this;
  }

  public DockerExecCommand setTTY() {
    super.addCommandArguments("tty", "true");
    return this;
  }

  public DockerExecCommand setOverrideCommandWithArgs(
      List<String> overrideCommandWithArgs) {
    for(String override: overrideCommandWithArgs) {
      super.addCommandArguments("launch-command", override);
    }
    return this;
  }

  @Override
  public Map<String, List<String>> getDockerCommandWithArguments() {
    return super.getDockerCommandWithArguments();
  }

}
