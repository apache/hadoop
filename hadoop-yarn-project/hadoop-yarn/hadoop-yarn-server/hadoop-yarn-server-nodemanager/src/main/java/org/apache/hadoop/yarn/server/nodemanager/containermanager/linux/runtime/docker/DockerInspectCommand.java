/*
 * *
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
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;

import java.util.Map;

/**
 * Encapsulates the docker inspect command and its command
 * line arguments.
 */
public class DockerInspectCommand extends DockerCommand {
  private static final String INSPECT_COMMAND = "inspect";
  private String commandArguments;

  public DockerInspectCommand(String containerName) {
    super(INSPECT_COMMAND);
    super.addCommandArguments("name", containerName);
  }

  public DockerInspectCommand getContainerStatus() {
    super.addCommandArguments("format", STATUS_TEMPLATE);
    this.commandArguments = String.format("--format=%s", STATUS_TEMPLATE);
    return this;
  }

  public DockerInspectCommand getIpAndHost() {
    // Be sure to not use space in the argument, otherwise the
    // extract_values_delim method in container-executor binary
    // cannot parse the arguments correctly.
    super.addCommandArguments("format", "{{range(.NetworkSettings.Networks)}}"
        + "{{.IPAddress}},{{end}}{{.Config.Hostname}}");
    this.commandArguments = "--format={{range(.NetworkSettings.Networks)}}"
        + "{{.IPAddress}},{{end}}{{.Config.Hostname}}";
    return this;
  }

  public DockerInspectCommand get(String[] templates, char delimiter) {
    String format = StringUtils.join(delimiter, templates);
    super.addCommandArguments("format", format);
    this.commandArguments = String.format("--format=%s", format);
    return this;
  }

  @Override
  public PrivilegedOperation preparePrivilegedOperation(
      DockerCommand dockerCommand, String containerName, Map<String,
      String> env, Context nmContext) {
    PrivilegedOperation dockerOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.INSPECT_DOCKER_CONTAINER);
    dockerOp.appendArgs(commandArguments, containerName);
    return dockerOp;
  }

  public static final String STATUS_TEMPLATE = "{{.State.Status}}";
  public static final String STOPSIGNAL_TEMPLATE = "{{.Config.StopSignal}}";
}
