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

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DockerRunCommand extends DockerCommand {
  private static final String RUN_COMMAND = "run";

  /** The following are mandatory: */
  public DockerRunCommand(String containerId, String user, String image) {
    super(RUN_COMMAND);
    super.addCommandArguments("name", containerId);
    super.addCommandArguments("user", user);
    super.addCommandArguments("image", image);
  }

  public DockerRunCommand removeContainerOnExit() {
    super.addCommandArguments("rm", "true");
    return this;
  }

  public DockerRunCommand detachOnRun() {
    super.addCommandArguments("detach", "true");
    return this;
  }

  public DockerRunCommand setContainerWorkDir(String workdir) {
    super.addCommandArguments("workdir", workdir);
    return this;
  }

  public DockerRunCommand setNetworkType(String type) {
    super.addCommandArguments("net", type);
    return this;
  }

  public DockerRunCommand addMountLocation(String sourcePath, String
      destinationPath, boolean createSource) {
    boolean sourceExists = new File(sourcePath).exists();
    if (!sourceExists && !createSource) {
      return this;
    }
    super.addCommandArguments("rw-mounts", sourcePath + ":" + destinationPath);
    return this;
  }

  public DockerRunCommand addReadOnlyMountLocation(String sourcePath, String
      destinationPath, boolean createSource) {
    boolean sourceExists = new File(sourcePath).exists();
    if (!sourceExists && !createSource) {
      return this;
    }
    super.addCommandArguments("ro-mounts", sourcePath + ":" + destinationPath);
    return this;
  }

  public DockerRunCommand setCGroupParent(String parentPath) {
    super.addCommandArguments("cgroup-parent", parentPath);
    return this;
  }

  /* Run a privileged container. Use with extreme care */
  public DockerRunCommand setPrivileged() {
    super.addCommandArguments("privileged", "true");
    return this;
  }

  public DockerRunCommand setCapabilities(Set<String> capabilties) {
    //first, drop all capabilities
    super.addCommandArguments("cap-drop", "ALL");

    //now, add the capabilities supplied
    for (String capability : capabilties) {
      super.addCommandArguments("cap-add", capability);
    }

    return this;
  }

  public DockerRunCommand setHostname(String hostname) {
    super.addCommandArguments("hostname", hostname);
    return this;
  }

  public DockerRunCommand addDevice(String sourceDevice, String
      destinationDevice) {
    super.addCommandArguments("devices", sourceDevice + ":" +
        destinationDevice);
    return this;
  }

  public DockerRunCommand enableDetach() {
    super.addCommandArguments("detach", "true");
    return this;
  }

  public DockerRunCommand disableDetach() {
    super.addCommandArguments("detach", "false");
    return this;
  }

  public DockerRunCommand groupAdd(String[] groups) {
    super.addCommandArguments("group-add", String.join(",", groups));
    return this;
  }

  public DockerRunCommand setOverrideCommandWithArgs(
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
