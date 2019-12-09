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

import java.util.regex.Pattern;

/**
 * Docker Volume Command, run "docker volume --help" for more details.
 */
public class DockerVolumeCommand extends DockerCommand {
  public static final String VOLUME_COMMAND = "volume";
  public static final String VOLUME_CREATE_SUB_COMMAND = "create";
  public static final String VOLUME_LS_SUB_COMMAND = "ls";

  // Regex pattern for volume name
  public static final Pattern VOLUME_NAME_PATTERN = Pattern.compile(
      "[a-zA-Z0-9][a-zA-Z0-9_.-]*");

  private String volumeName;
  private String driverName;
  private String subCommand;

  public DockerVolumeCommand(String subCommand) {
    super(VOLUME_COMMAND);
    this.subCommand = subCommand;
    super.addCommandArguments("sub-command", subCommand);
  }

  public DockerVolumeCommand setVolumeName(String volumeName) {
    super.addCommandArguments("volume", volumeName);
    this.volumeName = volumeName;
    return this;
  }

  public DockerVolumeCommand setDriverName(String driverName) {
    super.addCommandArguments("driver", driverName);
    this.driverName = driverName;
    return this;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getDriverName() {
    return driverName;
  }

  public String getSubCommand() {
    return subCommand;
  }

  public DockerVolumeCommand setFormat(String format) {
    super.addCommandArguments("format", format);
    return this;
  }

}
