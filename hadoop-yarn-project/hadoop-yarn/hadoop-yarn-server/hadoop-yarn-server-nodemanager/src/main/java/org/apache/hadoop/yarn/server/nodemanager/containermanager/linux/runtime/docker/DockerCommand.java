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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@InterfaceAudience.Private
@InterfaceStability.Unstable

/** Represents a docker sub-command
 * e.g 'run', 'load', 'inspect' etc.,
 */

public abstract class DockerCommand {
  private final String command;
  private final Map<String, List<String>> commandArguments;

  protected DockerCommand(String command) {
    String dockerCommandKey = "docker-command";
    this.command = command;
    this.commandArguments = new TreeMap<>();
    commandArguments.put(dockerCommandKey, new ArrayList<>());
    commandArguments.get(dockerCommandKey).add(command);
  }

  /**
   * Returns the docker sub-command string being used
   * e.g 'run'.
   */
  public final String getCommandOption() {
    return this.command;
  }

  /**
   * Add command commandWithArguments - this method is only meant for use by
   * sub-classes.
   *
   * @param key   name of the key to be added
   * @param value value of the key
   */
  protected final void addCommandArguments(String key, String value) {
    List<String> list = commandArguments.get(key);
    if (list != null) {
      list.add(value);
      return;
    }
    list = new ArrayList<>();
    list.add(value);
    this.commandArguments.put(key, list);
  }

  public Map<String, List<String>> getDockerCommandWithArguments() {
    return Collections.unmodifiableMap(commandArguments);
  }

  @Override
  public String toString() {
    StringBuffer ret = new StringBuffer(this.command);
    for (Map.Entry<String, List<String>> entry : commandArguments.entrySet()) {
      ret.append(" ").append(entry.getKey());
      ret.append("=").append(StringUtils.join(",", entry.getValue()));
    }
    return ret.toString();
  }
}
