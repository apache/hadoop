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
package org.apache.slider.providers.agent.application.metadata;

import org.apache.slider.core.exceptions.SliderException;

/**
 *
 */
public class CommandOrder implements Validate {
  String command;
  String requires;

  public CommandOrder() {
  }

  public String getCommand() {
    return command;
  }

  public void setCommand(String command) {
    this.command = command;
  }

  public String getRequires() {
    return requires;
  }

  public void setRequires(String requires) {
    this.requires = requires;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("{");
    sb.append(",\n\"command\": ").append(command);
    sb.append(",\n\"requires\": ").append(requires);
    sb.append('}');
    return sb.toString();
  }

  public void validate(String version) throws SliderException {
    Metainfo.checkNonNull(getCommand(), "command", "package");
    Metainfo.checkNonNull(getRequires(), "requires", "package");
  }
}
