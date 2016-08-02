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

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 *  Component defined in master package metainfo.json
 */
public abstract class AbstractComponent implements Validate {
  public static final String TYPE_STANDARD = "STANDARD";
  public static final String TYPE_DOCKER = "DOCKER";
  public static final String TYPE_PYTHON = "PYTHON";
  public static final String CATEGORY_MASTER = "MASTER";
  public static final String CATEGORY_SLAVE = "SLAVE";
  public static final String CATEGORY_CLIENT = "CLIENT";
  public static final String MASTER_PACKAGE_NAME = "MASTER";

  protected String name;
  protected CommandScript commandScript;
  protected List<ComponentCommand> commands = new ArrayList<>();

  public AbstractComponent() {
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public CommandScript getCommandScript() {
    return commandScript;
  }

  public void addCommandScript(CommandScript commandScript) {
    this.commandScript = commandScript;
  }

  @JsonProperty("commands")
  public List<ComponentCommand> getCommands() {
    return commands;
  }

  public void setCommands(List<ComponentCommand> commands) {
    this.commands = commands;
  }

  public void addCommand(ComponentCommand command) {
    commands.add(command);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("{");
    sb.append("\n\"name\": ").append(name);
    sb.append(",\n\"commandScript\" :").append(commandScript);
    sb.append('}');
    return sb.toString();
  }
}
