/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.providers.agent.application.metadata;

import org.apache.slider.core.exceptions.SliderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the metadata associated with the application.
 */
public class ComponentCommand implements Validate {
  protected static final Logger
      log = LoggerFactory.getLogger(ComponentCommand.class);


  private String exec;
  private String name = "START";
  private String type = "SHELL";

  /**
   * Creator.
   */
  public ComponentCommand() {
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setExec(String exec) {
    this.exec = exec;
  }

  public String getExec() {
    return exec;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getType() {
    return type;
  }

  public void validate(String version) throws SliderException {
    Metainfo.checkNonNull(getName(), "name", "componentCommand");

    Metainfo.checkNonNull(getType(), "version", "application");
  }

  public static ComponentCommand getDefaultComponentCommand() {
    ComponentCommand cc = new ComponentCommand();
    cc.setExec("DEFAULT");
    return cc;
  }

  public static ComponentCommand getDefaultComponentCommand(String commandName) {
    ComponentCommand cc = new ComponentCommand();
    cc.setExec("DEFAULT");
    cc.setName(commandName);
    return cc;
  }
}
