/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.slider.server.appmaster.actions;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.slider.server.appmaster.SliderAppMaster;
import org.apache.slider.server.appmaster.state.AppState;

public class ActionUpgradeContainers extends AsyncAction {
  private int exitCode;
  private FinalApplicationStatus finalApplicationStatus;
  private String message;
  private Set<String> containers = new HashSet<>();
  private Set<String> components = new HashSet<>();

  public ActionUpgradeContainers(String name,
      long delay,
      TimeUnit timeUnit,
      int exitCode,
      FinalApplicationStatus finalApplicationStatus,
      List<String> containers,
      List<String> components,
      String message) {
    super(name, delay, timeUnit);
    this.exitCode = exitCode;
    this.finalApplicationStatus = finalApplicationStatus;
    this.containers.addAll(containers);
    this.components.addAll(components);
    this.message = message;
  }

  @Override
  public void execute(SliderAppMaster appMaster, QueueAccess queueService,
      AppState appState) throws Exception {
    if (CollectionUtils.isNotEmpty(this.containers)
        || CollectionUtils.isNotEmpty(this.components)) {
      SliderAppMaster.getLog().info("SliderAppMaster.upgradeContainers: {}",
          message);
      appMaster.onUpgradeContainers(this);
    }
  }

  public int getExitCode() {
    return exitCode;
  }

  public void setExitCode(int exitCode) {
    this.exitCode = exitCode;
  }

  public FinalApplicationStatus getFinalApplicationStatus() {
    return finalApplicationStatus;
  }

  public void setFinalApplicationStatus(
      FinalApplicationStatus finalApplicationStatus) {
    this.finalApplicationStatus = finalApplicationStatus;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public Set<String> getContainers() {
    return containers;
  }

  public void setContainers(Set<String> containers) {
    this.containers = containers;
  }

  public Set<String> getComponents() {
    return components;
  }

  public void setComponents(Set<String> components) {
    this.components = components;
  }

}
