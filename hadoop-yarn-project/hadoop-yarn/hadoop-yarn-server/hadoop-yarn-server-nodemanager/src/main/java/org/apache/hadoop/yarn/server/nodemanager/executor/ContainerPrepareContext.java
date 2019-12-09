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

package org.apache.hadoop.yarn.server.nodemanager.executor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates information required for preparing containers.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerPrepareContext {
  private final Container container;
  private final Map<Path, List<String>> localizedResources;
  private final String user;
  private final List<String> containerLocalDirs;
  private final List<String> commands;

  /**
   * Builder for ContainerPrepareContext.
   */
  public static final class Builder {
    private Container container;
    private Map<Path, List<String>> localizedResources;
    private String user;
    private List<String> containerLocalDirs;
    private List<String> commands;

    public Builder() {
    }

    public ContainerPrepareContext.Builder setContainer(Container container) {
      this.container = container;
      return this;
    }

    public ContainerPrepareContext.Builder setLocalizedResources(Map<Path,
        List<String>> localizedResources) {
      this.localizedResources = localizedResources;
      return this;
    }

    public ContainerPrepareContext.Builder setUser(String user) {
      this.user = user;
      return this;
    }
    public ContainerPrepareContext.Builder setContainerLocalDirs(
        List<String> containerLocalDirs) {
      this.containerLocalDirs = containerLocalDirs;
      return this;
    }

    public ContainerPrepareContext build() {
      return new ContainerPrepareContext(this);
    }

    public ContainerPrepareContext.Builder setCommands(List<String> commands) {
      this.commands = commands;
      return this;
    }
  }

  private ContainerPrepareContext(ContainerPrepareContext.Builder builder) {
    this.container = builder.container;
    this.localizedResources = builder.localizedResources;
    this.user = builder.user;
    this.containerLocalDirs = builder.containerLocalDirs;
    this.commands = builder.commands;
  }

  public Container getContainer() {
    return this.container;
  }

  public Map<Path, List<String>> getLocalizedResources() {
    if (this.localizedResources != null) {
      return Collections.unmodifiableMap(this.localizedResources);
    } else {
      return null;
    }
  }

  public String getUser() {
    return this.user;
  }

  public List<String> getContainerLocalDirs() {
    return Collections.unmodifiableList(this.containerLocalDirs);
  }

  public List<String> getCommands(){
    return this.commands;
  }
}