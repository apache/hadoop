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
 * Encapsulates information required for starting/launching containers.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerStartContext {
  private final Container container;
  private final Map<Path, List<String>> localizedResources;
  private final Path nmPrivateContainerScriptPath;
  private final Path nmPrivateTokensPath;
  private final Path nmPrivateKeystorePath;
  private final Path nmPrivateTruststorePath;
  private final String user;
  private final String appId;
  private final Path containerWorkDir;
  private final Path csiVolumesRootDir;
  private final List<String> localDirs;
  private final List<String> logDirs;
  private final List<String> filecacheDirs;
  private final List<String> userLocalDirs;
  private final List<String> containerLocalDirs;
  private final List<String> containerLogDirs;
  private final List<String> userFilecacheDirs;
  private final List<String> applicationLocalDirs;

  public static final class Builder {
    private Container container;
    private Map<Path, List<String>> localizedResources;
    private Path nmPrivateContainerScriptPath;
    private Path nmPrivateTokensPath;
    private Path nmPrivateKeystorePath;
    private Path nmPrivateTruststorePath;
    private String user;
    private String appId;
    private Path containerWorkDir;
    private Path csiVolumesRoot;
    private List<String> localDirs;
    private List<String> logDirs;
    private List<String> filecacheDirs;
    private List<String> userLocalDirs;
    private List<String> containerLocalDirs;
    private List<String> containerLogDirs;
    private List<String> userFilecacheDirs;
    private List<String> applicationLocalDirs;

    public Builder() {
    }

    public Builder setContainer(Container container) {
      this.container = container;
      return this;
    }

    public Builder setLocalizedResources(Map<Path,
        List<String>> localizedResources) {
      this.localizedResources = localizedResources;
      return this;
    }

    public Builder setNmPrivateContainerScriptPath(
        Path nmPrivateContainerScriptPath) {
      this.nmPrivateContainerScriptPath = nmPrivateContainerScriptPath;
      return this;
    }

    public Builder setNmPrivateTokensPath(Path nmPrivateTokensPath) {
      this.nmPrivateTokensPath = nmPrivateTokensPath;
      return this;
    }

    public Builder setNmPrivateKeystorePath(Path nmPrivateKeystorePath) {
      this.nmPrivateKeystorePath = nmPrivateKeystorePath;
      return this;
    }

    public Builder setNmPrivateTruststorePath(Path nmPrivateTruststorePath) {
      this.nmPrivateTruststorePath = nmPrivateTruststorePath;
      return this;
    }

    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    public Builder setAppId(String appId) {
      this.appId = appId;
      return this;
    }

    public Builder setContainerCsiVolumesRootDir(Path csiVolumesRootDir) {
      this.csiVolumesRoot = csiVolumesRootDir;
      return this;
    }

    public Builder setContainerWorkDir(Path containerWorkDir) {
      this.containerWorkDir = containerWorkDir;
      return this;
    }

    public Builder setLocalDirs(List<String> localDirs) {
      this.localDirs = localDirs;
      return this;
    }

    public Builder setLogDirs(List<String> logDirs) {
      this.logDirs = logDirs;
      return this;
    }

    public Builder setFilecacheDirs(List<String> filecacheDirs) {
      this.filecacheDirs = filecacheDirs;
      return this;
    }

    public Builder setUserLocalDirs(List<String> userLocalDirs) {
      this.userLocalDirs = userLocalDirs;
      return this;
    }

    public Builder setContainerLocalDirs(List<String> containerLocalDirs) {
      this.containerLocalDirs = containerLocalDirs;
      return this;
    }

    public Builder setContainerLogDirs(List<String> containerLogDirs) {
      this.containerLogDirs = containerLogDirs;
      return this;
    }

    @SuppressWarnings("checkstyle:hiddenfield")
    public Builder setUserFilecacheDirs(List<String> userFilecacheDirs) {
      this.userFilecacheDirs = userFilecacheDirs;
      return this;
    }

    @SuppressWarnings("checkstyle:hiddenfield")
    public Builder setApplicationLocalDirs(List<String> applicationLocalDirs) {
      this.applicationLocalDirs = applicationLocalDirs;
      return this;
    }

    public ContainerStartContext build() {
      return new ContainerStartContext(this);
    }
  }

  private ContainerStartContext(Builder builder) {
    this.container = builder.container;
    this.localizedResources = builder.localizedResources;
    this.nmPrivateContainerScriptPath = builder.nmPrivateContainerScriptPath;
    this.nmPrivateTokensPath = builder.nmPrivateTokensPath;
    this.nmPrivateKeystorePath = builder.nmPrivateKeystorePath;
    this.nmPrivateTruststorePath = builder.nmPrivateTruststorePath;
    this.user = builder.user;
    this.appId = builder.appId;
    this.containerWorkDir = builder.containerWorkDir;
    this.localDirs = builder.localDirs;
    this.logDirs = builder.logDirs;
    this.filecacheDirs = builder.filecacheDirs;
    this.userLocalDirs = builder.userLocalDirs;
    this.containerLocalDirs = builder.containerLocalDirs;
    this.containerLogDirs = builder.containerLogDirs;
    this.userFilecacheDirs = builder.userFilecacheDirs;
    this.applicationLocalDirs = builder.applicationLocalDirs;
    this.csiVolumesRootDir = builder.csiVolumesRoot;
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

  public Path getNmPrivateContainerScriptPath() {
    return this.nmPrivateContainerScriptPath;
  }

  public Path getNmPrivateTokensPath() {
    return this.nmPrivateTokensPath;
  }

  public Path getNmPrivateKeystorePath() {
    return this.nmPrivateKeystorePath;
  }

  public Path getNmPrivateTruststorePath() {
    return this.nmPrivateTruststorePath;
  }

  public String getUser() {
    return this.user;
  }

  public String getAppId() {
    return this.appId;
  }

  public Path getContainerWorkDir() {
    return this.containerWorkDir;
  }

  public List<String> getLocalDirs() {
    return Collections.unmodifiableList(this.localDirs);
  }

  public List<String> getLogDirs() {
    return Collections.unmodifiableList(this.logDirs);
  }

  public List<String> getFilecacheDirs() {
    return Collections.unmodifiableList(this.filecacheDirs);
  }

  public List<String> getUserLocalDirs() {
    return Collections.unmodifiableList(this.userLocalDirs);
  }

  public List<String> getContainerLocalDirs() {
    return Collections.unmodifiableList(this.containerLocalDirs);
  }

  public List<String> getContainerLogDirs() {
    return Collections.unmodifiableList(this
        .containerLogDirs);
  }

  public List<String> getUserFilecacheDirs() {
    return Collections.unmodifiableList(this.userFilecacheDirs);
  }

  public List<String> getApplicationLocalDirs() {
    return Collections.unmodifiableList(this.applicationLocalDirs);
  }

  public Path getCsiVolumesRootDir() {
    return this.csiVolumesRootDir;
  }
}