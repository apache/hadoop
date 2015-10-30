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
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;

import java.net.InetSocketAddress;

/**
 * Encapsulates information required for starting a localizer.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class LocalizerStartContext {
  private final Path nmPrivateContainerTokens;
  private final InetSocketAddress nmAddr;
  private final String user;
  private final String appId;
  private final String locId;
  private final LocalDirsHandlerService dirsHandler;

  public static final class Builder {
    private Path nmPrivateContainerTokens;
    private InetSocketAddress nmAddr;
    private String user;
    private String appId;
    private String locId;
    private LocalDirsHandlerService dirsHandler;

    public Builder() {
    }

    public Builder setNmPrivateContainerTokens(Path nmPrivateContainerTokens) {
      this.nmPrivateContainerTokens = nmPrivateContainerTokens;
      return this;
    }

    public Builder setNmAddr(InetSocketAddress nmAddr) {
      this.nmAddr = nmAddr;
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

    public Builder setLocId(String locId) {
      this.locId = locId;
      return this;
    }

    public Builder setDirsHandler(LocalDirsHandlerService dirsHandler) {
      this.dirsHandler = dirsHandler;
      return this;
    }

    public LocalizerStartContext build() {
      return new LocalizerStartContext(this);
    }
  }

  private LocalizerStartContext(Builder builder) {
    this.nmPrivateContainerTokens = builder.nmPrivateContainerTokens;
    this.nmAddr = builder.nmAddr;
    this.user = builder.user;
    this.appId = builder.appId;
    this.locId = builder.locId;
    this.dirsHandler = builder.dirsHandler;
  }

  public Path getNmPrivateContainerTokens() {
    return this.nmPrivateContainerTokens;
  }

  public InetSocketAddress getNmAddr() {
    return this.nmAddr;
  }

  public String getUser() {
    return this.user;
  }

  public String getAppId() {
    return this.appId;
  }

  public String getLocId() {
    return this.locId;
  }

  public LocalDirsHandlerService getDirsHandler() {
    return this.dirsHandler;
  }
}