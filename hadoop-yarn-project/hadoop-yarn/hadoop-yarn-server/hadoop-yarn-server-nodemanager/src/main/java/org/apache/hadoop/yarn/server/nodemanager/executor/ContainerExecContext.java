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
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

/**
 * Encapsulates information required for starting/launching containers.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerExecContext {
  private final String user;
  private final String appId;
  private final Container container;
  private String command;
  private final LocalDirsHandlerService localDirsHandler;

  /**
   *  Builder for ContainerExecContext.
   */
  public static final class Builder {
    private String user;
    private String appId;
    private Container container;
    private String command;
    private LocalDirsHandlerService localDirsHandler;

    public Builder() {
    }

    public Builder setContainer(Container c) {
      this.container = c;
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

    public ContainerExecContext build() {
      return new ContainerExecContext(this);
    }

    public Builder setNMLocalPath(
        LocalDirsHandlerService ldhs) {
      this.localDirsHandler = ldhs;
      return this;
    }

    public Builder setShell(String command) {
      this.command = command;
      return this;
    }
  }

  private ContainerExecContext(Builder builder) {
    this.user = builder.user;
    this.appId = builder.appId;
    this.container = builder.container;
    this.command = builder.command;
    this.localDirsHandler = builder.localDirsHandler;
  }

  public String getUser() {
    return this.user;
  }

  public String getAppId() {
    return this.appId;
  }

  public Container getContainer() {
    return this.container;
  }

  public String getShell() {
    return this.command;
  }

  public LocalDirsHandlerService getLocalDirsHandlerService() {
    return this.localDirsHandler;
  }
}
