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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

/**
 * Encapsulates information required for container reacquisition.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerReacquisitionContext {
  private final Container container;
  private final String user;
  private final ContainerId containerId;

  public static final class Builder {
    private Container container;
    private String user;
    private ContainerId containerId;

    public Builder() {
    }

    public Builder setContainer(Container container) {
      this.container = container;
      return this;
    }

    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    public Builder setContainerId(ContainerId containerId) {
      this.containerId = containerId;
      return this;
    }

    public ContainerReacquisitionContext build() {
      return new ContainerReacquisitionContext(this);
    }
  }

  private ContainerReacquisitionContext(Builder builder) {
    this.container = builder.container;
    this.user = builder.user;
    this.containerId = builder.containerId;
  }

  public Container getContainer() {
    return this.container;
  }

  public String getUser() {
    return this.user;
  }

  public ContainerId getContainerId() {
    return this.containerId;
  }
}