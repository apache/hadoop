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

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.Signal;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

/**
 * Encapsulates information required for container signaling.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerSignalContext {
  private final Container container;
  private final String user;
  private final String pid;
  private final Signal signal;

  public static final class Builder {
    private Container container;
    private String user;
    private String pid;
    private Signal signal;

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

    public Builder setPid(String pid) {
      this.pid = pid;
      return this;
    }

    public Builder setSignal(Signal signal) {
      this.signal = signal;
      return this;
    }

    public ContainerSignalContext build() {
      return new ContainerSignalContext(this);
    }
  }

  private ContainerSignalContext(Builder builder) {
    this.container = builder.container;
    this.user = builder.user;
    this.pid = builder.pid;
    this.signal = builder.signal;
  }

  public Container getContainer() {
    return this.container;
  }

  public String getUser() {
    return this.user;
  }

  public String getPid() {
    return this.pid;
  }

  public Signal getSignal() {
    return this.signal;
  }

  /**
   * Retrun true if we are trying to signal the same process.
   * @param obj compare to this object
   * @return whether we try to signal the same process id
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ContainerSignalContext) {
      ContainerSignalContext other = (ContainerSignalContext)obj;
      boolean ret =
          (other.getPid() == null && getPid() == null) ||
              (other.getPid() != null && getPid() != null &&
                  other.getPid().equals(getPid()));
      ret = ret &&
          (other.getSignal() == null && getSignal() == null) ||
          (other.getSignal() != null && getSignal() != null &&
              other.getSignal().equals(getSignal()));
      ret = ret &&
          (other.getContainer() == null && getContainer() == null) ||
          (other.getContainer() != null && getContainer() != null &&
              other.getContainer().equals(getContainer()));
      ret = ret &&
          (other.getUser() == null && getUser() == null) ||
          (other.getUser() != null && getUser() != null &&
              other.getUser().equals(getUser()));
      return ret;
    }
    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().
        append(getPid()).
        append(getSignal()).
        append(getContainer()).
        append(getUser()).
        toHashCode();
  }
}