/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.scm.node.states;

import org.apache.hadoop.hdds.scm.container.ContainerID;

import java.util.Collections;
import java.util.Set;

import com.google.common.base.Preconditions;

/**
 * A Container Report gets processsed by the Node2Container and returns
 * Report Result class.
 */
public class ReportResult {
  private Node2ContainerMap.ReportStatus status;
  private Set<ContainerID> missingContainers;
  private Set<ContainerID> newContainers;

  ReportResult(Node2ContainerMap.ReportStatus status,
      Set<ContainerID> missingContainers,
      Set<ContainerID> newContainers) {
    this.status = status;
    Preconditions.checkNotNull(missingContainers);
    Preconditions.checkNotNull(newContainers);
    this.missingContainers = missingContainers;
    this.newContainers = newContainers;
  }

  public Node2ContainerMap.ReportStatus getStatus() {
    return status;
  }

  public Set<ContainerID> getMissingContainers() {
    return missingContainers;
  }

  public Set<ContainerID> getNewContainers() {
    return newContainers;
  }

  static class ReportResultBuilder {
    private Node2ContainerMap.ReportStatus status;
    private Set<ContainerID> missingContainers;
    private Set<ContainerID> newContainers;

    static ReportResultBuilder newBuilder() {
      return new ReportResultBuilder();
    }

    public ReportResultBuilder setStatus(
        Node2ContainerMap.ReportStatus newstatus) {
      this.status = newstatus;
      return this;
    }

    public ReportResultBuilder setMissingContainers(
        Set<ContainerID> missingContainersLit) {
      this.missingContainers = missingContainersLit;
      return this;
    }

    public ReportResultBuilder setNewContainers(
        Set<ContainerID> newContainersList) {
      this.newContainers = newContainersList;
      return this;
    }

    ReportResult build() {

      Set<ContainerID> nullSafeMissingContainers = this.missingContainers;
      Set<ContainerID> nullSafeNewContainers = this.newContainers;
      if (nullSafeNewContainers == null) {
        nullSafeNewContainers = Collections.emptySet();
      }
      if (nullSafeMissingContainers == null) {
        nullSafeMissingContainers = Collections.emptySet();
      }
      return new ReportResult(status, nullSafeMissingContainers, nullSafeNewContainers);
    }
  }
}
