/**
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

package org.apache.hadoop.yarn.server.nodemanager;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class CMgrCompletedContainersEvent extends ContainerManagerEvent {

  private final List<ContainerId> containerToCleanup;
  private final Reason reason;

  public CMgrCompletedContainersEvent(List<ContainerId> containersToCleanup,
                                      Reason reason) {
    super(ContainerManagerEventType.FINISH_CONTAINERS);
    this.containerToCleanup = containersToCleanup;
    this.reason = reason;
  }

  public List<ContainerId> getContainersToCleanup() {
    return this.containerToCleanup;
  }

  public Reason getReason() {
    return reason;
  }

  public enum Reason {
    /**
     * Container is killed as NodeManager is shutting down
     */
    ON_SHUTDOWN,

    /**
     * Container is killed as the Nodemanager is re-syncing with the
     * ResourceManager
     */
    ON_NODEMANAGER_RESYNC,

    /**
     * Container is killed on request by the ResourceManager
     */
    BY_RESOURCEMANAGER
  }

}
