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

import org.apache.hadoop.yarn.api.records.Container;
import java.util.List;

/**
 * Event used by the NodeStatusUpdater to notify the ContainerManager of
 * container update commands it received from the RM.
 */
public class CMgrUpdateContainersEvent extends ContainerManagerEvent {

  private final List<Container> containersToUpdate;

  /**
   * Create event.
   * @param containersToUpdate Container to update.
   */
  public CMgrUpdateContainersEvent(List<Container> containersToUpdate) {
    super(ContainerManagerEventType.UPDATE_CONTAINERS);
    this.containersToUpdate = containersToUpdate;
  }

  /**
   * Get containers to update.
   * @return List of containers to update.
   */
  public List<Container> getContainersToUpdate() {
    return this.containersToUpdate;
  }
}
