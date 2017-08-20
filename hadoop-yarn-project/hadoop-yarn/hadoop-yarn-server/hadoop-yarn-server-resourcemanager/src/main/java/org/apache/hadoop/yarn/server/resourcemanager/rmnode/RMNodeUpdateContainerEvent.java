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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import java.util.List;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;

/**
 * This class is used to create update container event
 * for the containers running on a node.
 *
 */
public class RMNodeUpdateContainerEvent extends RMNodeEvent {
  private List<Container> toBeUpdatedContainers;

  public RMNodeUpdateContainerEvent(NodeId nodeId,
      List<Container> toBeUpdatedContainers) {
    super(nodeId, RMNodeEventType.UPDATE_CONTAINER);

    this.toBeUpdatedContainers = toBeUpdatedContainers;
  }

  public List<Container> getToBeUpdatedContainers() {
    return toBeUpdatedContainers;
  }
}
