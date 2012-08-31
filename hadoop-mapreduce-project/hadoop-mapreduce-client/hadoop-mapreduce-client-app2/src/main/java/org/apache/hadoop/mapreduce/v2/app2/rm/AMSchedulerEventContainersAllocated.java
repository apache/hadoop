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

package org.apache.hadoop.mapreduce.v2.app2.rm;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class AMSchedulerEventContainersAllocated extends AMSchedulerEvent {

  private final List<ContainerId> containerIds;
  private final boolean headRoomChanged;

  // TODO XXX: Maybe distinguish between newly allocated containers and 
  // existing containers being re-used.
  // headRoomChanged is a strange API - making an assumption about how the
  // scheduler will use this info.
  public AMSchedulerEventContainersAllocated(List<ContainerId> containerIds,
      boolean headRoomChanged) {
    super(AMSchedulerEventType.S_CONTAINERS_ALLOCATED);
    this.containerIds = containerIds;
    this.headRoomChanged = headRoomChanged;
  }

  public List<ContainerId> getContainerIds() {
    return this.containerIds;
  }

  public boolean didHeadroomChange() {
    return headRoomChanged;
  }
}
