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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl.ProcessTreeInfo;

public interface ContainersMonitor extends Service,
    EventHandler<ContainersMonitorEvent>, ResourceView {
  public ResourceUtilization getContainersUtilization();

  ResourceUtilization getContainersAllocation();

  boolean hasResourcesAvailable(ProcessTreeInfo pti);

  void increaseContainersAllocation(ProcessTreeInfo pti);

  void decreaseContainersAllocation(ProcessTreeInfo pti);

  void increaseResourceUtilization(ResourceUtilization resourceUtil,
      ProcessTreeInfo pti);

  void decreaseResourceUtilization(ResourceUtilization resourceUtil,
      ProcessTreeInfo pti);

  void subtractNodeResourcesFromResourceUtilization(
      ResourceUtilization resourceUtil);
}
