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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityNode;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * DAO object to display request allocation detailed information.
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class AppRequestAllocationInfo {
  private Integer requestPriority;
  private Long allocationRequestId;
  private String allocationState;
  private String diagnostic;
  private List<ActivityNodeInfo> children;

  AppRequestAllocationInfo() {
  }

  AppRequestAllocationInfo(List<ActivityNode> activityNodes,
      RMWSConsts.ActivitiesGroupBy groupBy) {
    ActivityNode lastActivityNode = Iterables.getLast(activityNodes);
    this.requestPriority = lastActivityNode.getRequestPriority();
    this.allocationRequestId = lastActivityNode.getAllocationRequestId();
    this.allocationState = lastActivityNode.getState().name();
    if (lastActivityNode.isRequestType()
        && lastActivityNode.getDiagnostic() != null) {
      this.diagnostic = lastActivityNode.getDiagnostic();
    }
    this.children = ActivitiesUtils
        .getRequestActivityNodeInfos(activityNodes, groupBy);
  }

  public Integer getRequestPriority() {
    return requestPriority;
  }

  public Long getAllocationRequestId() {
    return allocationRequestId;
  }

  public String getAllocationState() {
    return allocationState;
  }

  public List<ActivityNodeInfo> getChildren() {
    return children;
  }

  public String getDiagnostic() {
    return diagnostic;
  }
}
