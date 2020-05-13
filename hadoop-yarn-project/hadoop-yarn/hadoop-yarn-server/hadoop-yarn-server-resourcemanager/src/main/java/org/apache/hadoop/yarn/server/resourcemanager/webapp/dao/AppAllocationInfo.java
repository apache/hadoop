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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.AppAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * DAO object to display application allocation detailed information.
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class AppAllocationInfo {
  private String nodeId;
  private Long timestamp;
  private String dateTime;
  private String queueName;
  private Integer appPriority;
  private String allocationState;
  private String diagnostic;
  private List<AppRequestAllocationInfo> children;

  AppAllocationInfo() {
  }

  AppAllocationInfo(AppAllocation allocation,
      RMWSConsts.ActivitiesGroupBy groupBy) {
    this.children = new ArrayList<>();
    this.nodeId = allocation.getNodeId();
    this.queueName = allocation.getQueueName();
    this.appPriority = allocation.getPriority() == null ?
        null : allocation.getPriority().getPriority();
    this.timestamp = allocation.getTime();
    this.dateTime = new Date(allocation.getTime()).toString();
    this.allocationState = allocation.getActivityState().name();
    this.diagnostic = allocation.getDiagnostic();
    Map<String, List<ActivityNode>> requestToActivityNodes =
        allocation.getAllocationAttempts().stream().collect(Collectors
            .groupingBy((e) -> e.getRequestPriority() + "_" + e
                .getAllocationRequestId(), Collectors.toList()));
    for (List<ActivityNode> requestActivityNodes : requestToActivityNodes
        .values()) {
      AppRequestAllocationInfo requestAllocationInfo =
          new AppRequestAllocationInfo(requestActivityNodes, groupBy);
      this.children.add(requestAllocationInfo);
    }
  }

  public String getNodeId() {
    return nodeId;
  }

  public String getQueueName() {
    return queueName;
  }

  public Integer getAppPriority() {
    return appPriority;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public String getDateTime() {
    return dateTime;
  }

  public String getAllocationState() {
    return allocationState;
  }

  public List<AppRequestAllocationInfo> getChildren() {
    return children;
  }

  public String getDiagnostic() {
    return diagnostic;
  }
}
