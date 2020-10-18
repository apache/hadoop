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

import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;
import java.util.stream.Collectors;

/*
 * DAO object to display node information in allocation tree.
 * It corresponds to "ActivityNode" class.
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ActivityNodeInfo {
  private String name;  // The name for activity node
  private Integer appPriority;
  private Integer requestPriority;
  private Long allocationRequestId;
  private String allocationState;
  private String diagnostic;
  private String nodeId;

  // Used for groups of activities
  private Integer count;
  private List<String> nodeIds;

  protected List<ActivityNodeInfo> children;

  ActivityNodeInfo() {
  }

  public ActivityNodeInfo(String name, ActivityState activityState,
      String diagnostic, NodeId nId) {
    this.name = name;
    this.allocationState = activityState.name();
    this.diagnostic = diagnostic;
    setNodeId(nId);
  }

  public ActivityNodeInfo(ActivityState groupActivityState,
      String groupDiagnostic, List<String> groupNodeIds) {
    this.allocationState = groupActivityState.name();
    this.diagnostic = groupDiagnostic;
    this.count = groupNodeIds.size();
    this.nodeIds = groupNodeIds;
  }

  ActivityNodeInfo(ActivityNode node,
      RMWSConsts.ActivitiesGroupBy groupBy) {
    this.name = node.getName();
    setPriority(node);
    setNodeId(node.getNodeId());
    this.allocationState = node.getState().name();
    this.diagnostic = node.getDiagnostic();
    this.requestPriority = node.getRequestPriority();
    this.allocationRequestId = node.getAllocationRequestId();
    // only consider grouping for request type
    if (node.isRequestType()) {
      this.children = ActivitiesUtils
          .getRequestActivityNodeInfos(node.getChildren(), groupBy);
    } else {
      this.children = node.getChildren().stream()
          .map(e -> new ActivityNodeInfo(e, groupBy))
          .collect(Collectors.toList());
    }
  }

  public void setNodeId(NodeId nId) {
    if (nId != null && !Strings.isNullOrEmpty(nId.getHost())) {
      this.nodeId = nId.toString();
    }
  }

  private void setPriority(ActivityNode node) {
    if (node.isAppType()) {
      this.appPriority = node.getAppPriority();
    } else {
      this.requestPriority = node.getRequestPriority();
    }
  }

  public String getNodeId() {
    return nodeId;
  }

  public void setNodeIds(List<String> nodeIds) {
    this.nodeIds = nodeIds;
  }

  public Long getAllocationRequestId() {
    return allocationRequestId;
  }

  public Integer getCount() {
    return count;
  }

  public List<String> getNodeIds() {
    return nodeIds;
  }

  public List<ActivityNodeInfo> getChildren() {
    return children;
  }

  public String getAllocationState() {
    return allocationState;
  }

  public String getName() {
    return name;
  }

  public Integer getAppPriority() {
    return appPriority;
  }

  public Integer getRequestPriority() {
    return requestPriority;
  }

  public String getDiagnostic() {
    return diagnostic;
  }
}
