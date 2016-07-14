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

package org.apache.hadoop.yarn.server.timelineservice;

/**
 * Encapsulates timeline context information.
 */
public class TimelineContext {

  private String clusterId;
  private String userId;
  private String flowName;
  private Long flowRunId;
  private String appId;

  public TimelineContext() {
    this(null, null, null, 0L, null);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((appId == null) ? 0 : appId.hashCode());
    result = prime * result + ((clusterId == null) ? 0 : clusterId.hashCode());
    result = prime * result + ((flowName == null) ? 0 : flowName.hashCode());
    result = prime * result + ((flowRunId == null) ? 0 : flowRunId.hashCode());
    result = prime * result + ((userId == null) ? 0 : userId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TimelineContext other = (TimelineContext) obj;
    if (appId == null) {
      if (other.appId != null) {
        return false;
      }
    } else if (!appId.equals(other.appId)) {
      return false;
    }
    if (clusterId == null) {
      if (other.clusterId != null) {
        return false;
      }
    } else if (!clusterId.equals(other.clusterId)) {
      return false;
    }
    if (flowName == null) {
      if (other.flowName != null) {
        return false;
      }
    } else if (!flowName.equals(other.flowName)) {
      return false;
    }
    if (flowRunId == null) {
      if (other.flowRunId != null) {
        return false;
      }
    } else if (!flowRunId.equals(other.flowRunId)) {
      return false;
    }
    if (userId == null) {
      if (other.userId != null) {
        return false;
      }
    } else if (!userId.equals(other.userId)) {
      return false;
    }
    return true;
  }

  public TimelineContext(String clusterId, String userId, String flowName,
      Long flowRunId, String appId) {
    this.clusterId = clusterId;
    this.userId = userId;
    this.flowName = flowName;
    this.flowRunId = flowRunId;
    this.appId = appId;
  }

  public String getClusterId() {
    return clusterId;
  }

  public void setClusterId(String cluster) {
    this.clusterId = cluster;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String user) {
    this.userId = user;
  }

  public String getFlowName() {
    return flowName;
  }

  public void setFlowName(String flow) {
    this.flowName = flow;
  }

  public Long getFlowRunId() {
    return flowRunId;
  }

  public void setFlowRunId(long runId) {
    this.flowRunId = runId;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String app) {
    this.appId = app;
  }
}