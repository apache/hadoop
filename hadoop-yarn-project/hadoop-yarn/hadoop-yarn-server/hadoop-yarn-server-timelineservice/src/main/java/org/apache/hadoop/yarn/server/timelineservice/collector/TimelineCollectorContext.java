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

package org.apache.hadoop.yarn.server.timelineservice.collector;

public class TimelineCollectorContext {

  private String clusterId;
  private String userId;
  private String flowName;
  private String flowVersion;
  private long flowRunId;
  private String appId;

  public TimelineCollectorContext() {
    this(null, null, null, null, 0L, null);
  }

  public TimelineCollectorContext(String clusterId, String userId,
      String flowName, String flowVersion, long flowRunId, String appId) {
    this.clusterId = clusterId;
    this.userId = userId;
    this.flowName = flowName;
    this.flowVersion = flowVersion;
    this.flowRunId = flowRunId;
    this.appId = appId;
  }

  public String getClusterId() {
    return clusterId;
  }

  public void setClusterId(String clusterId) {
    this.clusterId = clusterId;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public String getFlowName() {
    return flowName;
  }

  public void setFlowName(String flowName) {
    this.flowName = flowName;
  }

  public String getFlowVersion() {
    return flowVersion;
  }

  public void setFlowVersion(String flowVersion) {
    this.flowVersion = flowVersion;
  }

  public long getFlowRunId() {
    return flowRunId;
  }

  public void setFlowRunId(long flowRunId) {
    this.flowRunId = flowRunId;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }
}
