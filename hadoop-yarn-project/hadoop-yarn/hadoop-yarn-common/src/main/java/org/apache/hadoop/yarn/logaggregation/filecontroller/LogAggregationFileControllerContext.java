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
package org.apache.hadoop.yarn.logaggregation.filecontroller;

import java.util.Map;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;

/**
 * {@code LogAggregationFileControllerContext} is a record used in
 * the log aggregation process.
 */
@Private
@Unstable
public class LogAggregationFileControllerContext {
  private final boolean logAggregationInRolling;
  private final long rollingMonitorInterval;
  private final Path remoteNodeLogFileForApp;
  private final NodeId nodeId;
  private final UserGroupInformation userUgi;
  private final ApplicationId appId;
  private final Path remoteNodeTmpLogFileForApp;
  private final Map<ApplicationAccessType, String> appAcls;
  private int logAggregationTimes = 0;
  private int cleanOldLogsTimes = 0;

  private boolean uploadedLogsInThisCycle;
  private long logUploadedTimeStamp;

  public LogAggregationFileControllerContext(Path remoteNodeLogFileForApp,
      Path remoteNodeTmpLogFileForApp,
      boolean logAggregationInRolling,
      long rollingMonitorInterval,
      ApplicationId appId,
      Map<ApplicationAccessType, String> appAcls,
      NodeId nodeId, UserGroupInformation userUgi) {
    this.remoteNodeLogFileForApp = remoteNodeLogFileForApp;
    this.remoteNodeTmpLogFileForApp = remoteNodeTmpLogFileForApp;
    this.logAggregationInRolling = logAggregationInRolling;
    this.rollingMonitorInterval = rollingMonitorInterval;
    this.nodeId = nodeId;
    this.appId = appId;
    this.appAcls = appAcls;
    this.userUgi = userUgi;
  }

  public boolean isUploadedLogsInThisCycle() {
    return uploadedLogsInThisCycle;
  }

  public void setUploadedLogsInThisCycle(boolean uploadedLogsInThisCycle) {
    this.uploadedLogsInThisCycle = uploadedLogsInThisCycle;
  }

  public Path getRemoteNodeLogFileForApp() {
    return remoteNodeLogFileForApp;
  }

  public long getRollingMonitorInterval() {
    return rollingMonitorInterval;
  }

  public boolean isLogAggregationInRolling() {
    return logAggregationInRolling;
  }

  public long getLogUploadTimeStamp() {
    return logUploadedTimeStamp;
  }

  public void setLogUploadTimeStamp(long uploadTimeStamp) {
    this.logUploadedTimeStamp = uploadTimeStamp;
  }

  public NodeId getNodeId() {
    return nodeId;
  }

  public UserGroupInformation getUserUgi() {
    return userUgi;
  }

  public ApplicationId getAppId() {
    return appId;
  }

  public Path getRemoteNodeTmpLogFileForApp() {
    return remoteNodeTmpLogFileForApp;
  }

  public void increLogAggregationTimes() {
    this.logAggregationTimes++;
  }

  public void increcleanupOldLogTimes() {
    this.cleanOldLogsTimes++;
  }

  public int getLogAggregationTimes() {
    return logAggregationTimes;
  }

  public int getCleanOldLogsTimes() {
    return cleanOldLogsTimes;
  }

  public Map<ApplicationAccessType, String> getAppAcls() {
    return appAcls;
  }
}
