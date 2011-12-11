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

package org.apache.hadoop.mapred;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.jobhistory.JobHistory;
import org.apache.hadoop.mapreduce.test.system.JobInfo;

/**
 * Aspect to add a utility method in the JobInProgress for easing up the
 * construction of the JobInfo object.
 */
privileged aspect JobInProgressAspect {

  /**
   * Returns a read only view of the JobInProgress object which is used by the
   * client.
   * 
   * @return JobInfo of the current JobInProgress object
   */
  public JobInfo JobInProgress.getJobInfo() {
    String historyLoc = getHistoryPath();
    boolean isHistoryFileCopied =
        this.status.getHistoryFile() == null ? false : true;
    if (tasksInited.get()) {
      return new JobInfoImpl(
          this.getJobID(), this.isSetupLaunched(), this.isSetupFinished(), this
              .isCleanupLaunched(), this.runningMaps(), this.runningReduces(),
          this.pendingMaps(), this.pendingReduces(), this.finishedMaps(), this
              .finishedReduces(), this.getStatus(), historyLoc, this
              .getBlackListedTrackers(), false, this.numMapTasks,
          this.numReduceTasks, isHistoryFileCopied);
    } else {
      return new JobInfoImpl(
          this.getJobID(), false, false, false, 0, 0, this.pendingMaps(), this
              .pendingReduces(), this.finishedMaps(), this.finishedReduces(),
          this.getStatus(), historyLoc, this.getBlackListedTrackers(), this
              .isComplete(), this.numMapTasks, this.numReduceTasks, false);
    }
  }

  private String JobInProgress.getHistoryPath() {
    String historyLoc = "";
    if (this.isComplete()) {
      historyLoc = this.getStatus().getHistoryFile();
    } else {
      Path jobHistoryDirectory = this.jobHistory.getJobHistoryLocation();
      Path historypath =
          JobHistory.getJobHistoryFile(
              jobHistoryDirectory, this.getJobID(), this.profile.getUser());
      historyLoc = historypath.toString();
    }
    return historyLoc;
  }

}
