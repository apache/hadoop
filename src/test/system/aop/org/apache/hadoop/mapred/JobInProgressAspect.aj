package org.apache.hadoop.mapred;

import java.io.IOException;
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
    if (tasksInited.get()) {
      return new JobInfoImpl(
          this.getJobID(), this.isSetupLaunched(), this.isSetupFinished(), this
              .isCleanupLaunched(), this.runningMaps(), this.runningReduces(),
          this.pendingMaps(), this.pendingReduces(), this.finishedMaps(), this
              .finishedReduces(), this.getStatus(), historyLoc, this
              .getBlackListedTrackers(), false, this.numMapTasks,
          this.numReduceTasks, this.isHistoryFileCopied());
    } else {
      return new JobInfoImpl(
          this.getJobID(), false, false, false, 0, 0, this.pendingMaps(), this
              .pendingReduces(), this.finishedMaps(), this.finishedReduces(),
          this.getStatus(), historyLoc, this.getBlackListedTrackers(), this
              .isComplete(), this.numMapTasks, this.numReduceTasks, 
              this.isHistoryFileCopied());
    }
  }
  
  private String JobInProgress.getHistoryPath() {
    String historyLoc = "";
    if(this.isComplete()) {
      historyLoc = this.getHistoryFile();
    } else {
      String historyFileName = null;
      try {
        historyFileName  = JobHistory.JobInfo.getJobHistoryFileName(conf, 
            jobId);
      } catch(IOException e) {
      }
      if(historyFileName != null) {
        historyLoc = JobHistory.JobInfo.getJobHistoryLogLocation(
            historyFileName).toString();
      }
    }
    return historyLoc;
  }

}
