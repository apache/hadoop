package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.mapreduce.TaskType;

public class JobSchedulableWithNodeGroup extends JobSchedulable {

  public JobSchedulableWithNodeGroup(FairScheduler scheduler,
      JobInProgress job, TaskType taskType) {
    super(scheduler, job, taskType);
  }

  public JobSchedulableWithNodeGroup() {
  }

  @Override
  public Task assignTask(TaskTrackerStatus tts, long currentTime,
      Collection<JobInProgress> visited) throws IOException {
    if (isRunnable()) {
      visited.add(job);
      TaskTrackerManager ttm = scheduler.taskTrackerManager;
      ClusterStatus clusterStatus = ttm.getClusterStatus();
      int numTaskTrackers = clusterStatus.getTaskTrackers();

      // check with the load manager whether it is safe to 
      // launch this task on this taskTracker.
      LoadManager loadMgr = scheduler.getLoadManager();
      if (!loadMgr.canLaunchTask(tts, job, taskType)) {
        return null;
      }
      if (taskType == TaskType.MAP) {
        LocalityLevel localityLevel = scheduler.getAllowedLocalityLevel(
            job, currentTime);
        scheduler.getEventLog().log(
            "ALLOWED_LOC_LEVEL", job.getJobID(), localityLevel);
        switch (localityLevel) {
          case NODE:
            return job.obtainNewNodeLocalMapTask(tts, numTaskTrackers,
                ttm.getNumberOfUniqueHosts());
          case NODEGROUP:
            // locality level for nodegroup is 2
            return job.obtainNewMapTaskCommon(tts, numTaskTrackers, 
                ttm.getNumberOfUniqueHosts(), 2);
          case RACK:
            return job.obtainNewNodeOrRackLocalMapTask(tts, numTaskTrackers,
                ttm.getNumberOfUniqueHosts());
          default:
            return job.obtainNewMapTask(tts, numTaskTrackers,
                ttm.getNumberOfUniqueHosts());
        }
      } else {
        return job.obtainNewReduceTask(tts, numTaskTrackers,
            ttm.getNumberOfUniqueHosts());
      }
    } else {
      return null;
    }
  }

}
