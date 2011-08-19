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
import java.util.List;
import java.util.ArrayList;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.TTInfo;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.system.DaemonProtocol;

/**
 * Aspect class which injects the code for {@link JobTracker} class.
 * 
 */
public privileged aspect JobTrackerAspect {


  public Configuration JobTracker.getDaemonConf() throws IOException {
    return conf;
  }
  /**
   * Method to get the read only view of the job and its associated information.
   * 
   * @param jobID
   *          id of the job for which information is required.
   * @return JobInfo of the job requested
   * @throws IOException
   */
  public JobInfo JobTracker.getJobInfo(JobID jobID) throws IOException {
    JobInProgress jip = jobs.get(org.apache.hadoop.mapred.JobID
        .downgrade(jobID));
    if (jip == null) {
      LOG.warn("No job present for : " + jobID);
      return null;
    }
    JobInfo info;
    synchronized (jip) {
      info = jip.getJobInfo();
    }
    return info;
  }

  /**
   * Method to get the read only view of the task and its associated
   * information.
   * 
   * @param taskID
   * @return
   * @throws IOException
   */
  public TaskInfo JobTracker.getTaskInfo(TaskID taskID) throws IOException {
    TaskInProgress tip = getTip(org.apache.hadoop.mapred.TaskID
        .downgrade(taskID));

    if (tip == null) {
      LOG.warn("No task present for : " + taskID);
      return null;
    }
    return getTaskInfo(tip);
  }

  public TTInfo JobTracker.getTTInfo(String trackerName) throws IOException {
    org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker tt = taskTrackers
        .get(trackerName);
    if (tt == null) {
      LOG.warn("No task tracker with name : " + trackerName + " found");
      return null;
    }
    TaskTrackerStatus status = tt.getStatus();
    TTInfo info = new TTInfoImpl(status.trackerName, status);
    return info;
  }

  // XXX Below two method don't reuse getJobInfo and getTaskInfo as there is a
  // possibility that retire job can run and remove the job from JT memory
  // during
  // processing of the RPC call.
  public JobInfo[] JobTracker.getAllJobInfo() throws IOException {
    List<JobInfo> infoList = new ArrayList<JobInfo>();
    synchronized (jobs) {
      for (JobInProgress jip : jobs.values()) {
        JobInfo info = jip.getJobInfo();
        infoList.add(info);
      }
    }
    return (JobInfo[]) infoList.toArray(new JobInfo[infoList.size()]);
  }

  public TaskInfo[] JobTracker.getTaskInfo(JobID jobID) throws IOException {
    JobInProgress jip = jobs.get(org.apache.hadoop.mapred.JobID
        .downgrade(jobID));
    if (jip == null) {
      LOG.warn("Unable to find job : " + jobID);
      return null;
    }
    List<TaskInfo> infoList = new ArrayList<TaskInfo>();
    synchronized (jip) {
      for (TaskInProgress tip : jip.setup) {
        infoList.add(getTaskInfo(tip));
      }
      for (TaskInProgress tip : jip.maps) {
        infoList.add(getTaskInfo(tip));
      }
      for (TaskInProgress tip : jip.reduces) {
        infoList.add(getTaskInfo(tip));
      }
      for (TaskInProgress tip : jip.cleanup) {
        infoList.add(getTaskInfo(tip));
      }
    }
    return (TaskInfo[]) infoList.toArray(new TaskInfo[infoList.size()]);
  }

  public TTInfo[] JobTracker.getAllTTInfo() throws IOException {
    List<TTInfo> infoList = new ArrayList<TTInfo>();
    synchronized (taskTrackers) {
      for (TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus();
        TTInfo info = new TTInfoImpl(status.trackerName, status);
        infoList.add(info);
      }
    }
    return (TTInfo[]) infoList.toArray(new TTInfo[infoList.size()]);
  }
  
  public boolean JobTracker.isJobRetired(JobID id) throws IOException {
    return retireJobs.get(
        org.apache.hadoop.mapred.JobID.downgrade(id))!=null?true:false;
  }

  public String JobTracker.getJobHistoryLocationForRetiredJob(
      JobID id) throws IOException {
    String historyFile = this.getJobStatus(id).getHistoryFile();
    if(historyFile == null) {
      throw new IOException("The retired job information for the job : " 
          + id +" is not found");
    } else {
      return historyFile;
    }
  }
  pointcut getVersionAspect(String protocol, long clientVersion) : 
    execution(public long JobTracker.getProtocolVersion(String , 
      long) throws IOException) && args(protocol, clientVersion);

  long around(String protocol, long clientVersion) :  
    getVersionAspect(protocol, clientVersion) {
    if (protocol.equals(DaemonProtocol.class.getName())) {
      return DaemonProtocol.versionID;
    } else if (protocol.equals(JTProtocol.class.getName())) {
      return JTProtocol.versionID;
    } else {
      return proceed(protocol, clientVersion);
    }
  }

  /**
   * Point cut which monitors for the start of the jobtracker and sets the right
   * value if the jobtracker is started.
   */
  pointcut jtConstructorPointCut() : 
        call(JobTracker.new(..));

  after() returning (JobTracker tracker): jtConstructorPointCut() {
    try {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      tracker.setUser(ugi.getShortUserName());
    } catch (IOException e) {
      tracker.LOG.warn("Unable to get the user information for the "
          + "Jobtracker");
    }
    tracker.setReady(true);
  }
  
  private TaskInfo JobTracker.getTaskInfo(TaskInProgress tip) {
    TaskStatus[] status = tip.getTaskStatuses();
    if (status == null) {
      if (tip.isMapTask()) {
        status = new MapTaskStatus[]{};
      }
      else {
        status = new ReduceTaskStatus[]{};
      }
    }
    String[] trackers =
        (String[]) (tip.getActiveTasks().values()).toArray(new String[tip
            .getActiveTasks().values().size()]);
    TaskInfo info =
        new TaskInfoImpl(tip.getTIPId(), tip.getProgress(), tip
            .getActiveTasks().size(), tip.numKilledTasks(), tip
            .numTaskFailures(), status, (tip.isJobSetupTask() || tip
            .isJobCleanupTask()), trackers);
    return info;
  }
}
