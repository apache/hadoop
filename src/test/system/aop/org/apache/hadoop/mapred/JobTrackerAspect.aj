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
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobTracker.RetireJobInfo;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.TTInfo;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.system.DaemonProtocol;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

/**
 * Aspect class which injects the code for {@link JobTracker} class.
 * 
 */
public privileged aspect JobTrackerAspect {


  private static JobTracker tracker;
  
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
  
  public boolean JobTracker.isBlackListed(String trackerName) throws IOException {
    return isBlacklisted(trackerName);
  }

  public String JobTracker.getJobHistoryLocationForRetiredJob(
      JobID id) throws IOException {
    RetireJobInfo retInfo = retireJobs.get(
        org.apache.hadoop.mapred.JobID.downgrade(id));
    if(retInfo == null) {
      throw new IOException("The retired job information for the job : " 
          + id +" is not found");
    } else {
      return retInfo.getHistoryFile();
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
   * 
   * @param conf
   * @param jobtrackerIndentifier
   */
  pointcut jtConstructorPointCut(JobConf conf, String jobtrackerIndentifier) : 
        call(JobTracker.new(JobConf,String)) 
        && args(conf, jobtrackerIndentifier) ;

  after(JobConf conf, String jobtrackerIndentifier) 
    returning (JobTracker tracker): jtConstructorPointCut(conf, 
        jobtrackerIndentifier) {
    try {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      tracker.setUser(ugi.getShortUserName());
    } catch (IOException e) {
      tracker.LOG.warn("Unable to get the user information for the " +
      		"Jobtracker");
    }
    this.tracker = tracker;
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
  
  /**
   * Get the job summary details from the jobtracker log files.
   * @param jobId - job id
   * @param filePattern - jobtracker log file pattern.
   * @return String - Job summary details of given job id.
   * @throws IOException if any I/O error occurs.
   */
  public String JobTracker.getJobSummaryFromLogs(JobID jobId,
      String filePattern) throws IOException {
    String pattern = "JobId=" + jobId.toString() + ",submitTime";
    String[] cmd = new String[] {
                   "bash",
                   "-c",
                   "grep -i " 
                 + pattern + " " 
                 + filePattern + " " 
                 + "| sed s/'JobSummary: '/'^'/g | cut -d'^' -f2"};
    ShellCommandExecutor shexec = new ShellCommandExecutor(cmd);
    shexec.execute();
    return shexec.getOutput();
  }
  
  /**
   * Get the job summary information for given job id.
   * @param jobId - job id.
   * @return String - Job summary details as key value pair.
   * @throws IOException if any I/O error occurs.
   */
  public String JobTracker.getJobSummaryInfo(JobID jobId) throws IOException {
    StringBuffer jobSummary = new StringBuffer();
    JobInProgress jip = jobs.
        get(org.apache.hadoop.mapred.JobID.downgrade(jobId));
    if (jip == null) {
      LOG.warn("Job has not been found - " + jobId);
      return null;
    }
    JobProfile profile = jip.getProfile();
    JobStatus status = jip.getStatus();
    final char[] charsToEscape = {StringUtils.COMMA, '=', 
        StringUtils.ESCAPE_CHAR};
    String user = StringUtils.escapeString(profile.getUser(), 
        StringUtils.ESCAPE_CHAR, charsToEscape);
    String queue = StringUtils.escapeString(profile.getQueueName(), 
        StringUtils.ESCAPE_CHAR, charsToEscape);
    Counters jobCounters = jip.getJobCounters();
    long mapSlotSeconds = (jobCounters.getCounter(
        JobInProgress.Counter.SLOTS_MILLIS_MAPS) + 
        jobCounters.getCounter(JobInProgress.
        Counter.FALLOW_SLOTS_MILLIS_MAPS)) / 1000;
    long reduceSlotSeconds = (jobCounters.getCounter(
        JobInProgress.Counter.SLOTS_MILLIS_REDUCES) + 
       jobCounters.getCounter(JobInProgress.
       Counter.FALLOW_SLOTS_MILLIS_REDUCES)) / 1000;
    jobSummary.append("jobId=");
    jobSummary.append(jip.getJobID());
    jobSummary.append(",");
    jobSummary.append("startTime=");
    jobSummary.append(jip.getStartTime());
    jobSummary.append(",");
    jobSummary.append("launchTime=");
    jobSummary.append(jip.getLaunchTime());
    jobSummary.append(",");
    jobSummary.append("finishTime=");
    jobSummary.append(jip.getFinishTime());
    jobSummary.append(",");
    jobSummary.append("numMaps=");
    jobSummary.append(jip.getTasks(TaskType.MAP).length);
    jobSummary.append(",");
    jobSummary.append("numSlotsPerMap=");
    jobSummary.append(jip.getNumSlotsPerMap() );
    jobSummary.append(",");
    jobSummary.append("numReduces=");
    jobSummary.append(jip.getTasks(TaskType.REDUCE).length);
    jobSummary.append(",");
    jobSummary.append("numSlotsPerReduce=");
    jobSummary.append(jip.getNumSlotsPerReduce());
    jobSummary.append(",");
    jobSummary.append("user=");
    jobSummary.append(user);
    jobSummary.append(",");
    jobSummary.append("queue=");
    jobSummary.append(queue);
    jobSummary.append(",");
    jobSummary.append("status=");
    jobSummary.append(JobStatus.getJobRunState(status.getRunState()));
    jobSummary.append(",");
    jobSummary.append("mapSlotSeconds=");
    jobSummary.append(mapSlotSeconds);
    jobSummary.append(",");
    jobSummary.append("reduceSlotsSeconds=");
    jobSummary.append(reduceSlotSeconds);
    jobSummary.append(",");
    jobSummary.append("clusterMapCapacity=");
    jobSummary.append(tracker.getClusterMetrics().getMapSlotCapacity());
    jobSummary.append(",");
    jobSummary.append("clusterReduceCapacity=");
    jobSummary.append(tracker.getClusterMetrics().getReduceSlotCapacity());
    return jobSummary.toString();
  }
}
