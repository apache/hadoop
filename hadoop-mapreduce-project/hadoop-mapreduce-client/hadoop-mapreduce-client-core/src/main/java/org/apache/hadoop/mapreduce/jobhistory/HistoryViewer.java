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
package org.apache.hadoop.mapreduce.jobhistory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.util.HostUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

/**
 * HistoryViewer is used to parse and view the JobHistory files 
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HistoryViewer {
  private static SimpleDateFormat dateFormat = 
    new SimpleDateFormat("d-MMM-yyyy HH:mm:ss");
  private FileSystem fs;
  private JobInfo job;
  private String jobId;
  private boolean printAll;

/**
 * Constructs the HistoryViewer object
 * @param historyFile The fully qualified Path of the History File
 * @param conf The Configuration file
 * @param printAll Toggle to print all status to only killed/failed status
 * @throws IOException
 */
  public HistoryViewer(String historyFile, 
                       Configuration conf,
                       boolean printAll) throws IOException {
    this.printAll = printAll;
    String errorMsg = "Unable to initialize History Viewer";
    try {
      Path jobFile = new Path(historyFile);
      fs = jobFile.getFileSystem(conf);
      String[] jobDetails =
        jobFile.getName().split("_");
      if (jobDetails.length < 2) {
        // NOT a valid name
        System.err.println("Ignore unrecognized file: " + jobFile.getName());
        throw new IOException(errorMsg);
      }
      JobHistoryParser parser = new JobHistoryParser(fs, jobFile);
      job = parser.parse();
      jobId = job.getJobId().toString();
    } catch(Exception e) {
      throw new IOException(errorMsg, e);
    }
  }

  /**
   * Print the job/task/attempt summary information
   * @throws IOException
   */
  public void print() throws IOException{
    printJobDetails();
    printTaskSummary();
    printJobAnalysis();
    printTasks(TaskType.JOB_SETUP, TaskStatus.State.FAILED.toString());
    printTasks(TaskType.JOB_SETUP, TaskStatus.State.KILLED.toString());
    printTasks(TaskType.MAP, TaskStatus.State.FAILED.toString());
    printTasks(TaskType.MAP, TaskStatus.State.KILLED.toString());
    printTasks(TaskType.REDUCE, TaskStatus.State.FAILED.toString());
    printTasks(TaskType.REDUCE, TaskStatus.State.KILLED.toString());
    printTasks(TaskType.JOB_CLEANUP, TaskStatus.State.FAILED.toString());
    printTasks(TaskType.JOB_CLEANUP, 
        JobStatus.getJobRunState(JobStatus.KILLED));
    if (printAll) {
      printTasks(TaskType.JOB_SETUP, TaskStatus.State.SUCCEEDED.toString());
      printTasks(TaskType.MAP, TaskStatus.State.SUCCEEDED.toString());
      printTasks(TaskType.REDUCE, TaskStatus.State.SUCCEEDED.toString());
      printTasks(TaskType.JOB_CLEANUP, TaskStatus.State.SUCCEEDED.toString());
      printAllTaskAttempts(TaskType.JOB_SETUP);
      printAllTaskAttempts(TaskType.MAP);
      printAllTaskAttempts(TaskType.REDUCE);
      printAllTaskAttempts(TaskType.JOB_CLEANUP);
    }
    
    FilteredJob filter = new FilteredJob(job, 
        TaskStatus.State.FAILED.toString());
    printFailedAttempts(filter);
    
    filter = new FilteredJob(job,
        TaskStatus.State.KILLED.toString());
    printFailedAttempts(filter);
  }
 
  private void printJobDetails() {
    StringBuffer jobDetails = new StringBuffer();
    jobDetails.append("\nHadoop job: " ).append(job.getJobId());
    jobDetails.append("\n=====================================");
    jobDetails.append("\nUser: ").append(job.getUsername()); 
    jobDetails.append("\nJobName: ").append(job.getJobname()); 
    jobDetails.append("\nJobConf: ").append(job.getJobConfPath()); 
    jobDetails.append("\nSubmitted At: ").append(StringUtils.
                        getFormattedTimeWithDiff(dateFormat,
                        job.getSubmitTime(), 0)); 
    jobDetails.append("\nLaunched At: ").append(StringUtils.
                        getFormattedTimeWithDiff(dateFormat,
                        job.getLaunchTime(),
                        job.getSubmitTime()));
    jobDetails.append("\nFinished At: ").append(StringUtils.
                        getFormattedTimeWithDiff(dateFormat,
                        job.getFinishTime(),
                        job.getLaunchTime()));
    jobDetails.append("\nStatus: ").append(((job.getJobStatus() == null) ? 
                      "Incomplete" :job.getJobStatus()));
    printCounters(jobDetails, job.getTotalCounters(), job.getMapCounters(),
        job.getReduceCounters());
    jobDetails.append("\n");
    jobDetails.append("\n=====================================");
    System.out.println(jobDetails.toString());
  }

  private void printCounters(StringBuffer buff, Counters totalCounters,
      Counters mapCounters, Counters reduceCounters) {
    // Killed jobs might not have counters
    if (totalCounters == null) {
      return;
    }
    buff.append("\nCounters: \n\n");
    buff.append(String.format("|%1$-30s|%2$-30s|%3$-10s|%4$-10s|%5$-10s|", 
        "Group Name",
        "Counter name",
        "Map Value",
        "Reduce Value",
        "Total Value"));
    buff.append("\n------------------------------------------"+
        "---------------------------------------------");
    for (String groupName : totalCounters.getGroupNames()) {
         CounterGroup totalGroup = totalCounters.getGroup(groupName);
         CounterGroup mapGroup = mapCounters.getGroup(groupName);
         CounterGroup reduceGroup = reduceCounters.getGroup(groupName);
      
         Format decimal = new DecimalFormat();
         Iterator<org.apache.hadoop.mapreduce.Counter> ctrItr =
           totalGroup.iterator();
         while(ctrItr.hasNext()) {
           org.apache.hadoop.mapreduce.Counter counter = ctrItr.next();
           String name = counter.getName();
           String mapValue = 
             decimal.format(mapGroup.findCounter(name).getValue());
           String reduceValue = 
             decimal.format(reduceGroup.findCounter(name).getValue());
           String totalValue = 
             decimal.format(counter.getValue());

           buff.append(
               String.format("%n|%1$-30s|%2$-30s|%3$-10s|%4$-10s|%5$-10s", 
                   totalGroup.getDisplayName(),
                   counter.getDisplayName(),
                   mapValue, reduceValue, totalValue));
      }
    }
  }
  
  private void printAllTaskAttempts(TaskType taskType) {
    Map<TaskID, TaskInfo> tasks = job.getAllTasks();
    StringBuffer taskList = new StringBuffer();
    taskList.append("\n").append(taskType);
    taskList.append(" task list for ").append(job.getJobId());
    taskList.append("\nTaskId\t\tStartTime");
    if (TaskType.REDUCE.equals(taskType)) {
      taskList.append("\tShuffleFinished\tSortFinished");
    }
    taskList.append("\tFinishTime\tHostName\tError\tTaskLogs");
    taskList.append("\n====================================================");
    System.out.println(taskList.toString());
    for (JobHistoryParser.TaskInfo task : tasks.values()) {
      for (JobHistoryParser.TaskAttemptInfo attempt : 
        task.getAllTaskAttempts().values()) {
        if (taskType.equals(task.getTaskType())){
          taskList.setLength(0); 
          taskList.append(attempt.getAttemptId()).append("\t");
          taskList.append(StringUtils.getFormattedTimeWithDiff(dateFormat,
                          attempt.getStartTime(), 0)).append("\t");
          if (TaskType.REDUCE.equals(taskType)) {
            taskList.append(StringUtils.getFormattedTimeWithDiff(dateFormat,
                            attempt.getShuffleFinishTime(),
                            attempt.getStartTime()));
            taskList.append("\t"); 
            taskList.append(StringUtils.getFormattedTimeWithDiff(dateFormat, 
                            attempt.getSortFinishTime(),
                            attempt.getShuffleFinishTime())); 
          } 
          taskList.append(StringUtils.getFormattedTimeWithDiff(dateFormat,
                          attempt.getFinishTime(),
                          attempt.getStartTime())); 
          taskList.append("\t"); 
          taskList.append(attempt.getHostname()).append("\t");
          taskList.append(attempt.getError());
          String taskLogsUrl = getTaskLogsUrl(
              WebAppUtils.getHttpSchemePrefix(fs.getConf()), attempt);
          taskList.append(taskLogsUrl != null ? taskLogsUrl : "n/a");
          System.out.println(taskList.toString());
        }
      }
    }
  }

  private void printTaskSummary() {
    SummarizedJob ts = new SummarizedJob(job);
    StringBuffer taskSummary = new StringBuffer();
    taskSummary.append("\nTask Summary");
    taskSummary.append("\n============================");
    taskSummary.append("\nKind\tTotal\t");
    taskSummary.append("Successful\tFailed\tKilled\tStartTime\tFinishTime");
    taskSummary.append("\n");
    taskSummary.append("\nSetup\t").append(ts.totalSetups);
    taskSummary.append("\t").append(ts.numFinishedSetups);
    taskSummary.append("\t\t").append(ts.numFailedSetups);
    taskSummary.append("\t").append(ts.numKilledSetups);
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                               dateFormat, ts.setupStarted, 0));
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                               dateFormat, ts.setupFinished, ts.setupStarted));
    taskSummary.append("\nMap\t").append(ts.totalMaps);
    taskSummary.append("\t").append(job.getFinishedMaps());
    taskSummary.append("\t\t").append(ts.numFailedMaps);
    taskSummary.append("\t").append(ts.numKilledMaps);
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                               dateFormat, ts.mapStarted, 0));
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                               dateFormat, ts.mapFinished, ts.mapStarted));
    taskSummary.append("\nReduce\t").append(ts.totalReduces);
    taskSummary.append("\t").append(job.getFinishedReduces());
    taskSummary.append("\t\t").append(ts.numFailedReduces);
    taskSummary.append("\t").append(ts.numKilledReduces);
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                               dateFormat, ts.reduceStarted, 0));
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                            dateFormat, ts.reduceFinished, ts.reduceStarted));
    taskSummary.append("\nCleanup\t").append(ts.totalCleanups);
    taskSummary.append("\t").append(ts.numFinishedCleanups);
    taskSummary.append("\t\t").append(ts.numFailedCleanups);
    taskSummary.append("\t").append(ts.numKilledCleanups);
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                            dateFormat, ts.cleanupStarted, 0));
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                            dateFormat, ts.cleanupFinished, 
                            ts.cleanupStarted)); 
    taskSummary.append("\n============================\n");
    System.out.println(taskSummary.toString());
  }

  private void printJobAnalysis() {
    if (!job.getJobStatus().equals
        (JobStatus.getJobRunState(JobStatus.SUCCEEDED))) {
      System.out.println("No Analysis available as job did not finish");
      return;
    }
    
    AnalyzedJob avg = new AnalyzedJob(job);
    
    System.out.println("\nAnalysis");
    System.out.println("=========");
    printAnalysis(avg.getMapTasks(), cMap, "map", avg.getAvgMapTime(), 10);
    printLast(avg.getMapTasks(), "map", cFinishMapRed);

    if (avg.getReduceTasks().length > 0) {
      printAnalysis(avg.getReduceTasks(), cShuffle, "shuffle", 
          avg.getAvgShuffleTime(), 10);
      printLast(avg.getReduceTasks(), "shuffle", cFinishShuffle);

      printAnalysis(avg.getReduceTasks(), cReduce, "reduce",
          avg.getAvgReduceTime(), 10);
      printLast(avg.getReduceTasks(), "reduce", cFinishMapRed);
    }
    System.out.println("=========");
  }

  private void printAnalysis(JobHistoryParser.TaskAttemptInfo [] tasks,
      Comparator<JobHistoryParser.TaskAttemptInfo> cmp,
      String taskType,
      long avg,
      int showTasks) {
    Arrays.sort(tasks, cmp);
    JobHistoryParser.TaskAttemptInfo min = tasks[tasks.length-1];
    StringBuffer details = new StringBuffer();
    details.append("\nTime taken by best performing ");
    details.append(taskType).append(" task ");
    details.append(min.getAttemptId().getTaskID().toString()).append(": ");
    if ("map".equals(taskType)) {
      details.append(StringUtils.formatTimeDiff(
          min.getFinishTime(),
          min.getStartTime()));
    } else if ("shuffle".equals(taskType)) {
      details.append(StringUtils.formatTimeDiff(
          min.getShuffleFinishTime(),
          min.getStartTime()));
    } else {
      details.append(StringUtils.formatTimeDiff(
          min.getFinishTime(),
          min.getShuffleFinishTime()));
    }
    details.append("\nAverage time taken by ");
    details.append(taskType).append(" tasks: "); 
    details.append(StringUtils.formatTimeDiff(avg, 0));
    details.append("\nWorse performing ");
    details.append(taskType).append(" tasks: ");
    details.append("\nTaskId\t\tTimetaken");
    System.out.println(details.toString());
    for (int i = 0; i < showTasks && i < tasks.length; i++) {
      details.setLength(0);
      details.append(tasks[i].getAttemptId().getTaskID()).append(" ");
      if ("map".equals(taskType)) {
        details.append(StringUtils.formatTimeDiff(
            tasks[i].getFinishTime(),
            tasks[i].getStartTime()));
      } else if ("shuffle".equals(taskType)) {
        details.append(StringUtils.formatTimeDiff(
            tasks[i].getShuffleFinishTime(),
            tasks[i].getStartTime()));
      } else {
        details.append(StringUtils.formatTimeDiff(
            tasks[i].getFinishTime(),
            tasks[i].getShuffleFinishTime()));
      }
      System.out.println(details.toString());
    }
  }

  private void printLast(JobHistoryParser.TaskAttemptInfo [] tasks,
      String taskType,
      Comparator<JobHistoryParser.TaskAttemptInfo> cmp
  ) {
    Arrays.sort(tasks, cFinishMapRed);
    JobHistoryParser.TaskAttemptInfo last = tasks[0];
    StringBuffer lastBuf = new StringBuffer();
    lastBuf.append("The last ").append(taskType);
    lastBuf.append(" task ").append(last.getAttemptId().getTaskID());
    Long finishTime;
    if ("shuffle".equals(taskType)) {
      finishTime = last.getShuffleFinishTime();
    } else {
      finishTime = last.getFinishTime();
    }
    lastBuf.append(" finished at (relative to the Job launch time): ");
    lastBuf.append(StringUtils.getFormattedTimeWithDiff(dateFormat,
        finishTime, job.getLaunchTime()));
    System.out.println(lastBuf.toString());
  }

  private void printTasks(TaskType taskType, String status) {
    Map<TaskID, JobHistoryParser.TaskInfo> tasks = job.getAllTasks();
    StringBuffer header = new StringBuffer();
    header.append("\n").append(status).append(" ");
    header.append(taskType).append(" task list for ").append(jobId);
    header.append("\nTaskId\t\tStartTime\tFinishTime\tError");
    if (TaskType.MAP.equals(taskType)) {
      header.append("\tInputSplits");
    }
    header.append("\n====================================================");
    StringBuffer taskList = new StringBuffer();
    for (JobHistoryParser.TaskInfo task : tasks.values()) {
      if (taskType.equals(task.getTaskType()) &&
         (status.equals(task.getTaskStatus())
          || status.equalsIgnoreCase("ALL"))) {
        taskList.setLength(0);
        taskList.append(task.getTaskId());
        taskList.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                   dateFormat, task.getStartTime(), 0));
        taskList.append("\t").append(StringUtils.getFormattedTimeWithDiff(
                   dateFormat, task.getFinishTime(),
                   task.getStartTime())); 
        taskList.append("\t").append(task.getError());
        if (TaskType.MAP.equals(taskType)) {
          taskList.append("\t").append(task.getSplitLocations());
        }
        if (taskList != null) {
          System.out.println(header.toString());
          System.out.println(taskList.toString());
        }
      }
    }
  }

  private void printFailedAttempts(FilteredJob filteredJob) {
      Map<String, Set<TaskID>> badNodes = filteredJob.getFilteredMap();
      StringBuffer attempts = new StringBuffer();
      if (badNodes.size() > 0) {
        attempts.append("\n").append(filteredJob.getFilter());
        attempts.append(" task attempts by nodes");
        attempts.append("\nHostname\tFailedTasks");
        attempts.append("\n===============================");
        System.out.println(attempts.toString());
        for (Map.Entry<String, 
            Set<TaskID>> entry : badNodes.entrySet()) {
          String node = entry.getKey();
          Set<TaskID> failedTasks = entry.getValue();
          attempts.setLength(0);
          attempts.append(node).append("\t");
          for (TaskID t : failedTasks) {
            attempts.append(t).append(", ");
          }
          System.out.println(attempts.toString());
        }
      }
  }
  
  /**
   * Return the TaskLogsUrl of a particular TaskAttempt
   * 
   * @param attempt
   * @return the taskLogsUrl. null if http-port or tracker-name or
   *         task-attempt-id are unavailable.
   */
  public static String getTaskLogsUrl(String scheme,
      JobHistoryParser.TaskAttemptInfo attempt) {
    if (attempt.getHttpPort() == -1
        || attempt.getTrackerName().equals("")
        || attempt.getAttemptId() == null) {
      return null;
    }
  
    String taskTrackerName =
      HostUtil.convertTrackerNameToHostName(
        attempt.getTrackerName());
    return HostUtil.getTaskLogUrl(scheme, taskTrackerName,
        Integer.toString(attempt.getHttpPort()),
        attempt.getAttemptId().toString());
  }

  private Comparator<JobHistoryParser.TaskAttemptInfo> cMap = 
    new Comparator<JobHistoryParser.TaskAttemptInfo>() {
    public int compare(JobHistoryParser.TaskAttemptInfo t1, 
        JobHistoryParser.TaskAttemptInfo t2) {
      long l1 = t1.getFinishTime() - t1.getStartTime();
      long l2 = t2.getFinishTime() - t2.getStartTime();
      return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
    }
  };

  private Comparator<JobHistoryParser.TaskAttemptInfo> cShuffle = 
    new Comparator<JobHistoryParser.TaskAttemptInfo>() {
    public int compare(JobHistoryParser.TaskAttemptInfo t1, 
        JobHistoryParser.TaskAttemptInfo t2) {
      long l1 = t1.getShuffleFinishTime() - t1.getStartTime();
      long l2 = t2.getShuffleFinishTime() - t2.getStartTime();
      return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
    }
  };

  private Comparator<JobHistoryParser.TaskAttemptInfo> cFinishShuffle = 
    new Comparator<JobHistoryParser.TaskAttemptInfo>() {
    public int compare(JobHistoryParser.TaskAttemptInfo t1, 
        JobHistoryParser.TaskAttemptInfo t2) {
      long l1 = t1.getShuffleFinishTime(); 
      long l2 = t2.getShuffleFinishTime();
      return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
    }
  };

  private Comparator<JobHistoryParser.TaskAttemptInfo> cFinishMapRed = 
    new Comparator<JobHistoryParser.TaskAttemptInfo>() {
    public int compare(JobHistoryParser.TaskAttemptInfo t1, 
        JobHistoryParser.TaskAttemptInfo t2) {
      long l1 = t1.getFinishTime(); 
      long l2 = t2.getFinishTime();
      return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
    }
  };
  
  private Comparator<JobHistoryParser.TaskAttemptInfo> cReduce = 
    new Comparator<JobHistoryParser.TaskAttemptInfo>() {
    public int compare(JobHistoryParser.TaskAttemptInfo t1, 
        JobHistoryParser.TaskAttemptInfo t2) {
      long l1 = t1.getFinishTime() -
                t1.getShuffleFinishTime();
      long l2 = t2.getFinishTime() -
                t2.getShuffleFinishTime();
      return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
    }
  }; 

  /**
   * Utility class used the summarize the job. 
   * Used by HistoryViewer and the JobHistory UI.
   *
   */
  public static class SummarizedJob {
    Map<TaskID, JobHistoryParser.TaskInfo> tasks; 
     int totalMaps = 0; 
     int totalReduces = 0; 
     int totalCleanups = 0;
     int totalSetups = 0;
     int numFailedMaps = 0; 
     int numKilledMaps = 0;
     int numFailedReduces = 0; 
     int numKilledReduces = 0;
     int numFinishedCleanups = 0;
     int numFailedCleanups = 0;
     int numKilledCleanups = 0;
     int numFinishedSetups = 0;
     int numFailedSetups = 0;
     int numKilledSetups = 0;
     long mapStarted = 0; 
     long mapFinished = 0; 
     long reduceStarted = 0; 
     long reduceFinished = 0; 
     long cleanupStarted = 0;
     long cleanupFinished = 0;
     long setupStarted = 0;
     long setupFinished = 0;
     
     /** Get total maps */
     public int getTotalMaps() { return totalMaps; } 
     /** Get total reduces */
     public int getTotalReduces() { return totalReduces; } 
     /** Get number of clean up tasks */ 
     public int getTotalCleanups() { return totalCleanups; }
     /** Get number of set up tasks */
     public int getTotalSetups() { return totalSetups; }
     /** Get number of failed maps */
     public int getNumFailedMaps() { return numFailedMaps; }
     /** Get number of killed maps */
     public int getNumKilledMaps() { return numKilledMaps; }
     /** Get number of failed reduces */
     public int getNumFailedReduces() { return numFailedReduces; } 
     /** Get number of killed reduces */
     public int getNumKilledReduces() { return numKilledReduces; }
     /** Get number of cleanup tasks that finished */
     public int getNumFinishedCleanups() { return numFinishedCleanups; }
     /** Get number of failed cleanup tasks */
     public int getNumFailedCleanups() { return numFailedCleanups; }
     /** Get number of killed cleanup tasks */
     public int getNumKilledCleanups() { return numKilledCleanups; }
     /** Get number of finished set up tasks */
     public int getNumFinishedSetups() { return numFinishedSetups; }
     /** Get number of failed set up tasks */
     public int getNumFailedSetups() { return numFailedSetups; }
     /** Get number of killed set up tasks */
     public int getNumKilledSetups() { return numKilledSetups; }
     /** Get number of maps that were started */
     public long getMapStarted() { return mapStarted; } 
     /** Get number of maps that finished */
     public long getMapFinished() { return mapFinished; } 
     /** Get number of Reducers that were started */
     public long getReduceStarted() { return reduceStarted; } 
     /** Get number of reducers that finished */
     public long getReduceFinished() { return reduceFinished; } 
     /** Get number of cleanup tasks started */ 
     public long getCleanupStarted() { return cleanupStarted; }
     /** Get number of cleanup tasks that finished */
     public long getCleanupFinished() { return cleanupFinished; }
     /** Get number of setup tasks that started */
     public long getSetupStarted() { return setupStarted; }
     /** Get number of setup tasks that finished */
     public long getSetupFinished() { return setupFinished; }

     /** Create summary information for the parsed job */
    public SummarizedJob(JobInfo job) {
      tasks = job.getAllTasks();

      for (JobHistoryParser.TaskInfo task : tasks.values()) {
        Map<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> attempts = 
          task.getAllTaskAttempts();
        //allHosts.put(task.getHo(Keys.HOSTNAME), "");
        for (JobHistoryParser.TaskAttemptInfo attempt : attempts.values()) {
          long startTime = attempt.getStartTime(); 
          long finishTime = attempt.getFinishTime();
          if (attempt.getTaskType().equals(TaskType.MAP)) {
            if (mapStarted== 0 || mapStarted > startTime) {
              mapStarted = startTime; 
            }
            if (mapFinished < finishTime) {
              mapFinished = finishTime; 
            }
            totalMaps++; 
            if (attempt.getTaskStatus().equals
                (TaskStatus.State.FAILED.toString())) {
              numFailedMaps++; 
            } else if (attempt.getTaskStatus().equals
                (TaskStatus.State.KILLED.toString())) {
              numKilledMaps++;
            }
          } else if (attempt.getTaskType().equals(TaskType.REDUCE)) {
            if (reduceStarted==0||reduceStarted > startTime) {
              reduceStarted = startTime; 
            }
            if (reduceFinished < finishTime) {
              reduceFinished = finishTime; 
            }
            totalReduces++; 
            if (attempt.getTaskStatus().equals
                (TaskStatus.State.FAILED.toString())) {
              numFailedReduces++; 
            } else if (attempt.getTaskStatus().equals
                (TaskStatus.State.KILLED.toString())) {
              numKilledReduces++;
            }
          } else if (attempt.getTaskType().equals(TaskType.JOB_CLEANUP)) {
            if (cleanupStarted==0||cleanupStarted > startTime) {
              cleanupStarted = startTime; 
            }
            if (cleanupFinished < finishTime) {
              cleanupFinished = finishTime; 
            }
            totalCleanups++; 
            if (attempt.getTaskStatus().equals
                (TaskStatus.State.SUCCEEDED.toString())) {
              numFinishedCleanups++; 
            } else if (attempt.getTaskStatus().equals
                (TaskStatus.State.FAILED.toString())) {
              numFailedCleanups++;
            } else if (attempt.getTaskStatus().equals
                (TaskStatus.State.KILLED.toString())) {
              numKilledCleanups++;
            }
          } else if (attempt.getTaskType().equals(TaskType.JOB_SETUP)) {
            if (setupStarted==0||setupStarted > startTime) {
              setupStarted = startTime; 
            }
            if (setupFinished < finishTime) {
              setupFinished = finishTime; 
            }
            totalSetups++; 
            if (attempt.getTaskStatus().equals
                (TaskStatus.State.SUCCEEDED.toString())) {
              numFinishedSetups++;
            } else if (attempt.getTaskStatus().equals
                (TaskStatus.State.FAILED.toString())) {
              numFailedSetups++;
            } else if (attempt.getTaskStatus().equals
                (TaskStatus.State.KILLED.toString())) {
              numKilledSetups++;
            }
          }
        }
      }
    }
  }

  /**
   * Utility class used while analyzing the job. 
   * Used by HistoryViewer and the JobHistory UI.
   */
 
  public static class AnalyzedJob {
    private long avgMapTime;
    private long avgReduceTime;
    private long avgShuffleTime;
    
    private JobHistoryParser.TaskAttemptInfo [] mapTasks;
    private JobHistoryParser.TaskAttemptInfo [] reduceTasks;

    /** Get the average map time */
    public long getAvgMapTime() { return avgMapTime; }
    /** Get the average reduce time */
    public long getAvgReduceTime() { return avgReduceTime; }
    /** Get the average shuffle time */
    public long getAvgShuffleTime() { return avgShuffleTime; }
    /** Get the map tasks list */
    public JobHistoryParser.TaskAttemptInfo [] getMapTasks() { 
      return mapTasks;
    }
    /** Get the reduce tasks list */
    public JobHistoryParser.TaskAttemptInfo [] getReduceTasks() { 
      return reduceTasks;
    }
    /** Generate analysis information for the parsed job */
    public AnalyzedJob (JobInfo job) {
      Map<TaskID, JobHistoryParser.TaskInfo> tasks = job.getAllTasks();
      int finishedMaps = (int) job.getFinishedMaps();
      int finishedReduces = (int) job.getFinishedReduces();
      mapTasks = 
        new JobHistoryParser.TaskAttemptInfo[finishedMaps]; 
      reduceTasks = 
        new JobHistoryParser.TaskAttemptInfo[finishedReduces]; 
      int mapIndex = 0 , reduceIndex=0; 
      avgMapTime = 0;
      avgReduceTime = 0;
      avgShuffleTime = 0;

      for (JobHistoryParser.TaskInfo task : tasks.values()) {
        Map<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> attempts =
          task.getAllTaskAttempts();
        for (JobHistoryParser.TaskAttemptInfo attempt : attempts.values()) {
          if (attempt.getTaskStatus().
              equals(TaskStatus.State.SUCCEEDED.toString())) {
            long avgFinishTime = (attempt.getFinishTime() -
                attempt.getStartTime());
            if (attempt.getTaskType().equals(TaskType.MAP)) {
              mapTasks[mapIndex++] = attempt; 
              avgMapTime += avgFinishTime;
            } else if (attempt.getTaskType().equals(TaskType.REDUCE)) {
              reduceTasks[reduceIndex++] = attempt;
              avgShuffleTime += (attempt.getShuffleFinishTime() - 
                  attempt.getStartTime());
              avgReduceTime += (attempt.getFinishTime() -
                  attempt.getShuffleFinishTime());
            }
            break;
          }
        }
      }
      if (finishedMaps > 0) {
        avgMapTime /= finishedMaps;
      }
      if (finishedReduces > 0) {
        avgReduceTime /= finishedReduces;
        avgShuffleTime /= finishedReduces;
      }
    }
  }

  /**
   * Utility to filter out events based on the task status
   *
   */
  public static class FilteredJob {
    
    private Map<String, Set<TaskID>> badNodesToFilteredTasks =
      new HashMap<String, Set<TaskID>>();
    
    private String filter;
    
    /** Get the map of the filtered tasks */
    public Map<String, Set<TaskID>> getFilteredMap() {
      return badNodesToFilteredTasks;
    }
    
    /** Get the current filter */
    public String getFilter() { return filter; }
    
    /** Apply the filter (status) on the parsed job and generate summary */
    public FilteredJob(JobInfo job, String status) {

      filter = status;
      
      Map<TaskID, JobHistoryParser.TaskInfo> tasks = job.getAllTasks();

      for (JobHistoryParser.TaskInfo task : tasks.values()) {
        Map<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> attempts =
          task.getAllTaskAttempts();
        for (JobHistoryParser.TaskAttemptInfo attempt : attempts.values()) {
          if (attempt.getTaskStatus().equals(status)) {
            String hostname = attempt.getHostname();
            TaskID id = attempt.getAttemptId().getTaskID();

            Set<TaskID> set = badNodesToFilteredTasks.get(hostname);

            if (set == null) {
              set = new TreeSet<TaskID>();
              set.add(id);
              badNodesToFilteredTasks.put(hostname, set);
            }else{
              set.add(id);
            }
          }
        }
      }
    }
  }
}
