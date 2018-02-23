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
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.util.HostUtil;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

/**
 * HistoryViewer is used to parse and view the JobHistory files.  They can be
 * printed in human-readable format or machine-readable JSON format using the
 * {@link HistoryViewerPrinter}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HistoryViewer {
  private FileSystem fs;
  private JobInfo job;
  private HistoryViewerPrinter jhvp;
  public static final String HUMAN_FORMAT = "human";
  public static final String JSON_FORMAT = "json";

  /**
   * Constructs the HistoryViewer object.
   * @param historyFile the fully qualified Path of the History File
   * @param conf the Configuration file
   * @param printAll toggle to print all status to only killed/failed status
   * @throws IOException when there is a problem parsing the history file
   */
  public HistoryViewer(String historyFile, Configuration conf,
                       boolean printAll) throws IOException {
    this(historyFile, conf, printAll, HUMAN_FORMAT);
  }

  /**
   * Constructs the HistoryViewer object.
   * @param historyFile the fully qualified Path of the History File
   * @param conf the Configuration file
   * @param printAll toggle to print all status to only killed/failed status
   * @param format the output format to use
   * @throws IOException when there is a problem parsing the history file
   */
  public HistoryViewer(String historyFile, Configuration conf, boolean printAll,
                       String format) throws IOException {
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
      String scheme = WebAppUtils.getHttpSchemePrefix(fs.getConf());
      if (HUMAN_FORMAT.equalsIgnoreCase(format)) {
        jhvp = new HumanReadableHistoryViewerPrinter(job, printAll, scheme);
      } else if (JSON_FORMAT.equalsIgnoreCase(format)) {
        jhvp = new JSONHistoryViewerPrinter(job, printAll, scheme);
      } else {
        System.err.println("Invalid format specified: " + format);
        throw new IllegalArgumentException(errorMsg);
      }
    } catch(IOException e) {
      throw new IOException(errorMsg, e);
    }
  }

  /**
   * Print the job/task/attempt summary information to stdout.
   * @throws IOException when there is a problem printing the history
   */
  public void print() throws IOException {
    print(System.out);
  }

  /**
   * Print the job/task/attempt summary information to the PrintStream.
   * @param ps The PrintStream to print to
   * @throws IOException when there is a problem printing the history
   */
  public void print(PrintStream ps) throws IOException {
    jhvp.print(ps);
  }
  
  /**
   * Return the TaskLogsUrl of a particular TaskAttempt.
   * 
   * @param attempt info about the task attempt
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
      int succeededMaps = (int) job.getSucceededMaps();
      int succeededReduces = (int) job.getSucceededReduces();
      mapTasks = 
        new JobHistoryParser.TaskAttemptInfo[succeededMaps];
      reduceTasks = 
        new JobHistoryParser.TaskAttemptInfo[succeededReduces];
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
      if (succeededMaps > 0) {
        avgMapTime /= succeededMaps;
      }
      if (succeededReduces > 0) {
        avgReduceTime /= succeededReduces;
        avgShuffleTime /= succeededReduces;
      }
    }
  }

  /**
   * Utility to filter out events based on the task status
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
