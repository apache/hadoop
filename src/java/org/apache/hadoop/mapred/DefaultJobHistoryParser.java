package org.apache.hadoop.mapred;

import java.util.*;
import java.io.*;
import org.apache.hadoop.mapred.JobHistory.Keys ; 

/**
 * Default parser for job history files. It creates object model from 
 * job history file. 
 * 
 */
public class DefaultJobHistoryParser {

  /**
   * Parses a master index file and returns a Map of 
   * (jobTrakerId - Map (job Id - JobHistory.JobInfo)). 
   * @param historyFile master index history file. 
   * @return a map of values, as described as described earlier.  
   * @throws IOException
   */
  public static Map<String, Map<String, JobHistory.JobInfo>> parseMasterIndex(File historyFile)
      throws IOException {
    MasterIndexParseListener parser = new MasterIndexParseListener();
    JobHistory.parseHistory(historyFile, parser);

    return parser.getValues();
  }

  /**
   * Populates a JobInfo object from the job's history log file. 
   * @param jobHistoryFile history file for this job. 
   * @param job a precreated JobInfo object, should be non-null. 
   * @throws IOException
   */
  public static void parseJobTasks(File jobHistoryFile, JobHistory.JobInfo job)
      throws IOException {
    JobHistory.parseHistory(jobHistoryFile, 
        new JobTasksParseListener(job));
  }
/**
 * Listener for Job's history log file, it populates JobHistory.JobInfo 
 * object with data from log file. 
 */
  static class JobTasksParseListener
      implements JobHistory.Listener {
    JobHistory.JobInfo job;

    JobTasksParseListener(JobHistory.JobInfo job) {
      this.job = job;
    }

    private JobHistory.Task getTask(String taskId) {
      JobHistory.Task task = job.getAllTasks().get(taskId);
      if (null == task) {
        task = new JobHistory.Task();
        task.set(Keys.TASKID, taskId);
        job.getAllTasks().put(taskId, task);
      }
      return task;
    }

    private JobHistory.MapAttempt getMapAttempt(
        String jobid, String jobTrackerId, String taskId, String taskAttemptId) {

      JobHistory.Task task = getTask(taskId);
      JobHistory.MapAttempt mapAttempt = 
        (JobHistory.MapAttempt) task.getTaskAttempts().get(taskAttemptId);
      if (null == mapAttempt) {
        mapAttempt = new JobHistory.MapAttempt();
        mapAttempt.set(Keys.TASK_ATTEMPT_ID, taskAttemptId);
        task.getTaskAttempts().put(taskAttemptId, mapAttempt);
      }
      return mapAttempt;
    }

    private JobHistory.ReduceAttempt getReduceAttempt(
        String jobid, String jobTrackerId, String taskId, String taskAttemptId) {

      JobHistory.Task task = getTask(taskId);
      JobHistory.ReduceAttempt reduceAttempt = 
        (JobHistory.ReduceAttempt) task.getTaskAttempts().get(taskAttemptId);
      if (null == reduceAttempt) {
        reduceAttempt = new JobHistory.ReduceAttempt();
        reduceAttempt.set(Keys.TASK_ATTEMPT_ID, taskAttemptId);
        task.getTaskAttempts().put(taskAttemptId, reduceAttempt);
      }
      return reduceAttempt;
    }

    // JobHistory.Listener implementation 
    public void handle(JobHistory.RecordTypes recType, Map<Keys, String> values)
        throws IOException {
      String jobTrackerId = values.get(JobHistory.Keys.JOBTRACKERID);
      String jobid = values.get(Keys.JOBID);
      
      if (recType == JobHistory.RecordTypes.Job) {
        job.handle(values);
      }if (recType.equals(JobHistory.RecordTypes.Task)) {
        String taskid = values.get(JobHistory.Keys.TASKID);
        getTask(taskid).handle(values);
      } else if (recType.equals(JobHistory.RecordTypes.MapAttempt)) {
        String taskid =  values.get(Keys.TASKID);
        String mapAttemptId = values.get(Keys.TASK_ATTEMPT_ID);

        getMapAttempt(jobid, jobTrackerId, taskid, mapAttemptId).handle(values);
      } else if (recType.equals(JobHistory.RecordTypes.ReduceAttempt)) {
        String taskid = values.get(Keys.TASKID);
        String reduceAttemptId = values.get(Keys.TASK_ATTEMPT_ID);

        getReduceAttempt(jobid, jobTrackerId, taskid, reduceAttemptId).handle(values);
      }
    }
  }

  /**
   * Parses and returns a map of values in master index. 
   * @author sanjaydahiya
   * 
   */
  static class MasterIndexParseListener
      implements JobHistory.Listener {
    Map<String, Map<String, JobHistory.JobInfo>> jobTrackerToJobs = new TreeMap<String, Map<String, JobHistory.JobInfo>>();

    Map<String, JobHistory.JobInfo> activeJobs = null;
    String currentTracker ; 
    
    // Implement JobHistory.Listener

    public void handle(JobHistory.RecordTypes recType, Map<Keys, String> values)
        throws IOException {
 
      if (recType.equals(JobHistory.RecordTypes.Jobtracker)) {
        activeJobs = new TreeMap<String, JobHistory.JobInfo>();
        currentTracker = values.get(Keys.START_TIME);
        jobTrackerToJobs.put(currentTracker, activeJobs);
      } else if (recType.equals(JobHistory.RecordTypes.Job)) {
        String jobId = (String) values.get(Keys.JOBID);
        JobHistory.JobInfo job = activeJobs.get(jobId);
        if (null == job) {
          job = new JobHistory.JobInfo(jobId);
          job.set(Keys.JOBTRACKERID, currentTracker);
          activeJobs.put(jobId, job);
        }
        job.handle(values);
      }
    }

    /**
     * Return map of parsed values. 
     * @return
     */ 
    Map<String, Map<String, JobHistory.JobInfo>> getValues() {
      return jobTrackerToJobs;
    }
  }
}
