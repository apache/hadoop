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
package org.apache.hadoop.mapreduce.tools;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.jobhistory.HistoryViewer;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.logaggregation.LogCLIHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;

/**
 * Interprets the map reduce cli options 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CLI extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(CLI.class);
  protected Cluster cluster;
  private final Set<String> taskStates = new HashSet<String>(
              Arrays.asList("pending", "running", "completed", "failed", "killed"));
  private static final Set<String> taskTypes = new HashSet<String>(
      Arrays.asList("MAP", "REDUCE"));
  
  public CLI() {
  }
  
  public CLI(Configuration conf) {
    setConf(conf);
  }
  
  public int run(String[] argv) throws Exception {
    int exitCode = -1;
    if (argv.length < 1) {
      displayUsage("");
      return exitCode;
    }    
    // process arguments
    String cmd = argv[0];
    String submitJobFile = null;
    String jobid = null;
    String taskid = null;
    String historyFileOrJobId = null;
    String historyOutFile = null;
    String historyOutFormat = HistoryViewer.HUMAN_FORMAT;
    String counterGroupName = null;
    String counterName = null;
    JobPriority jp = null;
    String taskType = null;
    String taskState = null;
    int fromEvent = 0;
    int nEvents = 0;
    int jpvalue = 0;
    String configOutFile = null;
    boolean getStatus = false;
    boolean getCounter = false;
    boolean killJob = false;
    boolean listEvents = false;
    boolean viewHistory = false;
    boolean viewAllHistory = false;
    boolean listJobs = false;
    boolean listAllJobs = false;
    boolean listActiveTrackers = false;
    boolean listBlacklistedTrackers = false;
    boolean displayTasks = false;
    boolean killTask = false;
    boolean failTask = false;
    boolean setJobPriority = false;
    boolean logs = false;
    boolean downloadConfig = false;

    if ("-submit".equals(cmd)) {
      if (argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      submitJobFile = argv[1];
    } else if ("-status".equals(cmd)) {
      if (argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      getStatus = true;
    } else if("-counter".equals(cmd)) {
      if (argv.length != 4) {
        displayUsage(cmd);
        return exitCode;
      }
      getCounter = true;
      jobid = argv[1];
      counterGroupName = argv[2];
      counterName = argv[3];
    } else if ("-kill".equals(cmd)) {
      if (argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      killJob = true;
    } else if ("-set-priority".equals(cmd)) {
      if (argv.length != 3) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      try {
        jp = JobPriority.valueOf(argv[2]);
      } catch (IllegalArgumentException iae) {
        try {
          jpvalue = Integer.parseInt(argv[2]);
        } catch (NumberFormatException ne) {
          LOG.info("Error number format: ", ne);
          displayUsage(cmd);
          return exitCode;
        }
      }
      setJobPriority = true; 
    } else if ("-events".equals(cmd)) {
      if (argv.length != 4) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      fromEvent = Integer.parseInt(argv[2]);
      nEvents = Integer.parseInt(argv[3]);
      listEvents = true;
    } else if ("-history".equals(cmd)) {
      viewHistory = true;
      if (argv.length < 2 || argv.length > 7) {
        displayUsage(cmd);
        return exitCode;
      }

      // Some arguments are optional while others are not, and some require
      // second arguments.  Due to this, the indexing can vary depending on
      // what's specified and what's left out, as summarized in the below table:
      // [all] <jobHistoryFile|jobId> [-outfile <file>] [-format <human|json>]
      //   1                  2            3       4         5         6
      //   1                  2            3       4
      //   1                  2                              3         4
      //   1                  2
      //                      1            2       3         4         5
      //                      1            2       3
      //                      1                              2         3
      //                      1

      // "all" is optional, but comes first if specified
      int index = 1;
      if ("all".equals(argv[index])) {
        index++;
        viewAllHistory = true;
        if (argv.length == 2) {
          displayUsage(cmd);
          return exitCode;
        }
      }
      // Get the job history file or job id argument
      historyFileOrJobId = argv[index++];
      // "-outfile" is optional, but if specified requires a second argument
      if (argv.length > index + 1 && "-outfile".equals(argv[index])) {
        index++;
        historyOutFile = argv[index++];
      }
      // "-format" is optional, but if specified required a second argument
      if (argv.length > index + 1 && "-format".equals(argv[index])) {
        index++;
        historyOutFormat = argv[index++];
      }
      // Check for any extra arguments that don't belong here
      if (argv.length > index) {
        displayUsage(cmd);
        return exitCode;
      }
    } else if ("-list".equals(cmd)) {
      if (argv.length != 1 && !(argv.length == 2 && "all".equals(argv[1]))) {
        displayUsage(cmd);
        return exitCode;
      }
      if (argv.length == 2 && "all".equals(argv[1])) {
        listAllJobs = true;
      } else {
        listJobs = true;
      }
    } else if("-kill-task".equals(cmd)) {
      if (argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      killTask = true;
      taskid = argv[1];
    } else if("-fail-task".equals(cmd)) {
      if (argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      failTask = true;
      taskid = argv[1];
    } else if ("-list-active-trackers".equals(cmd)) {
      if (argv.length != 1) {
        displayUsage(cmd);
        return exitCode;
      }
      listActiveTrackers = true;
    } else if ("-list-blacklisted-trackers".equals(cmd)) {
      if (argv.length != 1) {
        displayUsage(cmd);
        return exitCode;
      }
      listBlacklistedTrackers = true;
    } else if ("-list-attempt-ids".equals(cmd)) {
      if (argv.length != 4) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      taskType = argv[2];
      taskState = argv[3];
      displayTasks = true;
      if (!taskTypes.contains(
          org.apache.hadoop.util.StringUtils.toUpperCase(taskType))) {
        System.out.println("Error: Invalid task-type: " + taskType);
        displayUsage(cmd);
        return exitCode;
      }
      if (!taskStates.contains(
          org.apache.hadoop.util.StringUtils.toLowerCase(taskState))) {
        System.out.println("Error: Invalid task-state: " + taskState);
        displayUsage(cmd);
        return exitCode;
      }
    } else if ("-logs".equals(cmd)) {
      if (argv.length == 2 || argv.length ==3) {
        logs = true;
        jobid = argv[1];
        if (argv.length == 3) {
          taskid = argv[2];
        }  else {
          taskid = null;
        }
      } else {
        displayUsage(cmd);
        return exitCode;
      }
    } else if ("-config".equals(cmd)) {
      downloadConfig = true;
      if (argv.length != 3) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      configOutFile = argv[2];
    } else {
      displayUsage(cmd);
      return exitCode;
    }

    // initialize cluster
    cluster = createCluster();
        
    // Submit the request
    try {
      if (submitJobFile != null) {
        Job job = Job.getInstance(new JobConf(submitJobFile));
        job.submit();
        System.out.println("Created job " + job.getJobID());
        exitCode = 0;
      } else if (getStatus) {
        Job job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          Counters counters = job.getCounters();
          System.out.println();
          System.out.println(job);
          if (counters != null) {
            System.out.println(counters);
          } else {
            System.out.println("Counters not available. Job is retired.");
          }
          exitCode = 0;
        }
      } else if (getCounter) {
        Job job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          Counters counters = job.getCounters();
          if (counters == null) {
            System.out.println("Counters not available for retired job " + 
            jobid);
            exitCode = -1;
          } else {
            System.out.println(getCounter(counters,
              counterGroupName, counterName));
            exitCode = 0;
          }
        }
      } else if (killJob) {
        Job job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          JobStatus jobStatus = job.getStatus();
          if (jobStatus.getState() == JobStatus.State.FAILED) {
            System.out.println("Could not mark the job " + jobid
                + " as killed, as it has already failed.");
            exitCode = -1;
          } else if (jobStatus.getState() == JobStatus.State.KILLED) {
            System.out
                .println("The job " + jobid + " has already been killed.");
            exitCode = -1;
          } else if (jobStatus.getState() == JobStatus.State.SUCCEEDED) {
            System.out.println("Could not kill the job " + jobid
                + ", as it has already succeeded.");
            exitCode = -1;
          } else {
            job.killJob();
            System.out.println("Killed job " + jobid);
            exitCode = 0;
          }
        }
      } else if (setJobPriority) {
        Job job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          if (jp != null) {
            job.setPriority(jp);
          } else {
            job.setPriorityAsInteger(jpvalue);
          }
          System.out.println("Changed job priority.");
          exitCode = 0;
        } 
      } else if (viewHistory) {
        // If it ends with .jhist, assume it's a jhist file; otherwise, assume
        // it's a Job ID
        if (historyFileOrJobId.endsWith(".jhist")) {
          viewHistory(historyFileOrJobId, viewAllHistory, historyOutFile,
              historyOutFormat);
          exitCode = 0;
        } else {
          Job job = getJob(JobID.forName(historyFileOrJobId));
          if (job == null) {
            System.out.println("Could not find job " + jobid);
          } else {
            String historyUrl = job.getHistoryUrl();
            if (historyUrl == null || historyUrl.isEmpty()) {
              System.out.println("History file for job " + historyFileOrJobId +
                  " is currently unavailable.");
            } else {
              viewHistory(historyUrl, viewAllHistory, historyOutFile,
                  historyOutFormat);
              exitCode = 0;
            }
          }
        }
      } else if (listEvents) {
        Job job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          listEvents(job, fromEvent, nEvents);
          exitCode = 0;
        }
      } else if (listJobs) {
        listJobs(cluster);
        exitCode = 0;
      } else if (listAllJobs) {
        listAllJobs(cluster);
        exitCode = 0;
      } else if (listActiveTrackers) {
        listActiveTrackers(cluster);
        exitCode = 0;
      } else if (listBlacklistedTrackers) {
        listBlacklistedTrackers(cluster);
        exitCode = 0;
      } else if (displayTasks) {
        Job job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          displayTasks(getJob(JobID.forName(jobid)), taskType, taskState);
          exitCode = 0;
        }
      } else if(killTask) {
        TaskAttemptID taskID = TaskAttemptID.forName(taskid);
        Job job = getJob(taskID.getJobID());
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else if (job.killTask(taskID, false)) {
          System.out.println("Killed task " + taskid);
          exitCode = 0;
        } else {
          System.out.println("Could not kill task " + taskid);
          exitCode = -1;
        }
      } else if(failTask) {
        TaskAttemptID taskID = TaskAttemptID.forName(taskid);
        Job job = getJob(taskID.getJobID());
        if (job == null) {
            System.out.println("Could not find job " + jobid);
        } else if(job.killTask(taskID, true)) {
          System.out.println("Killed task " + taskID + " by failing it");
          exitCode = 0;
        } else {
          System.out.println("Could not fail task " + taskid);
          exitCode = -1;
        }
      } else if (logs) {
        JobID jobID = JobID.forName(jobid);
        if (getJob(jobID) == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          try {
            TaskAttemptID taskAttemptID = TaskAttemptID.forName(taskid);
            LogParams logParams = cluster.getLogParams(jobID, taskAttemptID);
            LogCLIHelpers logDumper = new LogCLIHelpers();
            logDumper.setConf(getConf());
            exitCode = logDumper.dumpAContainersLogs(
                    logParams.getApplicationId(), logParams.getContainerId(),
                    logParams.getNodeId(), logParams.getOwner());
          } catch (IOException e) {
            if (e instanceof RemoteException) {
              throw e;
            }
            System.out.println(e.getMessage());
          }
        }
      } else if (downloadConfig) {
        Job job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          String jobFile = job.getJobFile();
          if (jobFile == null || jobFile.isEmpty()) {
            System.out.println("Config file for job " + jobFile +
                " could not be found.");
          } else {
            Path configPath = new Path(jobFile);
            FileSystem fs = FileSystem.get(getConf());
            fs.copyToLocalFile(configPath, new Path(configOutFile));
            exitCode = 0;
          }
        }
      }
    } catch (RemoteException re) {
      IOException unwrappedException = re.unwrapRemoteException();
      if (unwrappedException instanceof AccessControlException) {
        System.out.println(unwrappedException.getMessage());
      } else {
        throw re;
      }
    } finally {
      cluster.close();
    }
    return exitCode;
  }

  Cluster createCluster() throws IOException {
    return new Cluster(getConf());
  }
  
  private String getJobPriorityNames() {
    StringBuffer sb = new StringBuffer();
    for (JobPriority p : JobPriority.values()) {
      // UNDEFINED_PRIORITY need not to be displayed in usage
      if (JobPriority.UNDEFINED_PRIORITY == p) {
        continue;
      }
      sb.append(p.name()).append(" ");
    }
    return sb.substring(0, sb.length()-1);
  }

  private String getTaskTypes() {
    return StringUtils.join(taskTypes, " ");
  }
  
  /**
   * Display usage of the command-line tool and terminate execution.
   */
  private void displayUsage(String cmd) {
    String prefix = "Usage: job ";
    String jobPriorityValues = getJobPriorityNames();
    String taskStates = "pending, running, completed, failed, killed";
    
    if ("-submit".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-file>]");
    } else if ("-status".equals(cmd) || "-kill".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-id>]");
    } else if ("-counter".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + 
        " <job-id> <group-name> <counter-name>]");
    } else if ("-events".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + 
        " <job-id> <from-event-#> <#-of-events>]. Event #s start from 1.");
    } else if ("-history".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " [all] <jobHistoryFile|jobId> " +
          "[-outfile <file>] [-format <human|json>]]");
    } else if ("-list".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " [all]]");
    } else if ("-kill-task".equals(cmd) || "-fail-task".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <task-attempt-id>]");
    } else if ("-set-priority".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-id> <priority>]. " +
          "Valid values for priorities are: " 
          + jobPriorityValues
          + ". In addition to this, integers also can be used.");
    } else if ("-list-active-trackers".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + "]");
    } else if ("-list-blacklisted-trackers".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + "]");
    } else if ("-list-attempt-ids".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + 
          " <job-id> <task-type> <task-state>]. " +
          "Valid values for <task-type> are " + getTaskTypes() + ". " +
          "Valid values for <task-state> are " + taskStates);
    } else if ("-logs".equals(cmd)) {
      System.err.println(prefix + "[" + cmd +
          " <job-id> <task-attempt-id>]. " +
          " <task-attempt-id> is optional to get task attempt logs.");
    } else if ("-config".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-id> <file>]");
    } else {
      System.err.printf(prefix + "<command> <args>%n");
      System.err.printf("\t[-submit <job-file>]%n");
      System.err.printf("\t[-status <job-id>]%n");
      System.err.printf("\t[-counter <job-id> <group-name> <counter-name>]%n");
      System.err.printf("\t[-kill <job-id>]%n");
      System.err.printf("\t[-set-priority <job-id> <priority>]. " +
          "Valid values for priorities are: " + jobPriorityValues +
          ". In addition to this, integers also can be used." + "%n");
      System.err.printf("\t[-events <job-id> <from-event-#> <#-of-events>]%n");
      System.err.printf("\t[-history [all] <jobHistoryFile|jobId> " +
          "[-outfile <file>] [-format <human|json>]]%n");
      System.err.printf("\t[-list [all]]%n");
      System.err.printf("\t[-list-active-trackers]%n");
      System.err.printf("\t[-list-blacklisted-trackers]%n");
      System.err.println("\t[-list-attempt-ids <job-id> <task-type> " +
        "<task-state>]. " +
        "Valid values for <task-type> are " + getTaskTypes() + ". " +
        "Valid values for <task-state> are " + taskStates);
      System.err.printf("\t[-kill-task <task-attempt-id>]%n");
      System.err.printf("\t[-fail-task <task-attempt-id>]%n");
      System.err.printf("\t[-logs <job-id> <task-attempt-id>]%n");
      System.err.printf("\t[-config <job-id> <file>%n%n");
      ToolRunner.printGenericCommandUsage(System.out);
    }
  }
    
  private void viewHistory(String historyFile, boolean all,
      String historyOutFile, String format) throws IOException {
    HistoryViewer historyViewer = new HistoryViewer(historyFile,
        getConf(), all, format);
    PrintStream ps = System.out;
    if (historyOutFile != null) {
      ps = new PrintStream(new BufferedOutputStream(new FileOutputStream(
          new File(historyOutFile))), true, "UTF-8");
    }
    historyViewer.print(ps);
  }

  protected long getCounter(Counters counters, String counterGroupName,
      String counterName) throws IOException {
    return counters.findCounter(counterGroupName, counterName).getValue();
  }
  
  /**
   * List the events for the given job
   * @param job the job to list
   * @param fromEventId event id for the job's events to list from
   * @param numEvents number of events we want to list
   * @throws IOException
   */
  private void listEvents(Job job, int fromEventId, int numEvents)
      throws IOException, InterruptedException {
    TaskCompletionEvent[] events = job.
      getTaskCompletionEvents(fromEventId, numEvents);
    System.out.println("Task completion events for " + job.getJobID());
    System.out.println("Number of events (from " + fromEventId + ") are: " 
      + events.length);
    for(TaskCompletionEvent event: events) {
      System.out.println(event.getStatus() + " " + 
        event.getTaskAttemptId() + " " + 
        getTaskLogURL(event.getTaskAttemptId(), event.getTaskTrackerHttp()));
    }
  }

  protected static String getTaskLogURL(TaskAttemptID taskId, String baseUrl) {
    return (baseUrl + "/tasklog?plaintext=true&attemptid=" + taskId); 
  }

  @VisibleForTesting
  Job getJob(JobID jobid) throws IOException, InterruptedException {

    int maxRetry = getConf().getInt(MRJobConfig.MR_CLIENT_JOB_MAX_RETRIES,
        MRJobConfig.DEFAULT_MR_CLIENT_JOB_MAX_RETRIES);
    long retryInterval = getConf()
        .getLong(MRJobConfig.MR_CLIENT_JOB_RETRY_INTERVAL,
            MRJobConfig.DEFAULT_MR_CLIENT_JOB_RETRY_INTERVAL);
    Job job = cluster.getJob(jobid);

    for (int i = 0; i < maxRetry; ++i) {
      if (job != null) {
        return job;
      }
      LOG.info("Could not obtain job info after " + String.valueOf(i + 1)
          + " attempt(s). Sleeping for " + String.valueOf(retryInterval / 1000)
          + " seconds and retrying.");
      Thread.sleep(retryInterval);
      job = cluster.getJob(jobid);
    }
    return job;
  }
  

  /**
   * Dump a list of currently running jobs
   * @throws IOException
   */
  private void listJobs(Cluster cluster) 
      throws IOException, InterruptedException {
    List<JobStatus> runningJobs = new ArrayList<JobStatus>();
    for (JobStatus job : cluster.getAllJobStatuses()) {
      if (!job.isJobComplete()) {
        runningJobs.add(job);
      }
    }
    displayJobList(runningJobs.toArray(new JobStatus[0]));
  }
    
  /**
   * Dump a list of all jobs submitted.
   * @throws IOException
   */
  private void listAllJobs(Cluster cluster) 
      throws IOException, InterruptedException {
    displayJobList(cluster.getAllJobStatuses());
  }
  
  /**
   * Display the list of active trackers
   */
  private void listActiveTrackers(Cluster cluster) 
      throws IOException, InterruptedException {
    TaskTrackerInfo[] trackers = cluster.getActiveTaskTrackers();
    for (TaskTrackerInfo tracker : trackers) {
      System.out.println(tracker.getTaskTrackerName());
    }
  }

  /**
   * Display the list of blacklisted trackers
   */
  private void listBlacklistedTrackers(Cluster cluster) 
      throws IOException, InterruptedException {
    TaskTrackerInfo[] trackers = cluster.getBlackListedTaskTrackers();
    if (trackers.length > 0) {
      System.out.println("BlackListedNode \t Reason");
    }
    for (TaskTrackerInfo tracker : trackers) {
      System.out.println(tracker.getTaskTrackerName() + "\t" + 
        tracker.getReasonForBlacklist());
    }
  }

  private void printTaskAttempts(TaskReport report) {
    if (report.getCurrentStatus() == TIPStatus.COMPLETE) {
      System.out.println(report.getSuccessfulTaskAttemptId());
    } else if (report.getCurrentStatus() == TIPStatus.RUNNING) {
      for (TaskAttemptID t : 
        report.getRunningTaskAttemptIds()) {
        System.out.println(t);
      }
    }
  }

  /**
   * Display the information about a job's tasks, of a particular type and
   * in a particular state
   * 
   * @param job the job
   * @param type the type of the task (map/reduce/setup/cleanup)
   * @param state the state of the task 
   * (pending/running/completed/failed/killed)
   * @throws IOException when there is an error communicating with the master
   * @throws InterruptedException
   * @throws IllegalArgumentException if an invalid type/state is passed
   */
  protected void displayTasks(Job job, String type, String state) 
  throws IOException, InterruptedException {
	  
    TaskReport[] reports=null;
    reports = job.getTaskReports(TaskType.valueOf(
        org.apache.hadoop.util.StringUtils.toUpperCase(type)));
    for (TaskReport report : reports) {
      TIPStatus status = report.getCurrentStatus();
      if ((state.equalsIgnoreCase("pending") && status ==TIPStatus.PENDING) ||
          (state.equalsIgnoreCase("running") && status ==TIPStatus.RUNNING) ||
          (state.equalsIgnoreCase("completed") && status == TIPStatus.COMPLETE) ||
          (state.equalsIgnoreCase("failed") && status == TIPStatus.FAILED) ||
          (state.equalsIgnoreCase("killed") && status == TIPStatus.KILLED)) {
        printTaskAttempts(report);
      }
    }
  }

  public void displayJobList(JobStatus[] jobs) 
      throws IOException, InterruptedException {
    displayJobList(jobs, new PrintWriter(new OutputStreamWriter(System.out,
        Charsets.UTF_8)));
  }

  @Private
  public static String headerPattern = "%23s\t%20s\t%10s\t%14s\t%12s\t%12s" +
      "\t%10s\t%15s\t%15s\t%8s\t%8s\t%10s\t%10s\n";
  @Private
  public static String dataPattern   = "%23s\t%20s\t%10s\t%14d\t%12s\t%12s" +
      "\t%10s\t%15s\t%15s\t%8s\t%8s\t%10s\t%10s\n";
  private static String memPattern   = "%dM";
  private static String UNAVAILABLE  = "N/A";

  @Private
  public void displayJobList(JobStatus[] jobs, PrintWriter writer) {
    writer.println("Total jobs:" + jobs.length);
    writer.printf(headerPattern, "JobId", "JobName", "State", "StartTime",
      "UserName", "Queue", "Priority", "UsedContainers",
      "RsvdContainers", "UsedMem", "RsvdMem", "NeededMem", "AM info");
    for (JobStatus job : jobs) {
      int numUsedSlots = job.getNumUsedSlots();
      int numReservedSlots = job.getNumReservedSlots();
      long usedMem = job.getUsedMem();
      long rsvdMem = job.getReservedMem();
      long neededMem = job.getNeededMem();
      int jobNameLength = job.getJobName().length();
      writer.printf(dataPattern, job.getJobID().toString(),
          job.getJobName().substring(0, jobNameLength > 20 ? 20 : jobNameLength),
          job.getState(), job.getStartTime(), job.getUsername(),
          job.getQueue(), job.getPriority().name(),
          numUsedSlots < 0 ? UNAVAILABLE : numUsedSlots,
          numReservedSlots < 0 ? UNAVAILABLE : numReservedSlots,
          usedMem < 0 ? UNAVAILABLE : String.format(memPattern, usedMem),
          rsvdMem < 0 ? UNAVAILABLE : String.format(memPattern, rsvdMem),
          neededMem < 0 ? UNAVAILABLE : String.format(memPattern, neededMem),
          job.getSchedulingInfo());
    }
    writer.flush();
  }
  
  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new CLI(), argv);
    ExitUtil.terminate(res);
  }
}
