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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

/**
 * Used by the {@link HistoryViewer} to print job history in a human-readable
 * format.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class HumanReadableHistoryViewerPrinter implements HistoryViewerPrinter {

  private JobHistoryParser.JobInfo job;
  private final SimpleDateFormat dateFormat;
  private boolean printAll;
  private String scheme;

  HumanReadableHistoryViewerPrinter(JobHistoryParser.JobInfo job,
                                    boolean printAll, String scheme) {
    this.job = job;
    this.printAll = printAll;
    this.scheme = scheme;
    this.dateFormat = new SimpleDateFormat("d-MMM-yyyy HH:mm:ss");
  }

  HumanReadableHistoryViewerPrinter(JobHistoryParser.JobInfo job,
                                    boolean printAll, String scheme,
                                    TimeZone tz) {
    this(job, printAll, scheme);
    this.dateFormat.setTimeZone(tz);
  }

  /**
   * Print out the Job History to the given {@link PrintStream} in a
   * human-readable format.
   * @param ps the {@link PrintStream} to print to
   * @throws IOException when a problem occurs while printing
   */
  @Override
  public void print(PrintStream ps) throws IOException {
    printJobDetails(ps);
    printTaskSummary(ps);
    printJobAnalysis(ps);
    printTasks(ps, TaskType.JOB_SETUP, TaskStatus.State.FAILED.toString());
    printTasks(ps, TaskType.JOB_SETUP, TaskStatus.State.KILLED.toString());
    printTasks(ps, TaskType.MAP, TaskStatus.State.FAILED.toString());
    printTasks(ps, TaskType.MAP, TaskStatus.State.KILLED.toString());
    printTasks(ps, TaskType.REDUCE, TaskStatus.State.FAILED.toString());
    printTasks(ps, TaskType.REDUCE, TaskStatus.State.KILLED.toString());
    printTasks(ps, TaskType.JOB_CLEANUP, TaskStatus.State.FAILED.toString());
    printTasks(ps, TaskType.JOB_CLEANUP,
        JobStatus.getJobRunState(JobStatus.KILLED));
    if (printAll) {
      printTasks(ps, TaskType.JOB_SETUP, TaskStatus.State.SUCCEEDED.toString());
      printTasks(ps, TaskType.MAP, TaskStatus.State.SUCCEEDED.toString());
      printTasks(ps, TaskType.REDUCE, TaskStatus.State.SUCCEEDED.toString());
      printTasks(ps, TaskType.JOB_CLEANUP,
          TaskStatus.State.SUCCEEDED.toString());
      printAllTaskAttempts(ps, TaskType.JOB_SETUP);
      printAllTaskAttempts(ps, TaskType.MAP);
      printAllTaskAttempts(ps, TaskType.REDUCE);
      printAllTaskAttempts(ps, TaskType.JOB_CLEANUP);
    }

    HistoryViewer.FilteredJob filter = new HistoryViewer.FilteredJob(job,
        TaskStatus.State.FAILED.toString());
    printFailedAttempts(ps, filter);

    filter = new HistoryViewer.FilteredJob(job,
        TaskStatus.State.KILLED.toString());
    printFailedAttempts(ps, filter);
  }

  private void printJobDetails(PrintStream ps) {
    StringBuilder jobDetails = new StringBuilder();
    jobDetails.append("\nHadoop job: ").append(job.getJobId());
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
    printJobCounters(jobDetails, job.getTotalCounters(), job.getMapCounters(),
        job.getReduceCounters());
    jobDetails.append("\n");
    jobDetails.append("\n=====================================");
    ps.println(jobDetails);
  }

  private void printJobCounters(StringBuilder buff, Counters totalCounters,
                                Counters mapCounters, Counters reduceCounters) {
    // Killed jobs might not have counters
    if (totalCounters != null) {
      buff.append("\nCounters: \n\n");
      buff.append(String.format("|%1$-30s|%2$-30s|%3$-10s|%4$-10s|%5$-10s|",
          "Group Name",
          "Counter name",
          "Map Value",
          "Reduce Value",
          "Total Value"));
      buff.append("\n------------------------------------------" +
          "---------------------------------------------");
      for (String groupName : totalCounters.getGroupNames()) {
        CounterGroup totalGroup = totalCounters.getGroup(groupName);
        CounterGroup mapGroup = mapCounters.getGroup(groupName);
        CounterGroup reduceGroup = reduceCounters.getGroup(groupName);

        Format decimal = new DecimalFormat();
        Iterator<Counter> ctrItr =
            totalGroup.iterator();
        while (ctrItr.hasNext()) {
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
  }

  private void printAllTaskAttempts(PrintStream ps, TaskType taskType) {
    Map<TaskID, JobHistoryParser.TaskInfo> tasks = job.getAllTasks();
    StringBuilder taskList = new StringBuilder();
    taskList.append("\n").append(taskType);
    taskList.append(" task list for ").append(job.getJobId());
    taskList.append("\nTaskId\t\tStartTime");
    if (TaskType.REDUCE.equals(taskType)) {
      taskList.append("\tShuffleFinished\tSortFinished");
    }
    taskList.append("\tFinishTime\tHostName\tError\tTaskLogs");
    taskList.append("\n====================================================");
    ps.println(taskList.toString());
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
          String taskLogsUrl = HistoryViewer.getTaskLogsUrl(scheme, attempt);
          taskList.append(taskLogsUrl != null ? taskLogsUrl : "n/a");
          ps.println(taskList);
        }
      }
    }
  }

  private void printTaskSummary(PrintStream ps) {
    HistoryViewer.SummarizedJob ts = new HistoryViewer.SummarizedJob(job);
    StringBuilder taskSummary = new StringBuilder();
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
    ps.println(taskSummary);
  }

  private void printJobAnalysis(PrintStream ps) {
    if (job.getJobStatus().equals(
        JobStatus.getJobRunState(JobStatus.SUCCEEDED))) {
      HistoryViewer.AnalyzedJob avg = new HistoryViewer.AnalyzedJob(job);

      ps.println("\nAnalysis");
      ps.println("=========");
      printAnalysis(ps, avg.getMapTasks(), cMap, "map", avg.getAvgMapTime(),
          10);
      printLast(ps, avg.getMapTasks(), "map", cFinishMapRed);

      if (avg.getReduceTasks().length > 0) {
        printAnalysis(ps, avg.getReduceTasks(), cShuffle, "shuffle",
            avg.getAvgShuffleTime(), 10);
        printLast(ps, avg.getReduceTasks(), "shuffle", cFinishShuffle);

        printAnalysis(ps, avg.getReduceTasks(), cReduce, "reduce",
            avg.getAvgReduceTime(), 10);
        printLast(ps, avg.getReduceTasks(), "reduce", cFinishMapRed);
      }
      ps.println("=========");
    } else {
      ps.println("No Analysis available as job did not finish");
    }
  }

  protected void printAnalysis(PrintStream ps,
      JobHistoryParser.TaskAttemptInfo[] tasks,
      Comparator<JobHistoryParser.TaskAttemptInfo> cmp,
      String taskType, long avg, int showTasks) {
    Arrays.sort(tasks, cmp);
    JobHistoryParser.TaskAttemptInfo min = tasks[tasks.length-1];
    StringBuilder details = new StringBuilder();
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
    ps.println(details);
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
      ps.println(details);
    }
  }

  protected void printLast(PrintStream ps,
      JobHistoryParser.TaskAttemptInfo[] tasks, String taskType,
      Comparator<JobHistoryParser.TaskAttemptInfo> cmp) {
    Arrays.sort(tasks, cFinishMapRed);
    JobHistoryParser.TaskAttemptInfo last = tasks[0];
    StringBuilder lastBuf = new StringBuilder();
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
    ps.println(lastBuf);
  }

  private void printTasks(PrintStream ps, TaskType taskType, String status) {
    Map<TaskID, JobHistoryParser.TaskInfo> tasks = job.getAllTasks();
    StringBuilder header = new StringBuilder();
    header.append("\n").append(status).append(" ");
    header.append(taskType).append(" task list for ")
        .append(job.getJobId().toString());
    header.append("\nTaskId\t\tStartTime\tFinishTime\tError");
    if (TaskType.MAP.equals(taskType)) {
      header.append("\tInputSplits");
    }
    header.append("\n====================================================");
    StringBuilder taskList = new StringBuilder();
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
          ps.println(header);
          ps.println(taskList);
        }
      }
    }
  }

  private void printFailedAttempts(PrintStream ps,
                                   HistoryViewer.FilteredJob filteredJob) {
    Map<String, Set<TaskID>> badNodes = filteredJob.getFilteredMap();
    StringBuilder attempts = new StringBuilder();
    if (badNodes.size() > 0) {
      attempts.append("\n").append(filteredJob.getFilter());
      attempts.append(" task attempts by nodes");
      attempts.append("\nHostname\tFailedTasks");
      attempts.append("\n===============================");
      ps.println(attempts);
      for (Map.Entry<String, Set<TaskID>> entry : badNodes.entrySet()) {
        String node = entry.getKey();
        Set<TaskID> failedTasks = entry.getValue();
        attempts.setLength(0);
        attempts.append(node).append("\t");
        for (TaskID t : failedTasks) {
          attempts.append(t).append(", ");
        }
        ps.println(attempts);
      }
    }
  }

  private static Comparator<JobHistoryParser.TaskAttemptInfo> cMap =
      new Comparator<JobHistoryParser.TaskAttemptInfo>() {
        public int compare(JobHistoryParser.TaskAttemptInfo t1,
                           JobHistoryParser.TaskAttemptInfo t2) {
          long l1 = t1.getFinishTime() - t1.getStartTime();
          long l2 = t2.getFinishTime() - t2.getStartTime();
          return Long.compare(l2, l1);
        }
      };

  private static Comparator<JobHistoryParser.TaskAttemptInfo> cShuffle =
      new Comparator<JobHistoryParser.TaskAttemptInfo>() {
        public int compare(JobHistoryParser.TaskAttemptInfo t1,
                           JobHistoryParser.TaskAttemptInfo t2) {
          long l1 = t1.getShuffleFinishTime() - t1.getStartTime();
          long l2 = t2.getShuffleFinishTime() - t2.getStartTime();
          return Long.compare(l2, l1);
        }
      };

  private static Comparator<JobHistoryParser.TaskAttemptInfo> cFinishShuffle =
      new Comparator<JobHistoryParser.TaskAttemptInfo>() {
        public int compare(JobHistoryParser.TaskAttemptInfo t1,
                           JobHistoryParser.TaskAttemptInfo t2) {
          long l1 = t1.getShuffleFinishTime();
          long l2 = t2.getShuffleFinishTime();
          return Long.compare(l2, l1);
        }
      };

  private static Comparator<JobHistoryParser.TaskAttemptInfo> cFinishMapRed =
      new Comparator<JobHistoryParser.TaskAttemptInfo>() {
        public int compare(JobHistoryParser.TaskAttemptInfo t1,
                           JobHistoryParser.TaskAttemptInfo t2) {
          long l1 = t1.getFinishTime();
          long l2 = t2.getFinishTime();
          return Long.compare(l2, l1);
        }
      };

  private static Comparator<JobHistoryParser.TaskAttemptInfo> cReduce =
      new Comparator<JobHistoryParser.TaskAttemptInfo>() {
        public int compare(JobHistoryParser.TaskAttemptInfo t1,
                           JobHistoryParser.TaskAttemptInfo t2) {
          long l1 = t1.getFinishTime() - t1.getShuffleFinishTime();
          long l2 = t2.getFinishTime() - t2.getShuffleFinishTime();
          return Long.compare(l2, l1);
        }
      };
}
