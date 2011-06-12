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

package org.apache.hadoop.mapreduce.v2.app;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;


public class MockJobs extends MockApps {
  static final Iterator<JobState> JOB_STATES = Iterators.cycle(
      JobState.values());
  static final Iterator<TaskState> TASK_STATES = Iterators.cycle(
      TaskState.values());
  static final Iterator<TaskAttemptState> TASK_ATTEMPT_STATES = Iterators.cycle(
      TaskAttemptState.values());
  static final Iterator<TaskType> TASK_TYPES = Iterators.cycle(
      TaskType.values());
  static final Iterator<JobCounter> JOB_COUNTERS = Iterators.cycle(
      JobCounter.values());
  static final Iterator<FileSystemCounter> FS_COUNTERS = Iterators.cycle(
      FileSystemCounter.values());
  static final Iterator<TaskCounter> TASK_COUNTERS = Iterators.cycle(
      TaskCounter.values());
  static final Iterator<String> FS_SCHEMES = Iterators.cycle("FILE", "HDFS",
      "LAFS", "CEPH");
  static final Iterator<String> USER_COUNTER_GROUPS = Iterators.cycle(
      "com.company.project.subproject.component.subcomponent.UserDefinedSpecificSpecialTask$Counters",
      "PigCounters");
  static final Iterator<String> USER_COUNTERS = Iterators.cycle(
      "counter1", "counter2", "counter3");
  static final Iterator<Phase> PHASES = Iterators.cycle(Phase.values());
  static final Iterator<String> DIAGS = Iterators.cycle(
      "Error: java.lang.OutOfMemoryError: Java heap space",
      "Lost task tracker: tasktracker.domain/127.0.0.1:40879");
  static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  public static String newJobName() {
    return newAppName();
  }

  public static Map<JobId, Job> newJobs(ApplicationId appID, int numJobsPerApp,
                                        int numTasksPerJob,
                                        int numAttemptsPerTask) {
    Map<JobId, Job> map = Maps.newHashMap();
    for (int j = 0; j < numJobsPerApp; ++j) {
      Job job = newJob(appID, j, numTasksPerJob, numAttemptsPerTask);
      map.put(job.getID(), job);
    }
    return map;
  }

  public static JobId newJobID(ApplicationId appID, int i) {
    JobId id = recordFactory.newRecordInstance(JobId.class);
    id.setAppId(appID);
    id.setId(i);
    return id;
  }

  public static JobReport newJobReport(JobId id) {
    JobReport report = recordFactory.newRecordInstance(JobReport.class);
    report.setJobId(id);
    report.setStartTime(System.currentTimeMillis() - (int)(Math.random() * 1000000));
    report.setFinishTime(System.currentTimeMillis() + (int)(Math.random() * 1000000) + 1);
    report.setMapProgress((float)Math.random());
    report.setReduceProgress((float)Math.random());
    report.setJobState(JOB_STATES.next());
    return report;
  }

  public static TaskReport newTaskReport(TaskId id) {
    TaskReport report = recordFactory.newRecordInstance(TaskReport.class);
    report.setTaskId(id);
    report.setStartTime(System.currentTimeMillis() - (int)(Math.random() * 1000000));
    report.setFinishTime(System.currentTimeMillis() + (int)(Math.random() * 1000000) + 1);
    report.setProgress((float)Math.random());
    report.setCounters(newCounters());
    report.setTaskState(TASK_STATES.next());
    return report;
  }

  public static TaskAttemptReport newTaskAttemptReport(TaskAttemptId id) {
    TaskAttemptReport report = recordFactory.newRecordInstance(TaskAttemptReport.class);
    report.setTaskAttemptId(id);
    report.setStartTime(System.currentTimeMillis() - (int)(Math.random() * 1000000));
    report.setFinishTime(System.currentTimeMillis() + (int)(Math.random() * 1000000) + 1);
    report.setPhase(PHASES.next());
    report.setTaskAttemptState(TASK_ATTEMPT_STATES.next());
    report.setProgress((float)Math.random());
    report.setCounters(newCounters());
    return report;
  }

  @SuppressWarnings("deprecation")
  public static Counters newCounters() {
    org.apache.hadoop.mapred.Counters hc =
        new org.apache.hadoop.mapred.Counters();
    for (JobCounter c : JobCounter.values()) {
      hc.findCounter(c).setValue((long)(Math.random() * 1000));
    }
    for (TaskCounter c : TaskCounter.values()) {
      hc.findCounter(c).setValue((long)(Math.random() * 1000));
    }
    int nc = FileSystemCounter.values().length * 4;
    for (int i = 0; i < nc; ++i) {
      for (FileSystemCounter c : FileSystemCounter.values()) {
        hc.findCounter(FS_SCHEMES.next(), c).
            setValue((long)(Math.random() * 1000000));
      }
    }
    for (int i = 0; i < 2 * 3; ++i) {
      hc.findCounter(USER_COUNTER_GROUPS.next(), USER_COUNTERS.next()).
          setValue((long)(Math.random() * 100000));
    }
    return TypeConverter.toYarn(hc);
  }

  public static Map<TaskAttemptId, TaskAttempt> newTaskAttempts(TaskId tid,
                                                                int m) {
    Map<TaskAttemptId, TaskAttempt> map = Maps.newHashMap();
    for (int i = 0; i < m; ++i) {
      TaskAttempt ta = newTaskAttempt(tid, i);
      map.put(ta.getID(), ta);
    }
    return map;
  }

  public static TaskAttempt newTaskAttempt(TaskId tid, int i) {
    final TaskAttemptId taid = recordFactory.newRecordInstance(TaskAttemptId.class);
    taid.setTaskId(tid);
    taid.setId(i);
    final TaskAttemptReport report = newTaskAttemptReport(taid);
    final List<String> diags = Lists.newArrayList();
    diags.add(DIAGS.next());
    return new TaskAttempt() {
      @Override
      public TaskAttemptId getID() {
        return taid;
      }

      @Override
      public TaskAttemptReport getReport() {
        return report;
      }

      @Override
      public long getLaunchTime() {
        return 0;
      }

      @Override
      public long getFinishTime() {
        return 0;
      }

      @Override
      public Counters getCounters() {
        return report.getCounters();
      }

      @Override
      public float getProgress() {
        return report.getProgress();
      }

      @Override
      public TaskAttemptState getState() {
        return report.getTaskAttemptState();
      }

      @Override
      public boolean isFinished() {
        switch (report.getTaskAttemptState()) {
          case SUCCEEDED:
          case FAILED:
          case KILLED: return true;
        }
        return false;
      }

      @Override
      public ContainerId getAssignedContainerID() {
        ContainerId id = recordFactory.newRecordInstance(ContainerId.class);
        id.setAppId(taid.getTaskId().getJobId().getAppId());
        return id;
      }

      @Override
      public String getNodeHttpAddress() {
        return "localhost:9999";
      }

      @Override
      public List<String> getDiagnostics() {
        return diags;
      }
    };
  }

  public static Map<TaskId, Task> newTasks(JobId jid, int n, int m) {
    Map<TaskId, Task> map = Maps.newHashMap();
    for (int i = 0; i < n; ++i) {
      Task task = newTask(jid, i, m);
      map.put(task.getID(), task);
    }
    return map;
  }

  public static Task newTask(JobId jid, int i, int m) {
    final TaskId tid = recordFactory.newRecordInstance(TaskId.class);
    tid.setJobId(jid);
    tid.setId(i);
    tid.setTaskType(TASK_TYPES.next());
    final TaskReport report = newTaskReport(tid);
    final Map<TaskAttemptId, TaskAttempt> attempts = newTaskAttempts(tid, m);
    return new Task() {
      @Override
      public TaskId getID() {
        return tid;
      }

      @Override
      public TaskReport getReport() {
        return report;
      }

      @Override
      public Counters getCounters() {
        return report.getCounters();
      }

      @Override
      public float getProgress() {
        return report.getProgress();
      }

      @Override
      public TaskType getType() {
        return tid.getTaskType();
      }

      @Override
      public Map<TaskAttemptId, TaskAttempt> getAttempts() {
        return attempts;
      }

      @Override
      public TaskAttempt getAttempt(TaskAttemptId attemptID) {
        return attempts.get(attemptID);
      }

      @Override
      public boolean isFinished() {
        switch (report.getTaskState()) {
          case SUCCEEDED:
          case KILLED:
          case FAILED: return true;
        }
        return false;
      }

      @Override
      public boolean canCommit(TaskAttemptId taskAttemptID) {
        return false;
      }

      @Override
      public TaskState getState() {
        return report.getTaskState();
      }
    };
  }

  public static Counters getCounters(Collection<Task> tasks) {
    Counters counters = JobImpl.newCounters();
    return JobImpl.incrTaskCounters(counters, tasks);
  }

  static class TaskCount {
    int maps;
    int reduces;
    int completedMaps;
    int completedReduces;

    void incr(Task task) {
      TaskType type = task.getType();
      boolean finished = task.isFinished();
      if (type == TaskType.MAP) {
        if (finished) {
          ++completedMaps;
        }
        ++maps;
      } else if (type == TaskType.REDUCE) {
        if (finished) {
          ++completedReduces;
        }
        ++reduces;
      }
    }
  }

  static TaskCount getTaskCount(Collection<Task> tasks) {
    TaskCount tc = new TaskCount();
    for (Task task : tasks) {
      tc.incr(task);
    }
    return tc;
  }

  public static Job newJob(ApplicationId appID, int i, int n, int m) {
    final JobId id = newJobID(appID, i);
    final String name = newJobName();
    final JobReport report = newJobReport(id);
    final Map<TaskId, Task> tasks = newTasks(id, n, m);
    final TaskCount taskCount = getTaskCount(tasks.values());
    final Counters counters = getCounters(tasks.values());
    return new Job() {
      @Override
      public JobId getID() {
        return id;
      }

      @Override
      public String getName() {
        return name;
      }

      @Override
      public JobState getState() {
        return report.getJobState();
      }

      @Override
      public JobReport getReport() {
        return report;
      }

      @Override
      public Counters getCounters() {
        return counters;
      }

      @Override
      public Map<TaskId, Task> getTasks() {
        return tasks;
      }

      @Override
      public Task getTask(TaskId taskID) {
        return tasks.get(taskID);
      }

      @Override
      public int getTotalMaps() {
        return taskCount.maps;
      }

      @Override
      public int getTotalReduces() {
        return taskCount.reduces;
      }

      @Override
      public int getCompletedMaps() {
        return taskCount.completedMaps;
      }

      @Override
      public int getCompletedReduces() {
        return taskCount.completedReduces;
      }

      @Override
      public boolean isUber() {
        return false;
      }

      @Override
      public TaskAttemptCompletionEvent[] getTaskAttemptCompletionEvents(
          int fromEventId, int maxEvents) {
        return null;
      }

      @Override
      public Map<TaskId, Task> getTasks(TaskType taskType) {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public List<String> getDiagnostics() {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public boolean checkAccess(UserGroupInformation callerUGI,
          JobACL jobOperation) {
        return true;
      }
    };
  }
}
