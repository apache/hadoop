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

package org.apache.hadoop.mapreduce.v2.app.speculate;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;

abstract class StartEndTimesBase implements TaskRuntimeEstimator {
  static final float MINIMUM_COMPLETE_PROPORTION_TO_SPECULATE
      = 0.05F;
  static final int MINIMUM_COMPLETE_NUMBER_TO_SPECULATE
      = 1;

  protected AppContext context = null;

  protected final Map<TaskAttemptId, Long> startTimes
      = new ConcurrentHashMap<TaskAttemptId, Long>();

  // XXXX This class design assumes that the contents of AppContext.getAllJobs
  //   never changes.  Is that right?
  //
  // This assumption comes in in several places, mostly in data structure that
  //   can grow without limit if a AppContext gets new Job's when the old ones
  //   run out.  Also, these mapper statistics blocks won't cover the Job's
  //   we don't know about.
  protected final Map<Job, DataStatistics> mapperStatistics
      = new HashMap<Job, DataStatistics>();
  protected final Map<Job, DataStatistics> reducerStatistics
      = new HashMap<Job, DataStatistics>();


  private final Map<Job, Float> slowTaskRelativeTresholds
      = new HashMap<Job, Float>();

  protected final Set<Task> doneTasks = new HashSet<Task>();

  @Override
  public void enrollAttempt(TaskAttemptStatus status, long timestamp) {
    startTimes.put(status.id,timestamp);
  }

  @Override
  public long attemptEnrolledTime(TaskAttemptId attemptID) {
    Long result = startTimes.get(attemptID);

    return result == null ? Long.MAX_VALUE : result;
  }


  @Override
  public void contextualize(Configuration conf, AppContext context) {
    this.context = context;

    Map<JobId, Job> allJobs = context.getAllJobs();

    for (Map.Entry<JobId, Job> entry : allJobs.entrySet()) {
      final Job job = entry.getValue();
      mapperStatistics.put(job, new DataStatistics());
      reducerStatistics.put(job, new DataStatistics());
      slowTaskRelativeTresholds.put
          (job, conf.getFloat(MRJobConfig.SPECULATIVE_SLOWTASK_THRESHOLD,1.0f));
    }
  }

  protected DataStatistics dataStatisticsForTask(TaskId taskID) {
    JobId jobID = taskID.getJobId();
    Job job = context.getJob(jobID);

    if (job == null) {
      return null;
    }

    Task task = job.getTask(taskID);

    if (task == null) {
      return null;
    }

    return task.getType() == TaskType.MAP
            ? mapperStatistics.get(job)
            : task.getType() == TaskType.REDUCE
                ? reducerStatistics.get(job)
                : null;
  }

  @Override
  public long thresholdRuntime(TaskId taskID) {
    JobId jobID = taskID.getJobId();
    Job job = context.getJob(jobID);

    TaskType type = taskID.getTaskType();

    DataStatistics statistics
        = dataStatisticsForTask(taskID);

    int completedTasksOfType
        = type == TaskType.MAP
            ? job.getCompletedMaps() : job.getCompletedReduces();

    int totalTasksOfType
        = type == TaskType.MAP
            ? job.getTotalMaps() : job.getTotalReduces();

    if (completedTasksOfType < MINIMUM_COMPLETE_NUMBER_TO_SPECULATE
        || (((float)completedTasksOfType) / totalTasksOfType)
              < MINIMUM_COMPLETE_PROPORTION_TO_SPECULATE ) {
      return Long.MAX_VALUE;
    }

    long result =  statistics == null
        ? Long.MAX_VALUE
        : (long)statistics.outlier(slowTaskRelativeTresholds.get(job));
    return result;
  }

  @Override
  public long estimatedNewAttemptRuntime(TaskId id) {
    DataStatistics statistics = dataStatisticsForTask(id);

    if (statistics == null) {
      return -1L;
    }
    return (long) statistics.mean();
  }

  @Override
  public void updateAttempt(TaskAttemptStatus status, long timestamp) {

    TaskAttemptId attemptID = status.id;
    TaskId taskID = attemptID.getTaskId();
    JobId jobID = taskID.getJobId();
    Job job = context.getJob(jobID);

    if (job == null) {
      return;
    }

    Task task = job.getTask(taskID);

    if (task == null) {
      return;
    }

    Long boxedStart = startTimes.get(attemptID);
    long start = boxedStart == null ? Long.MIN_VALUE : boxedStart;
    
    TaskAttempt taskAttempt = task.getAttempt(attemptID);

    if (taskAttempt.getState() == TaskAttemptState.SUCCEEDED) {
      boolean isNew = false;
      // is this  a new success?
      synchronized (doneTasks) {
        if (!doneTasks.contains(task)) {
          doneTasks.add(task);
          isNew = true;
        }
      }

      // It's a new completion
      // Note that if a task completes twice [because of a previous speculation
      //  and a race, or a success followed by loss of the machine with the
      //  local data] we only count the first one.
      if (isNew) {
        long finish = timestamp;
        if (start > 1L && finish > 1L && start <= finish) {
          long duration = finish - start;

          DataStatistics statistics
          = dataStatisticsForTask(taskID);

          if (statistics != null) {
            statistics.add(duration);
          }
        }
      }
    }
  }
}
