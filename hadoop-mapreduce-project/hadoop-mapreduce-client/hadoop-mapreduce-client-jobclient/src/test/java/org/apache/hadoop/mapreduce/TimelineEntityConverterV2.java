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

package org.apache.hadoop.mapreduce;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

class TimelineEntityConverterV2 {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineEntityConverterV2.class);

  static final String JOB = "MAPREDUCE_JOB";
  static final String TASK = "MAPREDUCE_TASK";
  static final String TASK_ATTEMPT = "MAPREDUCE_TASK_ATTEMPT";

  /**
   * Creates job, task, and task attempt entities based on the job history info
   * and configuration.
   *
   * Note: currently these are plan timeline entities created for mapreduce
   * types. These are not meant to be the complete and accurate entity set-up
   * for mapreduce jobs. We do not leverage hierarchical timeline entities. If
   * we create canonical mapreduce hierarchical timeline entities with proper
   * parent-child relationship, we could modify this to use that instead.
   *
   * Note that we also do not add info to the YARN application entity, which
   * would be needed for aggregation.
   */
  public List<TimelineEntity> createTimelineEntities(JobInfo jobInfo,
      Configuration conf) {
    List<TimelineEntity> entities = new ArrayList<>();

    // create the job entity
    TimelineEntity job = createJobEntity(jobInfo, conf);
    entities.add(job);

    // create the task and task attempt entities
    List<TimelineEntity> tasksAndAttempts =
        createTaskAndTaskAttemptEntities(jobInfo);
    entities.addAll(tasksAndAttempts);

    return entities;
  }

  private TimelineEntity createJobEntity(JobInfo jobInfo, Configuration conf) {
    TimelineEntity job = new TimelineEntity();
    job.setType(JOB);
    job.setId(jobInfo.getJobId().toString());
    job.setCreatedTime(jobInfo.getSubmitTime());

    job.addInfo("JOBNAME", jobInfo.getJobname());
    job.addInfo("USERNAME", jobInfo.getUsername());
    job.addInfo("JOB_QUEUE_NAME", jobInfo.getJobQueueName());
    job.addInfo("SUBMIT_TIME", jobInfo.getSubmitTime());
    job.addInfo("LAUNCH_TIME", jobInfo.getLaunchTime());
    job.addInfo("FINISH_TIME", jobInfo.getFinishTime());
    job.addInfo("JOB_STATUS", jobInfo.getJobStatus());
    job.addInfo("PRIORITY", jobInfo.getPriority());
    job.addInfo("TOTAL_MAPS", jobInfo.getTotalMaps());
    job.addInfo("TOTAL_REDUCES", jobInfo.getTotalReduces());
    job.addInfo("UBERIZED", jobInfo.getUberized());
    job.addInfo("ERROR_INFO", jobInfo.getErrorInfo());

    // add metrics from total counters
    // we omit the map counters and reduce counters for now as it's kind of
    // awkward to put them (map/reduce/total counters are really a group of
    // related counters)
    Counters totalCounters = jobInfo.getTotalCounters();
    if (totalCounters != null) {
      addMetrics(job, totalCounters);
    }
    // finally add configuration to the job
    addConfiguration(job, conf);
    LOG.info("converted job " + jobInfo.getJobId() + " to a timeline entity");
    return job;
  }

  private void addConfiguration(TimelineEntity job, Configuration conf) {
    for (Map.Entry<String, String> e: conf) {
      job.addConfig(e.getKey(), e.getValue());
    }
  }

  private void addMetrics(TimelineEntity entity, Counters counters) {
    for (CounterGroup g: counters) {
      String groupName = g.getName();
      for (Counter c: g) {
        String name = groupName + ":" + c.getName();
        TimelineMetric metric = new TimelineMetric();
        metric.setId(name);
        metric.addValue(System.currentTimeMillis(), c.getValue());
        entity.addMetric(metric);
      }
    }
  }

  private List<TimelineEntity> createTaskAndTaskAttemptEntities(
      JobInfo jobInfo) {
    List<TimelineEntity> entities = new ArrayList<>();
    Map<TaskID, TaskInfo> taskInfoMap = jobInfo.getAllTasks();
    LOG.info("job " + jobInfo.getJobId()+ " has " + taskInfoMap.size() +
        " tasks");
    for (TaskInfo taskInfo: taskInfoMap.values()) {
      TimelineEntity task = createTaskEntity(taskInfo);
      entities.add(task);
      // add the task attempts from this task
      Set<TimelineEntity> taskAttempts = createTaskAttemptEntities(taskInfo);
      entities.addAll(taskAttempts);
    }
    return entities;
  }

  private TimelineEntity createTaskEntity(TaskInfo taskInfo) {
    TimelineEntity task = new TimelineEntity();
    task.setType(TASK);
    task.setId(taskInfo.getTaskId().toString());
    task.setCreatedTime(taskInfo.getStartTime());

    task.addInfo("START_TIME", taskInfo.getStartTime());
    task.addInfo("FINISH_TIME", taskInfo.getFinishTime());
    task.addInfo("TASK_TYPE", taskInfo.getTaskType());
    task.addInfo("TASK_STATUS", taskInfo.getTaskStatus());
    task.addInfo("ERROR_INFO", taskInfo.getError());

    // add metrics from counters
    Counters counters = taskInfo.getCounters();
    if (counters != null) {
      addMetrics(task, counters);
    }
    LOG.info("converted task " + taskInfo.getTaskId() +
        " to a timeline entity");
    return task;
  }

  private Set<TimelineEntity> createTaskAttemptEntities(TaskInfo taskInfo) {
    Set<TimelineEntity> taskAttempts = new HashSet<TimelineEntity>();
    Map<TaskAttemptID, TaskAttemptInfo> taskAttemptInfoMap =
        taskInfo.getAllTaskAttempts();
    LOG.info("task " + taskInfo.getTaskId() + " has " +
        taskAttemptInfoMap.size() + " task attempts");
    for (TaskAttemptInfo taskAttemptInfo: taskAttemptInfoMap.values()) {
      TimelineEntity taskAttempt = createTaskAttemptEntity(taskAttemptInfo);
      taskAttempts.add(taskAttempt);
    }
    return taskAttempts;
  }

  private TimelineEntity createTaskAttemptEntity(
      TaskAttemptInfo taskAttemptInfo) {
    TimelineEntity taskAttempt = new TimelineEntity();
    taskAttempt.setType(TASK_ATTEMPT);
    taskAttempt.setId(taskAttemptInfo.getAttemptId().toString());
    taskAttempt.setCreatedTime(taskAttemptInfo.getStartTime());

    taskAttempt.addInfo("START_TIME", taskAttemptInfo.getStartTime());
    taskAttempt.addInfo("FINISH_TIME", taskAttemptInfo.getFinishTime());
    taskAttempt.addInfo("MAP_FINISH_TIME",
        taskAttemptInfo.getMapFinishTime());
    taskAttempt.addInfo("SHUFFLE_FINISH_TIME",
        taskAttemptInfo.getShuffleFinishTime());
    taskAttempt.addInfo("SORT_FINISH_TIME",
        taskAttemptInfo.getSortFinishTime());
    taskAttempt.addInfo("TASK_STATUS", taskAttemptInfo.getTaskStatus());
    taskAttempt.addInfo("STATE", taskAttemptInfo.getState());
    taskAttempt.addInfo("ERROR", taskAttemptInfo.getError());
    taskAttempt.addInfo("CONTAINER_ID",
        taskAttemptInfo.getContainerId().toString());

    // add metrics from counters
    Counters counters = taskAttemptInfo.getCounters();
    if (counters != null) {
      addMetrics(taskAttempt, counters);
    }
    LOG.info("converted task attempt " + taskAttemptInfo.getAttemptId() +
        " to a timeline entity");
    return taskAttempt;
  }
}
