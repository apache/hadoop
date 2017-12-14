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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TimelineEntityConverterV1 {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineEntityConverterV1.class);

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
  public Set<TimelineEntity> createTimelineEntities(JobInfo jobInfo,
      Configuration conf) {
    Set<TimelineEntity> entities = new HashSet<>();

    // create the job entity
    TimelineEntity job = createJobEntity(jobInfo, conf);
    entities.add(job);

    // create the task and task attempt entities
    Set<TimelineEntity> tasksAndAttempts =
        createTaskAndTaskAttemptEntities(jobInfo);
    entities.addAll(tasksAndAttempts);

    return entities;
  }

  private TimelineEntity createJobEntity(JobInfo jobInfo, Configuration conf) {
    TimelineEntity job = new TimelineEntity();
    job.setEntityType(JOB);
    job.setEntityId(jobInfo.getJobId().toString());
    job.setStartTime(jobInfo.getSubmitTime());

    job.addPrimaryFilter("JOBNAME", jobInfo.getJobname());
    job.addPrimaryFilter("USERNAME", jobInfo.getUsername());
    job.addOtherInfo("JOB_QUEUE_NAME", jobInfo.getJobQueueName());
    job.addOtherInfo("SUBMIT_TIME", jobInfo.getSubmitTime());
    job.addOtherInfo("LAUNCH_TIME", jobInfo.getLaunchTime());
    job.addOtherInfo("FINISH_TIME", jobInfo.getFinishTime());
    job.addOtherInfo("JOB_STATUS", jobInfo.getJobStatus());
    job.addOtherInfo("PRIORITY", jobInfo.getPriority());
    job.addOtherInfo("TOTAL_MAPS", jobInfo.getTotalMaps());
    job.addOtherInfo("TOTAL_REDUCES", jobInfo.getTotalReduces());
    job.addOtherInfo("UBERIZED", jobInfo.getUberized());
    job.addOtherInfo("ERROR_INFO", jobInfo.getErrorInfo());

    LOG.info("converted job " + jobInfo.getJobId() + " to a timeline entity");
    return job;
  }

  private Set<TimelineEntity>
      createTaskAndTaskAttemptEntities(JobInfo jobInfo) {
    Set<TimelineEntity> entities = new HashSet<>();
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
    task.setEntityType(TASK);
    task.setEntityId(taskInfo.getTaskId().toString());
    task.setStartTime(taskInfo.getStartTime());

    task.addOtherInfo("START_TIME", taskInfo.getStartTime());
    task.addOtherInfo("FINISH_TIME", taskInfo.getFinishTime());
    task.addOtherInfo("TASK_TYPE", taskInfo.getTaskType());
    task.addOtherInfo("TASK_STATUS", taskInfo.getTaskStatus());
    task.addOtherInfo("ERROR_INFO", taskInfo.getError());

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

  private TimelineEntity
      createTaskAttemptEntity(TaskAttemptInfo taskAttemptInfo) {
    TimelineEntity taskAttempt = new TimelineEntity();
    taskAttempt.setEntityType(TASK_ATTEMPT);
    taskAttempt.setEntityId(taskAttemptInfo.getAttemptId().toString());
    taskAttempt.setStartTime(taskAttemptInfo.getStartTime());

    taskAttempt.addOtherInfo("START_TIME", taskAttemptInfo.getStartTime());
    taskAttempt.addOtherInfo("FINISH_TIME", taskAttemptInfo.getFinishTime());
    taskAttempt.addOtherInfo("MAP_FINISH_TIME",
        taskAttemptInfo.getMapFinishTime());
    taskAttempt.addOtherInfo("SHUFFLE_FINISH_TIME",
        taskAttemptInfo.getShuffleFinishTime());
    taskAttempt.addOtherInfo("SORT_FINISH_TIME",
        taskAttemptInfo.getSortFinishTime());
    taskAttempt.addOtherInfo("TASK_STATUS", taskAttemptInfo.getTaskStatus());
    taskAttempt.addOtherInfo("STATE", taskAttemptInfo.getState());
    taskAttempt.addOtherInfo("ERROR", taskAttemptInfo.getError());
    taskAttempt.addOtherInfo("CONTAINER_ID",
        taskAttemptInfo.getContainerId().toString());

    LOG.info("converted task attempt " + taskAttemptInfo.getAttemptId() +
        " to a timeline entity");
    return taskAttempt;
  }
}
