/*
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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageConfig;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitter.TASK_COMMITTER;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterTestSupport.randomJobId;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.createIOStatisticsStore;

/**
 * This class represents a binding to a job in the target dir with TA, JA
 * and associated paths.
 * It's self contained so as to be usable in any test suite.
 */
class CommitterTestBinding implements
    IOStatisticsSource {

  /**
   * IOStatistics counter for progress events.
   */
  public static final String PROGRESS_EVENTS = "progress_events";

  /**
   * IOStatistics to update with progress events.
   */
  private final IOStatisticsStore iostatistics;

  /**
   * Job attempt ID:.
   */
  private final String jobAttemptId;

  /**
   * Job ID.
   */
  private final JobID jobId;

  /**
   * Task Attempt ID, under the job attempt.
   */
  private final TaskAttemptID taskAttemptId;

  /**
   * Task ID.
   */
  private final TaskID taskId;

  /**
   * Task attempt context for the given task Attempt.
   */
  private final TaskAttemptContext taskAttemptContext;

  /**
   * Construct.
   * @param conf job/task config. This is patched with the app attempt.
   * @param appAttempt application attempt.
   * @param taskNumber task number
   * @param taskAttemptNumber which attempt on this task is it
   */
  CommitterTestBinding(
      Configuration conf,
      int appAttempt, int taskNumber, int taskAttemptNumber) {
    iostatistics = createIOStatisticsStore()
        .withCounters(PROGRESS_EVENTS)
        .build();


    // this is the job ID, with no attempt info.
    jobId = JobID.forName(randomJobId());
    jobAttemptId = jobId.toString() + "_ " + appAttempt;
    taskId = new TaskID(jobId, TaskType.MAP, taskNumber);
    taskAttemptId = new TaskAttemptID(taskId,
        taskAttemptNumber);
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, appAttempt);
    taskAttemptContext = new TaskAttemptContextImpl(conf, taskAttemptId);

  }

  /**
   * Create a committer config for the given output path.
   * @param outputPath output path in destFS.
   * @return a committer for the active task.
   */
  ManifestCommitterConfig createCommitterConfig(
      Path outputPath) {
    return new ManifestCommitterConfig(outputPath,
        TASK_COMMITTER,
        taskAttemptContext,
        iostatistics,
        null);
  }

  /**
   * Create a stage config from the committer config.
   * All stats go to the local IOStatisticsStore;
   * there's a progress callback also set to increment
   * the counter {@link #PROGRESS_EVENTS}
   * @return a stage config
   */
  StageConfig createStageConfig(Path outputPath) {
    return createCommitterConfig(outputPath)
        .createStageConfig()
        .withProgressable(new ProgressCallback());
  }

  @Override
  public IOStatisticsStore getIOStatistics() {
    return iostatistics;
  }

  /**
   * Whenever this progress callback is invoked, the progress_events
   * counter is incremented. This allows for tests to verify that
   * callbacks have occurred by asserting on the event counter.
   */
  private final class ProgressCallback implements Progressable {

    @Override
    public void progress() {
      iostatistics.incrementCounter(PROGRESS_EVENTS, 1);
    }
  }
}
