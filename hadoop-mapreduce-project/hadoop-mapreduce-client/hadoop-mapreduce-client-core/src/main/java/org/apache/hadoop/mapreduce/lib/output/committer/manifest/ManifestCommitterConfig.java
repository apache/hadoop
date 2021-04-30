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

import java.io.IOException;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.DEFAULT_CREATE_SUCCESSFUL_JOB_DIR_MARKER;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_IO_PROCESSORS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_IO_PROCESSORS_DEFAULT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_VALIDATE_OUTPUT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_VALIDATE_OUTPUT_DEFAULT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterSupport.buildJobUUID;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterSupport.getAppAttemptId;

/**
 * The configuration for the committer as built up from the job configuration
 * and data passed down from the committer factory.
 * Isolated for ease of dev/test
 */
final class ManifestCommitterConfig implements IOStatisticsSource {

  /**
   * Final destination of work.
   * This is <i>unqualified</i>.
   */
  private final Path destinationDir;

  /**
   * Role: used in log/text messages.
   */
  private final String role;

  /**
   * This is the directory for all intermediate work: where the output
   * format will write data.
   * Will be null if built from a job context.
   */
  private final Path taskAttemptDir;

  /** Configuration of the job. */
  private final Configuration conf;

  /** The job context. For a task, this can be cast to a TaskContext. */
  private final JobContext jobContext;

  /** Should a job marker be created? */
  private final boolean createJobMarker;

  /**
   * Job ID Or UUID -without any attempt suffix.
   * This is expected/required to be unique, though
   * Spark has had "issues" there until recently
   * with lack of uniqueness of generated MR Job IDs.
   */
  private final String jobUniqueId;

  /**
   * Where did the job Unique ID come from?
   */
  private final String jobUniqueIdSource;

  /**
   * Number of this attempt; starts at zero.
   */
  private final int jobAttemptNumber;

  /**
   * Job ID + AttemptID.
   */
  private final String jobAttemptId;

  /**
   * Task ID: used as the filename of the manifest.
   * Will be "" if built from a job context.
   */
  private final String taskId;

  /**
   * Task attempt ID. Determines the working
   * directory for task attempts to write data into,
   * and for the task committer to scan.
   * Will be "" if built from a job context.
   */
  private final String taskAttemptId;

  /** Any progressable for progress callbacks. */
  private final Progressable progressable;

  /**
   * IOStatistics to update.
   */
  private final IOStatisticsStore iostatistics;

  /** Should the output be validated after the commit? */
  private final boolean validateOutput;

  /**
   * Attempt directory management.
   */
  private final ManifestCommitterSupport.AttemptDirectories dirs;

  /**
   * Callback when a stage is entered.
   */
  private final StageEventCallbacks stageEventCallbacks;

  /**
   * Constructor.
   * @param outputPath destination path of the job.
   * @param role role for log messages.
   * @param context job/task context
   * @param iostatistics IO Statistics
   * @param stageEventCallbacks stage event callbacks.
   */
  ManifestCommitterConfig(
      final Path outputPath,
      final String role,
      final JobContext context,
      final IOStatisticsStore iostatistics,
      final StageEventCallbacks stageEventCallbacks) {
    this.role = role;
    this.jobContext = context;
    this.conf = context.getConfiguration();
    this.destinationDir = outputPath;
    this.iostatistics = iostatistics;
    this.stageEventCallbacks = stageEventCallbacks;
    this.createJobMarker = conf.getBoolean(
        SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
        DEFAULT_CREATE_SUCCESSFUL_JOB_DIR_MARKER);
    this.validateOutput = conf.getBoolean(
        OPT_VALIDATE_OUTPUT,
        OPT_VALIDATE_OUTPUT_DEFAULT);
    Pair<String, String> pair = buildJobUUID(conf, context.getJobID());
    this.jobUniqueId = pair.getLeft();
    this.jobUniqueIdSource = pair.getRight();
    this.jobAttemptNumber = getAppAttemptId(context);
    this.jobAttemptId = this.jobUniqueId + "_" + jobAttemptNumber;

    // build directories
    this.dirs = new ManifestCommitterSupport.AttemptDirectories(outputPath,
        this.jobUniqueId, jobAttemptNumber);

    // if constructed with a task attempt, build the task ID and path.
    if (context instanceof TaskAttemptContext) {
      // it's a task
      final TaskAttemptContext tac = (TaskAttemptContext) context;
      TaskAttemptID taskAttempt = Objects.requireNonNull(
          tac.getTaskAttemptID());
      taskAttemptId = taskAttempt.toString();
      taskId = taskAttempt.getTaskID().toString();
      // Task attempt dir; must be different across instances
      taskAttemptDir = dirs.getTaskAttemptPath(taskAttemptId);
      // the context is also the progress callback.
      progressable = tac;

    } else {
      // it's a job
      taskId = "";
      taskAttemptId = "";
      taskAttemptDir = null;
      progressable = null;
    }
  }

  @Override
  public String toString() {
    return "ManifestCommitterConfig{" +
        "destinationDir=" + destinationDir +
        ", role='" + role + '\'' +
        ", taskAttemptDir=" + taskAttemptDir +
        ", createJobMarker=" + createJobMarker +
        ", jobUniqueId='" + jobUniqueId + '\'' +
        ", jobUniqueIdSource='" + jobUniqueIdSource + '\'' +
        ", jobAttemptNumber=" + jobAttemptNumber +
        ", jobAttemptId='" + jobAttemptId + '\'' +
        ", taskId='" + taskId + '\'' +
        ", taskAttemptId='" + taskAttemptId + '\'' +
        '}';
  }

  /**
   * Get the destination filesystem.
   * @return destination FS.
   * @throws IOException Problems binding to the destination FS.
   */
  FileSystem getDestinationFileSystem() throws IOException {
    return FileSystem.get(destinationDir.toUri(), conf);
  }

  /**
   * Create the job stage config from the committer
   * configuration.
   * This does not bind the store operations
   * or processors.
   * @return a stage config with configuration options passed in.
   */
  StageConfig createJobStageConfig() {
    StageConfig stageConfig = new StageConfig();
    stageConfig
        .withStageEventCallbacks(stageEventCallbacks)
        .withIOStatistics(iostatistics)
        .withJobDirectories(dirs)
        .withJobId(jobUniqueId)
        .withJobIdSource(jobUniqueIdSource)
        .withProgressable(progressable)
        .withJobAttemptNumber(jobAttemptNumber)
        .withTaskAttemptDir(taskAttemptDir)
        .withTaskAttemptId(taskAttemptId)
        .withTaskId(taskId);

    return stageConfig;
  }

  Path getDestinationDir() {
    return destinationDir;
  }

  String getRole() {
    return role;
  }

  Path getTaskAttemptDir() {
    return taskAttemptDir;
  }

  Path getJobAttemptDir() {
    return dirs.getJobAttemptDir();
  }

  Configuration getConf() {
    return conf;
  }

  JobContext getJobContext() {
    return jobContext;
  }

  boolean getCreateJobMarker() {
    return createJobMarker;
  }

  String getJobAttemptId() {
    return jobAttemptId;
  }

  String getTaskAttemptId() {
    return taskAttemptId;
  }

  String getTaskId() {
    return taskId;
  }

  String getJobUniqueId() {
    return jobUniqueId;
  }

  boolean getValidateOutput() {
    return validateOutput;
  }

  @Override
  public IOStatisticsStore getIOStatistics() {
    return iostatistics;
  }

  /**
   * Create a new thread pool from the
   * {@link ManifestCommitterConstants#OPT_IO_PROCESSORS}
   * settings.
   * @return a new thread pool.
   */
  CloseableTaskSubmitter createSubmitter() {
    return createSubmitter(
        OPT_IO_PROCESSORS, OPT_IO_PROCESSORS_DEFAULT);
  }

  /**
   * Create a new thread pool.
   * This must be shut down.
   * @param key config key with pool size.
   * @param defVal default value.
   * @return a new thread pool.
   */
  CloseableTaskSubmitter createSubmitter(String key, int defVal) {
    int numThreads = conf.getInt(key, defVal);
    if (numThreads <= 0) {
      // ignore the setting if it is too invalid.
      numThreads = defVal;
    }
    return createCloseableTaskSubmitter(numThreads, getJobAttemptId());
  }

  @VisibleForTesting
  static CloseableTaskSubmitter createCloseableTaskSubmitter(
      final int numThreads,
      final String jobAttemptId) {
    return new CloseableTaskSubmitter(
        HadoopExecutors.newFixedThreadPool(numThreads,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("manifest-committer-" + jobAttemptId + "-%d")
                .build()));
  }

}
