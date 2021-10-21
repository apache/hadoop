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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages;

import java.util.Objects;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.StoreOperations;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.util.JsonSerialization;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.RateLimiting;
import org.apache.hadoop.util.RateLimitingFactory;
import org.apache.hadoop.util.functional.TaskPool;

import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.PENDING_DIR_NAME;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.SUCCESS_MARKER;

/**
 * Stage Config.
 * Everything to configure a stage which is common to all.
 *
 * It's isolated from the details of MR datatypes (taskID, taskattempt etc);
 * at this point it expects parsed values.
 *
 * It uses the builder API, but once {@link #build()} is called it goes
 * read only. This is to ensure that changes cannot
 * take place when shared across stages.
 */
public class StageConfig {

  /**
   * A flag which freezes the config for
   * further updates.
   */
  private boolean frozen;

  /**
   * IOStatistics to update.
   */
  private IOStatisticsStore iostatistics;

  /**
   * Job ID; constant over multiple attempts.
   */
  private String jobId;

  /**
   * Where did the job Unique ID come from?
   */
  private String jobIdSource = "";

  /**
   * Number of the job attempt; starts at zero.
   */
  private int jobAttemptNumber;

  /**
   * ID of the task.
   */
  private String taskId;

  /**
   * ID of this specific attempt at a task.
   */
  private String taskAttemptId;

  /**
   * Destination of job.
   */
  private Path destinationDir;

  /**
   * Job attempt dir.
   */
  private Path jobAttemptDir;

  /**
   * temp directory under job dest dir.
   */
  private Path outputTempSubDir;

  /**
   * Task attempt dir.
   */
  private Path taskAttemptDir;

  /**
   * Callbacks to update store.
   * This is not made visible to the stages; they must
   * go through the superclass which
   * adds statistics and logging.
   */
  private StoreOperations operations;

  /**
   * Submitter for doing IO against the store other than
   * manifest processing.
   */
  private TaskPool.Submitter ioProcessors;

  /**
   * Optional progress callback.
   */
  private Progressable progressable;

  /**
   * Callback when a stage is entered.
   */
  private StageEventCallbacks enterStageEventHandler;

  /**
   * Thread local serializer; created on demand
   * and shareable across a sequence of stages.
   */
  private final ThreadLocal<JsonSerialization<TaskManifest>> threadLocalSerializer =
      ThreadLocal.withInitial(TaskManifest::serializer);

  /**
   * Rate limiter for read operations.
   */
  private RateLimiting readLimiter = RateLimitingFactory.unlimitedRate();

  /**
   * Rate limiter for read operations.
   */
  private RateLimiting writeLimiter = RateLimitingFactory.unlimitedRate();

  /**
   * Name for logging.
   */
  private String name = "";

  public StageConfig() {
  }

  /**
   * Verify that the config is not yet frozen.
   */
  private void checkOpen() {
    Preconditions.checkState(!frozen,
        "StageConfig is now read-only");
  }

  /**
   * The build command makes the config immutable.
   * Idempotent.
   * @return the now-frozen config
   */
  public StageConfig build() {
    frozen = true;
    return this;
  }

  /**
   * Set job destination dir.
   * @param dir new dir
   * @return this
   */
  public StageConfig withDestinationDir(final Path dir) {
    destinationDir = dir;
    return this;
  }

  /**
   * Set IOStatistics store.
   * @param store new store
   * @return this
   */
  public StageConfig withIOStatistics(final IOStatisticsStore store) {
    checkOpen();
    iostatistics = store;
    return this;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return this
   */
  public StageConfig withIOProcessors(final TaskPool.Submitter value) {
    checkOpen();
    ioProcessors = value;
    return this;
  }

  /**
   * Set Job attempt directory.
   * @param dir new dir
   * @return this
   */
  public StageConfig withJobAttemptDir(final Path dir) {
    checkOpen();
    jobAttemptDir = dir;
    return this;
  }

  /**
   * Set the job directories from the attempt directories
   * information. Does not set task attempt fields.
   * @param dirs source of directories.
   * @return this
   */
  public StageConfig withJobDirectories(
      final ManifestCommitterSupport.AttemptDirectories dirs) {

    checkOpen();
    withJobAttemptDir(dirs.getJobAttemptDir())
        .withDestinationDir(dirs.getOutputPath())
        .withOutputTempSubDir(dirs.getOutputTempSubDir());

    return this;
  }

  /**
   * Set job ID with no attempt included.
   * @param value new value
   * @return this
   */
  public StageConfig withJobId(final String value) {
    checkOpen();
    jobId = value;
    return this;
  }

  public Path getOutputTempSubDir() {
    return outputTempSubDir;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return this
   */
  public StageConfig withOutputTempSubDir(final Path value) {
    checkOpen();
    outputTempSubDir = value;
    return this;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return this
   */
  public StageConfig withOperations(final StoreOperations value) {
    checkOpen();
    operations = value;
    return this;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return this
   */
  public StageConfig withTaskAttemptId(final String value) {
    checkOpen();
    taskAttemptId = value;
    return this;
  }

  /**
   * Set builder value.
   * @param value new value
   * @return this
   */
  public StageConfig withTaskId(final String value) {
    checkOpen();
    taskId = value;
    return this;
  }

  /**
   * Set handler for stage entry events..
   * @param value new value
   * @return this
   */
  public StageConfig withStageEventCallbacks(StageEventCallbacks value) {
    checkOpen();
    enterStageEventHandler = value;
    return this;
  }

  /**
   * Optional progress callback.
   * @param value new value
   * @return this
   */
  public StageConfig withProgressable(final Progressable value) {
    checkOpen();
    progressable = value;
    return this;
  }

  /**
   * Set the Task attempt directory.
   * @param value new value
   * @return this
   */
  public StageConfig withTaskAttemptDir(final Path value) {
    checkOpen();
    taskAttemptDir = value;
    return this;
  }

  /**
   * Set the job attempt number.
   * @param value new value
   * @return this
   */
  public StageConfig withJobAttemptNumber(final int value) {
    checkOpen();
    jobAttemptNumber = value;
    return this;
  }

  /**
   * Set the Job ID source.
   * @param value new value
   * @return this
   */
  public StageConfig withJobIdSource(final String value) {
    checkOpen();
    jobIdSource = value;
    return this;
  }

  /**
   * Set read limiter.
   * @param value new value
   * @return the builder
   */
  public StageConfig withReadLimiter(RateLimiting value) {
    checkOpen();
    readLimiter = Objects.requireNonNull(value);
    return this;
  }

  /**
   * Set write limiter value.
   * @param value new value
   * @return the builder
   */
  public StageConfig withWriteLimiter(RateLimiting value) {
    checkOpen();
    writeLimiter = Objects.requireNonNull(value);
    return this;
  }

  /**
   * Set name of task/job.
   * @param value new value
   * @return the builder
   */
  public StageConfig withName(String value) {
    name = value;
    return this;
  }

  /**
   * Get name of task/job.
   * @return name for logging.
   */
  public String getName() {
    return name;
  }

  /**
   * Handler for stage entry events.
   * @return the handler.
   */
  public StageEventCallbacks getEnterStageEventHandler() {
    return enterStageEventHandler;
  }

  /**
   * IOStatistics to update.
   */
  public IOStatisticsStore getIOStatistics() {
    return iostatistics;
  }

  /**
   * Job ID.
   */
  public String getJobId() {
    return jobId;
  }

  /**
   * ID of the task.
   */
  public String getTaskId() {
    return taskId;
  }

  /**
   * ID of this specific attempt at a task.
   */
  public String getTaskAttemptId() {
    return taskAttemptId;
  }

  /**
   * Job attempt dir.
   */
  public Path getJobAttemptDir() {
    return jobAttemptDir;
  }

  /**
   * Destination of job.
   */
  public Path getDestinationDir() {
    return destinationDir;
  }

  /**
   * Get the location of the success marker.
   * @return a path under the destination directory.
   */
  public Path getJobSuccessMarkerPath() {
    return new Path(destinationDir, SUCCESS_MARKER);
  }

  /**
   * Callbacks to update store.
   * This is not made visible to the stages; they must
   * go through the wrapper classes in this class, which
   * add statistics and logging.
   */
  public StoreOperations getOperations() {
    return operations;
  }

  /**
   * Submitter for doing IO against the store other than
   * manifest processing.
   */
  public TaskPool.Submitter getIoProcessors() {
    return ioProcessors;
  }

  /**
   * Get optional progress callback.
   * @return callback or null
   */
  public Progressable getProgressable() {
    return progressable;
  }

  /**
   * Task attempt directory.
   * @return the task attempt dir.
   */
  public Path getTaskAttemptDir() {
    return taskAttemptDir;
  }

  /**
   * Get the job attempt number.
   * @return the value
   */
  public int getJobAttemptNumber() {
    return jobAttemptNumber;
  }

  public String getJobIdSource() {
    return jobIdSource;
  }

  /**
   * Get the path to the subdirectory under $jobID where task
   * attempts are. List this dir to find all task attempt dirs.
   * @return a path under the job attempt dir.
   */
  public Path getJobAttemptTaskSubDir() {
    return new Path(jobAttemptDir, PENDING_DIR_NAME);
  }

  /**
   * Get a thread local task manifest serializer.
   * @return a serializer.
   */
  public JsonSerialization<TaskManifest> currentManifestSerializer() {
    return threadLocalSerializer.get();
  }

  /**
   * Read limiter.
   * @return Read limiter.
   */
  public RateLimiting getReadLimiter() {
    return readLimiter;
  }

  /**
   * Write limiter.
   * @return Write limiter.
   */
  public RateLimiting getWriteLimiter() {
    return writeLimiter;
  }

  /**
   * Acquire a given number of read permits.
   * The subsequent caller will block if the rate
   * limiter mandates it.
   * no-op if (in test setups) there's no rate limiter.
   * @param permits permit count.
   * @return delay in milliseconds; 0 if none.
   */
  public int acquireReadPermits(int permits) {
    return readLimiter.acquire(permits);
  }

  /**
   * Acquire a given number of write permits.
   * The subsequent caller will block if the rate
   * limiter mandates it.
   * no-op if (in test setups) there's no rate limiter.
   * @param permits permit count.
   * @return delay in milliseconds; 0 if none.
   */
  public int acquireWritePermits(int permits) {
    return writeLimiter.acquire(permits);
  }

  /**
   * Enter the stage; calls back to
   * {@link #enterStageEventHandler} if non-null.
   * @param stage stage entered
   */
  public void enterStage(String stage) {
    if (enterStageEventHandler != null) {
      enterStageEventHandler.enterStage(stage);
    }
  }

  /**
   * Exit the stage; calls back to
   * {@link #enterStageEventHandler} if non-null.
   * @param stage stage entered
   */
  public void exitStage(String stage) {
    if (enterStageEventHandler != null) {
      enterStageEventHandler.exitStage(stage);
    }
  }
}
