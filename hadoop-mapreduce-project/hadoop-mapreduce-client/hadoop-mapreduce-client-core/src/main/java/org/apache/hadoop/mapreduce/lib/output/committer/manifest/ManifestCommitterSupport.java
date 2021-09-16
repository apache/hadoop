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
import java.time.ZonedDateTime;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticsAggregator;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStoreBuilder;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.PENDING_DIR_NAME;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.COUNTER_STATISTICS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.DURATION_STATISTICS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.INITIAL_APP_ATTEMPT_ID;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.JOB_ATTEMPT_DIR_FORMAT_STR;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.JOB_DIR_FORMAT_STR;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.JOB_ID_SOURCE_MAPREDUCE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.MANIFEST_COMMITTER_CLASSNAME;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.MANIFEST_SUFFIX;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DiagnosticKeys.PRINCIPAL;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.SPARK_WRITE_UUID;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.SUMMARY_FILENAME_FORMAT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.TMP_SUFFIX;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DiagnosticKeys.STAGE;

/**
 * Class for commit support util methods.
 */
public final class ManifestCommitterSupport {

  private ManifestCommitterSupport() {
  }

  /**
   * Create an IOStatistics Store with the standard statistics
   * set up.
   * @return a store builder preconfigured with the standard stats.
   */
  public static IOStatisticsStoreBuilder createIOStatisticsStore() {

    final IOStatisticsStoreBuilder store
        = iostatisticsStore();

    store.withCounters(COUNTER_STATISTICS);
    store.withMaximums(COUNTER_STATISTICS);
    store.withMinimums(COUNTER_STATISTICS);
    store.withMeanStatistics(COUNTER_STATISTICS);
    store.withDurationTracking(DURATION_STATISTICS);
    return store;
  }

  /**
   * If the object is an IOStatisticsSource, get and add
   * its IOStatistics.
   * @param o source object.
   */
  public static void maybeAddIOStatistics(IOStatisticsAggregator ios,
      Object o) {
    if (o instanceof IOStatisticsSource) {
      ios.aggregate(((IOStatisticsSource) o).getIOStatistics());
    }
  }

  /**
   * Build a Job UUID from the job conf (if it is
   * {@link ManifestCommitterConstants#SPARK_WRITE_UUID}
   * or the MR job ID.
   * @param conf job/task configuration
   * @param jobId job ID from YARN or spark.
   * @return (a job ID, source)
   */
  public static Pair<String, String> buildJobUUID(Configuration conf,
      JobID jobId) {
    String jobUUID = conf.getTrimmed(SPARK_WRITE_UUID, "");
    if (jobUUID.isEmpty()) {
      jobUUID = jobId.toString();
      return Pair.of(jobUUID, JOB_ID_SOURCE_MAPREDUCE);
    } else  {
      return Pair.of(jobUUID, SPARK_WRITE_UUID);
    }
  }

  /**
   * Get the location of pending job attempts.
   * @param out the base output directory.
   * @return the location of pending job attempts.
   */
  public static Path getPendingJobAttemptsPath(Path out) {
    return new Path(out, PENDING_DIR_NAME);
  }

  /**
   * Get the Application Attempt Id for this job.
   * @param context the context to look in
   * @return the Application Attempt Id for a given job.
   */
  public static int getAppAttemptId(JobContext context) {
    return getAppAttemptId(context.getConfiguration());
  }

  /**
   * Get the Application Attempt Id for this job
   * by looking for {@link MRJobConfig#APPLICATION_ATTEMPT_ID}
   * in the configuration, falling back to 0 if unset.
   * For spark it will always be 0, for MR it will be set in the AM
   * to the {@code ApplicationAttemptId} the AM is launched with.
   * @param conf job configuration.
   * @return the Application Attempt Id for the job.
   */
  public static int getAppAttemptId(Configuration conf) {
    return conf.getInt(MRJobConfig.APPLICATION_ATTEMPT_ID,
        INITIAL_APP_ATTEMPT_ID);
  }

  /**
   * Get the path in the job attempt dir for a manifest for a task.
   * @param jobAttemptDir job attempt dir.
   * @param taskId taskID.
   * @return the final path to rename the manifest file to
   */
  public static Path manifestPathForTask(Path jobAttemptDir, String taskId) {

    return new Path(jobAttemptDir, taskId + MANIFEST_SUFFIX);
  }

  /**
   * Get the path in the job attempt dir for the temp path to save a
   * task attempt's manifest before renaming it to the
   * path defined by {@link #manifestPathForTask(Path, String)}.
   * @param jobAttemptDir job attempt dir.
   * @param taskAttemptId task attempt ID.
   * @return the path to save/load the manifest.
   */
  public static Path manifestTempPathForTaskAttempt(Path jobAttemptDir,
      String taskAttemptId) {
    return new Path(jobAttemptDir,
        taskAttemptId + MANIFEST_SUFFIX + TMP_SUFFIX);
  }

  /**
   * Create a task attempt dir; stage config must be for a task attempt.
   * @param stageConfig state config.
   * @return a manifest with job and task attempt info set up.
   */
  public static TaskManifest createTaskManifest(StageConfig stageConfig) {
    final TaskManifest manifest = new TaskManifest();
    manifest.setTaskAttemptID(stageConfig.getTaskAttemptId());
    manifest.setTaskID(stageConfig.getTaskId());
    manifest.setJobId(stageConfig.getJobId());
    manifest.setJobAttemptNumber(stageConfig.getJobAttemptNumber());
    manifest.setTaskAttemptDir(
        stageConfig.getTaskAttemptDir()
            .toUri().toString());
    return manifest;
  }

  /**
   * Create success/outcome data.
   * @param stageConfig configuration.
   * @param stage
   * @return a _SUCCESS object with some diagnostics.
   */
  public static ManifestSuccessData createManifestOutcome(
      StageConfig stageConfig, String stage) {
    final ManifestSuccessData outcome = new ManifestSuccessData();
    outcome.setJobId(stageConfig.getJobId());
    outcome.setJobIdSource(stageConfig.getJobIdSource());
    outcome.setCommitter(MANIFEST_COMMITTER_CLASSNAME);
    // real timestamp
    outcome.setTimestamp(System.currentTimeMillis());
    final ZonedDateTime now = ZonedDateTime.now();
    outcome.setDate(now.toString());
    outcome.setHostname(NetUtils.getLocalHostname());
    // add some extra diagnostics which can still be parsed by older
    // builds of test applications.
    // Audit Span information can go in here too, in future.
    try {
      outcome.putDiagnostic(PRINCIPAL,
          UserGroupInformation.getCurrentUser().getShortUserName());
    } catch (IOException ignored) {
      // don't know who we are? exclude from the diagnostics.
    }
    outcome.putDiagnostic(STAGE, stage);
    return outcome;
  }

  /**
   * Create the filename for a report from the jobID.
   * @param jobId jobId
   * @return filename for a report.
   */
  public static String createJobSummaryFilename(String jobId) {
    return String.format(SUMMARY_FILENAME_FORMAT, jobId);
  }

  /**
   * Logic to create directory names from job and attempt.
   * This is self-contained it so it can be used in tests
   * as well as in the committer.
   */
  public static class AttemptDirectories {

    /**
     * Job output path.
     */
    private final Path outputPath;

    /**
     * Path for the job attempt.
     */
    private final Path jobAttemptDir;

    /**
     * Path for the job.
     */
    private final Path jobPath;

    /**
     * Subdir under the job attempt dir where task
     * attempts will have subdirectories.
     */
    private final Path jobAttemptTaskSubDir;

    /**
     * temp directory under job test dir.
     */
    private final Path outputTempSubDir;

    /**
     * Build the attempt directories.
     * @param outputPath output path
     * @param jobUniqueId job ID/UUID
     * @param jobAttemptNumber job attempt number
     */
    public AttemptDirectories(
        Path outputPath,
        String jobUniqueId,
        int jobAttemptNumber) {
      this.outputPath = requireNonNull(outputPath, "Output path");

      this.outputTempSubDir = new Path(outputPath, PENDING_DIR_NAME);
      // build the path for the job
      this.jobPath = new Path(outputTempSubDir,
          String.format(JOB_DIR_FORMAT_STR, jobUniqueId));

      // then the specific path underneath that for the attempt.
      this.jobAttemptDir = new Path(jobPath,
          String.format(JOB_ATTEMPT_DIR_FORMAT_STR, jobAttemptNumber));

      // subdir for task attempts.
      this.jobAttemptTaskSubDir = new Path(jobAttemptDir, PENDING_DIR_NAME);
    }

    public Path getOutputPath() {
      return outputPath;
    }

    public Path getJobAttemptDir() {
      return jobAttemptDir;
    }

    public Path getJobPath() {
      return jobPath;
    }

    public Path getJobAttemptTaskSubDir() {
      return jobAttemptTaskSubDir;
    }

    public Path getTaskAttemptPath(String taskAttemptId) {
      return new Path(jobAttemptTaskSubDir, taskAttemptId);
    }

    public Path getOutputTempSubDir() {
      return outputTempSubDir;
    }
  }
}
