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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.AuditingIntegration;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperations;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperationsThroughFileSystem;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.AbortTaskStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CleanupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CommitJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CommitTaskStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.SetupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.SetupTaskStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageConfig;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageEventCallbacks;
import org.apache.hadoop.util.functional.CloseableTaskPoolSubmitter;

import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.logIOStatisticsAtDebug;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.CAPABILITY_DYNAMIC_PARTITIONING;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_DIAGNOSTICS_MANIFEST_DIR;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_IO_PROCESSORS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_IO_PROCESSORS_DEFAULT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_SUMMARY_REPORT_DIR;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASKS_COMPLETED_COUNT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASKS_FAILED_COUNT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_COMMIT_FILE_RENAME_RECOVERED;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_ABORT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_CLEANUP;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DiagnosticKeys.STAGE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.AuditingIntegration.updateCommonContextOnCommitterExit;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.AuditingIntegration.updateCommonContextOnCommitterEntry;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.createIOStatisticsStore;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.createJobSummaryFilename;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.createManifestOutcome;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.manifestPathForTask;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CleanupJobStage.cleanupStageOptionsFromConfig;

/**
 * This is the Intermediate-Manifest committer.
 * At every entry point it updates the thread's audit context with
 * the current stage info; this is a placeholder for
 * adding audit information to stores other than S3A.
 *
 * This is tagged as public/stable. This is mandatory
 * for the classname and PathOutputCommitter implementation
 * classes.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ManifestCommitter extends PathOutputCommitter implements
    IOStatisticsSource, StageEventCallbacks, StreamCapabilities {

  public static final Logger LOG = LoggerFactory.getLogger(
      ManifestCommitter.class);

  /**
   * Role: task committer.
   */
  public static final String TASK_COMMITTER = "task committer";

  /**
   * Role: job committer.
   */
  public static final String JOB_COMMITTER = "job committer";

  /**
   * Committer Configuration as extracted from
   * the job/task context and set in the constructor.
   */
  private final ManifestCommitterConfig baseConfig;

  /**
   * Destination of the job.
   */
  private final Path destinationDir;

  /**
   * For tasks, the attempt directory.
   * Null for jobs.
   */
  private final Path taskAttemptDir;

  /**
   * IOStatistics to update.
   */
  private final IOStatisticsStore iostatistics;

  /**
   *  The job Manifest Success data; only valid after a job successfully
   *  commits.
   */
  private ManifestSuccessData successReport;

  /**
   * The active stage; is updated by a callback from within the stages.
   */
  private String activeStage;

  /**
   * The task manifest of the task commit.
   * Null unless this is a task attempt and the
   * task has successfully been committed.
   */
  private TaskManifest taskAttemptCommittedManifest;

  /**
   * Create a committer.
   * @param outputPath output path
   * @param context job/task context
   * @throws IOException failure.
   */
  public ManifestCommitter(final Path outputPath,
      final TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    this.destinationDir = resolveDestinationDirectory(outputPath,
        context.getConfiguration());
    this.iostatistics = createIOStatisticsStore().build();
    this.baseConfig = enterCommitter(
        context.getTaskAttemptID() != null,
        context);

    this.taskAttemptDir = baseConfig.getTaskAttemptDir();
    LOG.info("Created ManifestCommitter with JobID {},"
            + " Task Attempt {} and destination {}",
        context.getJobID(), context.getTaskAttemptID(), outputPath);
  }

  /**
   * Committer method invoked; generates a config for it.
   * Calls {@code #updateCommonContextOnCommitterEntry()}
   * to update the audit context.
   * @param isTask is this a task entry point?
   * @param context context
   * @return committer config
   */
  private ManifestCommitterConfig enterCommitter(boolean isTask,
      JobContext context) {
    ManifestCommitterConfig committerConfig =
        new ManifestCommitterConfig(
            getOutputPath(),
            isTask ? TASK_COMMITTER : JOB_COMMITTER,
            context,
            iostatistics,
            this);
    updateCommonContextOnCommitterEntry(committerConfig);
    return committerConfig;
  }

  /**
   * Set up a job through a {@link SetupJobStage}.
   * @param jobContext Context of the job whose output is being written.
   * @throws IOException IO Failure.
   */
  @Override
  public void setupJob(final JobContext jobContext) throws IOException {
    ManifestCommitterConfig committerConfig = enterCommitter(false,
        jobContext);
    StageConfig stageConfig =
        committerConfig
            .createStageConfig()
            .withOperations(createManifestStoreOperations())
            .build();
    // set up the job.
    new SetupJobStage(stageConfig)
        .apply(committerConfig.getCreateJobMarker());
    logCommitterStatisticsAtDebug();
  }

  /**
   * Set up a task through a {@link SetupTaskStage}.
   * Classic FileOutputCommitter is a no-op here, relying
   * on RecordWriters to create the dir implicitly on file
   * create().
   * FileOutputCommitter also uses the existence of that
   * file as a flag to indicate task commit is needed.
   * @param context task context.
   * @throws IOException IO Failure.
   */
  @Override
  public void setupTask(final TaskAttemptContext context)
      throws IOException {
    ManifestCommitterConfig committerConfig =
        enterCommitter(true, context);
    StageConfig stageConfig =
        committerConfig
            .createStageConfig()
            .withOperations(createManifestStoreOperations())
            .build();
    // create task attempt dir; delete if present. Or fail?
    new SetupTaskStage(stageConfig).apply("");
    logCommitterStatisticsAtDebug();
  }

  /**
   * Always return true.
   * This way, even if there is no output, stats are collected.
   * @param context task context.
   * @return true
   * @throws IOException IO Failure.
   */
  @Override
  public boolean needsTaskCommit(final TaskAttemptContext context)
      throws IOException {
    LOG.info("Probe for needsTaskCommit({})",
        context.getTaskAttemptID());
    return true;
  }

  /**
   * Failure during Job Commit is not recoverable from.
   *
   * @param jobContext
   *          Context of the job whose output is being written.
   * @return false, always
   * @throws IOException never
   */
  @Override
  public boolean isCommitJobRepeatable(final JobContext jobContext)
      throws IOException {
    LOG.info("Probe for isCommitJobRepeatable({}): returning false",
        jobContext.getJobID());
    return false;
  }

  /**
   * Declare that task recovery is not supported.
   * It would be, if someone added the code *and tests*.
   * @param jobContext
   *          Context of the job whose output is being written.
   * @return false, always
   * @throws IOException never
   */
  @Override
  public boolean isRecoverySupported(final JobContext jobContext)
      throws IOException {
    LOG.info("Probe for isRecoverySupported({}): returning false",
        jobContext.getJobID());
    return false;
  }

  /**
   *
   * @param taskContext Context of the task whose output is being recovered
   * @throws IOException always
   */
  @Override
  public void recoverTask(final TaskAttemptContext taskContext)
      throws IOException {
    LOG.warn("Rejecting recoverTask({}) call", taskContext.getTaskAttemptID());
    throw new IOException("Cannot recover task "
        + taskContext.getTaskAttemptID());
  }

  /**
   * Commit the task.
   * This is where the task attempt tree list takes place.
   * @param context task context.
   * @throws IOException IO Failure.
   */
  @Override
  public void commitTask(final TaskAttemptContext context)
      throws IOException {
    ManifestCommitterConfig committerConfig = enterCommitter(true,
        context);
    try {
      StageConfig stageConfig = committerConfig.createStageConfig()
          .withOperations(createManifestStoreOperations())
          .build();
      taskAttemptCommittedManifest = new CommitTaskStage(stageConfig)
          .apply(null).getTaskManifest();
      iostatistics.incrementCounter(COMMITTER_TASKS_COMPLETED_COUNT, 1);
    } catch (IOException e) {
      iostatistics.incrementCounter(COMMITTER_TASKS_FAILED_COUNT, 1);
      throw e;
    } finally {
      logCommitterStatisticsAtDebug();
      updateCommonContextOnCommitterExit();
    }

  }

  /**
   * Abort a task.
   * @param context task context
   * @throws IOException failure during the delete
   */
  @Override
  public void abortTask(final TaskAttemptContext context)
      throws IOException {
    ManifestCommitterConfig committerConfig = enterCommitter(true,
        context);
    try {
      new AbortTaskStage(
          committerConfig.createStageConfig()
              .withOperations(createManifestStoreOperations())
              .build())
          .apply(false);
    } finally {
      logCommitterStatisticsAtDebug();
      updateCommonContextOnCommitterExit();
    }
  }

  /**
   * Get the manifest success data for this job; creating on demand if needed.
   * @param committerConfig source config.
   * @return the current {@link #successReport} value; never null.
   */
  private ManifestSuccessData getOrCreateSuccessData(
      ManifestCommitterConfig committerConfig) {
    if (successReport == null) {
      successReport = createManifestOutcome(
          committerConfig.createStageConfig(), activeStage);
    }
    return successReport;
  }

  /**
   * This is the big job commit stage.
   * Load the manifests, prepare the destination, rename
   * the files then cleanup the job directory.
   * @param jobContext Context of the job whose output is being written.
   * @throws IOException failure.
   */
  @Override
  public void commitJob(final JobContext jobContext) throws IOException {

    ManifestCommitterConfig committerConfig = enterCommitter(false, jobContext);

    // create the initial success data.
    // this is overwritten by that created during the operation sequence,
    // but if the sequence fails before that happens, it
    // will be saved to the report directory.
    ManifestSuccessData marker = getOrCreateSuccessData(committerConfig);
    IOException failure = null;
    try (CloseableTaskPoolSubmitter ioProcs =
             committerConfig.createSubmitter();
         ManifestStoreOperations storeOperations = createManifestStoreOperations()) {
      // the stage config will be shared across all stages.
      StageConfig stageConfig = committerConfig.createStageConfig()
          .withOperations(storeOperations)
          .withIOProcessors(ioProcs)
          .build();

      // commit the job, including any cleanup and validation.
      final Configuration conf = jobContext.getConfiguration();
      CommitJobStage.Result result = new CommitJobStage(stageConfig).apply(
          new CommitJobStage.Arguments(
              committerConfig.getCreateJobMarker(),
              committerConfig.getValidateOutput(),
              conf.getTrimmed(OPT_DIAGNOSTICS_MANIFEST_DIR, ""),
              cleanupStageOptionsFromConfig(
                  OP_STAGE_JOB_CLEANUP, conf)
          ));
      marker = result.getJobSuccessData();
      // update the cached success with the new report.
      setSuccessReport(marker);
      // patch in the #of threads as it is useful
      marker.putDiagnostic(OPT_IO_PROCESSORS,
          conf.get(OPT_IO_PROCESSORS, Long.toString(OPT_IO_PROCESSORS_DEFAULT)));
    } catch (IOException e) {
      // failure. record it for the summary
      failure = e;
      // rethrow
      throw e;
    } finally {
      // save the report summary, even on failure
      maybeSaveSummary(activeStage,
          committerConfig,
          marker,
          failure,
          true,
          true);
      // print job commit stats
      LOG.info("{}: Job Commit statistics {}",
          committerConfig.getName(),
          ioStatisticsToPrettyString(iostatistics));
      // and warn of rename problems
      final Long recoveries = iostatistics.counters().get(OP_COMMIT_FILE_RENAME_RECOVERED);
      if (recoveries != null && recoveries > 0) {
        LOG.warn("{}: rename failures were recovered from. Number of recoveries: {}",
            committerConfig.getName(), recoveries);
      }
      updateCommonContextOnCommitterExit();
    }
  }

  /**
   * Abort the job.
   * Invokes
   * {@link #executeCleanup(String, JobContext, ManifestCommitterConfig)}
   * then saves the (ongoing) job report data if reporting is enabled.
   * @param jobContext Context of the job whose output is being written.
   * @param state final runstate of the job
   * @throws IOException failure during cleanup; report failure are swallowed
   */
  @Override
  public void abortJob(final JobContext jobContext,
      final JobStatus.State state)
      throws IOException {
    LOG.info("Aborting Job {} in state {}", jobContext.getJobID(), state);
    ManifestCommitterConfig committerConfig = enterCommitter(false,
        jobContext);
    ManifestSuccessData report = getOrCreateSuccessData(
        committerConfig);
    IOException failure = null;

    try {
      executeCleanup(OP_STAGE_JOB_ABORT, jobContext, committerConfig);
    } catch (IOException e) {
      // failure.
      failure = e;
    }
    report.setSuccess(false);
    // job abort does not overwrite any existing report, so a job commit
    // failure cause will be preserved.
    maybeSaveSummary(activeStage, committerConfig, report, failure,
        true, false);
    // print job stats
    LOG.info("Job Abort statistics {}",
        ioStatisticsToPrettyString(iostatistics));
    updateCommonContextOnCommitterExit();
  }

  /**
   * Execute the {@code CleanupJobStage} to remove the job attempt dir.
   * This does
   * @param jobContext Context of the job whose output is being written.
   * @throws IOException failure during cleanup
   */
  @SuppressWarnings("deprecation")
  @Override
  public void cleanupJob(final JobContext jobContext) throws IOException {
    ManifestCommitterConfig committerConfig = enterCommitter(false,
        jobContext);
    try {
      executeCleanup(OP_STAGE_JOB_CLEANUP, jobContext, committerConfig);
    } finally {
      logCommitterStatisticsAtDebug();
      updateCommonContextOnCommitterExit();
    }
  }

  /**
   * Perform the cleanup operation for job cleanup or abort.
   * @param statisticName statistic/stage name
   * @param jobContext job context
   * @param committerConfig committer config
   * @throws IOException failure
   * @return the outcome
   */
  private CleanupJobStage.Result executeCleanup(
      final String statisticName,
      final JobContext jobContext,
      final ManifestCommitterConfig committerConfig) throws IOException {
    try (CloseableTaskPoolSubmitter ioProcs =
             committerConfig.createSubmitter()) {

      return new CleanupJobStage(
          committerConfig.createStageConfig()
              .withOperations(createManifestStoreOperations())
              .withIOProcessors(ioProcs)
              .build())
          .apply(cleanupStageOptionsFromConfig(
              statisticName,
              jobContext.getConfiguration()));
    }
  }

  /**
   * Output path: destination directory of the job.
   * @return the overall job destination directory.
   */
  @Override
  public Path getOutputPath() {
    return getDestinationDir();
  }

  /**
   * Work path of the current task attempt.
   * This is null if the task does not have one.
   * @return a path.
   */
  @Override
  public Path getWorkPath() {
    return getTaskAttemptDir();
  }

  /**
   * Get the job destination dir.
   * @return dest dir.
   */
  private Path getDestinationDir() {
    return destinationDir;
  }

  /**
   * Get the task attempt dir.
   * May be null.
   * @return a path or null.
   */
  private Path getTaskAttemptDir() {
    return taskAttemptDir;
  }

  /**
   * Callback on stage entry.
   * Sets {@link #activeStage} and updates the
   * common context.
   * @param stage new stage
   */
  @Override
  public void enterStage(String stage) {
    activeStage = stage;
    AuditingIntegration.enterStage(stage);
  }

  /**
   * Remove stage from common audit context.
   * @param stage stage exited.
   */
  @Override
  public void exitStage(String stage) {
    AuditingIntegration.exitStage();
  }

  /**
   * Get the unique ID of this job.
   * @return job ID (yarn, spark)
   */
  public String getJobUniqueId() {
    return baseConfig.getJobUniqueId();
  }

  /**
   * Get the config of the task attempt this instance was constructed
   * with.
   * @return a configuration.
   */
  public Configuration getConf() {
    return baseConfig.getConf();
  }

  /**
   * Get the manifest Success data; only valid after a job.
   * @return the job _SUCCESS data, or null.
   */
  public ManifestSuccessData getSuccessReport() {
    return successReport;
  }

  private void setSuccessReport(ManifestSuccessData successReport) {
    this.successReport = successReport;
  }

  /**
   * Get the manifest of the last committed task.
   * @return a task manifest or null.
   */
  @VisibleForTesting
  TaskManifest getTaskAttemptCommittedManifest() {
    return taskAttemptCommittedManifest;
  }

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   */
  @VisibleForTesting
  public Path getTaskAttemptPath(TaskAttemptContext context) {
    return enterCommitter(false, context).getTaskAttemptDir();
  }

  /**
   * The path to where the manifest file of a task attempt will be
   * saved when the task is committed.
   * This path will be the same for all attempts of the same task.
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   */
  @VisibleForTesting
  public Path getTaskManifestPath(TaskAttemptContext context) {
    final Path dir = enterCommitter(false, context).getTaskManifestDir();

    return manifestPathForTask(dir,
        context.getTaskAttemptID().getTaskID().toString());
  }

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   */
  @VisibleForTesting
  public Path getJobAttemptPath(JobContext context) {

    return enterCommitter(false, context).getJobAttemptDir();
  }

  /**
   * Get the final output path, including resolving any relative path.
   * @param outputPath output path
   * @param conf configuration to create any FS with
   * @return a resolved path.
   * @throws IOException failure.
   */
  private Path resolveDestinationDirectory(Path outputPath,
      Configuration conf) throws IOException {
    return FileSystem.get(outputPath.toUri(), conf).makeQualified(outputPath);
  }

  /**
   * Create manifest store operations for the destination store.
   * This MUST NOT be used for the success report operations, as
   * they may be to a different filesystem.
   * This is a point which can be overridden during testing.
   * @return a new store operations instance bonded to the destination fs.
   * @throws IOException failure to instantiate.
   */
  protected ManifestStoreOperations createManifestStoreOperations() throws IOException {
    return ManifestCommitterSupport.createManifestStoreOperations(
        baseConfig.getConf(),
        baseConfig.getDestinationFileSystem(),
        baseConfig.getDestinationDir());
  }

  /**
   * Log IO Statistics at debug.
   */
  private void logCommitterStatisticsAtDebug() {
    logIOStatisticsAtDebug(LOG, "Committer Statistics", this);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "ManifestCommitter{");
    sb.append(baseConfig);
    sb.append(", iostatistics=").append(ioStatisticsToPrettyString(iostatistics));
    sb.append('}');
    return sb.toString();
  }

  /**
   * Save a summary to the report dir if the config option
   * is set.
   * The IOStatistics of the summary will be updated to the latest
   * snapshot of the committer's statistics, so the report is up
   * to date.
   * The report will updated with the current active stage,
   * and if {@code thrown} is non-null, it will be added to the
   * diagnostics (and the job tagged as a failure).
   * Static for testability.
   * @param activeStage active stage
   * @param config configuration to use.
   * @param report summary file.
   * @param thrown any exception indicting failure.
   * @param quiet should exceptions be swallowed.
   * @param overwrite should the existing file be overwritten
   * @return the path of a file, if successfully saved
   * @throws IOException if a failure occured and quiet==false
   */
  private static Path maybeSaveSummary(
      String activeStage,
      ManifestCommitterConfig config,
      ManifestSuccessData report,
      Throwable thrown,
      boolean quiet,
      boolean overwrite) throws IOException {
    Configuration conf = config.getConf();
    String reportDir = conf.getTrimmed(OPT_SUMMARY_REPORT_DIR, "");
    if (reportDir.isEmpty()) {
      LOG.debug("No summary directory set in " + OPT_SUMMARY_REPORT_DIR);
      return null;
    }
    LOG.debug("Summary directory set in to {}" + OPT_SUMMARY_REPORT_DIR,
        reportDir);

    // update to the latest statistics
    report.snapshotIOStatistics(config.getIOStatistics());

    Path reportDirPath = new Path(reportDir);
    Path path = new Path(reportDirPath,
        createJobSummaryFilename(config.getJobUniqueId()));

    if (thrown != null) {
      report.recordJobFailure(thrown);
    }
    report.putDiagnostic(STAGE, activeStage);
    // the store operations here is explicitly created for the FS where
    // the reports go, which may not be the target FS of the job.

    final FileSystem fs = path.getFileSystem(conf);
    try (ManifestStoreOperations operations = new ManifestStoreOperationsThroughFileSystem(fs)) {
      if (!overwrite) {
        // check for file existence so there is no need to worry about
        // precisely what exception is raised when overwrite=false and dest file
        // exists
        try {
          FileStatus st = operations.getFileStatus(path);
          // get here and the file exists
          LOG.debug("Report already exists: {}", st);
          return null;
        } catch (FileNotFoundException ignored) {
        }
      }
      operations.save(report, path, overwrite);
      LOG.info("Job summary saved to {}", path);
      return path;
    } catch (IOException e) {
      LOG.debug("Failed to save summary to {}", path, e);
      if (quiet) {
        return null;
      } else {
        throw e;
      }
    }
  }

  @Override
  public IOStatisticsStore getIOStatistics() {
    return iostatistics;
  }

  /**
   * The committer is compatible with spark's dynamic partitioning
   * algorithm.
   * @param capability string to query the stream support for.
   * @return true if the requested capability is supported.
   */
  @Override
  public boolean hasCapability(final String capability) {
    return CAPABILITY_DYNAMIC_PARTITIONING.equals(capability);
  }
}
