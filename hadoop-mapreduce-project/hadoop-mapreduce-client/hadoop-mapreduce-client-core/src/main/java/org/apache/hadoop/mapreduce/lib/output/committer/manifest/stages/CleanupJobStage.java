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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.hadoop.util.functional.TaskPool;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED_DEFAULT;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED_DEFAULT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_CLEANUP_PARALLEL_DELETE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_CLEANUP_PARALLEL_DELETE_DIRS_DEFAULT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_CLEANUP;

/**
 * Clean up a job's temporary directory through parallel delete,
 * base _temporary delete.
 * Returns: the outcome of the overall operation
 * The result is detailed purely for the benefit of tests, which need
 * to make assertions about error handling and fallbacks.
 *
 * There's a few known issues with the azure and GCS stores which
 * this stage tries to address.
 * - Google GCS directory deletion is O(entries), so is slower for big jobs.
 * - Azure storage directory delete, when using OAuth authentication or
 *   when not the store owner triggers a scan down the tree to verify the
 *   caller has the permission to delete each subdir.
 *   If this scan takes over 90s, the operation can time out.
 *
 * The main solution for both of these is that task attempts are
 * deleted in parallel, in different threads.
 * This will speed up GCS cleanup and reduce the risk of
 * abfs related timeouts.
 * Exceptions during cleanup can be suppressed,
 * so that these do not cause the job to fail.
 *
 * Also, some users want to be able to run multiple independent jobs
 * targeting the same output directory simultaneously.
 * If one job deletes the directory `__temporary` all the others
 * will fail.
 *
 * This can be addressed by disabling cleanup entirely.
 *
 */
public class CleanupJobStage extends
    AbstractJobOrTaskStage<
            CleanupJobStage.Arguments,
            CleanupJobStage.Result> {

  private static final Logger LOG = LoggerFactory.getLogger(
      CleanupJobStage.class);

  /**
   * Count of deleted directories.
   */
  private final AtomicInteger deleteDirCount = new AtomicInteger();

  /**
   * Count of delete failures.
   */
  private final AtomicInteger deleteFailureCount = new AtomicInteger();

  /**
   * Last delete exception; non null if deleteFailureCount is not zero.
   */
  private IOException lastDeleteException;

  /**
   * Stage name as passed in from arguments.
   */
  private String stageName = OP_STAGE_JOB_CLEANUP;

  public CleanupJobStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_CLEANUP, true);
  }

  /**
   * Statistic name is extracted from the arguments.
   * @param arguments args to the invocation.
   * @return stage name.
   */
  @Override
  protected String getStageStatisticName(Arguments arguments) {
    return arguments.statisticName;
  }

  /**
   * Clean up the job attempt directory tree.
   * @param args arguments built up.
   * @return the result.
   * @throws IOException failure was raised an exceptions weren't surpressed.
   */
  @Override
  protected Result executeStage(
      final Arguments args)
      throws IOException {
    stageName = getStageName(args);
    // this is $dest/_temporary
    final Path baseDir = requireNonNull(getStageConfig().getOutputTempSubDir());
    LOG.debug("{}: Cleaup of directory {} with {}", getName(), baseDir, args);
    if (!args.enabled) {
      LOG.info("{}: Cleanup of {} disabled", getName(), baseDir);
      return new Result(Outcome.DISABLED, baseDir,
          0, null);
    }
    // shortcut of a single existence check before anything else
    if (getFileStatusOrNull(baseDir) == null) {
      return new Result(Outcome.NOTHING_TO_CLEAN_UP,
          baseDir,
          0, null);
    }

    Outcome outcome = null;
    IOException exception;


    // to delete.
    LOG.info("{}: Deleting job directory {}", getName(), baseDir);

    if (args.deleteTaskAttemptDirsInParallel) {
      // Attempt to do a parallel delete of task attempt dirs;
      // don't overreact if a delete fails, but stop trying
      // to delete the others, and fall back to deleting the
      // job dir.
      Path taskSubDir
          = getStageConfig().getJobAttemptTaskSubDir();
      try (DurationInfo info = new DurationInfo(LOG,
          "parallel deletion of task attempts in %s",
          taskSubDir)) {
        RemoteIterator<FileStatus> dirs =
            RemoteIterators.filteringRemoteIterator(
                listStatusIterator(taskSubDir),
                FileStatus::isDirectory);
        TaskPool.foreach(dirs)
            .executeWith(getIOProcessors())
            .stopOnFailure()
            .suppressExceptions(false)
            .run(this::rmTaskAttemptDir);
        getIOStatistics().aggregate((retrieveIOStatistics(dirs)));

        if (getLastDeleteException() != null) {
          // one of the task attempts failed.
          throw getLastDeleteException();
        }
        // success: record this as the outcome.
        outcome = Outcome.PARALLEL_DELETE;
      } catch (FileNotFoundException ex) {
        // not a problem if there's no dir to list.
        LOG.debug("{}: Task attempt dir {} not found", getName(), taskSubDir);
        outcome = Outcome.DELETED;
      } catch (IOException ex) {
        // failure. Log and continue
        LOG.info(
            "{}: Exception while listing/deleting task attempts under {}; continuing",
            getName(),
            taskSubDir, ex);
        // not overreacting here as the base delete will still get executing
        outcome = Outcome.DELETED;
      }
    }
    // Now the top-level deletion; exception gets saved
    exception = deleteOneDir(baseDir);
    if (exception != null) {
      // failure, report and continue
      // assume failure.
      outcome = Outcome.FAILURE;
    } else {
      // if the outcome isn't already recorded as parallel delete,
      // mark is a simple delete.
      if (outcome == null) {
        outcome = Outcome.DELETED;
      }
    }

    Result result = new Result(
        outcome,
        baseDir,
        deleteDirCount.get(),
        exception);
    if (!result.succeeded() && !args.suppressExceptions) {
      result.maybeRethrowException();
    }

    return result;
  }

  /**
   * Delete a single TA dir in a parallel task.
   * Updates the audit context.
   * Exceptions are swallowed so that attempts are still made
   * to delete the others, but the first exception
   * caught is saved in a field which can be retrieved
   * via {@link #getLastDeleteException()}.
   *
   * @param status dir to be deleted.
   * @throws IOException delete failure.
   */
  private void rmTaskAttemptDir(FileStatus status) throws IOException {
    // stage name in audit context is the one set in the arguments.
    updateAuditContext(stageName);
    // update the progress callback in case delete is really slow.
    progress();
    deleteOneDir(status.getPath());
  }

  /**
   * Delete a directory.
   * The {@link #deleteFailureCount} counter.
   * is incremented on every failure.
   * @param dir directory
   * @throws IOException if an IOE was raised
   * @return any IOE raised.
   */
  private IOException deleteOneDir(final Path dir)
      throws IOException {

    deleteDirCount.incrementAndGet();
    IOException ex = deleteDir(dir, true);
    if (ex != null) {
      deleteFailure(ex);
    }
    return ex;
  }

  /**
   * Note a failure.
   * @param ex exception
   */
  private synchronized void deleteFailure(IOException ex) {
    // excaption: add the count
    deleteFailureCount.incrementAndGet();
    lastDeleteException = ex;
  }

  /**
   * Get the last delete exception; synchronized.
   * @return the last delete exception or null.
   */
  public synchronized IOException getLastDeleteException() {
    return lastDeleteException;
  }

  /**
   * Options to pass down to the cleanup stage.
   */
  public static final class Arguments {

    /**
     * Statistic to update.
     */
    private final String statisticName;

    /** Delete is enabled? */
    private final boolean enabled;

    /** Attempt parallel delete of task attempt dirs? */
    private final boolean deleteTaskAttemptDirsInParallel;

    /** Ignore failures? */
    private final boolean suppressExceptions;

    /**
     * Arguments to the stage.
     * @param statisticName stage name to report
     * @param enabled is the stage enabled?
     * @param deleteTaskAttemptDirsInParallel delete task attempt dirs in
     * parallel?
     * @param suppressExceptions suppress exceptions?
     */
    public Arguments(
        final String statisticName,
        final boolean enabled,
        final boolean deleteTaskAttemptDirsInParallel,
        final boolean suppressExceptions) {
      this.statisticName = statisticName;
      this.enabled = enabled;
      this.deleteTaskAttemptDirsInParallel = deleteTaskAttemptDirsInParallel;
      this.suppressExceptions = suppressExceptions;
    }

    public String getStatisticName() {
      return statisticName;
    }

    public boolean isEnabled() {
      return enabled;
    }

    public boolean isDeleteTaskAttemptDirsInParallel() {
      return deleteTaskAttemptDirsInParallel;
    }

    public boolean isSuppressExceptions() {
      return suppressExceptions;
    }

    @Override
    public String toString() {
      return "Arguments{" +
          "statisticName='" + statisticName + '\''
          + ", enabled=" + enabled
          + ", deleteTaskAttemptDirsInParallel="
          + deleteTaskAttemptDirsInParallel
          + ", suppressExceptions=" + suppressExceptions
          + '}';
    }
  }

  /**
   * Static disabled arguments.
   */
  public static final Arguments DISABLED = new Arguments(OP_STAGE_JOB_CLEANUP,
      false,
      false,
      false
  );

  /**
   * Build an options argument from a configuration, using the
   * settings from FileOutputCommitter and manifest committer.
   * @param statisticName statistic name to use in duration tracking.
   * @param conf configuration to use.
   * @return the options to process
   */
  public static Arguments cleanupStageOptionsFromConfig(
      String statisticName, Configuration conf) {

    boolean enabled = !conf.getBoolean(FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED,
        FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED_DEFAULT);
    boolean suppressExceptions = conf.getBoolean(
        FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED,
        FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED_DEFAULT);
    boolean deleteTaskAttemptDirsInParallel = conf.getBoolean(
        OPT_CLEANUP_PARALLEL_DELETE,
        OPT_CLEANUP_PARALLEL_DELETE_DIRS_DEFAULT);
    return new Arguments(
        statisticName,
        enabled,
        deleteTaskAttemptDirsInParallel,
        suppressExceptions
    );
  }

  /**
   * Enum of outcomes.
   */
  public enum Outcome {
    DISABLED("Disabled", false),
    NOTHING_TO_CLEAN_UP("Nothing to clean up", true),
    PARALLEL_DELETE("Parallel Delete of Task Attempt Directories", true),
    DELETED("Delete of job directory", true),
    FAILURE("Delete failed", false);

    private final String description;

    private final boolean success;

    Outcome(String description, boolean success) {
      this.description = description;
      this.success = success;
    }

    @Override
    public String toString() {
      return "Outcome{" + name() +
          " '" + description + '\'' +
          "}";
    }

    /**
     * description.
     * @return text for logging
     */
    public String getDescription() {
      return description;
    }

    /**
     * Was this a success?
     * @return true if this outcome is good.
     */
    public boolean isSuccess() {
      return success;
    }
  }

  /**
   * Result of the cleanup.
   * If the outcome == FAILURE but exceptions were suppressed
   * (which they are implicitly if an instance of this object
   * is created and returned), then the exception
   * MUST NOT be null.
   */
  public static final class Result {

    /** Outcome. */
    private final Outcome outcome;

    /** Directory cleaned up. */
    private final Path directory;

    /**
     * Number of delete calls made across all threads.
     */
    private final int deleteCalls;

    /**
     * Any IOE raised.
     */
    private final IOException exception;

    public Result(
        final Outcome outcome,
        final Path directory,
        final int deleteCalls,
        IOException exception) {
      this.outcome = requireNonNull(outcome, "outcome");
      this.directory = directory;
      this.deleteCalls = deleteCalls;
      this.exception = exception;
      if (outcome == Outcome.FAILURE) {
        requireNonNull(exception, "No exception in failure result");
      }
    }

    public Path getDirectory() {
      return directory;
    }

    public boolean wasExecuted() {
      return outcome != Outcome.DISABLED;
    }

    /**
     * Was the outcome a success?
     * That is: either the dir wasn't there or through
     * delete/rename it is no longer there.
     * @return true if the temporary dir no longer exists.
     */
    public boolean succeeded() {
      return outcome.isSuccess();
    }

    public Outcome getOutcome() {
      return outcome;
    }

    public int getDeleteCalls() {
      return deleteCalls;
    }

    public IOException getException() {
      return exception;
    }

    /**
     * If there was an IOE caught, throw it.
     * For ease of use in (meaningful) lambda expressions
     * in tests, returns the string value if there
     * was no exception to throw (for use in tests)
     * @throws IOException exception.
     */
    public String maybeRethrowException() throws IOException {
      if (exception != null) {
        throw exception;
      }
      return toString();
    }

    @Override
    public String toString() {
      return "CleanupResult{" +
          "outcome=" + outcome +
          ", directory=" + directory +
          ", deleteCalls=" + deleteCalls +
          ", exception=" + exception +
          '}';
    }
  }
}
