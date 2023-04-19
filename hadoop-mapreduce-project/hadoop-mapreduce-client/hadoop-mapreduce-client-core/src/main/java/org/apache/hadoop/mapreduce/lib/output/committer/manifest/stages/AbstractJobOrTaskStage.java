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
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperations;
import org.apache.hadoop.util.OperationDuration;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.functional.CallableRaisingIOE;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.hadoop.util.functional.TaskPool;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_DELETE;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_GET_FILE_STATUS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_IS_FILE;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_LIST_STATUS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_MKDIRS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.STORE_IO_RATE_LIMITED;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.createTracker;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfInvocation;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.MANIFEST_SUFFIX;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_COMMIT_FILE_RENAME;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_COMMIT_FILE_RENAME_RECOVERED;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_LOAD_MANIFEST;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_MSYNC;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_RENAME_FILE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_SAVE_TASK_MANIFEST;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.AuditingIntegration.enterStageWorker;

/**
 * A Stage in Task/Job Commit.
 * A stage can be executed once only, creating the return value of the
 * {@link #apply(Object)} method, and, potentially, updating the state of the
 * store via {@link ManifestStoreOperations}.
 * IOStatistics will also be updated.
 * Stages are expected to be combined to form the commit protocol.
 * @param <IN> Type of arguments to the stage.
 * @param <OUT> Type of result.
 */
public abstract class AbstractJobOrTaskStage<IN, OUT>
    implements JobOrTaskStage<IN, OUT> {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractJobOrTaskStage.class);

  /**
   * Error text on rename failure: {@value}.
   */
  public static final String FAILED_TO_RENAME_PREFIX = "Failed to ";

  /**
   * Is this a task stage? If so, toString() includes task
   * info..
   */
  private final boolean isTaskStage;

  /**
   * Configuration of all the stages in the ongoing committer
   * operation.
   */
  private final StageConfig stageConfig;

  /**
   * Name of the stage for statistics and logging.
   */
  private final String stageStatisticName;

  /**
   * Callbacks to update store.
   * This is not made visible to the stages; they must
   * go through the wrapper classes in this class, which
   * add statistics and logging.
   */
  private final ManifestStoreOperations operations;

  /**
   * Submitter for doing IO against the store.
   */
  private final TaskPool.Submitter ioProcessors;

  /**
   * Used to stop any re-entrancy of the rename.
   * This is an execute-once operation.
   */
  private final AtomicBoolean executed = new AtomicBoolean(false);

  /**
   * Tracker of the duration of the execution of the stage.
   * set after {@link #executeStage(Object)} completes.
   */
  private DurationTracker stageExecutionTracker;

  /**
   * Name for logging.
   */
  private final String name;

  /**
   * Constructor.
   * @param isTaskStage Is this a task stage?
   * @param stageConfig stage-independent configuration.
   * @param stageStatisticName name of the stage for statistics/logging
   * @param requireIOProcessors are the IO processors required?
   */
  protected AbstractJobOrTaskStage(
      final boolean isTaskStage,
      final StageConfig stageConfig,
      final String stageStatisticName,
      final boolean requireIOProcessors) {
    this.isTaskStage = isTaskStage;
    this.stageStatisticName = stageStatisticName;
    this.stageConfig = stageConfig;
    requireNonNull(stageConfig.getDestinationDir(), "Destination Directory");
    requireNonNull(stageConfig.getJobId(), "Job ID");
    requireNonNull(stageConfig.getJobAttemptDir(), "Job attempt directory");
    this.operations = requireNonNull(stageConfig.getOperations(),
        "Operations callbacks");
    // and the processors of work if required.
    this.ioProcessors = bindProcessor(
        requireIOProcessors,
        stageConfig.getIoProcessors());
    String stageName;
    if (isTaskStage) {
      // force fast failure.
      getRequiredTaskId();
      getRequiredTaskAttemptId();
      getRequiredTaskAttemptDir();
      stageName = String.format("[Task-Attempt %s]", getRequiredTaskAttemptId());
    } else {
      stageName = String.format("[Job-Attempt %s/%02d]",
          stageConfig.getJobId(),
          stageConfig.getJobAttemptNumber());
    }
    name = stageName;
  }

  /**
   * Bind to the processor if it is required.
   * @param required is the processor required?
   * @param processor processor
   * @return the processor binding
   * @throws NullPointerException if required == true and processor is null.
   */
  private TaskPool.Submitter bindProcessor(
      final boolean required,
      final TaskPool.Submitter processor) {
    return required
        ? requireNonNull(processor, "required IO processor is null")
        : null;
  }

  /**
   * Stage entry point.
   * Verifies that this is the first and only time the stage is invoked,
   * then calls {@link #executeStage(Object)} for the subclass
   * to perform its part of the commit protocol.
   * The duration of the stage is collected as a statistic, and its
   * entry/exit logged at INFO.
   * @param arguments arguments to the function.
   * @return the result.
   * @throws IOException failures.
   */
  @Override
  public final OUT apply(final IN arguments) throws IOException {
    executeOnlyOnce();
    progress();
    String stageName = getStageName(arguments);
    getStageConfig().enterStage(stageName);
    String statisticName = getStageStatisticName(arguments);
    // The tracker here
    LOG.info("{}: Executing Stage {}", getName(), stageName);
    stageExecutionTracker = createTracker(getIOStatistics(), statisticName);
    try {
      // exec the input function and return its value
      final OUT out = executeStage(arguments);
      LOG.info("{}: Stage {} completed after {}",
          getName(),
          stageName,
          OperationDuration.humanTime(
              stageExecutionTracker.asDuration().toMillis()));
      return out;
    } catch (IOException | RuntimeException e) {
      LOG.error("{}: Stage {} failed: after {}: {}",
          getName(),
          stageName,
          OperationDuration.humanTime(
              stageExecutionTracker.asDuration().toMillis()),
          e.toString());
      LOG.debug("{}: Stage failure:", getName(), e);
      // input function failed: note it
      stageExecutionTracker.failed();
      // and rethrow
      throw e;
    } finally {
      // update the tracker.
      // this is called after the catch() call would have
      // set the failed flag.
      stageExecutionTracker.close();
      progress();
      getStageConfig().exitStage(stageName);
    }
  }

  /**
   * The work of a stage.
   * Executed exactly once.
   * @param arguments arguments to the function.
   * @return the result.
   * @throws IOException failures.
   */
  protected abstract OUT executeStage(IN arguments) throws IOException;

  /**
   * Check that the operation has not been invoked twice.
   * This is an atomic check.
   * @throws IllegalStateException on a second invocation.
   */
  private void executeOnlyOnce() {
    Preconditions.checkState(
        !executed.getAndSet(true),
        "Stage attempted twice");
  }

  /**
   * The stage statistic name.
   * @param arguments args to the invocation.
   * @return stage name.
   */
  protected String getStageStatisticName(IN arguments) {
    return stageStatisticName;
  }

  /**
   * Stage name for reporting; defaults to
   * call {@link #getStageStatisticName(IN)}.
   * @param arguments args to the invocation.
   * @return name used in updating reports.
   */
  protected String getStageName(IN arguments) {
    return getStageStatisticName(arguments);
  }

  /**
   * Get the execution tracker; non-null
   * after stage execution.
   * @return a tracker or null.
   */
  public DurationTracker getStageExecutionTracker() {
    return stageExecutionTracker;
  }

  /**
   * Adds the duration of the job to an IOStatistics store
   * (such as the manifest to be saved).
   * @param iostats store
   * @param statistic statistic name.
   */
  public void addExecutionDurationToStatistics(IOStatisticsStore iostats,
      String statistic) {
    iostats.addTimedOperation(
        statistic,
        getStageExecutionTracker().asDuration());
  }

  /**
   * Note any rate limiting to the given timing statistic.
   * If the wait was 0, no statistics are updated.
   * @param statistic statistic key.
   * @param wait wait duration.
   */
  private void noteAnyRateLimiting(String statistic, Duration wait) {
    if (!wait.isZero()) {
      // rate limiting took place
      getIOStatistics().addTimedOperation(
          statistic,
          wait.toMillis());
    }
  }


  /**
   * Get the operations callbacks.
   * @return the operations invocable against the destination.
   */
  public ManifestStoreOperations getOperations() {
    return operations;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "AbstractJobOrTaskStage{");
    sb.append(isTaskStage ? "Task Stage" : "Job Stage");
    sb.append(" name='").append(name).append('\'');
    sb.append(" stage='").append(stageStatisticName).append('\'');
    sb.append('}');
    return sb.toString();
  }

  /**
   * The stage configuration.
   * @return the stage configuration used by this stage.
   */
  protected StageConfig getStageConfig() {
    return stageConfig;
  }

  /**
   * Update the thread context with the stage name and
   * job ID.
   * This MUST be invoked at the start of methods invoked in helper threads,
   * to ensure that they are all annotated with job and stage.
   * @param stage stage name.
   */
  protected void updateAuditContext(final String stage) {
    enterStageWorker(stageConfig.getJobId(), stage);
  }

  /**
   * The IOStatistics are shared across all uses of the
   * StageConfig.
   * @return the (possibly shared) IOStatistics.
   */
  @Override
  public final IOStatisticsStore getIOStatistics() {
    return stageConfig.getIOStatistics();
  }

  /**
   * Call progress() on any Progressable passed in.
   */
  protected final void progress() {
    if (stageConfig.getProgressable() != null) {
      stageConfig.getProgressable().progress();
    }
  }

  /**
   * Get a file status value or, if the path doesn't exist, return null.
   * @param path path
   * @return status or null
   * @throws IOException IO Failure.
   */
  protected final FileStatus getFileStatusOrNull(
      final Path path)
      throws IOException {
    try {
      return getFileStatus(path);
    } catch (FileNotFoundException e) {
      return null;
    }
  }

  /**
   * Get a file status value or, if the path doesn't exist, return null.
   * @param path path
   * @return status or null
   * @throws IOException IO Failure.
   */
  protected final FileStatus getFileStatus(
      final Path path)
      throws IOException {
    LOG.trace("{}: getFileStatus('{}')", getName(), path);
    requireNonNull(path,
        () -> String.format("%s: Null path for getFileStatus() call", getName()));
    return trackDuration(getIOStatistics(), OP_GET_FILE_STATUS, () ->
        operations.getFileStatus(path));
  }

  /**
   * Get a file status value or, if the path doesn't exist, return null.
   * @param path path
   * @return true if the path resolves to a file
   * @throws IOException IO Failure.
   */
  protected final boolean isFile(
      final Path path)
      throws IOException {
    LOG.trace("{}: isFile('{}')", getName(), path);
    return trackDuration(getIOStatistics(), OP_IS_FILE, () -> {
      return operations.isFile(path);
    });
  }

  /**
   * Delete a path.
   * @param path path
   * @param recursive recursive delete.
   * @return status or null
   * @throws IOException IO Failure.
   */
  protected final boolean delete(
      final Path path,
      final boolean recursive)
      throws IOException {
    LOG.trace("{}: delete('{}, {}')", getName(), path, recursive);
    return delete(path, recursive, OP_DELETE);
  }

  /**
   * Delete a path.
   * @param path path
   * @param recursive recursive delete.
   * @param statistic statistic to update
   * @return status or null
   * @throws IOException IO Failure.
   */
  protected Boolean delete(
      final Path path,
      final boolean recursive,
      final String statistic)
      throws IOException {
    return trackDuration(getIOStatistics(), statistic, () -> {
      return operations.delete(path, recursive);
    });
  }

  /**
   * Create a directory.
   * @param path path
   * @param escalateFailure escalate "false" to PathIOE
   * @return true if the directory was created/exists.
   * @throws IOException IO Failure.
   */
  protected final boolean mkdirs(
      final Path path,
      final boolean escalateFailure)
      throws IOException {
    LOG.trace("{}: mkdirs('{}')", getName(), path);
    return trackDuration(getIOStatistics(), OP_MKDIRS, () -> {
      boolean success = operations.mkdirs(path);
      if (!success && escalateFailure) {
        throw new PathIOException(path.toUri().toString(),
            stageStatisticName + ": mkdirs() returned false");
      }
      return success;
    });

  }

  /**
   * List all directly files under a path.
   * Async implementations may under-report their durations.
   * @param path path
   * @return iterator over the results.
   * @throws IOException IO Failure.
   */
  protected final RemoteIterator<FileStatus> listStatusIterator(
      final Path path)
      throws IOException {
    LOG.trace("{}: listStatusIterator('{}')", getName(), path);
    return trackDuration(getIOStatistics(), OP_LIST_STATUS, () ->
        operations.listStatusIterator(path));
  }

  /**
   * Load a manifest file.
   * @param status source.
   * @return the manifest.
   * @throws IOException IO Failure.
   */
  protected final TaskManifest loadManifest(
      final FileStatus status)
      throws IOException {
    LOG.trace("{}: loadManifest('{}')", getName(), status);
    return trackDuration(getIOStatistics(), OP_LOAD_MANIFEST, () ->
        operations.loadTaskManifest(
            stageConfig.currentManifestSerializer(),
            status));
  }

  /**
   * List all the manifests in the task manifest dir.
   * @return a iterator of manifests.
   * @throws IOException IO Failure.
   */
  protected final RemoteIterator<FileStatus> listManifests()
      throws IOException {
    return RemoteIterators.filteringRemoteIterator(
        listStatusIterator(getTaskManifestDir()),
        st -> st.getPath().toUri().toString().endsWith(MANIFEST_SUFFIX));
  }

  /**
   * Make an msync() call; swallow when unsupported.
   * @param path path
   * @throws IOException IO failure
   */
  protected final void msync(Path path) throws IOException {
    LOG.trace("{}: msync('{}')", getName(), path);
    trackDurationOfInvocation(getIOStatistics(), OP_MSYNC, () ->
        operations.msync(path));
  }

  /**
   * Create a directory -failing if it exists or if
   * mkdirs() failed.
   * @param operation operation for error reporting.
   * @param path path path to create.
   * @return the path.
   * @throws IOException failure
   * @throws PathIOException mkdirs failed.
   * @throws FileAlreadyExistsException destination exists.
   */
  protected final Path createNewDirectory(
      final String operation,
      final Path path) throws IOException {
    LOG.trace("{}: {} createNewDirectory('{}')", getName(), operation, path);
    requireNonNull(path,
        () -> String.format("%s: Null path for operation %s", getName(), operation));
    // check for dir existence before trying to create.
    try {
      final FileStatus status = getFileStatus(path);
      // no exception, so the path exists.
      throw new FileAlreadyExistsException(operation
          + ": path " + path
          + " already exists and has status " + status);
    } catch (FileNotFoundException e) {
      // the path does not exist, so create it.
      mkdirs(path, true);
      return path;
    }
  }

  /**
   * Assert that a path is a directory which must exist.
   * @param operation operation for error reporting.
   * @param path path path to create.
   * @return the path
   * @throws IOException failure
   * @throws PathIOException mkdirs failed.
   * @throws FileAlreadyExistsException destination exists.
   */
  protected final Path directoryMustExist(
      final String operation,
      final Path path) throws IOException {
    final FileStatus status = getFileStatus(path);
    if (!status.isDirectory()) {
      throw new PathIOException(path.toString(),
          operation
              + ": Path is not a directory; its status is :" + status);
    }
    return path;
  }

  /**
   * Save a task manifest or summary. This will be done by
   * writing to a temp path and then renaming.
   * If the destination path exists: Delete it.
   * @param manifestData the manifest/success file
   * @param tempPath temp path for the initial save
   * @param finalPath final path for rename.
   * @throws IOException failure to load/parse
   */
  @SuppressWarnings("unchecked")
  protected final <T extends AbstractManifestData> void save(T manifestData,
      final Path tempPath,
      final Path finalPath) throws IOException {
    LOG.trace("{}: save('{}, {}, {}')", getName(), manifestData, tempPath, finalPath);
    trackDurationOfInvocation(getIOStatistics(), OP_SAVE_TASK_MANIFEST, () ->
        operations.save(manifestData, tempPath, true));
    renameFile(tempPath, finalPath);
  }

  /**
   * Get an etag from a FileStatus which MUST BE
   * an implementation of EtagSource and
   * whose etag MUST NOT BE null/empty.
   * @param status the status; may be null.
   * @return the etag or null if not provided
   */
  public String getEtag(FileStatus status) {
    return operations.getEtag(status);
  }

  /**
   * Rename a file from source to dest; if the underlying FS API call
   * returned false that's escalated to an IOE.
   * @param source source file.
   * @param dest dest file
   * @throws IOException failure
   * @throws PathIOException if the rename() call returned false.
   */
  protected final void renameFile(final Path source, final Path dest)
      throws IOException {
    maybeDeleteDest(true, dest);
    executeRenamingOperation("renameFile", source, dest,
        OP_RENAME_FILE, () ->
            operations.renameFile(source, dest));
  }

  /**
   * Rename a file from source to dest; if the underlying FS API call
   * returned false that's escalated to an IOE.
   * @param source source file.
   * @param dest dest file
   * @throws IOException failure
   * @throws PathIOException if the rename() call returned false.
   */
  protected final void renameDir(final Path source, final Path dest)
      throws IOException {

    maybeDeleteDest(true, dest);
    executeRenamingOperation("renameDir", source, dest,
        OP_RENAME_FILE, () ->
        operations.renameDir(source, dest)
    );
  }

  /**
   * Commit a file from the manifest using rename or, if available, resilient renaming.
   * @param entry entry from manifest
   * @throws PathIOException if the rename() call returned false and was uprated.
   * @throws IOException failure
   */
  protected final CommitOutcome commitFile(FileEntry entry,
      boolean deleteDest)
      throws IOException {

    final Path source = entry.getSourcePath();
    final Path dest = entry.getDestPath();

    maybeDeleteDest(deleteDest, dest);
    if (storeSupportsResilientCommit()) {
      // get the commit permits
      final ManifestStoreOperations.CommitFileResult result = trackDuration(getIOStatistics(),
          OP_COMMIT_FILE_RENAME, () ->
              operations.commitFile(entry));
      if (result.recovered()) {
        // recovery took place.
        getIOStatistics().incrementCounter(OP_COMMIT_FILE_RENAME_RECOVERED);
      }
      if (result.getWaitTime() != null) {
        // note any delay which took place
        noteAnyRateLimiting(STORE_IO_RATE_LIMITED, result.getWaitTime());
      }
    } else {
      // commit with a simple rename; failures will be escalated.
      executeRenamingOperation("renameFile", source, dest,
          OP_COMMIT_FILE_RENAME, () ->
              operations.renameFile(source, dest));
    }
    return new CommitOutcome();
  }

  /**
   * Does this store support resilient commit.
   * @return true if resilient commit operations are available.
   */
  protected boolean storeSupportsResilientCommit() {
    return operations.storeSupportsResilientCommit();
  }

  /**
   * Maybe delete the destination.
   * This routine is optimized for the data not existing, as HEAD seems to cost less
   * than a DELETE; assuming most calls don't have data, this is faster.
   * @param deleteDest should an attempt to delete the dest be made?
   * @param dest destination path
   * @throws IOException IO failure, including permissions.
   */
  private void maybeDeleteDest(final boolean deleteDest, final Path dest) throws IOException {

    if (deleteDest && getFileStatusOrNull(dest) != null) {

      boolean deleted = delete(dest, true);
      // log the outcome in case of emergency diagnostics traces
      // being needed.
      LOG.debug("{}: delete('{}') returned {}'", getName(), dest, deleted);
    }
  }

  /**
   * Execute an operation to rename a file/dir, commit a manifest entry.
   * The statistic is tracked; returning false from the operation is considered
   * a failure from the statistics perspective.
   * @param operation operation name
   * @param source source path
   * @param dest dest path
   * @param statistic statistic to track
   * @param action callable of the operation
   * @throws IOException on any failure
   */
  private void executeRenamingOperation(String operation,
      Path source,
      Path dest,
      String statistic,
      CallableRaisingIOE<Boolean> action) throws IOException {

    LOG.debug("{}: {} '{}' to '{}')", getName(), operation, source, dest);
    requireNonNull(source, "Null source");
    requireNonNull(dest, "Null dest");

    // duration tracking is a bit convoluted as it
    // ensures that rename failures as well as IOEs are
    // treated as failures from a statistics perspective.

    DurationTracker tracker = createTracker(getIOStatistics(), statistic);
    boolean success;
    try {
      success = action.apply();
      if (!success) {
        // record failure in the tracker before closing it
        tracker.failed();
      }
    } catch (IOException | RuntimeException e) {
      LOG.info("{}: {} raised an exception: {}", getName(), operation, e.toString());
      LOG.debug("{}: {} stack trace", getName(), operation, e);
      tracker.failed();
      throw e;
    } finally {
      // success
      // update the tracker.
      tracker.close();
    }
    // escalate the failure; this is done out of the duration tracker
    // so its file status probes aren't included.
    if (!success) {
      throw escalateRenameFailure(operation, source, dest);
    }
  }

  /**
   * Escalate a rename failure to an exception.
   * Returns an error exception to throw if one was not
   * triggered when probing for the source.
   * @param operation operation name
   * @param source source path
   * @param dest dest path
   * @return an exception to throw
   * @throws IOException raised probing for source or dest
   */
  private PathIOException escalateRenameFailure(String operation,
      Path source, Path dest) throws IOException {
    // rename just returned false.
    // collect information for a meaningful error message
    // and include in an exception raised.

    // get the source status; this will implicitly raise a FNFE.
    final FileStatus sourceStatus = getFileStatus(source);

    // and look to see if there is anything at the destination
    final FileStatus destStatus = getFileStatusOrNull(dest);

    LOG.error("{}: failure to {} {} to {} with" +
            " source status {} " +
            " and destination status {}",
        getName(), operation, source, dest,
        sourceStatus, destStatus);

    return new PathIOException(source.toString(),
        FAILED_TO_RENAME_PREFIX + operation + " to " + dest);
  }

  /**
   * Outcome from the commit.
   */
  public static final class CommitOutcome {

  }

  /**
   * Job ID: never null.
   */
  protected final String getJobId() {
    return stageConfig.getJobId();
  }

  /**
   * Job attempt number.
   */
  protected final int getJobAttemptNumber() {
    return stageConfig.getJobAttemptNumber();
  }

  /**
   * ID of the task.
   */
  protected final String getTaskId() {
    return stageConfig.getTaskId();
  }

  /**
   * Get the task ID; raise an NPE
   * if it is null.
   * @return a non-null task ID.
   */
  protected final String getRequiredTaskId() {
    return requireNonNull(getTaskId(),
        "No Task ID in stage config");
  }

  /**
   * ID of this specific attempt at a task.
   */
  protected final String getTaskAttemptId() {
    return stageConfig.getTaskAttemptId();
  }

  /**
   * Get the task attempt ID; raise an NPE
   * if it is null.
   * @return a non-null task attempt ID.
   */
  protected final String getRequiredTaskAttemptId() {
    return requireNonNull(getTaskAttemptId(),
        "No Task Attempt ID in stage config");
  }

  /**
   * Job attempt dir.
   */
  protected final Path getJobAttemptDir() {
    return stageConfig.getJobAttemptDir();
  }

  /**
   * Directory to put task manifests into.
   * @return a path under the job attempt dir.
   */
  protected final Path getTaskManifestDir() {
    return stageConfig.getTaskManifestDir();
  }


  /**
   * Task attempt dir.
   */
  protected final Path getTaskAttemptDir() {
    return stageConfig.getTaskAttemptDir();
  }

  /**
   * Get the task attemptDir; raise an NPE
   * if it is null.
   * @return a non-null task attempt dir.
   */
  protected final Path getRequiredTaskAttemptDir() {
    return requireNonNull(getTaskAttemptDir(),
        "No Task Attempt Dir");
  }

  /**
   * Destination of job.
   */
  protected final Path getDestinationDir() {
    return stageConfig.getDestinationDir();
  }

  /**
   * Stage confog name, for logging.
   * @return name.
   */
  public final String getName() {
    return name;
  }

  /**
   * Submitter for doing IO against the store other than
   * manifest processing.
   */
  protected final TaskPool.Submitter getIOProcessors() {
    return ioProcessors;
  }

  /**
   * Submitter for doing IO against the store other than
   * manifest processing.
   * The size parameter is used to select between sequential
   * and parallel runners.
   * no data, or one entry: serial.
   * everything else, parallel.
   * @param size number of items.
   * @return a submitter or null
   */
  protected final TaskPool.Submitter getIOProcessors(int size) {
    return size > 1
        ? getIOProcessors()
        : null;
  }

  /**
   * Delete a directory, possibly suppressing exceptions.
   * @param dir directory.
   * @param suppressExceptions should exceptions be suppressed?
   * @throws IOException exceptions raised in delete if not suppressed.
   * @return any exception caught and suppressed
   */
  protected IOException deleteDir(
      final Path dir,
      final Boolean suppressExceptions)
      throws IOException {
    try {
      delete(dir, true);
      return null;
    } catch (IOException ex) {
      LOG.info("Error deleting {}: {}", dir, ex.toString());
      if (!suppressExceptions) {
        throw ex;
      } else {
        return ex;
      }
    }
  }

  /**
   * Create an entry for a file to rename under the destination.
   * If the store operations supports extracting etags from file status
   * entries, that is included in the entry
   * @param status source file
   * @param destDir destination directory
   * @return an entry which includes the rename path
   */
  protected FileEntry fileEntry(FileStatus status, Path destDir) {
    // generate a new path under the dest dir
    Path dest = new Path(destDir, status.getPath().getName());
    return new FileEntry(status.getPath(),
        dest,
        status.getLen(),
        getEtag(status));
  }

}
