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

package org.apache.hadoop.fs.s3a.commit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.services.s3.model.MultipartUpload;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.files.PendingSet;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.fs.s3a.commit.files.SuccessData;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.net.NetUtils;

import static org.apache.hadoop.fs.s3a.Invoker.ignoreIOExceptions;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.fs.s3a.commit.CommitUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitUtilsWithMR.*;

/**
 * Abstract base class for S3A committers; allows for any commonality
 * between different architectures.
 *
 * Although the committer APIs allow for a committer to be created without
 * an output path, this is not supported in this class or its subclasses:
 * a destination must be supplied. It is left to the committer factory
 * to handle the creation of a committer when the destination is unknown.
 *
 * Requiring an output directory simplifies coding and testing.
 */
public abstract class AbstractS3ACommitter extends PathOutputCommitter {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractS3ACommitter.class);

  /**
   * Thread pool for task execution.
   */
  private ExecutorService threadPool;

  /** Underlying commit operations. */
  private final CommitOperations commitOperations;

  /**
   * Final destination of work.
   */
  private Path outputPath;

  /**
   * Role: used in log/text messages.
   */
  private final String role;

  /**
   * This is the directory for all intermediate work: where the output format
   * will write data.
   * <i>This may not be on the final file system</i>
   */
  private Path workPath;

  /** Configuration of the job. */
  private Configuration conf;

  /** Filesystem of {@link #outputPath}. */
  private FileSystem destFS;

  /** The job context. For a task, this can be cast to a TaskContext. */
  private final JobContext jobContext;

  /** Should a job marker be created? */
  private final boolean createJobMarker;

  /**
   * Create a committer.
   * This constructor binds the destination directory and configuration, but
   * does not update the work path: That must be calculated by the
   * implementation;
   * It is omitted here to avoid subclass methods being called too early.
   * @param outputPath the job's output path: MUST NOT be null.
   * @param context the task's context
   * @throws IOException on a failure
   */
  protected AbstractS3ACommitter(
      Path outputPath,
      TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    Preconditions.checkArgument(outputPath != null, "null output path");
    Preconditions.checkArgument(context != null, "null job context");
    this.jobContext = context;
    this.role = "Task committer " + context.getTaskAttemptID();
    setConf(context.getConfiguration());
    initOutput(outputPath);
    LOG.debug("{} instantiated for job \"{}\" ID {} with destination {}",
        role, jobName(context), jobIdString(context), outputPath);
    S3AFileSystem fs = getDestS3AFS();
    createJobMarker = context.getConfiguration().getBoolean(
        CREATE_SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
        DEFAULT_CREATE_SUCCESSFUL_JOB_DIR_MARKER);
    commitOperations = new CommitOperations(fs);
  }

  /**
   * Init the output filesystem and path.
   * TESTING ONLY; allows mock FS to cheat.
   * @param out output path
   * @throws IOException failure to create the FS.
   */
  @VisibleForTesting
  protected void initOutput(Path out) throws IOException {
    FileSystem fs = getDestinationFS(out, getConf());
    setDestFS(fs);
    setOutputPath(fs.makeQualified(out));
  }

  /**
   * Get the job/task context this committer was instantiated with.
   * @return the context.
   */
  public final JobContext getJobContext() {
    return jobContext;
  }

  /**
   * Final path of output, in the destination FS.
   * @return the path
   */
  @Override
  public final Path getOutputPath() {
    return outputPath;
  }

  /**
   * Set the output path.
   * @param outputPath new value
   */
  protected final void setOutputPath(Path outputPath) {
    Preconditions.checkNotNull(outputPath, "Null output path");
    this.outputPath = outputPath;
  }

  /**
   * This is the critical method for {@code FileOutputFormat}; it declares
   * the path for work.
   * @return the working path.
   */
  @Override
  public Path getWorkPath() {
    return workPath;
  }

  /**
   * Set the work path for this committer.
   * @param workPath the work path to use.
   */
  protected void setWorkPath(Path workPath) {
    LOG.debug("Setting work path to {}", workPath);
    this.workPath = workPath;
  }

  public Configuration getConf() {
    return conf;
  }

  protected void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Get the destination FS, creating it on demand if needed.
   * @return the filesystem; requires the output path to be set up
   * @throws IOException if the FS cannot be instantiated.
   */
  public FileSystem getDestFS() throws IOException {
    if (destFS == null) {
      FileSystem fs = getDestinationFS(outputPath, getConf());
      setDestFS(fs);
    }
    return destFS;
  }

  /**
   * Get the destination as an S3A Filesystem; casting it.
   * @return the dest S3A FS.
   * @throws IOException if the FS cannot be instantiated.
   */
  public S3AFileSystem getDestS3AFS() throws IOException {
    return (S3AFileSystem) getDestFS();
  }

  /**
   * Set the destination FS: the FS of the final output.
   * @param destFS destination FS.
   */
  protected void setDestFS(FileSystem destFS) {
    this.destFS = destFS;
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param context the context of the job.  This is used to get the
   * application attempt ID.
   * @return the path to store job attempt data.
   */
  public Path getJobAttemptPath(JobContext context) {
    return getJobAttemptPath(getAppAttemptId(context));
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path to store job attempt data.
   */
  protected abstract Path getJobAttemptPath(int appAttemptId);

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed. This may be the normal Task attempt path
   * or it may be a subdirectory.
   * The default implementation returns the value of
   * {@link #getBaseTaskAttemptPath(TaskAttemptContext)};
   * subclasses may return different values.
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   */
  public Path getTaskAttemptPath(TaskAttemptContext context) {
    return getBaseTaskAttemptPath(context);
  }

  /**
   * Compute the base path where the output of a task attempt is written.
   * This is the path which will be deleted when a task is cleaned up and
   * aborted.
   *
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   */
  protected abstract Path getBaseTaskAttemptPath(TaskAttemptContext context);

  /**
   * Get a temporary directory for data. When a task is aborted/cleaned
   * up, the contents of this directory are all deleted.
   * @param context task context
   * @return a path for temporary data.
   */
  public abstract Path getTempTaskAttemptPath(TaskAttemptContext context);

  /**
   * Get the name of this committer.
   * @return the committer name.
   */
  public abstract String getName();

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "AbstractS3ACommitter{");
    sb.append("role=").append(role);
    sb.append(", name").append(getName());
    sb.append(", outputPath=").append(getOutputPath());
    sb.append(", workPath=").append(workPath);
    sb.append('}');
    return sb.toString();
  }

  /**
   * Get the destination filesystem from the output path and the configuration.
   * @param out output path
   * @param config job/task config
   * @return the associated FS
   * @throws PathCommitException output path isn't to an S3A FS instance.
   * @throws IOException failure to instantiate the FS.
   */
  protected FileSystem getDestinationFS(Path out, Configuration config)
      throws IOException {
    return getS3AFileSystem(out, config,
        requiresDelayedCommitOutputInFileSystem());
  }

  /**
   * Flag to indicate whether or not the destination filesystem needs
   * to be configured to support magic paths where the output isn't immediately
   * visible. If the committer returns true, then committer setup will
   * fail if the FS doesn't have the capability.
   * Base implementation returns false.
   * @return what the requirements of the committer are of the filesystem.
   */
  protected boolean requiresDelayedCommitOutputInFileSystem() {
    return false;
  }
  /**
   * Task recovery considered unsupported: Warn and fail.
   * @param taskContext Context of the task whose output is being recovered
   * @throws IOException always.
   */
  @Override
  public void recoverTask(TaskAttemptContext taskContext) throws IOException {
    LOG.warn("Cannot recover task {}", taskContext.getTaskAttemptID());
    throw new PathCommitException(outputPath,
        String.format("Unable to recover task %s",
        taskContext.getTaskAttemptID()));
  }

  /**
   * if the job requires a success marker on a successful job,
   * create the file {@link CommitConstants#_SUCCESS}.
   *
   * While the classic committers create a 0-byte file, the S3Guard committers
   * PUT up a the contents of a {@link SuccessData} file.
   * @param context job context
   * @param pending the pending commits
   * @throws IOException IO failure
   */
  protected void maybeCreateSuccessMarkerFromCommits(JobContext context,
      List<SinglePendingCommit> pending) throws IOException {
    List<String> filenames = new ArrayList<>(pending.size());
    for (SinglePendingCommit commit : pending) {
      String key = commit.getDestinationKey();
      if (!key.startsWith("/")) {
        // fix up so that FS.makeQualified() sets up the path OK
        key = "/" + key;
      }
      filenames.add(key);
    }
    maybeCreateSuccessMarker(context, filenames);
  }

  /**
   * if the job requires a success marker on a successful job,
   * create the file {@link CommitConstants#_SUCCESS}.
   *
   * While the classic committers create a 0-byte file, the S3Guard committers
   * PUT up a the contents of a {@link SuccessData} file.
   * @param context job context
   * @param filenames list of filenames.
   * @throws IOException IO failure
   */
  protected void maybeCreateSuccessMarker(JobContext context,
      List<String> filenames)
      throws IOException {
    if (createJobMarker) {
      // create a success data structure and then save it
      SuccessData successData = new SuccessData();
      successData.setCommitter(getName());
      successData.setDescription(getRole());
      successData.setHostname(NetUtils.getLocalHostname());
      Date now = new Date();
      successData.setTimestamp(now.getTime());
      successData.setDate(now.toString());
      successData.setFilenames(filenames);
      commitOperations.createSuccessMarker(getOutputPath(), successData, true);
    }
  }

  /**
   * Base job setup deletes the success marker.
   * TODO: Do we need this?
   * @param context context
   * @throws IOException IO failure
   */
/*

  @Override
  public void setupJob(JobContext context) throws IOException {
    if (createJobMarker) {
      try (DurationInfo d = new DurationInfo("Deleting _SUCCESS marker")) {
        commitOperations.deleteSuccessMarker(getOutputPath());
      }
    }
  }
*/

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    try (DurationInfo d = new DurationInfo(LOG, "Setup Task %s",
        context.getTaskAttemptID())) {
      Path taskAttemptPath = getTaskAttemptPath(context);
      FileSystem fs = getTaskAttemptFilesystem(context);
      fs.mkdirs(taskAttemptPath);
    }
  }

  /**
   * Get the task attempt path filesystem. This may not be the same as the
   * final destination FS, and so may not be an S3A FS.
   * @param context task attempt
   * @return the filesystem
   * @throws IOException failure to instantiate
   */
  protected FileSystem getTaskAttemptFilesystem(TaskAttemptContext context)
      throws IOException {
    return getTaskAttemptPath(context).getFileSystem(getConf());
  }

  /**
   * Commit a list of pending uploads.
   * @param context job context
   * @param pending list of pending uploads
   * @throws IOException on any failure
   */
  protected void commitPendingUploads(JobContext context,
      List<SinglePendingCommit> pending) throws IOException {
    if (pending.isEmpty()) {
      LOG.warn("{}: No pending uploads to commit", getRole());
    }
    LOG.debug("{}: committing the output of {} task(s)",
        getRole(), pending.size());
    Tasks.foreach(pending)
        .stopOnFailure()
        .executeWith(buildThreadPool(context))
        .onFailure((commit, exception) ->
                getCommitOperations().abortSingleCommit(commit))
        .abortWith(commit -> getCommitOperations().abortSingleCommit(commit))
        .revertWith(commit -> getCommitOperations().revertCommit(commit))
        .run(commit -> getCommitOperations().commitOrFail(commit));
  }

  /**
   * Try to read every pendingset file and build a list of them/
   * In the case of a failure to read the file, exceptions are held until all
   * reads have been attempted.
   * @param context job context
   * @param suppressExceptions whether to suppress exceptions.
   * @param fs job attempt fs
   * @param pendingCommitFiles list of files found in the listing scan
   * @return the list of commits
   * @throws IOException on a failure when suppressExceptions is false.
   */
  protected List<SinglePendingCommit> loadPendingsetFiles(
      JobContext context,
      boolean suppressExceptions,
      FileSystem fs,
      Iterable<? extends FileStatus> pendingCommitFiles) throws IOException {

    final List<SinglePendingCommit> pending = Collections.synchronizedList(
        Lists.newArrayList());
    Tasks.foreach(pendingCommitFiles)
        .suppressExceptions(suppressExceptions)
        .executeWith(buildThreadPool(context))
        .run(pendingCommitFile ->
          pending.addAll(
              PendingSet.load(fs, pendingCommitFile.getPath()).getCommits())
      );
    return pending;
  }

  /**
   * Internal Job commit operation: where the S3 requests are made
   * (potentially in parallel).
   * @param context job context
   * @param pending pending request
   * @throws IOException any failure
   */
  protected void commitJobInternal(JobContext context,
      List<SinglePendingCommit> pending)
      throws IOException {

    commitPendingUploads(context, pending);
  }

  @Override
  public void abortJob(JobContext context, JobStatus.State state)
      throws IOException {
    LOG.info("{}: aborting job {} in state {}",
        getRole(), jobIdString(context), state);
    // final cleanup operations
    abortJobInternal(context, false);
  }


  /**
   * The internal job abort operation; can be overridden in tests.
   * This must clean up operations; it is called when a commit fails, as
   * well as in an {@link #abortJob(JobContext, JobStatus.State)} call.
   * The base implementation calls {@link #cleanup(JobContext, boolean)}
   * @param context job context
   * @param suppressExceptions should exceptions be suppressed?
   * @throws IOException any IO problem raised when suppressExceptions is false.
   */
  protected void abortJobInternal(JobContext context,
      boolean suppressExceptions)
      throws IOException {
    cleanup(context, suppressExceptions);
  }

  /**
   * Abort all pending uploads to the destination directory during
   * job cleanup operations.
   * @param suppressExceptions should exceptions be suppressed
   * @throws IOException IO problem
   */
  protected void abortPendingUploadsInCleanup(
      boolean suppressExceptions) throws IOException {
    Path dest = getOutputPath();
    try (DurationInfo d =
             new DurationInfo(LOG, "Aborting all pending commits under %s",
                 dest)) {
      CommitOperations ops = getCommitOperations();
      List<MultipartUpload> pending = ops
          .listPendingUploadsUnderPath(dest);
      Tasks.foreach(pending)
          .executeWith(buildThreadPool(getJobContext()))
          .suppressExceptions(suppressExceptions)
          .run(u -> ops.abortMultipartCommit(u.getKey(), u.getUploadId()));
    }
  }

  /**
   * Subclass-specific pre commit actions.
   * @param context job context
   * @param pending the pending operations
   * @throws IOException any failure
   */
  protected void preCommitJob(JobContext context,
      List<SinglePendingCommit> pending) throws IOException {
  }

  /**
   * Commit work.
   * This consists of two stages: precommit and commit.
   * <p>
   * Precommit: identify pending uploads, then allow subclasses
   * to validate the state of the destination and the pending uploads.
   * Any failure here triggers an abort of all pending uploads.
   * <p>
   * Commit internal: do the final commit sequence.
   * <p>
   * The final commit action is to build the {@code __SUCCESS} file entry.
   * </p>
   * @param context job context
   * @throws IOException any failure
   */
  @Override
  public void commitJob(JobContext context) throws IOException {
    String id = jobIdString(context);
    try (DurationInfo d = new DurationInfo(LOG,
        "%s: commitJob(%s)", getRole(), id)) {
      List<SinglePendingCommit> pending
          = listPendingUploadsToCommit(context);
      preCommitJob(context, pending);
      commitJobInternal(context, pending);
      jobCompleted(true);
      maybeCreateSuccessMarkerFromCommits(context, pending);
      cleanup(context, false);
    } catch (IOException e) {
      LOG.warn("Commit failure for job {}", id, e);
      jobCompleted(false);
      abortJobInternal(context, true);
      throw e;
    }
  }

  /**
   * Job completion outcome; this may be subclassed in tests.
   * @param success did the job succeed.
   */
  protected void jobCompleted(boolean success) {
    getCommitOperations().jobCompleted(success);
  }

  /**
   * Clean up any staging directories.
   * IOEs must be caught and swallowed.
   */
  public abstract void cleanupStagingDirs();

  /**
   * Get the list of pending uploads for this job attempt.
   * @param context job context
   * @return a list of pending uploads.
   * @throws IOException Any IO failure
   */
  protected abstract List<SinglePendingCommit> listPendingUploadsToCommit(
      JobContext context)
      throws IOException;

  /**
   * Cleanup the job context, including aborting anything pending.
   * @param context job context
   * @param suppressExceptions should exceptions be suppressed?
   * @throws IOException any failure if exceptions were not suppressed.
   */
  protected void cleanup(JobContext context,
      boolean suppressExceptions) throws IOException {
    try (DurationInfo d = new DurationInfo(LOG,
        "Cleanup job %s", jobIdString(context))) {
      abortPendingUploadsInCleanup(suppressExceptions);
    } finally {
      cleanupStagingDirs();
    }
  }

  @Override
  @SuppressWarnings("deprecation")
  public void cleanupJob(JobContext context) throws IOException {
    String r = getRole();
    String id = jobIdString(context);
    LOG.warn("{}: using deprecated cleanupJob call for {}", r, id);
    try (DurationInfo d = new DurationInfo(LOG, "%s: cleanup Job %s", r, id)) {
      cleanup(context, true);
    }
  }

  /**
   * Execute an operation; maybe suppress any raised IOException.
   * @param suppress should raised IOEs be suppressed?
   * @param action action (for logging when the IOE is supressed.
   * @param operation operation
   * @throws IOException if operation raised an IOE and suppress == false
   */
  protected void maybeIgnore(
      boolean suppress,
      String action,
      Invoker.VoidOperation operation) throws IOException {
    if (suppress) {
      ignoreIOExceptions(LOG, action, "", operation);
    } else {
      operation.execute();
    }
  }

  /**
   * Execute an operation; maybe suppress any raised IOException.
   * @param suppress should raised IOEs be suppressed?
   * @param action action (for logging when the IOE is suppressed.
   * @param ex  exception
   * @throws IOException if suppress == false
   */
  protected void maybeIgnore(
      boolean suppress,
      String action,
      IOException ex) throws IOException {
    if (suppress) {
      LOG.info(action, ex);
    } else {
      throw ex;
    }
  }

  /**
   * Get the commit actions instance.
   * Subclasses may provide a mock version of this.
   * @return the commit actions instance to use for operations.
   */
  protected CommitOperations getCommitOperations() {
    return commitOperations;
  }

  /**
   * Used in logging and reporting to help disentangle messages.
   * @return the committer's role.
   */
  protected String getRole() {
    return role;
  }

  /**
   * Returns an {@link ExecutorService} for parallel tasks. The number of
   * threads in the thread-pool is set by s3.multipart.committer.num-threads.
   * If num-threads is 0, this will return null;
   *
   * @param context the JobContext for this commit
   * @return an {@link ExecutorService} or null for the number of threads
   */
  protected final synchronized ExecutorService buildThreadPool(
      JobContext context) {

    if (threadPool == null) {
      int numThreads = context.getConfiguration().getInt(
          FS_S3A_COMMITTER_THREADS,
          DEFAULT_COMMITTER_THREADS);
      LOG.debug("{}: creating thread pool of size {}", getRole(), numThreads);
      if (numThreads > 0) {
        threadPool = Executors.newFixedThreadPool(numThreads,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("s3-committer-pool-%d")
                .build());
      } else {
        return null;
      }
    }
    return threadPool;
  }

  /**
   * Delete the task attempt path without raising any errors.
   * @param context task context
   */
  protected void deleteTaskAttemptPathQuietly(TaskAttemptContext context) {
    Path attemptPath = getBaseTaskAttemptPath(context);
    ignoreIOExceptions(LOG, "Delete task attempt path", attemptPath.toString(),
        () -> deleteQuietly(
            getTaskAttemptFilesystem(context), attemptPath, true));
  }

  /**
   * Abort all pending uploads in the list.
   * @param context job context
   * @param pending pending uploads
   * @param suppressExceptions should exceptions be suppressed
   * @throws IOException any exception raised
   */
  protected void abortPendingUploads(JobContext context,
      List<SinglePendingCommit> pending,
      boolean suppressExceptions)
      throws IOException {
    if (pending == null || pending.isEmpty()) {
      LOG.info("{}: no pending commits to abort", getRole());
    } else {
      try (DurationInfo d = new DurationInfo(LOG,
          "Aborting %s uploads", pending.size())) {
        Tasks.foreach(pending)
            .executeWith(buildThreadPool(context))
            .suppressExceptions(suppressExceptions)
            .run(commit -> getCommitOperations().abortSingleCommit(commit));
      }
    }
  }

}
