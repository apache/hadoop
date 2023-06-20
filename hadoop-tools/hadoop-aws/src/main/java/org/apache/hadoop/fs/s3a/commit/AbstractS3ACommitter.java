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

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.amazonaws.services.s3.model.MultipartUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.audit.AuditConstants;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.fs.store.audit.AuditSpanSource;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.files.PendingSet;
import org.apache.hadoop.fs.s3a.commit.files.PersistentCommitData;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.fs.s3a.commit.files.SuccessData;
import org.apache.hadoop.fs.s3a.commit.impl.AuditContextUpdater;
import org.apache.hadoop.fs.s3a.commit.impl.CommitContext;
import org.apache.hadoop.fs.s3a.commit.impl.CommitOperations;
import org.apache.hadoop.fs.s3a.statistics.CommitterStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperations;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperationsThroughFileSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.functional.InvocationRaisingIOE;
import org.apache.hadoop.util.functional.TaskPool;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_MAXIMUM_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_MAX_TOTAL_TASKS;
import static org.apache.hadoop.fs.s3a.Constants.MAXIMUM_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.MAX_TOTAL_TASKS;
import static org.apache.hadoop.fs.s3a.Invoker.ignoreIOExceptions;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.Statistic.COMMITTER_COMMIT_JOB;
import static org.apache.hadoop.fs.audit.CommonAuditContext.currentAuditContext;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.fs.s3a.commit.CommitUtils.*;
import static org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants.E_NO_SPARK_UUID;
import static org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants.FS_S3A_COMMITTER_UUID;
import static org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants.FS_S3A_COMMITTER_UUID_SOURCE;
import static org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants.SPARK_WRITE_UUID;
import static org.apache.hadoop.fs.s3a.commit.impl.CommitUtilsWithMR.*;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfInvocation;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DiagnosticKeys.STAGE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.createJobSummaryFilename;
import static org.apache.hadoop.util.functional.RemoteIterators.toList;

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
 *
 * The original implementation loaded all .pendingset files
 * before attempting any commit/abort operations.
 * While straightforward and guaranteeing that no changes were made to the
 * destination until all files had successfully been loaded -it didn't scale;
 * the list grew until it exceeded heap size.
 *
 * The second iteration builds up an {@link ActiveCommit} class with the
 * list of .pendingset files to load and then commit; that can be done
 * incrementally and in parallel.
 * As a side effect of this change, unless/until changed,
 * the commit/abort/revert of all files uploaded by a single task will be
 * serialized. This may slow down these operations if there are many files
 * created by a few tasks, <i>and</i> the HTTP connection pool in the S3A
 * committer was large enough for more all the parallel POST requests.
 */
public abstract class AbstractS3ACommitter extends PathOutputCommitter
    implements IOStatisticsSource {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractS3ACommitter.class);

  public static final String THREAD_PREFIX = "s3a-committer-pool-";

  /**
   * Error string when task setup fails.
   */
  @VisibleForTesting
  public static final String E_SELF_GENERATED_JOB_UUID
      = "has a self-generated job UUID";

  /**
   * Unique ID for a Job.
   * In MapReduce Jobs the YARN JobID suffices.
   * On Spark this only be the YARN JobID
   * it is known to be creating strongly unique IDs
   * (i.e. SPARK-33402 is on the branch).
   */
  private final String uuid;

  /**
   * Source of the {@link #uuid} value.
   */
  private final JobUUIDSource uuidSource;

  /**
   * Has this instance been used for job setup?
   * If so then it is safe for a locally generated
   * UUID to be used for task setup.
   */
  private boolean jobSetup;

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

  private final CommitterStatistics committerStatistics;

  /**
   * Source of Audit spans.
   */
  private final AuditSpanSource auditSpanSource;

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
    setOutputPath(outputPath);
    this.jobContext = requireNonNull(context, "null job context");
    this.role = "Task committer " + context.getTaskAttemptID();
    setConf(context.getConfiguration());
    Pair<String, JobUUIDSource> id = buildJobUUID(
        conf, context.getJobID());
    this.uuid = id.getLeft();
    this.uuidSource = id.getRight();
    LOG.info("Job UUID {} source {}", getUUID(), getUUIDSource().getText());
    initOutput(outputPath);
    LOG.debug("{} instantiated for job \"{}\" ID {} with destination {}",
        role, jobName(context), jobIdString(context), outputPath);
    S3AFileSystem fs = getDestS3AFS();
    if (!fs.isMultipartUploadEnabled()) {
      throw new PathCommitException(outputPath, "Multipart uploads are disabled for the FileSystem,"
          + " the committer can't proceed.");
    }
    // set this thread's context with the job ID.
    // audit spans created in this thread will pick
    // up this value., including the commit operations instance
    // soon to be created.
    new AuditContextUpdater(jobContext)
        .updateCurrentAuditContext();

    // the filesystem is the span source, always.
    this.auditSpanSource = fs.getAuditSpanSource();
    this.createJobMarker = context.getConfiguration().getBoolean(
        CREATE_SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
        DEFAULT_CREATE_SUCCESSFUL_JOB_DIR_MARKER);
    // the statistics are shared between this committer and its operations.
    this.committerStatistics = fs.newCommitterStatistics();
    this.commitOperations = new CommitOperations(fs, committerStatistics,
        outputPath.toString());
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
    this.outputPath = requireNonNull(outputPath, "Null output path");
  }

  /**
   * This is the critical method for {@code FileOutputFormat}; it declares
   * the path for work.
   * @return the working path.
   */
  @Override
  public final Path getWorkPath() {
    return workPath;
  }

  /**
   * Set the work path for this committer.
   * @param workPath the work path to use.
   */
  protected final void setWorkPath(Path workPath) {
    LOG.debug("Setting work path to {}", workPath);
    this.workPath = workPath;
  }

  public final Configuration getConf() {
    return conf;
  }

  protected final void setConf(Configuration conf) {
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
   * Compute the path under which all job attempts will be placed.
   * @return the path to store job attempt data.
   */
  protected abstract Path getJobPath();

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

  /**
   * The Job UUID, as passed in or generated.
   * @return the UUID for the job.
   */
  @VisibleForTesting
  public final String getUUID() {
    return uuid;
  }

  /**
   * Source of the UUID.
   * @return how the job UUID was retrieved/generated.
   */
  @VisibleForTesting
  public final JobUUIDSource getUUIDSource() {
    return uuidSource;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "AbstractS3ACommitter{");
    sb.append("role=").append(role);
    sb.append(", name=").append(getName());
    sb.append(", outputPath=").append(getOutputPath());
    sb.append(", workPath=").append(workPath);
    sb.append(", uuid='").append(getUUID()).append('\'');
    sb.append(", uuid source=").append(getUUIDSource());
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
   * Task recovery considered Unsupported: Warn and fail.
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
   * While the classic committers create a 0-byte file, the S3A committers
   * PUT up a the contents of a {@link SuccessData} file.
   * @param commitContext commit context
   * @param pending the pending commits
   *
   * @return the success data, even if the marker wasn't created
   *
   * @throws IOException IO failure
   */
  protected SuccessData maybeCreateSuccessMarkerFromCommits(
      final CommitContext commitContext,
      ActiveCommit pending) throws IOException {
    List<String> filenames = new ArrayList<>(pending.size());
    // The list of committed objects in pending is size limited in
    // ActiveCommit.uploadCommitted.
    filenames.addAll(pending.committedObjects);
    // load in all the pending statistics
    IOStatisticsSnapshot snapshot = new IOStatisticsSnapshot(
        pending.getIOStatistics());
    // and the current statistics
    snapshot.aggregate(getIOStatistics());

    // and include the context statistics if enabled
    if (commitContext.isCollectIOStatistics()) {
      snapshot.aggregate(commitContext.getIOStatisticsContext()
              .getIOStatistics());
    }

    return maybeCreateSuccessMarker(commitContext.getJobContext(), filenames, snapshot);
  }

  /**
   * if the job requires a success marker on a successful job,
   * create the {@code _SUCCESS} file.
   *
   * While the classic committers create a 0-byte file, the S3A committers
   * PUT up a the contents of a {@link SuccessData} file.
   * The file is returned, even if no marker is created.
   * This is so it can be saved to a report directory.
   * @param context job context
   * @param filenames list of filenames.
   * @param ioStatistics any IO Statistics to include
   * @throws IOException IO failure
   * @return the success data.
   */
  protected SuccessData maybeCreateSuccessMarker(
      final JobContext context,
      final List<String> filenames,
      final IOStatisticsSnapshot ioStatistics)
      throws IOException {

    SuccessData successData =
        createSuccessData(context, filenames, ioStatistics,
            getDestFS().getConf());
    if (createJobMarker) {
      // save it to the job dest dir
      commitOperations.createSuccessMarker(getOutputPath(), successData, true);
    }
    return successData;
  }

  /**
   * Create the success data structure from a job context.
   * @param context job context.
   * @param filenames short list of filenames; nullable
   * @param ioStatistics IOStatistics snapshot
   * @param destConf config of the dest fs, can be null
   * @return the structure
   *
   */
  private SuccessData createSuccessData(final JobContext context,
      final List<String> filenames,
      final IOStatisticsSnapshot ioStatistics,
      final Configuration destConf) {
    // create a success data structure
    SuccessData successData = new SuccessData();
    successData.setCommitter(getName());
    successData.setJobId(uuid);
    successData.setJobIdSource(uuidSource.getText());
    successData.setDescription(getRole());
    successData.setHostname(NetUtils.getLocalHostname());
    Date now = new Date();
    successData.setTimestamp(now.getTime());
    successData.setDate(now.toString());
    if (filenames != null) {
      successData.setFilenames(filenames);
    }
    successData.getIOStatistics().aggregate(ioStatistics);
    // attach some config options as diagnostics to assist
    // in debugging performance issues.

    // commit thread pool size
    successData.addDiagnostic(FS_S3A_COMMITTER_THREADS,
        Integer.toString(getJobCommitThreadCount(context)));

    // and filesystem http connection and thread pool sizes
    if (destConf != null) {
      successData.addDiagnostic(MAXIMUM_CONNECTIONS,
          destConf.get(MAXIMUM_CONNECTIONS,
              Integer.toString(DEFAULT_MAXIMUM_CONNECTIONS)));
      successData.addDiagnostic(MAX_TOTAL_TASKS,
          destConf.get(MAX_TOTAL_TASKS,
              Integer.toString(DEFAULT_MAX_TOTAL_TASKS)));
    }
    return successData;
  }

  /**
   * Base job setup (optionally) deletes the success marker and
   * always creates the destination directory.
   * When objects are committed that dest dir marker will inevitably
   * be deleted; creating it now ensures there is something at the end
   * while the job is in progress -and if nothing is created, that
   * it is still there.
   * <p>
   *   The option {@link InternalCommitterConstants#FS_S3A_COMMITTER_UUID}
   *   is set to the job UUID; if generated locally
   *   {@link InternalCommitterConstants#SPARK_WRITE_UUID} is also patched.
   *   The field {@link #jobSetup} is set to true to note that
   *   this specific committer instance was used to set up a job.
   * </p>
   * @param context context
   * @throws IOException IO failure
   */

  @Override
  public void setupJob(JobContext context) throws IOException {
    try (DurationInfo d = new DurationInfo(LOG,
        "Job %s setting up", getUUID())) {
      // record that the job has been set up
      jobSetup = true;
      // patch job conf with the job UUID.
      Configuration c = context.getConfiguration();
      c.set(FS_S3A_COMMITTER_UUID, getUUID());
      c.set(FS_S3A_COMMITTER_UUID_SOURCE, getUUIDSource().getText());
      Path dest = getOutputPath();
      if (createJobMarker){
        commitOperations.deleteSuccessMarker(dest);
      }
      getDestFS().mkdirs(dest);
      // do a scan for surplus markers
      warnOnActiveUploads(dest);
    }
  }

  /**
   * Task setup. Fails if the the UUID was generated locally, and
   * the same committer wasn't used for job setup.
   * {@inheritDoc}
   * @throws PathCommitException if the task UUID options are unsatisfied.
   */
  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    TaskAttemptID attemptID = context.getTaskAttemptID();

    // update the context so that task IO in the same thread has
    // the relevant values.
    new AuditContextUpdater(context)
        .updateCurrentAuditContext();

    try (DurationInfo d = new DurationInfo(LOG, "Setup Task %s",
        attemptID)) {
      // reject attempts to set up the task where the output won't be
      // picked up
      if (!jobSetup
          && getUUIDSource() == JobUUIDSource.GeneratedLocally) {
        // on anything other than a test run, the context must not have been
        // generated locally.
        throw new PathCommitException(getOutputPath().toString(),
            "Task attempt " + attemptID
                + " " + E_SELF_GENERATED_JOB_UUID);
      }
      Path taskAttemptPath = getTaskAttemptPath(context);
      FileSystem fs = taskAttemptPath.getFileSystem(getConf());
      // delete that ta path if somehow it was there
      fs.delete(taskAttemptPath, true);
      // create an empty directory
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
   * Commit all the pending uploads.
   * Each file listed in the ActiveCommit instance is queued for processing
   * in a separate thread; its contents are loaded and then (sequentially)
   * committed.
   * On a failure or abort of a single file's commit, all its uploads are
   * aborted.
   * The revert operation lists the files already committed and deletes them.
   * @param commitContext commit context
   * @param pending  pending uploads
   * @throws IOException on any failure
   */
  protected void commitPendingUploads(
      final CommitContext commitContext,
      final ActiveCommit pending) throws IOException {
    if (pending.isEmpty()) {
      LOG.warn("{}: No pending uploads to commit", getRole());
    }
    try (DurationInfo ignored = new DurationInfo(LOG,
        "committing the output of %s task(s)", pending.size())) {

      TaskPool.foreach(pending.getSourceFiles())
          .stopOnFailure()
          .suppressExceptions(false)
          .executeWith(commitContext.getOuterSubmitter())
          .abortWith(status ->
              loadAndAbort(commitContext, pending, status, true, false))
          .revertWith(status ->
              loadAndRevert(commitContext, pending, status))
          .run(status ->
              loadAndCommit(commitContext, pending, status));
    }
  }

  /**
   * Run a precommit check that all files are loadable.
   * This check avoids the situation where the inability to read
   * a file only surfaces partway through the job commit, so
   * results in the destination being tainted.
   * @param commitContext commit context
   * @param pending the pending operations
   * @throws IOException any failure
   */
  protected void precommitCheckPendingFiles(
      final CommitContext commitContext,
      final ActiveCommit pending) throws IOException {

    FileSystem sourceFS = pending.getSourceFS();
    try (DurationInfo ignored =
             new DurationInfo(LOG, "Preflight Load of pending files")) {

      TaskPool.foreach(pending.getSourceFiles())
          .stopOnFailure()
          .suppressExceptions(false)
          .executeWith(commitContext.getOuterSubmitter())
          .run(status -> PersistentCommitData.load(sourceFS, status,
              commitContext.getPendingSetSerializer()));
    }
  }

  /**
   * Load a pendingset file and commit all of its contents.
   * Invoked within a parallel run; the commitContext thread
   * pool is already busy/possibly full, so do not
   * execute work through the same submitter.
   * @param commitContext context to commit through
   * @param activeCommit commit state
   * @param status file to load
   * @throws IOException failure
   */
  private void loadAndCommit(
      final CommitContext commitContext,
      final ActiveCommit activeCommit,
      final FileStatus status) throws IOException {

    final Path path = status.getPath();
    commitContext.switchToIOStatisticsContext();
    try (DurationInfo ignored =
             new DurationInfo(LOG,
                 "Loading and committing files in pendingset %s", path)) {
      PendingSet pendingSet = PersistentCommitData.load(
          activeCommit.getSourceFS(),
          status,
          commitContext.getPendingSetSerializer());
      String jobId = pendingSet.getJobId();
      if (!StringUtils.isEmpty(jobId) && !getUUID().equals(jobId)) {
        throw new PathCommitException(path,
            String.format("Mismatch in Job ID (%s) and commit job ID (%s)",
                getUUID(), jobId));
      }
      TaskPool.foreach(pendingSet.getCommits())
          .stopOnFailure()
          .suppressExceptions(false)
          .executeWith(commitContext.getInnerSubmitter())
          .onFailure((commit, exception) ->
              commitContext.abortSingleCommit(commit))
          .abortWith(commitContext::abortSingleCommit)
          .revertWith(commitContext::revertCommit)
          .run(commit -> {
            commitContext.commitOrFail(commit);
            activeCommit.uploadCommitted(
                commit.getDestinationKey(), commit.getLength());
          });
      activeCommit.pendingsetCommitted(pendingSet.getIOStatistics());
    }
  }

  /**
   * Load a pendingset file and revert all of its contents.
   * Invoked within a parallel run; the commitContext thread
   * pool is already busy/possibly full, so do not
   * execute work through the same submitter.
   * @param commitContext context to commit through
   * @param activeCommit commit state
   * @param status status of file to load
   * @throws IOException failure
   */
  private void loadAndRevert(
      final CommitContext commitContext,
      final ActiveCommit activeCommit,
      final FileStatus status) throws IOException {

    final Path path = status.getPath();
    commitContext.switchToIOStatisticsContext();
    try (DurationInfo ignored =
             new DurationInfo(LOG, false, "Committing %s", path)) {
      PendingSet pendingSet = PersistentCommitData.load(
          activeCommit.getSourceFS(),
          status,
          commitContext.getPendingSetSerializer());
      TaskPool.foreach(pendingSet.getCommits())
          .suppressExceptions(true)
          .run(commitContext::revertCommit);
    }
  }

  /**
   * Load a pendingset file and abort all of its contents.
   * Invoked within a parallel run; the commitContext thread
   * pool is already busy/possibly full, so do not
   * execute work through the same submitter.
   * @param commitContext context to commit through
   * @param activeCommit commit state
   * @param status status of file to load
   * @param deleteRemoteFiles should remote files be deleted?
   * @throws IOException failure
   */
  private void loadAndAbort(
      final CommitContext commitContext,
      final ActiveCommit activeCommit,
      final FileStatus status,
      final boolean suppressExceptions,
      final boolean deleteRemoteFiles) throws IOException {

    final Path path = status.getPath();
    commitContext.switchToIOStatisticsContext();
    try (DurationInfo ignored =
             new DurationInfo(LOG, false, "Aborting %s", path)) {
      PendingSet pendingSet = PersistentCommitData.load(
          activeCommit.getSourceFS(),
          status,
          commitContext.getPendingSetSerializer());
      FileSystem fs = getDestFS();
      TaskPool.foreach(pendingSet.getCommits())
          .executeWith(commitContext.getInnerSubmitter())
          .suppressExceptions(suppressExceptions)
          .run(commit -> {
            try {
              commitContext.abortSingleCommit(commit);
            } catch (FileNotFoundException e) {
              // Commit ID was not known; file may exist.
              // delete it if instructed to do so.
              if (deleteRemoteFiles) {
                fs.delete(commit.destinationPath(), false);
              }
            }
          });
    }
  }

  /**
   * Start the final job commit/abort commit operations.
   * If configured to collect statistics,
   * The IO StatisticsContext is reset.
   * @param context job context
   * @return a commit context through which the operations can be invoked.
   * @throws IOException failure.
   */
  protected CommitContext initiateJobOperation(
      final JobContext context)
      throws IOException {

    IOStatisticsContext ioStatisticsContext =
        IOStatisticsContext.getCurrentIOStatisticsContext();
    CommitContext commitContext = getCommitOperations().createCommitContext(
        context,
        getOutputPath(),
        getJobCommitThreadCount(context),
        ioStatisticsContext);
    commitContext.maybeResetIOStatisticsContext();
    return commitContext;
  }

  /**
   * Start a ask commit/abort commit operations.
   * This may have a different thread count.
   * If configured to collect statistics,
   * The IO StatisticsContext is reset.
   * @param context job or task context
   * @return a commit context through which the operations can be invoked.
   * @throws IOException failure.
   */
  protected CommitContext initiateTaskOperation(
      final JobContext context)
      throws IOException {

    CommitContext commitContext = getCommitOperations().createCommitContext(
        context,
        getOutputPath(),
        getTaskCommitThreadCount(context),
        IOStatisticsContext.getCurrentIOStatisticsContext());
    commitContext.maybeResetIOStatisticsContext();
    return commitContext;
  }

  /**
   * Internal Job commit operation: where the S3 requests are made
   * (potentially in parallel).
   * @param commitContext commit context
   * @param pending pending commits
   * @throws IOException any failure
   */
  protected void commitJobInternal(
      final CommitContext commitContext,
      final ActiveCommit pending)
      throws IOException {
    trackDurationOfInvocation(committerStatistics,
        COMMITTER_COMMIT_JOB.getSymbol(),
        () -> commitPendingUploads(commitContext, pending));
  }

  @Override
  public void abortJob(JobContext context, JobStatus.State state)
      throws IOException {
    LOG.info("{}: aborting job {} in state {}",
        getRole(), jobIdString(context), state);
    // final cleanup operations
    try (CommitContext commitContext = initiateJobOperation(context)){
      abortJobInternal(commitContext, false);
    }
  }


  /**
   * The internal job abort operation; can be overridden in tests.
   * This must clean up operations; it is called when a commit fails, as
   * well as in an {@link #abortJob(JobContext, JobStatus.State)} call.
   * The base implementation calls {@link #cleanup(CommitContext, boolean)}
   * so cleans up the filesystems and destroys the thread pool.
   * Subclasses must always invoke this superclass method after their
   * own operations.
   * Creates and closes its own commit context.
   *
   * @param commitContext commit context
   * @param suppressExceptions should exceptions be suppressed?
   * @throws IOException any IO problem raised when suppressExceptions is false.
   */
  protected void abortJobInternal(CommitContext commitContext,
      boolean suppressExceptions)
      throws IOException {
    cleanup(commitContext, suppressExceptions);
  }

  /**
   * Abort all pending uploads to the destination directory during
   * job cleanup operations.
   * Note: this instantiates the thread pool if required -so
   * @param suppressExceptions should exceptions be suppressed
   * @param commitContext commit context
   * @throws IOException IO problem
   */
  protected void abortPendingUploadsInCleanup(
      boolean suppressExceptions,
      CommitContext commitContext) throws IOException {
    // return early if aborting is disabled.
    if (!shouldAbortUploadsInCleanup()) {
      LOG.debug("Not cleanup up pending uploads to {} as {} is false ",
          getOutputPath(),
          FS_S3A_COMMITTER_ABORT_PENDING_UPLOADS);
      return;
    }
    Path dest = getOutputPath();
    try (DurationInfo ignored =
             new DurationInfo(LOG, "Aborting all pending commits under %s",
                 dest)) {
      CommitOperations ops = getCommitOperations();
      List<MultipartUpload> pending;
      try {
        pending = ops.listPendingUploadsUnderPath(dest);
      } catch (IOException e) {
        // Swallow any errors given this is best effort
        LOG.debug("Failed to list pending uploads under {}", dest, e);
        return;
      }
      if (!pending.isEmpty()) {
        LOG.warn("{} pending uploads were found -aborting", pending.size());
        LOG.warn("If other tasks/jobs are writing to {},"
            + "this action may cause them to fail", dest);
        TaskPool.foreach(pending)
            .executeWith(commitContext.getOuterSubmitter())
            .suppressExceptions(suppressExceptions)
            .run(u -> commitContext.abortMultipartCommit(
                u.getKey(), u.getUploadId()));
      } else {
        LOG.info("No pending uploads were found");
      }
    }
  }

  private boolean shouldAbortUploadsInCleanup() {
    return getConf()
        .getBoolean(FS_S3A_COMMITTER_ABORT_PENDING_UPLOADS,
            DEFAULT_FS_S3A_COMMITTER_ABORT_PENDING_UPLOADS);
  }

  /**
   * Subclass-specific pre-Job-commit actions.
   * The staging committers all load the pending files to verify that
   * they can be loaded.
   * The Magic committer does not, because of the overhead of reading files
   * from S3 makes it too expensive.
   * @param commitContext commit context
   * @param pending the pending operations
   * @throws IOException any failure
   */
  @VisibleForTesting
  public void preCommitJob(CommitContext commitContext,
      ActiveCommit pending) throws IOException {
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
   * The final commit action is to build the {@code _SUCCESS} file entry.
   * </p>
   * @param context job context
   * @throws IOException any failure
   */
  @Override
  public void commitJob(JobContext context) throws IOException {
    String id = jobIdString(context);
    // the commit context is created outside a try-with-resources block
    // so it can be used in exception handling.
    CommitContext commitContext = null;

    SuccessData successData = null;
    IOException failure = null;
    String stage = "preparing";
    try (DurationInfo d = new DurationInfo(LOG,
        "%s: commitJob(%s)", getRole(), id)) {
      commitContext = initiateJobOperation(context);
      ActiveCommit pending
          = listPendingUploadsToCommit(commitContext);
      stage = "precommit";
      preCommitJob(commitContext, pending);
      stage = "commit";
      commitJobInternal(commitContext, pending);
      stage = "completed";
      jobCompleted(true);
      stage = "marker";
      successData = maybeCreateSuccessMarkerFromCommits(commitContext, pending);
      stage = "cleanup";
      cleanup(commitContext, false);
    } catch (IOException e) {
      // failure. record it for the summary
      failure = e;
      LOG.warn("Commit failure for job {}", id, e);
      jobCompleted(false);
      abortJobInternal(commitContext, true);
      throw e;
    } finally {
      // save the report summary, even on failure
      if (commitContext != null) {
        if (successData == null) {
          // if the commit did not get as far as creating success data, create one.
          successData = createSuccessData(context, null, null,
              getDestFS().getConf());
        }
        // save quietly, so no exceptions are raised
        maybeSaveSummary(stage,
            commitContext,
            successData,
            failure,
            true,
            true);
        // and close that commit context
        commitContext.close();
      }
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
   * @param commitContext commit context
   * @return a list of pending uploads.
   * @throws IOException Any IO failure
   */
  protected abstract ActiveCommit listPendingUploadsToCommit(
      CommitContext commitContext)
      throws IOException;

  /**
   * Cleanup the job context, including aborting anything pending
   * and destroying the thread pool.
   * @param commitContext commit context
   * @param suppressExceptions should exceptions be suppressed?
   * @throws IOException any failure if exceptions were not suppressed.
   */
  protected void cleanup(CommitContext commitContext,
      boolean suppressExceptions) throws IOException {
    try (DurationInfo d = new DurationInfo(LOG,
        "Cleanup job %s", jobIdString(commitContext.getJobContext()))) {
      abortPendingUploadsInCleanup(suppressExceptions, commitContext);
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
    try (DurationInfo d = new DurationInfo(LOG, "%s: cleanup Job %s", r, id);
         CommitContext commitContext = initiateJobOperation(context)) {
      cleanup(commitContext, true);
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
      InvocationRaisingIOE operation) throws IOException {
    if (suppress) {
      ignoreIOExceptions(LOG, action, "", operation);
    } else {
      operation.apply();
    }
  }

  /**
   * Log or rethrow a caught IOException.
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
      LOG.debug(action, ex);
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
   * Get the thread count for this job's commit operations.
   * @param context the JobContext for this commit
   * @return a possibly zero thread count.
   */
  private int getJobCommitThreadCount(final JobContext context) {
    return context.getConfiguration().getInt(
        FS_S3A_COMMITTER_THREADS,
        DEFAULT_COMMITTER_THREADS);
  }

  /**
   * Get the thread count for this task's commit operations.
   * @param context the JobContext for this commit
   * @return a possibly zero thread count.
   */
  private int getTaskCommitThreadCount(final JobContext context) {
    return context.getConfiguration().getInt(
        FS_S3A_COMMITTER_THREADS,
        DEFAULT_COMMITTER_THREADS);
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
   * This operation is used by the magic committer as part of its
   * rollback after a failure during task commit.
   * @param commitContext commit context
   * @param pending pending uploads
   * @param suppressExceptions should exceptions be suppressed
   * @throws IOException any exception raised
   */
  protected void abortPendingUploads(
      final CommitContext commitContext,
      final List<SinglePendingCommit> pending,
      final boolean suppressExceptions)
      throws IOException {
    if (pending == null || pending.isEmpty()) {
      LOG.info("{}: no pending commits to abort", getRole());
    } else {
      try (DurationInfo d = new DurationInfo(LOG,
          "Aborting %s uploads", pending.size())) {
        TaskPool.foreach(pending)
            .executeWith(commitContext.getOuterSubmitter())
            .suppressExceptions(suppressExceptions)
            .run(commitContext::abortSingleCommit);
      }
    }
  }

  /**
   * Abort all pending uploads in the list.
   * @param commitContext commit context
   * @param pending pending uploads
   * @param suppressExceptions should exceptions be suppressed?
   * @param deleteRemoteFiles should remote files be deleted?
   * @throws IOException any exception raised
   */
  protected void abortPendingUploads(
      final CommitContext commitContext,
      final ActiveCommit pending,
      final boolean suppressExceptions,
      final boolean deleteRemoteFiles) throws IOException {
    if (pending.isEmpty()) {
      LOG.info("{}: no pending commits to abort", getRole());
    } else {
      try (DurationInfo d = new DurationInfo(LOG,
          "Aborting %s uploads", pending.size())) {
        TaskPool.foreach(pending.getSourceFiles())
            .executeWith(commitContext.getOuterSubmitter())
            .suppressExceptions(suppressExceptions)
            .run(path ->
                loadAndAbort(commitContext,
                    pending,
                    path,
                    suppressExceptions,
                    deleteRemoteFiles));
      }
    }
  }

  @Override
  public IOStatistics getIOStatistics() {
    return committerStatistics.getIOStatistics();
  }

  /**
   * Scan for active uploads and list them along with a warning message.
   * Errors are ignored.
   * @param path output path of job.
   */
  protected void warnOnActiveUploads(final Path path) {
    List<MultipartUpload> pending;
    try {
      pending = getCommitOperations()
          .listPendingUploadsUnderPath(path);
    } catch (IOException e) {
      LOG.debug("Failed to list uploads under {}",
          path, e);
      return;
    }
    if (!pending.isEmpty()) {
      // log a warning
      LOG.warn("{} active upload(s) in progress under {}",
          pending.size(),
          path);
      LOG.warn("Either jobs are running concurrently"
          + " or failed jobs are not being cleaned up");
      // and the paths + timestamps
      DateFormat df = DateFormat.getDateTimeInstance();
      pending.forEach(u ->
          LOG.info("[{}] {}",
              df.format(u.getInitiated()),
              u.getKey()));
      if (shouldAbortUploadsInCleanup()) {
        LOG.warn("This committer will abort these uploads in job cleanup");
      }
    }
  }

  /**
   * Build the job UUID.
   *
   * <p>
   *  In MapReduce jobs, the application ID is issued by YARN, and
   *  unique across all jobs.
   * </p>
   * <p>
   * Spark will use a fake app ID based on the current time.
   * This can lead to collisions on busy clusters unless
   * the specific spark release has SPARK-33402 applied.
   * This appends a random long value to the timestamp, so
   * is unique enough that the risk of collision is almost
   * nonexistent.
   * </p>
   * <p>
   *   The order of selection of a uuid is
   * </p>
   * <ol>
   *   <li>Value of
   *   {@link InternalCommitterConstants#FS_S3A_COMMITTER_UUID}.</li>
   *   <li>Value of
   *   {@link InternalCommitterConstants#SPARK_WRITE_UUID}.</li>
   *   <li>If enabled through
   *   {@link CommitConstants#FS_S3A_COMMITTER_GENERATE_UUID}:
   *   Self-generated uuid.</li>
   *   <li>If {@link CommitConstants#FS_S3A_COMMITTER_REQUIRE_UUID}
   *   is not set: Application ID</li>
   * </ol>
   * The UUID bonding takes place during construction;
   * the staging committers use it to set up their wrapped
   * committer to a path in the cluster FS which is unique to the
   * job.
   * <p>
   *  In MapReduce jobs, the application ID is issued by YARN, and
   *  unique across all jobs.
   * </p>
   * In {@link #setupJob(JobContext)} the job context's configuration
   * will be patched
   * be valid in all sequences where the job has been set up for the
   * configuration passed in.
   * <p>
   *   If the option {@link CommitConstants#FS_S3A_COMMITTER_REQUIRE_UUID}
   *   is set, then an external UUID MUST be passed in.
   *   This can be used to verify that the spark engine is reliably setting
   *   unique IDs for staging.
   * </p>
   * @param conf job/task configuration
   * @param jobId job ID from YARN or spark.
   * @return Job UUID and source of it.
   * @throws PathCommitException no UUID was found and it was required
   */
  public static Pair<String, JobUUIDSource>
      buildJobUUID(Configuration conf, JobID jobId)
      throws PathCommitException {

    String jobUUID = conf.getTrimmed(FS_S3A_COMMITTER_UUID, "");

    if (!jobUUID.isEmpty()) {
      return Pair.of(jobUUID, JobUUIDSource.CommitterUUIDProperty);
    }
    // there is no job UUID.
    // look for one from spark
    jobUUID = conf.getTrimmed(SPARK_WRITE_UUID, "");
    if (!jobUUID.isEmpty()) {
      return Pair.of(jobUUID, JobUUIDSource.SparkWriteUUID);
    }

    // there is no UUID configuration in the job/task config

    // Check the job hasn't declared a requirement for the UUID.
    // This allows or fail-fast validation of Spark behavior.
    if (conf.getBoolean(FS_S3A_COMMITTER_REQUIRE_UUID,
        DEFAULT_S3A_COMMITTER_REQUIRE_UUID)) {
      throw new PathCommitException("", E_NO_SPARK_UUID);
    }

    // see if the job can generate a random UUI`
    if (conf.getBoolean(FS_S3A_COMMITTER_GENERATE_UUID,
        DEFAULT_S3A_COMMITTER_GENERATE_UUID)) {
      // generate a random UUID. This is OK for a job, for a task
      // it means that the data may not get picked up.
      String newId = UUID.randomUUID().toString();
      LOG.warn("No job ID in configuration; generating a random ID: {}",
          newId);
      return Pair.of(newId, JobUUIDSource.GeneratedLocally);
    }
    // if no other option was supplied, return the job ID.
    // This is exactly what MR jobs expect, but is not what
    // Spark jobs can do as there is a risk of jobID collision.
    return Pair.of(jobId.toString(), JobUUIDSource.JobID);
  }

  /**
   * Enumeration of Job UUID source.
   */
  public enum JobUUIDSource {
    SparkWriteUUID(SPARK_WRITE_UUID),
    CommitterUUIDProperty(FS_S3A_COMMITTER_UUID),
    JobID("JobID"),
    GeneratedLocally("Generated Locally");

    private final String text;

    JobUUIDSource(final String text) {
      this.text = text;
    }

    /**
     * Source for messages.
     * @return text
     */
    public String getText() {
      return text;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "JobUUIDSource{");
      sb.append("text='").append(text).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Add jobID to current context.
   */
  protected final void updateCommonContext() {
    currentAuditContext().put(AuditConstants.PARAM_JOB_ID, uuid);

  }

  protected AuditSpanSource getAuditSpanSource() {
    return auditSpanSource;
  }

  /**
   * Start an operation; retrieve an audit span.
   *
   * All operation names <i>SHOULD</i> come from
   * {@code StoreStatisticNames} or
   * {@code StreamStatisticNames}.
   * @param name operation name.
   * @param path1 first path of operation
   * @param path2 second path of operation
   * @return a span for the audit
   * @throws IOException failure
   */
  protected AuditSpan startOperation(String name,
      @Nullable String path1,
      @Nullable String path2)
      throws IOException {
    return getAuditSpanSource().createSpan(name, path1, path2);
  }


  /**
   * Save a summary to the report dir if the config option
   * is set.
   * The report will be updated with the current active stage,
   * and if {@code thrown} is non-null, it will be added to the
   * diagnostics (and the job tagged as a failure).
   * Static for testability.
   * @param activeStage active stage
   * @param context commit context.
   * @param report summary file.
   * @param thrown any exception indicting failure.
   * @param quiet should exceptions be swallowed.
   * @param overwrite should the existing file be overwritten
   * @return the path of a file, if successfully saved
   * @throws IOException if a failure occured and quiet==false
   */
  private static Path maybeSaveSummary(
      String activeStage,
      CommitContext context,
      SuccessData report,
      Throwable thrown,
      boolean quiet,
      boolean overwrite) throws IOException {
    Configuration conf = context.getConf();
    String reportDir = conf.getTrimmed(OPT_SUMMARY_REPORT_DIR, "");
    if (reportDir.isEmpty()) {
      LOG.debug("No summary directory set in " + OPT_SUMMARY_REPORT_DIR);
      return null;
    }
    LOG.debug("Summary directory set to {}", reportDir);

    Path reportDirPath = new Path(reportDir);
    Path path = new Path(reportDirPath,
        createJobSummaryFilename(context.getJobId()));

    if (thrown != null) {
      report.recordJobFailure(thrown);
    }
    report.putDiagnostic(STAGE, activeStage);
    // the store operations here is explicitly created for the FS where
    // the reports go, which may not be the target FS of the job.

    final FileSystem fs = path.getFileSystem(conf);
    try (ManifestStoreOperations operations = new ManifestStoreOperationsThroughFileSystem(
        fs)) {
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
      report.save(fs, path, SuccessData.serializer());
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

  /**
   * State of the active commit operation.
   *
   * It contains a list of all pendingset files to load as the source
   * of outstanding commits to complete/abort,
   * and tracks the files uploaded.
   *
   * To avoid running out of heap by loading all the source files
   * simultaneously:
   * <ol>
   *   <li>
   *     The list of files to load is passed round but
   *     the contents are only loaded on demand.
   *   </li>
   *   <li>
   *     The number of written files tracked for logging in
   *     the _SUCCESS file are limited to a small amount -enough
   *     for testing only.
   *   </li>
   * </ol>
   */
  public static final class ActiveCommit {

    private static final AbstractS3ACommitter.ActiveCommit EMPTY
        = new ActiveCommit(null, new ArrayList<>());

    /** All pendingset files to iterate through. */
    private final List<FileStatus> sourceFiles;

    /**
     * Filesystem for the source files.
     */
    private final FileSystem sourceFS;

    /**
     * List of committed objects; only built up until the commit limit is
     * reached.
     */
    private final List<String> committedObjects = new ArrayList<>();

    /**
     * The total number of committed objects.
     */
    private int committedObjectCount;

    /**
     * Total number of bytes committed.
     */
    private long committedBytes;

    /**
     * Aggregate statistics of all supplied by
     * committed uploads.
     */
    private final IOStatisticsSnapshot ioStatistics =
        new IOStatisticsSnapshot();

    /**
     * Construct from a source FS and list of files.
     * @param sourceFS filesystem containing the list of pending files
     * @param sourceFiles .pendingset files to load and commit.
     */
    @SuppressWarnings("unchecked")
    public ActiveCommit(
        final FileSystem sourceFS,
        final List<? extends FileStatus> sourceFiles) {
      this.sourceFiles = (List<FileStatus>) sourceFiles;
      this.sourceFS = sourceFS;
    }

    /**
     * Create an active commit of the given pending files.
     * @param pendingFS source filesystem.
     * @param statuses iterator of file status or subclass to use.
     * @return the commit
     * @throws IOException if the iterator raises one.
     */
    public static ActiveCommit fromStatusIterator(
        final FileSystem pendingFS,
        final RemoteIterator<? extends FileStatus> statuses) throws IOException {
      return new ActiveCommit(pendingFS, toList(statuses));
    }

    /**
     * Get the empty entry.
     * @return an active commit with no pending files.
     */
    public static ActiveCommit empty() {
      return EMPTY;
    }

    public List<FileStatus> getSourceFiles() {
      return sourceFiles;
    }

    public FileSystem getSourceFS() {
      return sourceFS;
    }

    /**
     * Note that a file was committed.
     * Increase the counter of files and total size.
     * If there is room in the committedFiles list, the file
     * will be added to the list and so end up in the _SUCCESS file.
     * @param key key of the committed object.
     * @param size size in bytes.
     */
    public synchronized void uploadCommitted(String key,
        long size) {
      if (committedObjects.size() < SUCCESS_MARKER_FILE_LIMIT) {
        committedObjects.add(
            key.startsWith("/") ? key : ("/" + key));
      }
      committedObjectCount++;
      committedBytes += size;
    }

    /**
     * Callback when a pendingset has been committed,
     * including any source statistics.
     * @param sourceStatistics any source statistics
     */
    public void pendingsetCommitted(final IOStatistics sourceStatistics) {
      ioStatistics.aggregate(sourceStatistics);
    }

    public IOStatisticsSnapshot getIOStatistics() {
      return ioStatistics;
    }

    public synchronized List<String> getCommittedObjects() {
      return committedObjects;
    }

    public synchronized int getCommittedFileCount() {
      return committedObjectCount;
    }

    public synchronized long getCommittedBytes() {
      return committedBytes;
    }

    public int size() {
      return sourceFiles.size();
    }

    public boolean isEmpty() {
      return sourceFiles.isEmpty();
    }

    public void add(FileStatus status) {
      sourceFiles.add(status);
    }
  }
}
