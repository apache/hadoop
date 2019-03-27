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

package org.apache.hadoop.fs.s3a.commit.staging;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.AbstractS3ACommitter;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants;
import org.apache.hadoop.fs.s3a.commit.Tasks;
import org.apache.hadoop.fs.s3a.commit.files.PendingSet;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.util.DurationInfo;

import static com.google.common.base.Preconditions.*;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.Invoker.*;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingCommitterConstants.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.fs.s3a.commit.CommitUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitUtilsWithMR.*;

/**
 * Committer based on the contributed work of the
 * <a href="https://github.com/rdblue/s3committer">Netflix multipart committers.</a>
 * <ol>
 *   <li>
 *   The working directory of each task is actually under a temporary
 *   path in the local filesystem; jobs write directly into it.
 *   </li>
 *   <li>
 *     Task Commit: list all files under the task working dir, upload
 *     each of them but do not commit the final operation.
 *     Persist the information for each pending commit into the cluster
 *     for enumeration and commit by the job committer.
 *   </li>
 *   <li>Task Abort: recursive delete of task working dir.</li>
 *   <li>Job Commit: list all pending PUTs to commit; commit them.</li>
 *   <li>
 *     Job Abort: list all pending PUTs to commit; abort them.
 *     Delete all task attempt directories.
 *   </li>
 * </ol>
 *
 * This is the base class of the Partitioned and Directory committers.
 * It does not do any conflict resolution, and is made non-abstract
 * primarily for test purposes. It is not expected to be used in production.
 */
public class StagingCommitter extends AbstractS3ACommitter {

  private static final Logger LOG = LoggerFactory.getLogger(
      StagingCommitter.class);

  /** Name: {@value}. */
  public static final String NAME = "staging";
  private final Path constructorOutputPath;
  private final long uploadPartSize;
  private final String uuid;
  private final boolean uniqueFilenames;
  private final FileOutputCommitter wrappedCommitter;

  private ConflictResolution conflictResolution;
  private String s3KeyPrefix;

  /** The directory in the cluster FS for commits to go to. */
  private Path commitsDirectory;

  /**
   * Committer for a single task attempt.
   * @param outputPath final output path
   * @param context task context
   * @throws IOException on a failure
   */
  public StagingCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    this.constructorOutputPath = checkNotNull(getOutputPath(), "output path");
    Configuration conf = getConf();
    this.uploadPartSize = conf.getLongBytes(
        MULTIPART_SIZE, DEFAULT_MULTIPART_SIZE);
    this.uuid = getUploadUUID(conf, context.getJobID());
    this.uniqueFilenames = conf.getBoolean(
        FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES,
        DEFAULT_STAGING_COMMITTER_UNIQUE_FILENAMES);
    setWorkPath(buildWorkPath(context, uuid));
    this.wrappedCommitter = createWrappedCommitter(context, conf);
    setOutputPath(constructorOutputPath);
    Path finalOutputPath = getOutputPath();
    Preconditions.checkNotNull(finalOutputPath, "Output path cannot be null");
    S3AFileSystem fs = getS3AFileSystem(finalOutputPath,
        context.getConfiguration(), false);
    s3KeyPrefix = fs.pathToKey(finalOutputPath);
    LOG.debug("{}: final output path is {}", getRole(), finalOutputPath);
    // forces evaluation and caching of the resolution mode.
    ConflictResolution mode = getConflictResolutionMode(getJobContext(),
        fs.getConf());
    LOG.debug("Conflict resolution mode: {}", mode);
  }

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * Create the wrapped committer.
   * This includes customizing its options, and setting up the destination
   * directory.
   * @param context job/task context.
   * @param conf config
   * @return the inner committer
   * @throws IOException on a failure
   */
  protected FileOutputCommitter createWrappedCommitter(JobContext context,
      Configuration conf) throws IOException {

    // explicitly choose commit algorithm
    initFileOutputCommitterOptions(context);
    commitsDirectory = Paths.getMultipartUploadCommitsDirectory(conf, uuid);
    return new FileOutputCommitter(commitsDirectory, context);
  }

  /**
   * Init the context config with everything needed for the file output
   * committer. In particular, this code currently only works with
   * commit algorithm 1.
   * @param context context to configure.
   */
  protected void initFileOutputCommitterOptions(JobContext context) {
    context.getConfiguration()
        .setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("StagingCommitter{");
    sb.append(super.toString());
    sb.append(", conflictResolution=").append(conflictResolution);
    if (wrappedCommitter != null) {
      sb.append(", wrappedCommitter=").append(wrappedCommitter);
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * Get the UUID of an upload; may be the job ID.
   * Spark will use a fake app ID based on the current minute and job ID 0.
   * To avoid collisions, the key policy is:
   * <ol>
   *   <li>Value of {@link InternalCommitterConstants#FS_S3A_COMMITTER_STAGING_UUID}.</li>
   *   <li>Value of {@code "spark.sql.sources.writeJobUUID"}.</li>
   *   <li>Value of {@code "spark.app.id"}.</li>
   *   <li>JobId passed in.</li>
   * </ol>
   * The staging UUID is set in in {@link #setupJob(JobContext)} and so will
   * be valid in all sequences where the job has been set up for the
   * configuration passed in.
   * @param conf job/task configuration
   * @param jobId Job ID
   * @return an ID for use in paths.
   */
  public static String getUploadUUID(Configuration conf, String jobId) {
    return conf.getTrimmed(
        InternalCommitterConstants.FS_S3A_COMMITTER_STAGING_UUID,
        conf.getTrimmed(SPARK_WRITE_UUID,
            conf.getTrimmed(SPARK_APP_ID, jobId)));
  }

  /**
   * Get the UUID of a Job.
   * @param conf job/task configuration
   * @param jobId Job ID
   * @return an ID for use in paths.
   */
  public static String getUploadUUID(Configuration conf, JobID jobId) {
    return getUploadUUID(conf, jobId.toString());
  }

  /**
   * Get the work path for a task.
   * @param context job/task complex
   * @param uuid UUID
   * @return a path or null if the context is not of a task
   * @throws IOException failure to build the path
   */
  private static Path buildWorkPath(JobContext context, String uuid)
      throws IOException {
    if (context instanceof TaskAttemptContext) {
      return taskAttemptWorkingPath((TaskAttemptContext) context, uuid);
    } else {
      return null;
    }
  }

  /**
   * Is this committer using unique filenames?
   * @return true if unique filenames are used.
   */
  public Boolean useUniqueFilenames() {
    return uniqueFilenames;
  }

  /**
   * Get the filesystem for the job attempt.
   * @param context the context of the job.  This is used to get the
   * application attempt ID.
   * @return the FS to store job attempt data.
   * @throws IOException failure to create the FS.
   */
  public FileSystem getJobAttemptFileSystem(JobContext context)
      throws IOException {
    Path p = getJobAttemptPath(context);
    return p.getFileSystem(context.getConfiguration());
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param context the context of the job.  This is used to get the
   * application attempt ID.
   * @param out the output path to place these in.
   * @return the path to store job attempt data.
   */
  public static Path getJobAttemptPath(JobContext context, Path out) {
    return getJobAttemptPath(getAppAttemptId(context), out);
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path to store job attempt data.
   */
  private static Path getJobAttemptPath(int appAttemptId, Path out) {
    return new Path(getPendingJobAttemptsPath(out),
        String.valueOf(appAttemptId));
  }

  @Override
  protected Path getJobAttemptPath(int appAttemptId) {
    return new Path(getPendingJobAttemptsPath(commitsDirectory),
        String.valueOf(appAttemptId));
  }

  /**
   * Compute the path where the output of pending task attempts are stored.
   * @param context the context of the job with pending tasks.
   * @return the path where the output of pending task attempts are stored.
   */
  private static Path getPendingTaskAttemptsPath(JobContext context, Path out) {
    return new Path(getJobAttemptPath(context, out), TEMPORARY);
  }

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   *
   * @param context the context of the task attempt.
   * @param out The output path to put things in.
   * @return the path where a task attempt should be stored.
   */
  public static Path getTaskAttemptPath(TaskAttemptContext context, Path out) {
    return new Path(getPendingTaskAttemptsPath(context, out),
        String.valueOf(context.getTaskAttemptID()));
  }

  /**
   * Get the location of pending job attempts.
   * @param out the base output directory.
   * @return the location of pending job attempts.
   */
  private static Path getPendingJobAttemptsPath(Path out) {
    Preconditions.checkNotNull(out, "Null 'out' path");
    return new Path(out, TEMPORARY);
  }

  /**
   * Compute the path where the output of a committed task is stored until
   * the entire job is committed.
   * @param context the context of the task attempt
   * @return the path where the output of a committed task is stored until
   * the entire job is committed.
   */
  public Path getCommittedTaskPath(TaskAttemptContext context) {
    return getCommittedTaskPath(getAppAttemptId(context), context);
  }

  /**
   * Validate the task attempt context; makes sure
   * that the task attempt ID data is valid.
   * @param context task context
   */
  private static void validateContext(TaskAttemptContext context) {
    Preconditions.checkNotNull(context, "null context");
    Preconditions.checkNotNull(context.getTaskAttemptID(),
        "null task attempt ID");
    Preconditions.checkNotNull(context.getTaskAttemptID().getTaskID(),
        "null task ID");
    Preconditions.checkNotNull(context.getTaskAttemptID().getJobID(),
        "null job ID");
  }

  /**
   * Compute the path where the output of a committed task is stored until the
   * entire job is committed for a specific application attempt.
   * @param appAttemptId the ID of the application attempt to use
   * @param context the context of any task.
   * @return the path where the output of a committed task is stored.
   */
  protected Path getCommittedTaskPath(int appAttemptId,
      TaskAttemptContext context) {
    validateContext(context);
    return new Path(getJobAttemptPath(appAttemptId),
        String.valueOf(context.getTaskAttemptID().getTaskID()));
  }

  @Override
  public Path getTempTaskAttemptPath(TaskAttemptContext context) {
    throw new UnsupportedOperationException("Unimplemented");
  }

  /**
   * Lists the output of a task under the task attempt path. Subclasses can
   * override this method to change how output files are identified.
   * <p>
   * This implementation lists the files that are direct children of the output
   * path and filters hidden files (file names starting with '.' or '_').
   * <p>
   * The task attempt path is provided by
   * {@link #getTaskAttemptPath(TaskAttemptContext)}
   *
   * @param context this task's {@link TaskAttemptContext}
   * @return the output files produced by this task in the task attempt path
   * @throws IOException on a failure
   */
  protected List<LocatedFileStatus> getTaskOutput(TaskAttemptContext context)
      throws IOException {

    // get files on the local FS in the attempt path
    Path attemptPath = getTaskAttemptPath(context);
    Preconditions.checkNotNull(attemptPath,
        "No attemptPath path in {}", this);

    LOG.debug("Scanning {} for files to commit", attemptPath);

    return listAndFilter(getTaskAttemptFilesystem(context),
        attemptPath, true, HIDDEN_FILE_FILTER);
  }

  /**
   * Returns the final S3 key for a relative path. Subclasses can override this
   * method to upload files to a different S3 location.
   * <p>
   * This implementation concatenates the relative path with the key prefix
   * from the output path.
   * If {@link CommitConstants#FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES} is
   * set, then the task UUID is also included in the calculation
   *
   * @param relative the path of a file relative to the task attempt path
   * @param context the JobContext or TaskAttemptContext for this job
   * @return the S3 key where the file will be uploaded
   */
  protected String getFinalKey(String relative, JobContext context) {
    if (uniqueFilenames) {
      return getS3KeyPrefix(context) + "/" + Paths.addUUID(relative, uuid);
    } else {
      return getS3KeyPrefix(context) + "/" + relative;
    }
  }

  /**
   * Returns the final S3 location for a relative path as a Hadoop {@link Path}.
   * This is a final method that calls {@link #getFinalKey(String, JobContext)}
   * to determine the final location.
   *
   * @param relative the path of a file relative to the task attempt path
   * @param context the JobContext or TaskAttemptContext for this job
   * @return the S3 Path where the file will be uploaded
   * @throws IOException IO problem
   */
  protected final Path getFinalPath(String relative, JobContext context)
      throws IOException {
    return getDestS3AFS().keyToQualifiedPath(getFinalKey(relative, context));
  }

  /**
   * Return the local work path as the destination for writing work.
   * @param context the context of the task attempt.
   * @return a path in the local filesystem.
   */
  @Override
  public Path getBaseTaskAttemptPath(TaskAttemptContext context) {
    // a path on the local FS for files that will be uploaded
    return getWorkPath();
  }

  /**
   * For a job attempt path, the staging committer returns that of the
   * wrapped committer.
   * @param context the context of the job.
   * @return a path in HDFS.
   */
  @Override
  public Path getJobAttemptPath(JobContext context) {
    return wrappedCommitter.getJobAttemptPath(context);
  }

  /**
   * Set up the job, including calling the same method on the
   * wrapped committer.
   * @param context job context
   * @throws IOException IO failure.
   */
  @Override
  public void setupJob(JobContext context) throws IOException {
    LOG.debug("{}, Setting up job {}", getRole(), jobIdString(context));
    context.getConfiguration().set(
        InternalCommitterConstants.FS_S3A_COMMITTER_STAGING_UUID, uuid);
    wrappedCommitter.setupJob(context);
  }

  /**
   * Get the list of pending uploads for this job attempt.
   * @param context job context
   * @return a list of pending uploads.
   * @throws IOException Any IO failure
   */
  @Override
  protected List<SinglePendingCommit> listPendingUploadsToCommit(
      JobContext context)
      throws IOException {
    return listPendingUploads(context, false);
  }

  /**
   * Get the list of pending uploads for this job attempt, swallowing
   * exceptions.
   * @param context job context
   * @return a list of pending uploads. If an exception was swallowed,
   * then this may not match the actual set of pending operations
   * @throws IOException shouldn't be raised, but retained for the compiler
   */
  protected List<SinglePendingCommit> listPendingUploadsToAbort(
      JobContext context) throws IOException {
    return listPendingUploads(context, true);
  }

  /**
   * Get the list of pending uploads for this job attempt.
   * @param context job context
   * @param suppressExceptions should exceptions be swallowed?
   * @return a list of pending uploads. If exceptions are being swallowed,
   * then this may not match the actual set of pending operations
   * @throws IOException Any IO failure which wasn't swallowed.
   */
  protected List<SinglePendingCommit> listPendingUploads(
      JobContext context, boolean suppressExceptions) throws IOException {
    try {
      Path wrappedJobAttemptPath = wrappedCommitter.getJobAttemptPath(context);
      final FileSystem attemptFS = wrappedJobAttemptPath.getFileSystem(
          context.getConfiguration());
      return loadPendingsetFiles(context, suppressExceptions, attemptFS,
          listAndFilter(attemptFS,
              wrappedJobAttemptPath, false,
              HIDDEN_FILE_FILTER));
    } catch (FileNotFoundException e) {
      // this can mean the job was aborted early on, so don't confuse people
      // with long stack traces that aren't the underlying problem.
      maybeIgnore(suppressExceptions, "Pending upload directory not found", e);
    } catch (IOException e) {
      // unable to work with endpoint, if suppressing errors decide our actions
      maybeIgnore(suppressExceptions, "Listing pending uploads", e);
    }
    // reached iff an IOE was caught and swallowed
    return new ArrayList<>(0);
  }

  @Override
  public void cleanupStagingDirs() {
    Path workPath = getWorkPath();
    if (workPath != null) {
      LOG.debug("Cleaning up work path {}", workPath);
      ignoreIOExceptions(LOG, "cleaning up", workPath.toString(),
          () -> deleteQuietly(workPath.getFileSystem(getConf()),
              workPath, true));
    }
  }

  @Override
  @SuppressWarnings("deprecation")
  protected void cleanup(JobContext context,
      boolean suppressExceptions)
      throws IOException {
    maybeIgnore(suppressExceptions, "Cleanup wrapped committer",
        () -> wrappedCommitter.cleanupJob(context));
    maybeIgnore(suppressExceptions, "Delete destination paths",
        () -> deleteDestinationPaths(context));
    super.cleanup(context, suppressExceptions);
  }

  @Override
  protected void abortPendingUploadsInCleanup(boolean suppressExceptions)
      throws IOException {
    if (getConf()
        .getBoolean(FS_S3A_COMMITTER_STAGING_ABORT_PENDING_UPLOADS, true)) {
      super.abortPendingUploadsInCleanup(suppressExceptions);
    } else {
      LOG.info("Not cleanup up pending uploads to {} as {} is false ",
          getOutputPath(),
          FS_S3A_COMMITTER_STAGING_ABORT_PENDING_UPLOADS);
    }
  }

  @Override
  protected void abortJobInternal(JobContext context,
      boolean suppressExceptions) throws IOException {
    String r = getRole();
    boolean failed = false;
    try (DurationInfo d = new DurationInfo(LOG,
        "%s: aborting job in state %s ", r, jobIdString(context))) {
      List<SinglePendingCommit> pending = listPendingUploadsToAbort(context);
      abortPendingUploads(context, pending, suppressExceptions);
    } catch (FileNotFoundException e) {
      // nothing to list
      LOG.debug("No job directory to read uploads from");
    } catch (IOException e) {
      failed = true;
      maybeIgnore(suppressExceptions, "aborting job", e);
    } finally {
      super.abortJobInternal(context, failed || suppressExceptions);
    }
  }

  /**
   * Delete the working paths of a job.
   * <ol>
   *   <li>The job attempt path</li>
   *   <li>{@code $dest/__temporary}</li>
   *   <li>the local working directory for staged files</li>
   * </ol>
   * Does not attempt to clean up the work of the wrapped committer.
   * @param context job context
   * @throws IOException IO failure
   */
  protected void deleteDestinationPaths(JobContext context) throws IOException {
    Path attemptPath = getJobAttemptPath(context);
    ignoreIOExceptions(LOG,
        "Deleting Job attempt Path", attemptPath.toString(),
        () -> deleteWithWarning(
            getJobAttemptFileSystem(context),
            attemptPath,
            true));

    // delete the __temporary directory. This will cause problems
    // if there is >1 task targeting the same dest dir
    deleteWithWarning(getDestFS(),
        new Path(getOutputPath(), TEMPORARY),
        true);
    // and the working path
    deleteTaskWorkingPathQuietly(context);
  }


  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    Path taskAttemptPath = getTaskAttemptPath(context);
    try (DurationInfo d = new DurationInfo(LOG,
        "%s: setup task attempt path %s ", getRole(), taskAttemptPath)) {
      // create the local FS
      taskAttemptPath.getFileSystem(getConf()).mkdirs(taskAttemptPath);
      wrappedCommitter.setupTask(context);
    }
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context)
      throws IOException {
    try (DurationInfo d = new DurationInfo(LOG,
        "%s: needsTaskCommit() Task %s",
        getRole(), context.getTaskAttemptID())) {
      // check for files on the local FS in the attempt path
      Path attemptPath = getTaskAttemptPath(context);
      FileSystem fs = getTaskAttemptFilesystem(context);

      // This could be made more efficient with a probe "hasChildren(Path)"
      // which returns true if there is >1 entry under a given path.
      FileStatus[] stats = fs.listStatus(attemptPath);
      LOG.debug("{} files to commit under {}", stats.length, attemptPath);
      return stats.length > 0;
    } catch (FileNotFoundException e) {
      // list didn't find a directory, so nothing to commit
      // TODO: throw this up as an error?
      LOG.info("No files to commit");
      throw e;
    }
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    try (DurationInfo d = new DurationInfo(LOG,
        "%s: commit task %s", getRole(), context.getTaskAttemptID())) {
      int count = commitTaskInternal(context, getTaskOutput(context));
      LOG.info("{}: upload file count: {}", getRole(), count);
    } catch (IOException e) {
      LOG.error("{}: commit of task {} failed",
          getRole(), context.getTaskAttemptID(), e);
      getCommitOperations().taskCompleted(false);
      throw e;
    }
    getCommitOperations().taskCompleted(true);
  }

  /**
   * Commit the task by uploading all created files and then
   * writing a pending entry for them.
   * @param context task context
   * @param taskOutput list of files from the output
   * @return number of uploads committed.
   * @throws IOException IO Failures.
   */
  protected int commitTaskInternal(final TaskAttemptContext context,
      List<? extends FileStatus> taskOutput)
      throws IOException {
    LOG.debug("{}: commitTaskInternal", getRole());
    Configuration conf = context.getConfiguration();

    final Path attemptPath = getTaskAttemptPath(context);
    FileSystem attemptFS = getTaskAttemptFilesystem(context);
    LOG.debug("{}: attempt path is {}", getRole(), attemptPath);

    // add the commits file to the wrapped committer's task attempt location.
    // of this method.
    Path commitsAttemptPath = wrappedCommitter.getTaskAttemptPath(context);
    FileSystem commitsFS = commitsAttemptPath.getFileSystem(conf);

    // keep track of unfinished commits in case one fails. if something fails,
    // we will try to abort the ones that had already succeeded.
    int commitCount = taskOutput.size();
    final Queue<SinglePendingCommit> commits = new ConcurrentLinkedQueue<>();
    LOG.info("{}: uploading from staging directory to S3", getRole());
    LOG.info("{}: Saving pending data information to {}",
        getRole(), commitsAttemptPath);
    if (taskOutput.isEmpty()) {
      // there is nothing to write. needsTaskCommit() should have caught
      // this, so warn that there is some kind of problem in the protocol.
      LOG.warn("{}: No files to commit", getRole());
    } else {
      boolean threw = true;
      // before the uploads, report some progress
      context.progress();

      PendingSet pendingCommits = new PendingSet(commitCount);
      try {
        Tasks.foreach(taskOutput)
            .stopOnFailure()
            .executeWith(buildThreadPool(context))
            .run(stat -> {
              Path path = stat.getPath();
              File localFile = new File(path.toUri().getPath());
              String relative = Paths.getRelativePath(attemptPath, path);
              String partition = Paths.getPartition(relative);
              String key = getFinalKey(relative, context);
              Path destPath = getDestS3AFS().keyToQualifiedPath(key);
              SinglePendingCommit commit = getCommitOperations()
                  .uploadFileToPendingCommit(
                      localFile,
                      destPath,
                      partition,
                      uploadPartSize);
              LOG.debug("{}: adding pending commit {}", getRole(), commit);
              commits.add(commit);
            });

        for (SinglePendingCommit commit : commits) {
          pendingCommits.add(commit);
        }

        // save the data
        // although overwrite=false, there's still a risk of > 1 entry being
        // committed if the FS doesn't have create-no-overwrite consistency.

        LOG.debug("Saving {} pending commit(s)) to file {}",
            pendingCommits.size(),
            commitsAttemptPath);
        pendingCommits.save(commitsFS, commitsAttemptPath, false);
        threw = false;

      } finally {
        if (threw) {
          LOG.error(
              "{}: Exception during commit process, aborting {} commit(s)",
              getRole(), commits.size());
          Tasks.foreach(commits)
              .suppressExceptions()
              .run(commit -> getCommitOperations().abortSingleCommit(commit));
          deleteTaskAttemptPathQuietly(context);
        }
      }
      // always purge attempt information at this point.
      Paths.clearTempFolderInfo(context.getTaskAttemptID());
    }

    LOG.debug("Committing wrapped task");
    wrappedCommitter.commitTask(context);

    LOG.debug("Cleaning up attempt dir {}", attemptPath);
    attemptFS.delete(attemptPath, true);
    return commits.size();
  }

  /**
   * Abort the task.
   * The API specifies that the task has not yet been committed, so there are
   * no uploads that need to be cancelled.
   * Accordingly just delete files on the local FS, and call abortTask in
   * the wrapped committer.
   * <b>Important: this may be called in the AM after a container failure.</b>
   * When that occurs and the failed container was on a different host in the
   * cluster, the local files will not be deleted.
   * @param context task context
   * @throws IOException any failure
   */
  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    // the API specifies that the task has not yet been committed, so there are
    // no uploads that need to be cancelled. just delete files on the local FS.
    try (DurationInfo d = new DurationInfo(LOG,
        "Abort task %s", context.getTaskAttemptID())) {
      deleteTaskAttemptPathQuietly(context);
      deleteTaskWorkingPathQuietly(context);
      wrappedCommitter.abortTask(context);
    } catch (IOException e) {
      LOG.error("{}: exception when aborting task {}",
          getRole(), context.getTaskAttemptID(), e);
      throw e;
    }
  }

  /**
   * Get the work path for a task.
   * @param context job/task complex
   * @param uuid UUID
   * @return a path
   * @throws IOException failure to build the path
   */
  private static Path taskAttemptWorkingPath(TaskAttemptContext context,
      String uuid) throws IOException {
    return getTaskAttemptPath(context,
        Paths.getLocalTaskAttemptTempDir(
            context.getConfiguration(),
            uuid,
            context.getTaskAttemptID()));
  }

  /**
   * Delete the working path of a task; no-op if there is none, that
   * is: this is a job.
   * @param context job/task context
   */
  protected void deleteTaskWorkingPathQuietly(JobContext context) {
    ignoreIOExceptions(LOG, "Delete working path", "",
        () -> {
          Path path = buildWorkPath(context, getUUID());
          if (path != null) {
            deleteQuietly(path.getFileSystem(getConf()), path, true);
          }
        });
  }

  /**
   * Get the key of the destination "directory" of the job/task.
   * @param context job context
   * @return key to write to
   */
  private String getS3KeyPrefix(JobContext context) {
    return s3KeyPrefix;
  }

  /**
   * A UUID for this upload, as calculated with.
   * {@link #getUploadUUID(Configuration, String)}
   * @return the UUID for files
   */
  protected String getUUID() {
    return uuid;
  }

  /**
   * Returns the {@link ConflictResolution} mode for this commit.
   *
   * @param context the JobContext for this commit
   * @param fsConf filesystem config
   * @return the ConflictResolution mode
   */
  public final ConflictResolution getConflictResolutionMode(
      JobContext context,
      Configuration fsConf) {
    if (conflictResolution == null) {
      this.conflictResolution = ConflictResolution.valueOf(
          getConfictModeOption(context, fsConf));
    }
    return conflictResolution;
  }

  /**
   * Generate a {@link PathExistsException} because the destination exists.
   * Lists some of the child entries first, to help diagnose the problem.
   * @param path path which exists
   * @param description description (usually task/job ID)
   * @return an exception to throw
   */
  protected PathExistsException failDestinationExists(final Path path,
      final String description) {

    LOG.error("{}: Failing commit by job {} to write"
            + " to existing output path {}.",
        description,
        getJobContext().getJobID(), path);
    // List the first 10 descendants, to give some details
    // on what is wrong but not overload things if there are many files.
    try {
      int limit = 10;
      RemoteIterator<LocatedFileStatus> lf
          = getDestFS().listFiles(path, true);
      LOG.info("Partial Directory listing");
      while (limit > 0 && lf.hasNext()) {
        limit--;
        LocatedFileStatus status = lf.next();
        LOG.info("{}: {}",
            status.getPath(),
            status.isDirectory()
                ? " dir"
                : ("file size " + status.getLen() + " bytes"));
      }
    } catch (IOException e) {
      LOG.info("Discarding exception raised when listing {}: " + e, path);
      LOG.debug("stack trace ", e);
    }
    return new PathExistsException(path.toString(),
        description + ": " + InternalCommitterConstants.E_DEST_EXISTS);
  }

  /**
   * Get the conflict mode option string.
   * @param context context with the config
   * @param fsConf filesystem config
   * @return the trimmed configuration option, upper case.
   */
  public static String getConfictModeOption(JobContext context,
      Configuration fsConf) {
    return getConfigurationOption(context,
        fsConf,
        FS_S3A_COMMITTER_STAGING_CONFLICT_MODE,
        DEFAULT_CONFLICT_MODE).toUpperCase(Locale.ENGLISH);
  }

}
