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

package org.apache.hadoop.fs.s3a.commit.magic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.s3a.commit.AbstractS3GuardCommitter;
import org.apache.hadoop.fs.s3a.commit.CommitOperations;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.CommitUtils;
import org.apache.hadoop.fs.s3a.commit.DurationInfo;
import org.apache.hadoop.fs.s3a.commit.PathCommitException;
import org.apache.hadoop.fs.s3a.commit.files.PendingSet;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.deleteQuietly;
import static org.apache.hadoop.fs.s3a.commit.CommitUtils.*;

/**
 * This is a dedicated committer which requires the "magic" directory feature
 * of the S3A Filesystem to be enabled; it then uses paths for task and job
 * attempts in magic paths, so as to ensure that the final output goes direct
 * to the destination directory.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MagicS3GuardCommitter extends AbstractS3GuardCommitter {
  private static final Logger LOG =
      LoggerFactory.getLogger(MagicS3GuardCommitter.class);

  public static final String NAME = "MagicS3GuardCommitter";

  /**
   * Create a task committer.
   * @param outputPath the job's output path
   * @param context the task's context
   * @throws IOException on a failure
   */
  public MagicS3GuardCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    setWorkPath(getTaskAttemptPath(context));
    verifyIsMagicCommitPath(getDestS3AFS(), getWorkPath());
    LOG.debug("Task attempt {} has work path {}",
        context.getTaskAttemptID(),
        getWorkPath());
  }

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * Require magic paths in the FS client.
   * @return true, always.
   */
  @Override
  protected boolean isMagicFileSystemRequired() {
    return true;
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    try (DurationInfo d = new DurationInfo(LOG,
        "Setup Job %s", jobIdString(context))) {
      Path jobAttemptPath = getJobAttemptPath(context);
      FileSystem fs = getDestinationFS(jobAttemptPath,
          context.getConfiguration());
      if (!fs.mkdirs(jobAttemptPath)) {
        throw new PathCommitException(jobAttemptPath,
            "Failed to create job attempt directory -it already exists");
      }
    }
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    List<SinglePendingCommit> pending = Collections.emptyList();
    String r = getRole();
    String id = jobIdString(context);
    try (DurationInfo d = new DurationInfo(LOG,
        "%s: preparing to commit Job", r)) {
      pending = listPendingUploadsToCommit(context);
    } catch (IOException e) {
      LOG.warn("Precommit failure for job {}", id, e);
      abortJobInternal(context, pending, true);
      getCommitOperations().jobCompleted(false);
      throw e;
    }
    try (DurationInfo d = new DurationInfo(LOG,
        "%s: committing Job %s", r, id)) {
      commitJobInternal(context, pending);
    } catch (IOException e) {
      getCommitOperations().jobCompleted(false);
      throw e;
    }
    getCommitOperations().jobCompleted(true);
    maybeCreateSuccessMarkerFromCommits(context, pending);
  }

  /**
   * Get the list of pending uploads for this job attempt, by listing
   * all .pendingset files in the job attempt directory.
   * @param context job context
   * @return a list of pending commits.
   * @throws IOException Any IO failure
   */
  protected List<SinglePendingCommit> listPendingUploadsToCommit(
      JobContext context)
      throws IOException {
    FileSystem fs = getDestFS();
    return loadMultiplePendingCommitFiles(context, false, fs,
        fs.listStatus(getJobAttemptPath(context), PENDINGSET_FILTER));
  }

  /**
   *
   * Get the list of pending uploads for this job attempt, by listing
   * all .pendingset files in the job attempt directory.
   * @param context job context
   * @return a list of pending commits.
   * @throws IOException Any IO failure which has not been swallowed
   */
  @Override
  protected List<SinglePendingCommit> listPendingUploadsToAbort(
      JobContext context)
      throws IOException {
    FileSystem fs = getDestFS();
    FileStatus[] pendingCommitFiles;
    Path jobAttemptPath = null;
    try {
      jobAttemptPath = getJobAttemptPath(context);
      pendingCommitFiles = fs.listStatus(jobAttemptPath,
          PENDINGSET_FILTER);
    } catch (IOException e) {
      // listing failure
      LOG.info("Failed to list contents of {}: {}",
          jobAttemptPath, e.toString());
      return new ArrayList<>(0);

    }
    return loadMultiplePendingCommitFiles(context, true, fs,
        pendingCommitFiles);
  }

  /**
   * Delete the magic directory.
   * @throws IOException IO problems
   */
  public void cleanupStagingDirs() throws IOException {
    deleteQuietly(getDestFS(), magicSubdir(getOutputPath()), true);
  }

  /**
   * Cleanup job: abort uploads, delete directories.
   * @param context Job context
   * @throws IOException IO failure
   */
  @Override
  protected void cleanup(JobContext context, boolean suppressExceptions)
      throws IOException {
    CommitOperations.MaybeIOE outcome = CommitOperations.MaybeIOE.NONE;
    try (DurationInfo d = new DurationInfo(LOG,
        "Cleanup: aborting pending uploads for Job %s",
        jobIdString(context))) {
      if (getCommitOperations() != null) {
        Path pending = getJobAttemptPath(context);
        outcome = getCommitOperations()
            .abortAllSinglePendingCommits(pending, true);
      }
    }
    try (DurationInfo d = new DurationInfo(LOG,
        "Cleanup job %s", jobIdString(context))) {
      deleteWithWarning(getDestFS(),
          getMagicJobAttemptsPath(getOutputPath()), true);
      deleteWithWarning(getDestFS(),
          getTempJobAttemptPath(getAppAttemptId(context),
              getMagicJobAttemptsPath(getOutputPath())), true);
      cleanupStagingDirs();
    }
    if (!suppressExceptions) {
      outcome.maybeRethrow();
    }
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    try (DurationInfo d = new DurationInfo(LOG,
        "Setup Task %s", context.getTaskAttemptID())) {
      Path taskAttemptPath = getTaskAttemptPath(context);
      FileSystem fs = taskAttemptPath.getFileSystem(getConf());
      fs.mkdirs(taskAttemptPath);
    }
  }

  /**
   * Did this task write any files in the work directory?
   * Probes for a task existing by looking to see if the attempt dir exists.
   * This adds more HTTP requests to the call. It may be better just to
   * return true and rely on the commit task doing the work.
   * @param context the task's context
   * @return true if the attempt path exists
   * @throws IOException failure to list the path
   */
  @Override
  public boolean needsTaskCommit(TaskAttemptContext context)
      throws IOException {
    Path taskAttemptPath = getTaskAttemptPath(context);
    try (DurationInfo d = new DurationInfo(LOG,
        "needsTaskCommit task %s", context.getTaskAttemptID())) {
      return taskAttemptPath.getFileSystem(
          context.getConfiguration())
          .exists(taskAttemptPath);
    }
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    try (DurationInfo d = new DurationInfo(LOG,
        "Commit task %s", context.getTaskAttemptID())) {
      PendingSet commits = innerCommitTask(context);
      LOG.info("Task {} committed {} files", context.getTaskAttemptID(),
          commits.size());
    } catch (IOException e) {
      getCommitOperations().taskCompleted(false);
      throw e;
    } finally {
      // delete the task attempt so there's no possibility of a second attempt
      deleteTaskAttemptPathQuietly(context);
    }
    getCommitOperations().taskCompleted(true);
  }

  /**
   * Inner routine for committing a task.
   * The list of pending commits is loaded and then saved to the job attempt
   * dir.
   * Failure to load any file or save the final file triggers an abort of
   * all known pending commits.
   * @param context context
   * @return the summary file
   * @throws IOException exception
   */
  private PendingSet innerCommitTask(
      TaskAttemptContext context) throws IOException {
    Path taskAttemptPath = getTaskAttemptPath(context);
    // load in all pending commits.
    CommitOperations actions = getCommitOperations();
    Pair<PendingSet, List<Pair<LocatedFileStatus, IOException>>>
        loaded = actions.loadSinglePendingCommits(
            taskAttemptPath, true);
    PendingSet pendingSet = loaded.getKey();
    List<Pair<LocatedFileStatus, IOException>> failures = loaded.getValue();
    if (!failures.isEmpty()) {
      // At least one file failed to load
      // revert all which did; report failure with first exception
      LOG.error("At least one commit file could not be read: failing");
      abortPendingUploads(context, pendingSet.getCommits(),
          true);
      throw failures.get(0).getValue();
    }
    // patch in IDs
    String jobId = String.valueOf(context.getJobID());
    String taskId = String.valueOf(context.getTaskAttemptID());
    for (SinglePendingCommit commit : pendingSet.getCommits()) {
      commit.setJobId(jobId);
      commit.setTaskId(taskId);
    }

    Path jobAttemptPath = getJobAttemptPath(context);
    TaskAttemptID taskAttemptID = context.getTaskAttemptID();
    Path taskOutcomePath = new Path(jobAttemptPath,
        taskAttemptID.getTaskID().toString() +
        CommitConstants.PENDINGSET_SUFFIX);
    LOG.info("Saving work of {} to {}", taskAttemptID, taskOutcomePath);
    try {
      pendingSet.save(getDestFS(), taskOutcomePath, false);
    } catch (IOException e) {
      LOG.warn("Failed to save task commit data to {} ",
          taskOutcomePath, e);
      abortPendingUploads(context, pendingSet.getCommits(),
          true);
      throw e;
    }
    return pendingSet;
  }

  /**
   * Abort a task. Attempt load then abort all pending files,
   * then try to delete the task attempt path.
   * This method may be called on the job committer, rather than the
   * task one (such as in the MapReduce AM after a task container failure).
   * It must extract all paths and state from the passed in context.
   * @param context task context
   * @throws IOException if there was some problem querying the path other
   * than it not actually existing.
   */
  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    Path attemptPath = getTaskAttemptPath(context);
    try (DurationInfo d = new DurationInfo(LOG,
        "Abort task %s", context.getTaskAttemptID())) {
      getCommitOperations().abortAllSinglePendingCommits(attemptPath, true);
    } finally {
      deleteQuietly(attemptPath.getFileSystem(context.getConfiguration()),
          attemptPath, true);
    }
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path to store job attempt data.
   */
  protected Path getJobAttemptPath(int appAttemptId) {
    return getMagicJobAttemptPath(appAttemptId, getOutputPath());
  }

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   *
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   */
  public Path getTaskAttemptPath(TaskAttemptContext context) {
    return getMagicTaskAttemptPath(context, getOutputPath());
  }

  @Override
  protected Path getBaseTaskAttemptPath(TaskAttemptContext context) {
    return getBaseMagicTaskAttemptPath(context, getOutputPath());
  }

  /**
   * Get a temporary directory for data. When a task is aborted/cleaned
   * up, the contents of this directory are all deleted.
   * @param context task context
   * @return a path for temporary data.
   */
  public Path getTempTaskAttemptPath(TaskAttemptContext context) {
    return CommitUtils.getTempTaskAttemptPath(context, getOutputPath());
  }

  /**
   * Filter to find all {code .pendingset} files.
   */
  private static final PathFilter PENDINGSET_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return path.toString().endsWith(CommitConstants.PENDINGSET_SUFFIX);
    }
  };
}
