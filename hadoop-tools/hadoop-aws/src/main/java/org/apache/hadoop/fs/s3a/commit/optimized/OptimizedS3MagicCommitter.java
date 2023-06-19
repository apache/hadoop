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

package org.apache.hadoop.fs.s3a.commit.optimized;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.files.PendingSet;
import org.apache.hadoop.fs.s3a.commit.impl.CommitContext;
import org.apache.hadoop.fs.s3a.commit.impl.CommitOperations;
import org.apache.hadoop.fs.s3a.commit.impl.CommitUtilsWithMR;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.functional.TaskPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.demandStringifyIOStatistics;

/**
 * OptimizedS3MagicCommitter is a type of {@link MagicS3GuardCommitter} where in during
 * commitTask operations, The files become visible in the final directory and commitJob
 * operation simply writes a SUCCESS marker and cleans up the magic directory, unlike
 * {@link MagicS3GuardCommitter} where in the files become visible in the final directory
 * only after the commitJob operation.
 *
 * OptimizedS3MagicCommitter have better performance as compared to @link{MagicS3GuardCommitter}
 * primarily due to distributed complete multiPartUpload call being made in the taskAttempts rather
 * than a single job driver and saves a couple of S3 calls in writing ".pendingset" file and
 * reading the same in commitJob operation. This comes with a tradeoff similar to the
 * {@link org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter} v2 version.
 * On a failure, all output must be deleted and the job needs to be restarted.
 */
public class OptimizedS3MagicCommitter extends MagicS3GuardCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(OptimizedS3MagicCommitter.class);

  /** Name: {@value}. */
  public static final String NAME = CommitConstants.COMMITTER_NAME_OPTIMIZED;

  /**
   * Create a task committer.
   *
   * @param outputPath the job's output path
   * @param context    the task's context
   * @throws IOException on a failure
   */
  public OptimizedS3MagicCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
  }

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * Lists all the ".pending" suffix files from the directory __magic/jobId/taskId/
   * and calls the commit operation for the same.
   * The files become visible in the destination path after this operation.
   * @param context TaskAttemptContext
   * @throws IOException
   */
  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    try (DurationInfo d = new DurationInfo(LOG,
        "Commit task %s", context.getTaskAttemptID())) {
      innerCommitTask(context);
    } catch (IOException e) {
      getCommitOperations().taskCompleted(false);
      throw e;
    } finally {
      deleteTaskAttemptPathQuietly(context);
    }
    getCommitOperations().taskCompleted(true);
    LOG.debug("aggregate statistics\n{}",
        demandStringifyIOStatistics(getIOStatistics()));
  }

  /**
   * build the {@code _SUCCESS} file entry and cleans up __magic directory
   * @param context job context
   * @throws IOException any failure
   */
  @Override
  public void commitJob(JobContext context) throws IOException {
    String id = CommitUtilsWithMR.jobIdString(context);
    try (DurationInfo d = new DurationInfo(LOG,
        "%s: commitJob(%s)", getRole(), id)) {
      jobCompleted(true);
      maybeCreateSuccessMarker(context, null, null);
      cleanupStagingDirs();
    } catch (IOException e) {
      LOG.warn("Commit failure for job {}", id, e);
      jobCompleted(false);
      cleanupStagingDirs();
      throw e;
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "OptimizedS3MagicCommitter{");
    sb.append(super.toString());
    sb.append('}');
    return sb.toString();
  }

  private void innerCommitTask(TaskAttemptContext context) throws IOException {
    Path taskAttemptPath = getTaskAttemptPath(context);
    // load in all pending commits.
    CommitOperations actions = getCommitOperations();
    PendingSet pendingSet;
    try (CommitContext commitContext = initiateTaskOperation(context)) {
      Pair<PendingSet, List<Pair<LocatedFileStatus, IOException>>> loaded =
          actions.loadSinglePendingCommits(taskAttemptPath, true, commitContext);
      pendingSet = loaded.getKey();
      List<Pair<LocatedFileStatus, IOException>> failures = loaded.getValue();
      if (!failures.isEmpty()) {
        // At least one file failed to load
        // revert all which did; report failure with first exception
        LOG.error("At least one commit file could not be read: failing");
        abortPendingUploads(commitContext, pendingSet.getCommits(), true);
        throw failures.get(0).getValue();
      }
    }
    // patch in IDs
    String jobId = getUUID();
    String taskAttemptId = String.valueOf(context.getTaskAttemptID());

    // for all pending commits in taskAttemptPath call final commit
    // to make the files visible in the final destination.
    try (CommitContext commitContext = initiateJobOperation(context)) {
      TaskPool.foreach(pendingSet.getCommits())
          .stopOnFailure()
          .suppressExceptions(false)
          .executeWith(commitContext.getInnerSubmitter())
          .onFailure((commit, exception) ->
              commitContext.abortSingleCommit(commit))
          .abortWith(commitContext::abortSingleCommit)
          .revertWith(commitContext::revertCommit)
          .run(commit -> {
            commit.setJobId(jobId);
            commit.setTaskId(taskAttemptId);
            commitContext.commitOrFail(commit);
          });
    }
  }
}
