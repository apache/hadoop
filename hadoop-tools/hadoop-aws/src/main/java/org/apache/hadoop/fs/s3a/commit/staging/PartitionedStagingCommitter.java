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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.PathCommitException;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;

/**
 * Partitioned committer.
 * This writes data to specific "partition" subdirectories, applying
 * conflict resolution on a partition-by-partition basis. The existence
 * and state of any parallel partitions for which there is no are output
 * files are not considered in the conflict resolution.
 *
 * The conflict policy is
 * <ul>
 *   <li>FAIL: fail the commit if any of the partitions have data.</li>
 *   <li>APPEND: add extra data to the destination partitions.</li>
 *   <li>REPLACE: delete the destination partition in the job commit
 *   (i.e. after and only if all tasks have succeeded.</li>
 * </ul>
 */
public class PartitionedStagingCommitter extends StagingCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(
      PartitionedStagingCommitter.class);

  /** Name: {@value}. */
  public static final String NAME = COMMITTER_NAME_PARTITIONED;

  public PartitionedStagingCommitter(Path outputPath,
      TaskAttemptContext context)
      throws IOException {
    super(outputPath, context);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "PartitionedStagingCommitter{");
    sb.append(super.toString());
    sb.append('}');
    return sb.toString();
  }

  @Override
  protected int commitTaskInternal(TaskAttemptContext context,
      List<? extends FileStatus> taskOutput) throws IOException {
    Path attemptPath = getTaskAttemptPath(context);
    Set<String> partitions = Paths.getPartitions(attemptPath, taskOutput);

    // enforce conflict resolution, but only if the mode is FAIL. for APPEND,
    // it doesn't matter that the partitions are already there, and for REPLACE,
    // deletion should be done during job commit.
    FileSystem fs = getDestFS();
    if (getConflictResolutionMode(context, fs.getConf())
        == ConflictResolution.FAIL) {
      for (String partition : partitions) {
        // getFinalPath adds the UUID to the file name. this needs the parent.
        Path partitionPath = getFinalPath(partition + "/file",
            context).getParent();
        if (fs.exists(partitionPath)) {
          throw failDestinationExists(partitionPath,
              "Committing task " + context.getTaskAttemptID());
        }
      }
    }
    return super.commitTaskInternal(context, taskOutput);
  }

  /**
   * Job-side conflict resolution.
   * The partition path conflict resolution actions are:
   * <ol>
   *   <li>FAIL: assume checking has taken place earlier; no more checks.</li>
   *   <li>APPEND: allowed.; no need to check.</li>
   *   <li>REPLACE deletes all existing partitions.</li>
   * </ol>
   * @param context job context
   * @param pending the pending operations
   * @throws IOException any failure
   */
  @Override
  protected void preCommitJob(JobContext context,
      List<SinglePendingCommit> pending) throws IOException {

    FileSystem fs = getDestFS();

    // enforce conflict resolution
    Configuration fsConf = fs.getConf();
    switch (getConflictResolutionMode(context, fsConf)) {
    case FAIL:
      // FAIL checking is done on the task side, so this does nothing
      break;
    case APPEND:
      // no check is needed because the output may exist for appending
      break;
    case REPLACE:
      Set<Path> partitions = pending.stream()
          .map(SinglePendingCommit::destinationPath)
          .map(Path::getParent)
          .collect(Collectors.toCollection(Sets::newLinkedHashSet));
      for (Path partitionPath : partitions) {
        LOG.debug("{}: removing partition path to be replaced: " +
            getRole(), partitionPath);
        fs.delete(partitionPath, true);
      }
      break;
    default:
      throw new PathCommitException("",
          getRole() + ": unknown conflict resolution mode: "
          + getConflictResolutionMode(context, fsConf));
    }
  }

}
