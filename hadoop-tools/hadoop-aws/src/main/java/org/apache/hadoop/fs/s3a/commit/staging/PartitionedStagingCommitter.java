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

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Partitioned committer.
 * This writes data to specific "partition" subdirectories.
 */
public class PartitionedStagingCommitter extends StagingCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(
      PartitionedStagingCommitter.class);
  public static final String NAME = "PartitionedStagingCommitter";

  public PartitionedStagingCommitter(Path outputPath, JobContext context)
      throws IOException {
    super(outputPath, context);
  }

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
      List<FileStatus> taskOutput) throws IOException {
    Path attemptPath = getTaskAttemptPath(context);
    Set<String> partitions = getPartitions(attemptPath, taskOutput);

    // enforce conflict resolution, but only if the mode is FAIL. for APPEND,
    // it doesn't matter that the partitions are already there, and for REPLACE,
    // deletion should be done during task commit.
    if (getConflictResolutionMode(context) == ConflictResolution.FAIL) {
      FileSystem fs = getDestFS();
      for (String partition : partitions) {
        // getFinalPath adds the UUID to the file name. this needs the parent.
        Path partitionPath = getFinalPath(partition + "/file",
            context).getParent();
        if (fs.exists(partitionPath)) {
          throw new PathExistsException(partitionPath.toString());
        }
      }
    }
    return super.commitTaskInternal(context, taskOutput);
  }

  /**
   * The partition path conflict resolution assumes that:
   * <ol>
   *   <li>FAIL checking has taken place earlier.</li>
   *   <li>APPEND is allowed</li>
   *   <li>REPLACE deletes all existing partitions</li>
   * </ol>
   * @param context job context
   * @param pending the pending operations
   * @throws IOException any failure
   */
  @Override
  protected void preCommitJob(JobContext context,
      List<SinglePendingCommit> pending) throws IOException {

    FileSystem fs = getDestFS();
    Set<Path> partitions = Sets.newLinkedHashSet();
    for (SinglePendingCommit commit : pending) {
      Path filePath = commit.destinationPath();
      partitions.add(filePath.getParent());
    }

    // enforce conflict resolution
    switch (getConflictResolutionMode(context)) {
    case FAIL:
      // FAIL checking is done on the task side, so this does nothing
      break;
    case APPEND:
      // no check is needed because the output may exist for appending
      break;
    case REPLACE:
      for (Path partitionPath : partitions) {
        LOG.info("{}: removing partition path to be replaced: " +
            getRole(), partitionPath);
        fs.delete(partitionPath, true);
      }
      break;
    default:
      throw new IOException(getRole() + ": unknown conflict resolution mode: "
          + getConflictResolutionMode(context));
    }
  }

  /**
   * Get the set of partitions from the list of files being staged.
   * This is all immediate parents of those files. If a file is in the root
   * dir, the partition is declared to be
   * {@link StagingCommitterConstants#TABLE_ROOT}.
   * @param attemptPath path for the attempt
   * @param taskOutput list of output files.
   * @return list of partitions.
   * @throws IOException IO failure
   */
  protected Set<String> getPartitions(Path attemptPath,
      List<FileStatus> taskOutput)
      throws IOException {
    // get a list of partition directories
    Set<String> partitions = Sets.newLinkedHashSet();
    for (FileStatus fileStatus : taskOutput) {
      // sanity check the output paths
      Path outputFile = fileStatus.getPath();
      if (!fileStatus.isFile()) {
        throw new PathIsDirectoryException(outputFile.toString());
      }
      String partition = getPartition(
          Paths.getRelativePath(attemptPath, outputFile));
      partitions.add(partition != null ?
          partition
          : StagingCommitterConstants.TABLE_ROOT);
    }

    return partitions;
  }
}
