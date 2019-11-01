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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;

/**
 * This commits to a directory.
 * The conflict policy is
 * <ul>
 *   <li>FAIL: fail the commit</li>
 *   <li>APPEND: add extra data to the destination.</li>
 *   <li>REPLACE: delete the destination directory in the job commit
 *   (i.e. after and only if all tasks have succeeded.</li>
 * </ul>
 */
public class DirectoryStagingCommitter extends StagingCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(
      DirectoryStagingCommitter.class);

  /** Name: {@value}. */
  public static final String NAME = COMMITTER_NAME_DIRECTORY;

  public DirectoryStagingCommitter(Path outputPath, TaskAttemptContext context)
      throws IOException {
    super(outputPath, context);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    Path outputPath = getOutputPath();
    FileSystem fs = getDestFS();
    ConflictResolution conflictResolution = getConflictResolutionMode(
        context, fs.getConf());
    LOG.info("Conflict Resolution mode is {}", conflictResolution);
    try {
      final FileStatus status = fs.getFileStatus(outputPath);

      // if it is not a directory, fail fast for all conflict options.
      if (!status.isDirectory()) {
        throw new PathExistsException(outputPath.toString(),
            "output path is not a directory: "
                + InternalCommitterConstants.E_DEST_EXISTS);
      }
      switch(conflictResolution) {
      case FAIL:
        throw failDestinationExists(outputPath,
            "Setting job as " + getRole());
      case APPEND:
      case REPLACE:
        LOG.debug("Destination directory exists; conflict policy permits this");
      }
    } catch (FileNotFoundException ignored) {
      // there is no destination path, hence, no conflict.
    }
    // make the parent directory, which also triggers a recursive directory
    // creation operation
    super.setupJob(context);
  }

  /**
   * Pre-commit actions for a job.
   * Here: look at the conflict resolution mode and choose
   * an action based on the current policy.
   * @param context job context
   * @param pending pending commits
   * @throws IOException any failure
   */
  @Override
  public void preCommitJob(
      final JobContext context,
      final ActiveCommit pending) throws IOException {

    // see if the files can be loaded.
    super.preCommitJob(context, pending);
    Path outputPath = getOutputPath();
    FileSystem fs = getDestFS();
    Configuration fsConf = fs.getConf();
    switch (getConflictResolutionMode(context, fsConf)) {
    case FAIL:
      // this was checked in setupJob; temporary files may have been
      // created, so do not check again.
      break;
    case APPEND:
      // do nothing
      break;
    case REPLACE:
      if (fs.delete(outputPath, true /* recursive */)) {
        LOG.info("{}: removed output path to be replaced: {}",
            getRole(), outputPath);
      }
      break;
    default:
      throw new IOException(getRole() + ": unknown conflict resolution mode: "
          + getConflictResolutionMode(context, fsConf));
    }
  }
}
