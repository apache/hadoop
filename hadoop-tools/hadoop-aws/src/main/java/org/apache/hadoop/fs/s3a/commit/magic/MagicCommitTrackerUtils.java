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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.MagicCommitPaths;

import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * Utility class for the class {@link MagicCommitTracker} and its subclasses.
 */
public final class MagicCommitTrackerUtils {

  private MagicCommitTrackerUtils() {
  }

  /**
   * The magic path is of the following format.
   * "s3://bucket-name/table-path/__magic_jobId/job-id/taskAttempt/id/tasks/taskAttemptId"
   * So the third child from the "__magic" path will give the task attempt id.
   * @param path Path
   * @return taskAttemptId
   */
  public static String extractTaskAttemptIdFromPath(Path path) {
    List<String> elementsInPath = MagicCommitPaths.splitPathToElements(path);
    List<String> childrenOfMagicPath = MagicCommitPaths.magicPathChildren(elementsInPath);

    checkArgument(childrenOfMagicPath.size() >= 3, "Magic Path is invalid");
    // 3rd child of the magic path is the taskAttemptId
    return childrenOfMagicPath.get(3);
  }

  /**
   * Is tracking of magic commit data in-memory enabled.
   * @param conf Configuration
   * @return true if in memory tracking of commit data is enabled.
   */
  public static boolean isTrackMagicCommitsInMemoryEnabled(Configuration conf) {
    return conf.getBoolean(
        CommitConstants.FS_S3A_COMMITTER_MAGIC_TRACK_COMMITS_IN_MEMORY_ENABLED,
        CommitConstants.FS_S3A_COMMITTER_MAGIC_TRACK_COMMITS_IN_MEMORY_ENABLED_DEFAULT);
  }

  /**
   * Is cleanup of magic committer staging dirs enabled.
   * @param conf Configuration
   * @return true if cleanup of staging dir is enabled.
   */
  public static boolean isCleanupMagicCommitterEnabled(
      Configuration conf) {
    return conf.getBoolean(
        CommitConstants.FS_S3A_COMMITTER_MAGIC_CLEANUP_ENABLED,
        CommitConstants.FS_S3A_COMMITTER_MAGIC_CLEANUP_ENABLED_DEFAULT);
  }
}
