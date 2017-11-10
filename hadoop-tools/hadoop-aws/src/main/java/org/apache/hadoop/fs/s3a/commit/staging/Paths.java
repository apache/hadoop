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
import java.util.concurrent.ExecutionException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingCommitterConstants.*;

/**
 * Path operations for the staging committers.
 */
public final class Paths {

  private Paths() {
  }

  /**
   * Insert the UUID to a path if it is not there already.
   * If there is a trailing "." in the prefix after the last slash, the
   * UUID is inserted before it with a "-" prefix; otherwise appended.
   *
   * Examples:
   * <pre>
   *   /example/part-0000  ==&gt; /example/part-0000-0ab34
   *   /example/part-0001.gz.csv  ==&gt; /example/part-0001-0ab34.gz.csv
   *   /example/part-0002-0abc3.gz.csv  ==&gt; /example/part-0002-0abc3.gz.csv
   *   /example0abc3/part-0002.gz.csv  ==&gt; /example0abc3/part-0002.gz.csv
   * </pre>
   *
   *
   * @param pathStr path as a string; must not have a trailing "/".
   * @param uuid UUID to append; must not be empty
   * @return new path.
   */
  public static String addUUID(String pathStr, String uuid) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(pathStr), "empty path");
    Preconditions.checkArgument(StringUtils.isNotEmpty(uuid), "empty uuid");
    // In some cases, Spark will add the UUID to the filename itself.
    if (pathStr.contains(uuid)) {
      return pathStr;
    }

    int dot; // location of the first '.' in the file name
    int lastSlash = pathStr.lastIndexOf('/');
    if (lastSlash >= 0) {
      Preconditions.checkState(lastSlash + 1 < pathStr.length(),
          "Bad path: " + pathStr);
      dot = pathStr.indexOf('.', lastSlash);
    } else {
      dot = pathStr.indexOf('.');
    }

    if (dot >= 0) {
      return pathStr.substring(0, dot) + "-" + uuid + pathStr.substring(dot);
    } else {
      return pathStr + "-" + uuid;
    }
  }

  /**
   * Get the parent path of a string path: everything up to but excluding
   * the last "/" in the path.
   * @param pathStr path as a string
   * @return the parent or null if there is no parent.
   */
  public static String getParent(String pathStr) {
    int lastSlash = pathStr.lastIndexOf('/');
    if (lastSlash >= 0) {
      return pathStr.substring(0, lastSlash);
    }
    return null;
  }

  /**
   * Using {@code URI#relativize()}, build the relative path from the
   * base path to the full path.
   * If {@code childPath} is not a child of {@code basePath} the outcome
   * os undefined.
   * @param basePath base path
   * @param fullPath full path under the base path.
   * @return the relative path
   */
  public static String getRelativePath(Path basePath,
      Path fullPath) {
    return basePath.toUri().relativize(fullPath.toUri()).getPath();
  }

  /**
   * Varags constructor of paths. Not very efficient.
   * @param parent parent path
   * @param child child entries. "" elements are skipped.
   * @return the full child path.
   */
  public static Path path(Path parent, String... child) {
    Path p = parent;
    for (String c : child) {
      if (!c.isEmpty()) {
        p = new Path(p, c);
      }
    }
    return p;
  }

  /**
   * A cache of temporary folders. There's a risk here that the cache
   * gets too big
   */
  private static Cache<TaskAttemptID, Path> tempFolders = CacheBuilder
      .newBuilder().build();

  /**
   * Get the task attempt temporary directory in the local filesystem.
   * @param conf configuration
   * @param uuid some UUID, such as a job UUID
   * @param attemptID attempt ID
   * @return a local task attempt directory.
   * @throws IOException IO problem.
   */
  public static Path getLocalTaskAttemptTempDir(final Configuration conf,
      final String uuid,
      final TaskAttemptID attemptID)
      throws IOException {
    try {
      final LocalDirAllocator allocator =
          new LocalDirAllocator(Constants.BUFFER_DIR);
      return tempFolders.get(attemptID,
          () -> {
            return FileSystem.getLocal(conf).makeQualified(
                allocator.getLocalPathForWrite(uuid, conf));
          });
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    } catch (UncheckedExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(e);
    }
  }

  /**
   * Remove all information held about task attempts.
   * @param attemptID attempt ID.
   */
  public static void clearTempFolderInfo(final TaskAttemptID attemptID) {
    tempFolders.invalidate(attemptID);
  }

  /**
   * Reset the temp folder cache; useful in tests.
   */
  @VisibleForTesting
  public static void resetTempFolderCache() {
    tempFolders.invalidateAll();
  }

  /**
   * Try to come up with a good temp directory for different filesystems.
   * @param fs filesystem
   * @param conf configuration
   * @return a qualified path under which temporary work can go.
   */
  public static Path tempDirForStaging(FileSystem fs,
      Configuration conf) {
    String fallbackPath = fs.getScheme().equals("file")
        ? System.getProperty(JAVA_IO_TMPDIR)
        : FILESYSTEM_TEMP_PATH;

    return fs.makeQualified(new Path(conf.getTrimmed(
        FS_S3A_COMMITTER_STAGING_TMP_PATH, fallbackPath)));
  }

  /**
   * Get the Application Attempt ID for this job.
   * @param conf the config to look in
   * @return the Application Attempt ID for a given job.
   */
  private static int getAppAttemptId(Configuration conf) {
    return conf.getInt(
        MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
  }

  /**
   * Build a qualified temporary path for the multipart upload commit
   * information in the cluster filesystem.
   * Path is built by
   * {@link #getMultipartUploadCommitsDirectory(FileSystem, Configuration, String)}
   * @param conf configuration defining default FS.
   * @param uuid uuid of job
   * @return a path which can be used for temporary work
   * @throws IOException on an IO failure.
   */
  public static Path getMultipartUploadCommitsDirectory(Configuration conf,
      String uuid) throws IOException {
    return getMultipartUploadCommitsDirectory(FileSystem.get(conf), conf, uuid);
  }

  /**
   * Build a qualified temporary path for the multipart upload commit
   * information in the supplied filesystem
   * (which is expected to be the cluster FS).
   * Currently {code $tempDir/$user/$uuid/staging-uploads} where
   * {@code tempDir} is from
   * {@link #tempDirForStaging(FileSystem, Configuration)}.
   * @param fs target FS
   * @param conf configuration
   * @param uuid uuid of job
   * @return a path which can be used for temporary work
   * @throws IOException on an IO failure.
   */
  @VisibleForTesting
  static Path getMultipartUploadCommitsDirectory(FileSystem fs,
      Configuration conf, String uuid) throws IOException {
    return path(
        tempDirForStaging(fs, conf),
        UserGroupInformation.getCurrentUser().getShortUserName(),
        uuid,
        STAGING_UPLOADS);
  }

  /**
   * Returns the partition of a relative file path, or null if the path is a
   * file name with no relative directory.
   *
   * @param relative a relative file path
   * @return the partition of the relative file path
   */
  protected static String getPartition(String relative) {
    return getParent(relative);
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
  public static Set<String> getPartitions(Path attemptPath,
      List<? extends FileStatus> taskOutput)
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
          getRelativePath(attemptPath, outputFile));
      partitions.add(partition != null ? partition : TABLE_ROOT);
    }

    return partitions;
  }

}
