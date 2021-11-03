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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.EtagSource;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.util.DurationInfo;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Support for committing work using etags to recover from failure.
 * This is for internal use only.
 */
@VisibleForTesting
public class ResilientCommitByRenameHelper {

  private static final Logger LOG =
      LoggerFactory.getLogger(ResilientCommitByRenameHelper.class);

  /**
   * IO callbacks.
   */
  private final FileSystemOperations operations;

  /**
   * Is attempt recovery enabled?
   */
  private final boolean renameRecoveryAvailable;

  private final AtomicInteger recoveryCount = new AtomicInteger();



  /**
   * Instantiate.
   * @param fileSystem filesystem to work with.
   * @param finalOutput output path under which renames take place
   * @param attemptRecovery attempt recovery if the store has etags.
   */
  public ResilientCommitByRenameHelper(final FileSystem fileSystem,
      final Path finalOutput,
      final boolean attemptRecovery) {
    this(new FileSystemOperations(requireNonNull(fileSystem)),
        finalOutput, attemptRecovery);
  }

  /**
   * Instantiate.
   * @param operations store operations
   * @param finalOutput output path under which renames take place
   * @param attemptRecovery attempt recovery if the store has etags.
   */
  @VisibleForTesting
  public ResilientCommitByRenameHelper(
      final FileSystemOperations operations,
      final Path finalOutput,
      final boolean attemptRecovery) {
    this.operations = operations;
    // enable recovery if requested and the store supports it.
    this.renameRecoveryAvailable = attemptRecovery
        && operations.storePreservesEtagsThroughRenames(finalOutput);
  }

  /**
   * Is resilient commit available?
   * @return true if the resilient commit API can be used
   */
  public boolean isRenameRecoveryAvailable() {

    return renameRecoveryAvailable;
  }

  /**
   * get count of rename failures recovered from.
   * @return count of recoveries.
   */
  public int getRecoveryCount() {
    return recoveryCount.get();
  }

  /**
   * What is the resilence of this filesystem?
   * @param fs filesystem
   * @param path path to use
   * @return true if the conditions of use are met.
   */
  public static boolean filesystemHasResilientCommmit(
      final FileSystem fs,
      final Path path) {
    try {
      return fs.hasPathCapability(path,
          CommonPathCapabilities.ETAGS_PRESERVED_IN_RENAME);
    } catch (IOException ignored) {
      return false;
    }
  }

  /**
   * Commit a file.
   * Rename a file from source to dest; if the underlying FS API call
   * returned false that's escalated to an IOE.
   * @param sourceStatus source status file
   * @param dest destination path
   * @return the outcome
   * @throws IOException failure
   * @throws PathIOException if the rename() call returned false.
   */
  public CommitOutcome commitFile(
      final FileStatus sourceStatus, final Path dest)
      throws IOException {
    final Path source = sourceStatus.getPath();
    String operation = String.format("rename(%s, %s)", source, dest);
    LOG.debug("{}", operation);

    try (DurationInfo du = new DurationInfo(LOG, "%s with status %s",
        operation, sourceStatus)) {
      if (operations.renameFile(source, dest)) {
        // success
        return new CommitOutcome();
      } else {
        // no caught exception; generate one with status info.
        // if this triggers a FNFE from the missing source,
        throw escalateRenameFailure(source, dest);
      }
    } catch (FileNotFoundException caughtException) {
      // any other IOE is passed up;
      // recovery only cares about reporting of
      // missing files
      LOG.debug("{} raised a FileNotFoundException", operation, caughtException);
      // failure.
      // Start with etag checking of the source entry and
      // the destination file status.
      String sourceEtag = getEtag(sourceStatus);

      if (renameRecoveryAvailable && !isEmpty(sourceEtag)) {
        LOG.info("{} Failure, starting etag checking with source etag {}",
            operation, sourceEtag);
        final FileStatus currentSourceStatus = operations.getFileStatusOrNull(source);
        if (currentSourceStatus != null) {
          // source is still there so whatever happened, the rename
          // hasn't taken place.
          // (for example, dest parent path not present)
          LOG.info("{}: source is still present; not checking destination", operation);
          throw caughtException;
        }

        // the source is missing, we have an etag passed down, so
        // probe for a destination
        LOG.debug("{}: source is missing; checking destination", operation);

        // get the destination status and its etag, if any.
        final FileStatus destStatus = operations.getFileStatusOrNull(dest);
        String destEtag = getEtag(destStatus);
        if (sourceEtag.equals(destEtag)) {
          // rename failed somehow
          // but the etag comparision implies all was good.
          LOG.info("{} failed but etag comparison of" +
                  " source {} and destination status {} determined the rename had succeeded",
              operation, sourceStatus, destStatus);

          // and so return successfully with a report which can be used by
          // the committer for its statistics
          recoveryCount.incrementAndGet();
          return new CommitOutcome(true, caughtException);
        } else {
          // failure of etag checking, either dest is absent
          // or the tags don't match. report and fall through
          // to the exception rethrow

          LOG.info("{}: etag comparison of" +
                  " source {} and destination status {} did not match; failing",
              operation, sourceStatus, destStatus);
        }
      }

      // etag comparison failure/unsupported. Fail the operation.
      // throw the caught exception
      throw caughtException;
    }

  }

  /**
   * Get an etag from a FileStatus if it provides one.
   * @param status the status; may be null.
   * @return the etag or null/empty if not provided
   */
  private String getEtag(FileStatus status) {
    if (status instanceof EtagSource) {
      return ((EtagSource) status).getEtag();
    } else {
      return null;
    }
  }

  /**
   * Escalate a rename failure to an exception.
   * This never returns
   * @param source source path
   * @param dest dest path
   * @return an exception to throw
   * @throws FileNotFoundException if source is absent
   * @throws IOException other getFileStatus failure
   */
  private PathIOException escalateRenameFailure(Path source, Path dest)
      throws IOException {
    // rename just returned false.
    // collect information for a meaningful error message
    // and include in an exception raised.

    // get the source status; this will implicitly raise
    // a FNFE.
    final FileStatus sourceStatus = operations.getFileStatus(source);

    LOG.error("Failure to rename {} to {} with" +
            " source status {}",
        source, dest,
        sourceStatus);

    return new PathIOException(source.toString(),
        "Failed to rename to " + dest);
  }

  @Override
  public String toString() {
    return "ResilientCommitByRenameHelper{" +
        "renameRecoveryAvailable=" + renameRecoveryAvailable +
        ", recoveries=" + recoveryCount.get() +
        '}';
  }


  /**
   * Outcome from the commit.
   */
  public static final class CommitOutcome {
    /**
     * Rename failed but etag checking concluded it finished.
     */
    private final boolean renameFailureResolvedThroughEtags;

    /**
     * Any exception caught before etag checking succeeded.
     */
    private final IOException caughtException;

    CommitOutcome() {
      this(false, null);
    }

    CommitOutcome(
        boolean renameFailureResolvedThroughEtags,
        IOException caughtException) {
      this.renameFailureResolvedThroughEtags = renameFailureResolvedThroughEtags;
      this.caughtException = caughtException;
    }

    public boolean isRenameFailureResolvedThroughEtags() {
      return renameFailureResolvedThroughEtags;
    }

    public IOException getCaughtException() {
      return caughtException;
    }

    @Override
    public String toString() {
      return "CommitOutcome{" +
          "renameFailureResolvedThroughEtags=" + renameFailureResolvedThroughEtags +
          ", caughtException=" + caughtException +
          '}';
    }
  }

  /**
   * Class for FS callbacks; designed to be overridden
   * for tests simulating etag mismatch.
   */
  @VisibleForTesting
  static class FileSystemOperations {

    /**
     * Target FS.
     */
    private final FileSystem fileSystem;

    public FileSystemOperations(final FileSystem fileSystem) {
      this.fileSystem = fileSystem;
    }

    public FileSystem getFileSystem() {
      return fileSystem;
    }

    /**
     * Forward to {@link FileSystem#getFileStatus(Path)}.
     * @param path path
     * @return status
     * @throws IOException failure.
     */
    public FileStatus getFileStatus(Path path) throws IOException {
      return fileSystem.getFileStatus(path);
    }

    /**
     * Get a file status value or, if the operation failed
     * for any reason, return null.
     * This is used for reporting/probing the files.
     * @param path path
     * @return status or null
     */
    public FileStatus getFileStatusOrNull(final Path path) {
      try {
        return getFileStatus(path);
      } catch (IOException e) {
        return null;
      }
    }

    /**
     * Forward to {@link FileSystem#rename(Path, Path)}.
     * Usual "what does 'false' mean" ambiguity.
     * @param source source file
     * @param dest destination path -which must not exist.
     * @return true if the file was renamed.
     * @throws IOException failure.
     */
    public boolean renameFile(Path source, Path dest)
        throws IOException {
      return fileSystem.rename(source, dest);
    }

    /**
     * Probe filesystem capabilities.
     * @param path path to probe.
     * @return true if the FS declares its renames work.
     */
    public boolean storePreservesEtagsThroughRenames(Path path) {
      return filesystemHasResilientCommmit(fileSystem, path);
    }

  }
}
