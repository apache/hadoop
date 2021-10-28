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
class ResilientCommitByRenameHelper {

  private static final Logger LOG =
      LoggerFactory.getLogger(ResilientCommitByRenameHelper.class);

  private final FileSystemOperations operations;

  /**
   * Instantiate.
   * @param fileSystem filesystem to work with.
   */
  ResilientCommitByRenameHelper(final FileSystem fileSystem) {
    this.operations = new FileSystemOperations(requireNonNull(fileSystem));
  }

  /**
   * Is resilient commit available on this filesystem/path?
   * @param sourcePath path to commit under.
   * @return true if the resilient commit API can b eused
   */
  boolean resilientCommitAvailable(Path sourcePath) {

    return operations.storePreservesEtagsThroughRenames(sourcePath);
  }

  /**
   * What is the resilence of this filesystem?
   * @param fs filesystem
   * @param sourcePath path to use
   * @return true if the conditions of use are met.
   */
  static boolean filesystemHasResilientCommmit(
      final FileSystem fs,
      final Path sourcePath) {
    try {
      return fs.hasPathCapability(sourcePath,
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
  CommitOutcome commitFile(
      final FileStatus sourceStatus, final Path dest)
      throws IOException {
    final Path source = sourceStatus.getPath();
    String operation = String.format("rename(%s, %s)", source, dest);
    LOG.debug("{}", operation);

    boolean renamed;
    IOException caughtException = null;
    try (DurationInfo du = new DurationInfo(LOG, "%s with status %s",
        operation, sourceStatus)) {
      renamed = operations.renameFile(source, dest);
    } catch (IOException e) {
      LOG.info("{} raised an exception: {}", operation, e.toString());
      LOG.debug("{} stack trace", operation, e);
      caughtException = e;
      renamed = false;
    }
    if (renamed) {
      // success
      return new CommitOutcome();
    }
    // failure.
    // Start with etag checking of the source entry and
    // the destination file status.
    final FileStatus destStatus = operations.getFileStatusOrNull(dest);
    if (operations.storePreservesEtagsThroughRenames(source)) {
      LOG.debug("{} Failure, starting etag checking", operation);
      String sourceEtag = getEtag(sourceStatus);
      String destEtag = getEtag(destStatus);
      if (!isEmpty(sourceEtag) && sourceEtag.equals(destEtag)) {
        // rename reported a failure or an exception was thrown,
        // but the etag comparision implies all was good.
        LOG.info("{} failed but etag comparison of" +
                " source {} and destination status {} determined the rename had succeeded",
            operation, sourceStatus, destStatus);

        // and report
        return new CommitOutcome(true, caughtException);
      }
    }

    // etag comparison failure/unsupported. Fail the operation.
    // throw any caught exception
    if (caughtException != null) {
      throw caughtException;
    }

    // no caught exception; generate one with status info.
    escalateRenameFailure(source, dest, destStatus);
    // never reached.
    return null;
  }

  /**
   * Get an etag from a FileStatus which MUST BE
   * an implementation of EtagFromFileStatus and
   * whose etag MUST NOT BE null/empty.
   * @param status the status; may be null.
   * @return the etag or null if not provided
   */
  static String getEtag(FileStatus status) {
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
   * @throws IOException always
   */
  private void escalateRenameFailure(Path source, Path dest, FileStatus destStatus)
      throws IOException {
    // rename just returned false.
    // collect information for a meaningful error message
    // and include in an exception raised.

    // get the source status; this will implicitly raise
    // a FNFE.
    final FileStatus sourceStatus = operations.getFileStatus(source);

    LOG.error("Failure to rename {} to {} with" +
            " source status {} " +
            " and destination status {}",
        source, dest,
        sourceStatus, destStatus);

    throw new PathIOException(source.toString(),
        "Failed to rename to " + dest);
  }

  /**
   * Outcome from the commit.
   */
  static final class CommitOutcome {
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

    boolean isRenameFailureResolvedThroughEtags() {
      return renameFailureResolvedThroughEtags;
    }

    IOException getCaughtException() {
      return caughtException;
    }

    @Override
    public String toString() {
      return "CommitOutcome{" +
          "renameFailureResolvedThroughEtags=" + renameFailureResolvedThroughEtags +
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

    FileSystemOperations(final FileSystem fileSystem) {
      this.fileSystem = fileSystem;
    }

    /**
     * Forward to {@link FileSystem#getFileStatus(Path)}.
     * @param path path
     * @return status
     * @throws IOException failure.
     */
    FileStatus getFileStatus(Path path) throws IOException {
      return fileSystem.getFileStatus(path);
    }

    /**
     * Get a file status value or, if the path doesn't exist, return null.
     * @param path path
     * @return status or null
     * @throws IOException IO Failure.
     */
    final FileStatus getFileStatusOrNull(
        final Path path)
        throws IOException {
      try {
        return getFileStatus(path);
      } catch (FileNotFoundException e) {
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
    boolean renameFile(Path source, Path dest)
        throws IOException {
      return fileSystem.rename(source, dest);
    }

    /**
     * Probe filesystem capabilities.
     * @param path path to probe.
     * @return true if the FS declares its renames work.
     */
    boolean storePreservesEtagsThroughRenames(Path path) {
      return filesystemHasResilientCommmit(fileSystem, path);
    }

  }
}
