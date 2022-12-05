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

package org.apache.hadoop.fs.s3a.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;

/**
 * The mkdir operation.
 * A walk up the ancestor list halting as soon as a directory (good)
 * or file (bad) is found.
 * Optimized with the expectation that there is a marker up the path
 * or (ultimately) a sibling of the path being created.
 * It performs the directory listing probe ahead of the simple object HEAD
 * call for this reason -the object is the failure mode which SHOULD NOT
 * be encountered on normal execution.
 *
 * Magic paths are handled specially
 * <ul>
 *   <li>The only path check is for a directory already existing there.</li>
 *   <li>No ancestors are checked</li>
 *   <li>Parent markers are never deleted, irrespective of FS settings</li>
 * </ul>
 * As a result, irrespective of depth, the operations performed are only
 * <ol>
 *   <li>One LIST</li>
 *   <li>If needed, one PUT</li>
 * </ol>
 */
public class MkdirOperation extends ExecutingStoreOperation<Boolean> {

  private static final Logger LOG = LoggerFactory.getLogger(
      MkdirOperation.class);

  private final Path dir;

  private final MkdirCallbacks callbacks;

  /**
   * Should checks for ancestors existing be skipped?
   * This flag is set when working with magic directories.
   */
  private final boolean isMagicPath;

  public MkdirOperation(
      final StoreContext storeContext,
      final Path dir,
      final MkdirCallbacks callbacks,
      final boolean isMagicPath) {
    super(storeContext);
    this.dir = dir;
    this.callbacks = callbacks;
    this.isMagicPath = isMagicPath;
  }

  /**
   *
   * Make the given path and all non-existent parents into
   * directories.
   * @return true if a directory was created or already existed
   * @throws FileAlreadyExistsException there is a file at the path specified
   * @throws IOException other IO problems
   */
  @Override
  @Retries.RetryTranslated
  public Boolean execute() throws IOException {
    LOG.debug("Making directory: {}", dir);
    if (dir.isRoot()) {
      // fast exit for root.
      return true;
    }

    // get the file status of the path.
    // this is done even for a magic path, to avoid always  issuing PUT
    // requests. Doing that without a check wouild seem to be an
    // optimization, but it is not because
    // 1. PUT is slower than HEAD
    // 2. Write capacity is less than read capacity on a shard
    // 3. It adds needless entries in versioned buckets, slowing
    //    down subsequent operations.
    FileStatus fileStatus = getPathStatusExpectingDir(dir);
    if (fileStatus != null) {
      if (fileStatus.isDirectory()) {
        return true;
      } else {
        throw new FileAlreadyExistsException("Path is a file: " + dir);
      }
    }
    // file status was null

    // is the path magic?
    // If so, we declare success without looking any further
    if (isMagicPath) {
      // Create the marker file immediately,
      // and don't delete markers
      callbacks.createFakeDirectory(dir, true);
      return true;
    }

    // Walk path to root, ensuring closest ancestor is a directory, not file
    Path fPart = dir.getParent();
    try {
      while (fPart != null && !fPart.isRoot()) {
        fileStatus = getPathStatusExpectingDir(fPart);
        if (fileStatus == null) {
          // nothing at this path, so validate the parent
          fPart = fPart.getParent();
          continue;
        }
        if (fileStatus.isDirectory()) {
          // the parent dir exists. All is good.
          break;
        }

        // there's a file at the parent entry
        throw new FileAlreadyExistsException(String.format(
            "Can't make directory for path '%s' since it is a file.",
            fPart));
      }
    } catch (AccessDeniedException e) {
      LOG.info("mkdirs({}}: Access denied when looking"
              + " for parent directory {}; skipping checks",
          dir, fPart);
      LOG.debug("{}", e, e);
    }

    // if we get here there is no directory at the destination.
    // so create one.

    // Create the marker file, delete the parent entries
    // if the filesystem isn't configured to retain them
    callbacks.createFakeDirectory(dir, false);
    return true;
  }

  /**
   * Get the status of a path, downgrading FNFE to null result.
   * @param path path to probe.
   * @param probes probes to exec
   * @return the status or null
   * @throws IOException failure other than FileNotFound
   */
  private S3AFileStatus probePathStatusOrNull(final Path path,
      final Set<StatusProbeEnum> probes) throws IOException {
    try {
      return callbacks.probePathStatus(path, probes);
    } catch (FileNotFoundException fnfe) {
      return null;
    }
  }

  /**
   * Get the status of a path -optimized for paths
   * where there is a directory marker or child entries.
   *
   * Under a magic path, there's no check for a file,
   * just the listing.
   *
   * @param path path to probe.
   *
   * @return the status
   *
   * @throws IOException failure
   */
  private S3AFileStatus getPathStatusExpectingDir(final Path path)
      throws IOException {
    S3AFileStatus status = probePathStatusOrNull(path,
        StatusProbeEnum.DIRECTORIES);
    if (status == null && !isMagicPath) {
      status = probePathStatusOrNull(path,
          StatusProbeEnum.FILE);
    }
    return status;
  }

  /**
   * Callbacks used by mkdir.
   */
  public interface MkdirCallbacks {

    /**
     * Get the status of a path.
     * @param path path to probe.
     * @param probes probes to exec
     * @return the status
     * @throws IOException failure
     */
    @Retries.RetryTranslated
    S3AFileStatus probePathStatus(Path path,
        Set<StatusProbeEnum> probes) throws IOException;

    /**
     * Create a fake directory, always ending in "/".
     * Retry policy: retrying; translated.
     * the keepMarkers flag controls whether or not markers
     * are automatically kept (this is set when creating
     * directories under a magic path, always)
     * @param dir dir to create
     * @param keepMarkers always keep markers
     *
     * @throws IOException IO failure
     */
    @Retries.RetryTranslated
    void createFakeDirectory(Path dir, boolean keepMarkers) throws IOException;
  }
}
