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
 */
public class MkdirOperation extends ExecutingStoreOperation<Boolean> {

  private static final Logger LOG = LoggerFactory.getLogger(
      MkdirOperation.class);

  private final Path dir;

  private final MkdirCallbacks callbacks;

  public MkdirOperation(
      final StoreContext storeContext,
      final Path dir,
      final MkdirCallbacks callbacks) {
    super(storeContext);
    this.dir = dir;
    this.callbacks = callbacks;
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

    FileStatus fileStatus = getPathStatusExpectingDir(dir);
    if (fileStatus != null) {
      if (fileStatus.isDirectory()) {
        return true;
      } else {
        throw new FileAlreadyExistsException("Path is a file: " + dir);
      }
    }
    // dir, walk up tree
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
      LOG.debug("{}", e.toString(), e);
    }

    // if we get here there is no directory at the destination.
    // so create one.
    String key = getStoreContext().pathToKey(dir);
    // this will create the marker file, delete the parent entries
    // and update S3Guard
    callbacks.createFakeDirectory(key);
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
   * @param path path to probe.
   * @return the status
   * @throws IOException failure
   */
  private S3AFileStatus getPathStatusExpectingDir(final Path path)
      throws IOException {
    S3AFileStatus status = probePathStatusOrNull(path,
        StatusProbeEnum.DIRECTORIES);
    if (status == null) {
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
     * @param key name of directory object.
     * @throws IOException IO failure
     */
    @Retries.RetryTranslated
    void createFakeDirectory(String key) throws IOException;
  }
}
