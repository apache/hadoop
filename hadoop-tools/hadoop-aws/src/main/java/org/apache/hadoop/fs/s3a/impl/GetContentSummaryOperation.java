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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;

/**
 * GetContentSummary operation.
 * This is based on {@code FileSystem.get#getContentSummary};
 * its still doing sequential treewalk with the efficiency
 * issues.
 * The sole optimisation is that on the recursive calls there
 * is no probe to see if the path is a file: we know the
 * recursion only happens with a dir.
 */
public class GetContentSummaryOperation extends
    ExecutingStoreOperation<ContentSummary> {

  private static final Logger LOG = LoggerFactory.getLogger(
      GetContentSummaryOperation.class);

  private final Path dir;

  private final GetContentSummaryCallbacks callbacks;

  public GetContentSummaryOperation(
      final StoreContext storeContext,
      final Path dir,
      final GetContentSummaryCallbacks callbacks) {
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
  public ContentSummary execute() throws IOException {
    FileStatus status = probePathStatusOrNull(dir, StatusProbeEnum.FILE);
    if (status != null && status.isFile()) {
      // f is a file
      long length = status.getLen();
      return new ContentSummary.Builder().length(length).
          fileCount(1).directoryCount(0).spaceConsumed(length).build();
    }
    return getDirSummary(dir);
  }

  /**
   * Return the {@link ContentSummary} of a given {@link Path}.
   * @param dir path to use
   * @throws FileNotFoundException if the path does not resolve
   * @throws IOException IO failure
   */
  public ContentSummary getDirSummary(Path dir) throws IOException {
    long[] summary = {0, 0, 1};
    final RemoteIterator<S3AFileStatus> it
        = callbacks.listStatusIterator(dir);

    while (it.hasNext()) {
      final S3AFileStatus s = it.next();
      long length = s.getLen();
      ContentSummary c = s.isDirectory()
          ? getDirSummary(s.getPath()) :
          new ContentSummary.Builder().length(length).
              fileCount(1).directoryCount(0).spaceConsumed(length).build();
      summary[0] += c.getLength();
      summary[1] += c.getFileCount();
      summary[2] += c.getDirectoryCount();
    }
    return new ContentSummary.Builder().length(summary[0]).
        fileCount(summary[1]).directoryCount(summary[2]).
        spaceConsumed(summary[0]).build();
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
   * Callbacks used by mkdir.
   */
  public interface GetContentSummaryCallbacks {

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
     * Incremental list of all entries in a directory.
     * @param path path of dir
     * @return an iterator
     */
    RemoteIterator<S3AFileStatus> listStatusIterator(Path path)
        throws IOException;

  }
}
