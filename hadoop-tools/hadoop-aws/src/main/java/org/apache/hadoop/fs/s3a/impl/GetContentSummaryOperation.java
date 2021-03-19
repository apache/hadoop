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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;

/**
 * GetContentSummary operation.
 * This is based on {@code FileSystem.get#getContentSummary};
 * its still doing sequential treewalk with the efficiency
 * issues.
 *
 * Changes:
 * 1. On the recursive calls there
 * is no probe to see if the path is a file: we know the
 * recursion only happens with a dir.
 * 2. If a subdirectory is not found during the walk, that
 * does not trigger an error. The directory is clearly
 * not part of the content any more.
 *
 * The Operation serves up IOStatistics; this counts
 * the cost of all the list operations, but not the
 * initial HEAD probe to see if the path is a file.
 */
public class GetContentSummaryOperation extends
    ExecutingStoreOperation<ContentSummary> implements IOStatisticsSource {

  private static final Logger LOG = LoggerFactory.getLogger(
      GetContentSummaryOperation.class);

  /**
   * Directory to scan.
   */
  private final Path path;

  /**
   * Callbacks to the store.
   */
  private final GetContentSummaryCallbacks callbacks;

  /**
   * IOStatistics to serve up.
   */
  private final IOStatisticsSnapshot iostatistics =
      new IOStatisticsSnapshot();

  /**
   * Constructor.
   * @param storeContext context.
   * @param path path to summarize
   * @param callbacks callbacks for S3 access.
   */
  public GetContentSummaryOperation(
      final StoreContext storeContext,
      final Path path,
      final GetContentSummaryCallbacks callbacks) {
    super(storeContext);
    this.path = path;
    this.callbacks = callbacks;
  }

  @Override
  public IOStatistics getIOStatistics() {
    return iostatistics;
  }

  /**
   * Return the {@link ContentSummary} of a given path.
   * @return the summary.
   * @throws FileNotFoundException if the path does not resolve
   * @throws IOException failure
   */
  @Override
  @Retries.RetryTranslated
  public ContentSummary execute() throws IOException {
    FileStatus status = probePathStatusOrNull(path, StatusProbeEnum.FILE);
    if (status != null && status.isFile()) {
      // f is a file
      long length = status.getLen();
      return new ContentSummary.Builder().length(length).
          fileCount(1).directoryCount(0).spaceConsumed(length).build();
    }
    final ContentSummary summary = getDirSummary(path);
    // Log the IOStatistics at debug so the cost of the operation
    // can be made visible.
    LOG.debug("IOStatistics of getContentSummary({}):\n{}", path, iostatistics);
    return summary;
  }

  /**
   * Return the {@link ContentSummary} of a given directory.
   * This is a recursive operation (as the original is);
   * it'd be more efficient of stack and heap if it managed its
   * own stack.
   * @param dir dir to scan
   * @throws FileNotFoundException if the path does not resolve
   * @throws IOException IO failure
   * @return the content summary
   * @throws FileNotFoundException the path does not exist
   * @throws IOException failure
   */
  public ContentSummary getDirSummary(Path dir) throws IOException {
    long totalLength = 0;
    long fileCount = 0;
    long dirCount = 1;
    final RemoteIterator<S3AFileStatus> it
        = callbacks.listStatusIterator(dir);

    while (it.hasNext()) {
      final S3AFileStatus s = it.next();
      if (s.isDirectory()) {
        try {
          ContentSummary c = getDirSummary(s.getPath());
          totalLength += c.getLength();
          fileCount += c.getFileCount();
          dirCount += c.getDirectoryCount();
        } catch (FileNotFoundException ignored) {
          // path was deleted during the scan; exclude from
          // summary.
        }
      } else {
        totalLength += s.getLen();
        fileCount += 1;
      }
    }
    // Add the list's IOStatistics
    iostatistics.aggregate(retrieveIOStatistics(it));
    return new ContentSummary.Builder().length(totalLength).
        fileCount(fileCount).directoryCount(dirCount).
        spaceConsumed(totalLength).build();
  }

  /**
   * Get the status of a path, downgrading FNFE to null result.
   * @param p path to probe.
   * @param probes probes to exec
   * @return the status or null
   * @throws IOException failure other than FileNotFound
   */
  private S3AFileStatus probePathStatusOrNull(final Path p,
      final Set<StatusProbeEnum> probes) throws IOException {
    try {
      return callbacks.probePathStatus(p, probes);
    } catch (FileNotFoundException fnfe) {
      return null;
    }
  }

  /**
   * Callbacks used by the operation.
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
     * @throws IOException failure
     */
    RemoteIterator<S3AFileStatus> listStatusIterator(Path path)
        throws IOException;

  }
}
